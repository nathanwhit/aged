package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/eventstore"
	"aged/internal/orchestrator"
	"aged/internal/worker"
)

func TestGoogleAuthProtectsAPIAndRedirectsPages(t *testing.T) {
	service := testService(t)
	auth := testAuth(t, nil)
	server := httptest.NewServer(NewWithAuth(service, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("dashboard"))
	}), auth).Routes())
	defer server.Close()

	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	apiRes, err := client.Get(server.URL + "/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer apiRes.Body.Close()
	if apiRes.StatusCode != http.StatusUnauthorized {
		t.Fatalf("api status = %d, want 401", apiRes.StatusCode)
	}

	mcpRes, err := client.Post(server.URL+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer mcpRes.Body.Close()
	if mcpRes.StatusCode != http.StatusUnauthorized {
		t.Fatalf("mcp status = %d, want 401", mcpRes.StatusCode)
	}

	pageRes, err := client.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer pageRes.Body.Close()
	if pageRes.StatusCode != http.StatusFound {
		t.Fatalf("page status = %d, want 302", pageRes.StatusCode)
	}
	if location := pageRes.Header.Get("Location"); location != "/auth/login" {
		t.Fatalf("redirect location = %q, want /auth/login", location)
	}

	healthRes, err := client.Get(server.URL + "/api/health")
	if err != nil {
		t.Fatal(err)
	}
	defer healthRes.Body.Close()
	if healthRes.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", healthRes.StatusCode)
	}
}

func TestGoogleAuthCallbackCreatesSessionForAllowedUser(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("code") != "ok-code" {
				t.Fatalf("code = %q", r.Form.Get("code"))
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"id_token": "verified-token"})
		case "/tokeninfo":
			if r.URL.Query().Get("id_token") != "verified-token" {
				t.Fatalf("id_token = %q", r.URL.Query().Get("id_token"))
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"aud":            "client-id",
				"email":          "me@example.com",
				"email_verified": "true",
				"exp":            "4102444800",
				"iss":            "https://accounts.google.com",
				"name":           "Me",
			})
		default:
			t.Fatalf("unexpected provider path %s", r.URL.Path)
		}
	}))
	defer provider.Close()

	service := testService(t)
	auth := testAuth(t, map[string]string{
		"tokenURL":     provider.URL + "/token",
		"tokenInfoURL": provider.URL + "/tokeninfo",
	})
	server := httptest.NewServer(NewWithAuth(service, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("dashboard"))
	}), auth).Routes())
	defer server.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	loginRes, err := client.Get(server.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	defer loginRes.Body.Close()
	if loginRes.StatusCode != http.StatusFound {
		t.Fatalf("login status = %d, want 302", loginRes.StatusCode)
	}
	state := loginRes.Cookies()[0].Value

	callbackRes, err := client.Get(server.URL + "/auth/callback?code=ok-code&state=" + url.QueryEscape(state))
	if err != nil {
		t.Fatal(err)
	}
	defer callbackRes.Body.Close()
	if callbackRes.StatusCode != http.StatusFound {
		t.Fatalf("callback status = %d, want 302", callbackRes.StatusCode)
	}

	meRes, err := client.Get(server.URL + "/api/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	defer meRes.Body.Close()
	if meRes.StatusCode != http.StatusOK {
		t.Fatalf("me status = %d, want 200", meRes.StatusCode)
	}
	var user AuthUser
	if err := json.NewDecoder(meRes.Body).Decode(&user); err != nil {
		t.Fatal(err)
	}
	if user.Email != "me@example.com" || user.Name != "Me" {
		t.Fatalf("user = %+v", user)
	}

	snapshotRes, err := client.Get(server.URL + "/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshotRes.Body.Close()
	if snapshotRes.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status = %d, want 200", snapshotRes.StatusCode)
	}
}

func TestGoogleAuthSessionOutlivesGoogleIDTokenExpiration(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			_ = json.NewEncoder(w).Encode(map[string]string{"id_token": "short-lived-token"})
		case "/tokeninfo":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"aud":            "client-id",
				"email":          "me@example.com",
				"email_verified": "true",
				"exp":            "1700000120",
				"iss":            "https://accounts.google.com",
				"name":           "Me",
			})
		default:
			t.Fatalf("unexpected provider path %s", r.URL.Path)
		}
	}))
	defer provider.Close()

	service := testService(t)
	auth := testAuth(t, map[string]string{
		"tokenURL":     provider.URL + "/token",
		"tokenInfoURL": provider.URL + "/tokeninfo",
	})
	server := httptest.NewServer(NewWithAuth(service, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("dashboard"))
	}), auth).Routes())
	defer server.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	loginRes, err := client.Get(server.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	defer loginRes.Body.Close()
	state := loginRes.Cookies()[0].Value

	callbackRes, err := client.Get(server.URL + "/auth/callback?code=ok-code&state=" + url.QueryEscape(state))
	if err != nil {
		t.Fatal(err)
	}
	defer callbackRes.Body.Close()
	if callbackRes.StatusCode != http.StatusFound {
		t.Fatalf("callback status = %d, want 302", callbackRes.StatusCode)
	}

	auth.now = func() time.Time { return time.Unix(1700000300, 0) }
	meRes, err := client.Get(server.URL + "/api/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	defer meRes.Body.Close()
	if meRes.StatusCode != http.StatusOK {
		t.Fatalf("me status after id token expiration = %d, want 200", meRes.StatusCode)
	}
	var user AuthUser
	if err := json.NewDecoder(meRes.Body).Decode(&user); err != nil {
		t.Fatal(err)
	}
	if user.Expires != time.Unix(1700000000, 0).Add(defaultSessionDuration).Unix() {
		t.Fatalf("session expires = %d, want %d", user.Expires, time.Unix(1700000000, 0).Add(defaultSessionDuration).Unix())
	}

	auth.now = func() time.Time { return time.Unix(1700000000, 0).Add(defaultSessionDuration).Add(time.Second) }
	expiredRes, err := client.Get(server.URL + "/api/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	defer expiredRes.Body.Close()
	if expiredRes.StatusCode != http.StatusUnauthorized {
		t.Fatalf("me status after session expiration = %d, want 401", expiredRes.StatusCode)
	}
}

func testService(t *testing.T) *orchestrator.Service {
	t.Helper()
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
}

func testAuth(t *testing.T, overrides map[string]string) *GoogleAuth {
	t.Helper()
	config := GoogleAuthConfig{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		AllowedEmail: []string{
			"me@example.com",
		},
		SessionKey: strings.Repeat("s", 32),
		AuthURL:    "https://accounts.google.com/o/oauth2/v2/auth",
	}
	if overrides != nil {
		config.TokenURL = overrides["tokenURL"]
		config.TokenInfoURL = overrides["tokenInfoURL"]
	}
	auth, err := NewGoogleAuth(config)
	if err != nil {
		t.Fatal(err)
	}
	auth.now = func() time.Time { return time.Unix(1700000000, 0) }
	return auth
}
