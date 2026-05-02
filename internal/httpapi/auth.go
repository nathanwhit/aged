package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	oauthStateCookie = "aged_oauth_state"
	sessionCookie    = "aged_session"

	oauthStateDuration     = 10 * time.Minute
	defaultSessionDuration = 7 * 24 * time.Hour
)

type contextKey string

const userContextKey contextKey = "auth_user"

type GoogleAuthConfig struct {
	ClientID     string
	ClientSecret string
	AllowedEmail []string
	SessionKey   string
	RedirectURL  string
	AuthURL      string
	TokenURL     string
	TokenInfoURL string
	HTTPClient   *http.Client
}

type GoogleAuth struct {
	clientID      string
	clientSecret  string
	allowedEmails map[string]bool
	sessionKey    []byte
	redirectURL   string
	authURL       string
	tokenURL      string
	tokenInfoURL  string
	httpClient    *http.Client
	now           func() time.Time
}

type AuthUser struct {
	Email   string `json:"email"`
	Name    string `json:"name,omitempty"`
	Picture string `json:"picture,omitempty"`
	Expires int64  `json:"expires"`
}

type tokenResponse struct {
	IDToken string `json:"id_token"`
}

type tokenInfoResponse struct {
	Audience      string `json:"aud"`
	Email         string `json:"email"`
	EmailVerified any    `json:"email_verified"`
	Expires       any    `json:"exp"`
	Issuer        string `json:"iss"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
}

func NewGoogleAuth(config GoogleAuthConfig) (*GoogleAuth, error) {
	if config.ClientID == "" {
		return nil, errors.New("google auth client id is required")
	}
	if config.ClientSecret == "" {
		return nil, errors.New("google auth client secret is required")
	}
	allowed := map[string]bool{}
	for _, email := range config.AllowedEmail {
		email = strings.ToLower(strings.TrimSpace(email))
		if email != "" {
			allowed[email] = true
		}
	}
	if len(allowed) == 0 {
		return nil, errors.New("google auth requires at least one allowed email")
	}
	sessionKey := []byte(config.SessionKey)
	if len(sessionKey) == 0 {
		sessionKey = make([]byte, 32)
		if _, err := rand.Read(sessionKey); err != nil {
			return nil, fmt.Errorf("generate session key: %w", err)
		}
	}
	if len(sessionKey) < 32 {
		return nil, errors.New("auth session key must be at least 32 bytes")
	}
	authURL := nonEmpty(config.AuthURL, "https://accounts.google.com/o/oauth2/v2/auth")
	tokenURL := nonEmpty(config.TokenURL, "https://oauth2.googleapis.com/token")
	tokenInfoURL := nonEmpty(config.TokenInfoURL, "https://oauth2.googleapis.com/tokeninfo")
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &GoogleAuth{
		clientID:      config.ClientID,
		clientSecret:  config.ClientSecret,
		allowedEmails: allowed,
		sessionKey:    sessionKey,
		redirectURL:   config.RedirectURL,
		authURL:       authURL,
		tokenURL:      tokenURL,
		tokenInfoURL:  tokenInfoURL,
		httpClient:    client,
		now:           time.Now,
	}, nil
}

func (a *GoogleAuth) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/login", a.login)
	mux.HandleFunc("GET /auth/callback", a.callback)
	mux.HandleFunc("GET /auth/logout", a.logout)
	mux.HandleFunc("GET /api/auth/me", a.me)
}

func (a *GoogleAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || a.isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		user, ok := a.SessionUser(r)
		if ok {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userContextKey, user)))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/mcp" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			return
		}
		http.Redirect(w, r, "/auth/login", http.StatusFound)
	})
}

func (a *GoogleAuth) SessionUser(r *http.Request) (AuthUser, bool) {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || cookie.Value == "" {
		return AuthUser{}, false
	}
	user, err := a.decodeSession(cookie.Value)
	if err != nil {
		return AuthUser{}, false
	}
	if user.Expires <= a.now().Unix() {
		return AuthUser{}, false
	}
	if !a.allowedEmails[strings.ToLower(user.Email)] {
		return AuthUser{}, false
	}
	return user, true
}

func (a *GoogleAuth) login(w http.ResponseWriter, r *http.Request) {
	state, err := randomToken(32)
	if err != nil {
		writeError(w, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     oauthStateCookie,
		Value:    state,
		Path:     "/auth",
		MaxAge:   int(oauthStateDuration.Seconds()),
		HttpOnly: true,
		Secure:   secureCookie(r),
		SameSite: http.SameSiteLaxMode,
	})
	values := url.Values{}
	values.Set("client_id", a.clientID)
	values.Set("redirect_uri", a.redirectURI(r))
	values.Set("response_type", "code")
	values.Set("scope", "openid email profile")
	values.Set("state", state)
	http.Redirect(w, r, a.authURL+"?"+values.Encode(), http.StatusFound)
}

func (a *GoogleAuth) callback(w http.ResponseWriter, r *http.Request) {
	if errText := r.URL.Query().Get("error"); errText != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": errText})
		return
	}
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing oauth code or state"})
		return
	}
	cookie, err := r.Cookie(oauthStateCookie)
	if err != nil || cookie.Value == "" || !hmac.Equal([]byte(cookie.Value), []byte(state)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid oauth state"})
		return
	}
	user, err := a.exchangeAndVerify(r.Context(), r, code)
	if err != nil {
		writeError(w, err)
		return
	}
	encoded, err := a.encodeSession(user)
	if err != nil {
		writeError(w, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     oauthStateCookie,
		Value:    "",
		Path:     "/auth",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   secureCookie(r),
		SameSite: http.SameSiteLaxMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    encoded,
		Path:     "/",
		MaxAge:   int(defaultSessionDuration.Seconds()),
		HttpOnly: true,
		Secure:   secureCookie(r),
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func (a *GoogleAuth) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   secureCookie(r),
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/auth/login", http.StatusFound)
}

func (a *GoogleAuth) me(w http.ResponseWriter, r *http.Request) {
	user, ok := a.SessionUser(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	writeJSON(w, http.StatusOK, user)
}

func (a *GoogleAuth) exchangeAndVerify(ctx context.Context, r *http.Request, code string) (AuthUser, error) {
	values := url.Values{}
	values.Set("code", code)
	values.Set("client_id", a.clientID)
	values.Set("client_secret", a.clientSecret)
	values.Set("redirect_uri", a.redirectURI(r))
	values.Set("grant_type", "authorization_code")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return AuthUser{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := a.httpClient.Do(req)
	if err != nil {
		return AuthUser{}, fmt.Errorf("exchange oauth code: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return AuthUser{}, fmt.Errorf("exchange oauth code: status %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	var token tokenResponse
	if err := json.NewDecoder(res.Body).Decode(&token); err != nil {
		return AuthUser{}, err
	}
	if token.IDToken == "" {
		return AuthUser{}, errors.New("google oauth response did not include id_token")
	}
	return a.verifyIDToken(ctx, token.IDToken)
}

func (a *GoogleAuth) verifyIDToken(ctx context.Context, idToken string) (AuthUser, error) {
	infoURL, err := url.Parse(a.tokenInfoURL)
	if err != nil {
		return AuthUser{}, err
	}
	query := infoURL.Query()
	query.Set("id_token", idToken)
	infoURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL.String(), nil)
	if err != nil {
		return AuthUser{}, err
	}
	res, err := a.httpClient.Do(req)
	if err != nil {
		return AuthUser{}, fmt.Errorf("verify google id token: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return AuthUser{}, fmt.Errorf("verify google id token: status %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	var info tokenInfoResponse
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return AuthUser{}, err
	}
	if info.Audience != a.clientID {
		return AuthUser{}, errors.New("google id token audience mismatch")
	}
	if info.Issuer != "accounts.google.com" && info.Issuer != "https://accounts.google.com" {
		return AuthUser{}, errors.New("google id token issuer mismatch")
	}
	verified, err := parseClaimBool(info.EmailVerified)
	if err != nil || !verified {
		return AuthUser{}, errors.New("google email is not verified")
	}
	email := strings.ToLower(strings.TrimSpace(info.Email))
	if !a.allowedEmails[email] {
		return AuthUser{}, errors.New("google account is not allowed")
	}
	expires, err := parseClaimUnix(info.Expires)
	if err != nil {
		return AuthUser{}, errors.New("google id token expiration is invalid")
	}
	if expires <= a.now().Unix() {
		return AuthUser{}, errors.New("google id token is expired")
	}
	return AuthUser{
		Email:   email,
		Name:    info.Name,
		Picture: info.Picture,
		Expires: a.now().Add(defaultSessionDuration).Unix(),
	}, nil
}

func (a *GoogleAuth) redirectURI(r *http.Request) string {
	if a.redirectURL != "" {
		return a.redirectURL
	}
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	host := r.Host
	if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		host = forwardedHost
	}
	return scheme + "://" + host + "/auth/callback"
}

func (a *GoogleAuth) encodeSession(user AuthUser) (string, error) {
	payload, err := json.Marshal(user)
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, a.sessionKey)
	_, _ = mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encodedPayload + "." + signature, nil
}

func (a *GoogleAuth) decodeSession(value string) (AuthUser, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return AuthUser{}, errors.New("invalid session")
	}
	mac := hmac.New(sha256.New, a.sessionKey)
	_, _ = mac.Write([]byte(parts[0]))
	expected := mac.Sum(nil)
	actual, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(actual, expected) {
		return AuthUser{}, errors.New("invalid session signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return AuthUser{}, err
	}
	var user AuthUser
	if err := json.Unmarshal(payload, &user); err != nil {
		return AuthUser{}, err
	}
	return user, nil
}

func (a *GoogleAuth) isPublicPath(path string) bool {
	return path == "/api/health" || path == "/auth/login" || path == "/auth/callback" || path == "/auth/logout" || path == "/api/auth/me"
}

func randomToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func secureCookie(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func parseClaimBool(value any) (bool, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		return strconv.ParseBool(typed)
	default:
		return false, errors.New("invalid bool claim")
	}
}

func parseClaimUnix(value any) (int64, error) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	default:
		return 0, errors.New("invalid unix claim")
	}
}

func nonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
