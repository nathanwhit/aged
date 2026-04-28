package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"aged/internal/eventstore"
	"aged/internal/orchestrator"
	"aged/internal/worker"
)

func TestCreateTaskRejectsUserWorkerSelection(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"title": "Do work",
		"prompt": "User request",
		"kind": "mock"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func TestCreateTaskAcceptsOnlyUserWorkRequest(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"title": "Do work",
		"prompt": "User request"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", res.StatusCode)
	}
}
