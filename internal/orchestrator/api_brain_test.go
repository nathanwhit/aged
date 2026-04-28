package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"aged/internal/core"
)

func TestAPIBrainPlansFromStructuredResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("Authorization header = %q", got)
		}
		var req chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.ResponseFormat["type"] != "json_schema" {
			t.Fatalf("response_format.type = %v", req.ResponseFormat["type"])
		}
		writeChatResponse(t, w, Plan{
			WorkerKind: "claude",
			Prompt:     "Review the design and report blockers.",
			Rationale:  "This is a review task.",
			Steps: []PlanStep{{
				Title:       "Review",
				Description: "Inspect the request.",
			}},
			RequiredApprovals: []ApprovalRequest{},
			Spawns:            []SpawnRequest{},
		})
	}))
	defer server.Close()

	brain := newTestAPIBrain(t, server.URL, nil)
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Review design",
		Prompt: "Look at the proposed design.",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerKind != "claude" {
		t.Fatalf("WorkerKind = %q", plan.WorkerKind)
	}
	if plan.Metadata["brain"] != "api" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
}

func TestAPIBrainFallsBackOnInvalidPlan(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{\"workerKind\":\"codex\"}"}}]}`))
	}))
	defer server.Close()

	brain := newTestAPIBrain(t, server.URL, StaticBrain{WorkerKind: "mock"})
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Implement task",
		Prompt: "Do work.",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerKind != "mock" {
		t.Fatalf("WorkerKind = %q", plan.WorkerKind)
	}
	if plan.Metadata["brain"] != "api-fallback" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
	if plan.Metadata["fallbackReason"] == "" {
		t.Fatalf("missing fallback reason")
	}
}

func newTestAPIBrain(t *testing.T, endpoint string, fallback BrainProvider) *APIBrain {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "scheduler.md")
	if err := os.WriteFile(path, []byte("schedule the work"), 0o644); err != nil {
		t.Fatal(err)
	}
	brain, err := NewAPIBrain(APIBrainConfig{
		Endpoint:     endpoint,
		APIKey:       "test-key",
		Model:        "test-model",
		TemplatePath: path,
		Fallback:     fallback,
	})
	if err != nil {
		t.Fatal(err)
	}
	return brain
}

func writeChatResponse(t *testing.T, w http.ResponseWriter, plan Plan) {
	t.Helper()
	content, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	response := map[string]any{
		"choices": []map[string]any{
			{
				"message": map[string]string{
					"role":    "assistant",
					"content": string(content),
				},
			},
		},
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.Fatal(err)
	}
}
