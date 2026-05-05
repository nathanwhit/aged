package orchestrator

import (
	"strings"
	"testing"

	"aged/internal/core"
)

func TestResolvePullRequestWorkerIDRequiresWorkerBeforeCompletion(t *testing.T) {
	task := core.Task{ID: "task", Status: core.TaskRunning}
	if _, err := resolvePullRequestWorkerID(core.Snapshot{}, task, ""); err == nil || !strings.Contains(err.Error(), "provide workerId") {
		t.Fatalf("resolve error = %v, want workerId requirement", err)
	}
}

func TestResolvePullRequestWorkerIDSelectsSingleUnappliedCandidate(t *testing.T) {
	task := core.Task{ID: "task", Status: core.TaskSucceeded}
	snapshot := core.Snapshot{
		Workers: []core.Worker{{
			ID:     "worker",
			TaskID: "task",
			Kind:   "codex",
			Status: core.WorkerSucceeded,
		}},
		Events: []core.Event{{
			Type:     core.EventWorkerCompleted,
			TaskID:   "task",
			WorkerID: "worker",
			Payload: core.MustJSON(map[string]any{
				"status": core.WorkerSucceeded,
				"workspaceChanges": WorkspaceChanges{
					ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
				},
			}),
		}},
	}
	workerID, err := resolvePullRequestWorkerID(snapshot, task, "")
	if err != nil {
		t.Fatal(err)
	}
	if workerID != "worker" {
		t.Fatalf("workerID = %q, want worker", workerID)
	}
}

func TestResolvePullRequestWorkerIDRejectsNonCandidate(t *testing.T) {
	task := core.Task{ID: "task", Status: core.TaskSucceeded}
	snapshot := core.Snapshot{
		Workers: []core.Worker{{
			ID:     "worker",
			TaskID: "task",
			Kind:   "codex",
			Status: core.WorkerCanceled,
		}},
		Events: []core.Event{{
			Type:     core.EventWorkerCompleted,
			TaskID:   "task",
			WorkerID: "worker",
			Payload: core.MustJSON(map[string]any{
				"status":           core.WorkerCanceled,
				"workspaceChanges": WorkspaceChanges{},
			}),
		}},
	}
	if _, err := resolvePullRequestWorkerID(snapshot, task, "worker"); err == nil || !strings.Contains(err.Error(), "successful worker") {
		t.Fatalf("resolve error = %v, want candidate rejection", err)
	}
}
