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

func TestResolvePullRequestWorkerIDSelectsSingleUnappliedCandidateWithUnlistedChanges(t *testing.T) {
	tests := []struct {
		name    string
		changes WorkspaceChanges
	}{
		{
			name:    "dirty",
			changes: WorkspaceChanges{Dirty: true},
		},
		{
			name:    "diff",
			changes: WorkspaceChanges{Diff: "diff --git a/main.go b/main.go\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
						"status":           core.WorkerSucceeded,
						"workspaceChanges": tt.changes,
					}),
				}},
			}
			candidates := applyCandidates(snapshot, task.ID)
			if len(candidates) != 1 {
				t.Fatalf("candidates = %+v, want one candidate", candidates)
			}
			if len(candidates[0].ChangedFiles) != 0 {
				t.Fatalf("changed files = %+v, want empty display metadata", candidates[0].ChangedFiles)
			}
			workerID, err := resolvePullRequestWorkerID(snapshot, task, "")
			if err != nil {
				t.Fatal(err)
			}
			if workerID != "worker" {
				t.Fatalf("workerID = %q, want worker", workerID)
			}
		})
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
