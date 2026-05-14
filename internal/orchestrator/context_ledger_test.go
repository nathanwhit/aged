package orchestrator

import (
	"fmt"
	"strings"
	"testing"

	"aged/internal/core"
)

func TestProjectTaskContextLedgerKeepsHighValueFactsAndTrimsRoutineResults(t *testing.T) {
	var events []core.Event
	for index := 0; index < 40; index++ {
		workerID := fmt.Sprintf("worker-%02d", index)
		events = append(events, core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   "task-1",
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		})
		summary := fmt.Sprintf("routine result %02d", index)
		if index == 1 {
			summary = "LEDGER_FACT: the repository requires the legacy plugin path"
		}
		payload := map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": summary,
			"workspaceChanges": WorkspaceChanges{
				ChangedFiles: nil,
			},
		}
		if index == 2 {
			payload["workspaceChanges"] = WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/ledger.go", Status: "modified"}},
			}
		}
		events = append(events, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   "task-1",
			WorkerID: workerID,
			Payload:  core.MustJSON(payload),
		})
	}

	ledger := projectTaskContextLedger(events, "task-1")
	if len(ledger) == 0 {
		t.Fatal("missing ledger entries")
	}
	if len(ledger) > maxContextLedgerEntries {
		t.Fatalf("ledger entries = %d, want bounded to %d", len(ledger), maxContextLedgerEntries)
	}
	if !ledgerContainsSummary(ledger, "legacy plugin path") {
		t.Fatalf("ledger dropped older high-value fact: %+v", ledger)
	}
	if !ledgerContainsWorker(ledger, "worker-02") {
		t.Fatalf("ledger dropped older candidate result: %+v", ledger)
	}
	if ledgerContainsWorker(ledger, "worker-00") {
		t.Fatalf("ledger kept routine old result: %+v", ledger)
	}
}

func TestProjectTaskContextLedgerKeepsOldHighValueFactAcrossManyChangedResults(t *testing.T) {
	events := []core.Event{
		{
			Type:     core.EventWorkerCreated,
			TaskID:   "task-1",
			WorkerID: "architecture-worker",
			Payload:  core.MustJSON(map[string]any{"kind": "codex"}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   "task-1",
			WorkerID: "architecture-worker",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "decision: durable workers must read only task-scoped ledger events because full daemon snapshots do not scale",
			}),
		},
	}
	for index := 0; index < maxContextLedgerEntries*3; index++ {
		workerID := fmt.Sprintf("candidate-worker-%02d", index)
		events = append(events,
			core.Event{
				Type:     core.EventWorkerCreated,
				TaskID:   "task-1",
				WorkerID: workerID,
				Payload:  core.MustJSON(map[string]any{"kind": "codex"}),
			},
			core.Event{
				Type:     core.EventWorkerCompleted,
				TaskID:   "task-1",
				WorkerID: workerID,
				Payload: core.MustJSON(map[string]any{
					"status":  core.WorkerSucceeded,
					"summary": fmt.Sprintf("changed result %02d", index),
					"workspaceChanges": WorkspaceChanges{
						Dirty:        true,
						ChangedFiles: []WorkspaceChangedFile{{Path: fmt.Sprintf("internal/generated_%02d.go", index), Status: "modified"}},
					},
				}),
			},
		)
	}

	ledger := projectTaskContextLedger(events, "task-1")
	if len(ledger) > maxContextLedgerEntries {
		t.Fatalf("ledger entries = %d, want bounded to %d", len(ledger), maxContextLedgerEntries)
	}
	if !ledgerContainsSummary(ledger, "task-scoped ledger events") {
		t.Fatalf("ledger dropped old high-value architecture decision after many changed results: %+v", ledger)
	}
	if !ledgerContainsWorker(ledger, "candidate-worker-71") {
		t.Fatalf("ledger dropped recent changed result: %+v", ledger)
	}
}

func TestProjectTaskContextLedgerKeepsWorkerCheckpointAndBoundsIt(t *testing.T) {
	events := []core.Event{
		{
			Type:     core.EventWorkerCreated,
			TaskID:   "task-1",
			WorkerID: "checkpoint-worker",
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
				"metadata": map[string]any{
					"spawnRole": "investigator",
				},
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   "task-1",
			WorkerID: "checkpoint-worker",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "routine completion without changed files",
				"checkpoint": WorkerCheckpoint{
					CurrentHypothesis: "checkpoint projection should survive routine summaries",
					CommandsRun:       []string{"go test ./internal/orchestrator -run TestProjectTaskContextLedger"},
					PendingChecks:     []string{"verify prompt render"},
				},
			}),
		},
	}
	for index := 0; index < maxContextLedgerEntries*2; index++ {
		events = append(events, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   "task-1",
			WorkerID: fmt.Sprintf("routine-worker-%02d", index),
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": fmt.Sprintf("routine result %02d", index),
			}),
		})
	}

	ledger := projectTaskContextLedger(events, "task-1")
	if len(ledger) > maxContextLedgerEntries {
		t.Fatalf("ledger entries = %d, want bounded to %d", len(ledger), maxContextLedgerEntries)
	}
	var checkpointEntry ContextLedgerEntry
	for _, entry := range ledger {
		if entry.WorkerID == "checkpoint-worker" {
			checkpointEntry = entry
			break
		}
	}
	if checkpointEntry.Kind != "worker_checkpoint" || checkpointEntry.Checkpoint == nil {
		t.Fatalf("missing checkpoint ledger entry: %+v", ledger)
	}
	rendered := renderContextLedgerForWorkerPrompt(ledger)
	if !strings.Contains(rendered, "checkpoint projection should survive routine summaries") || !strings.Contains(rendered, "Commands run") {
		t.Fatalf("rendered ledger missing checkpoint view:\n%s", rendered)
	}
}

func ledgerContainsSummary(entries []ContextLedgerEntry, needle string) bool {
	for _, entry := range entries {
		if strings.Contains(entry.Summary, needle) {
			return true
		}
	}
	return false
}

func ledgerContainsWorker(entries []ContextLedgerEntry, workerID string) bool {
	for _, entry := range entries {
		if entry.WorkerID == workerID {
			return true
		}
	}
	return false
}
