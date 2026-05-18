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

func TestProjectTaskContextLedgerKeepsDurableLoopIterationSummaries(t *testing.T) {
	events := []core.Event{
		{
			Type:   core.EventTaskAction,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"kind":      loopActionKind,
				"status":    "iteration_completed",
				"iteration": 7,
				"workerId":  "worker-7",
				"summary":   "Implemented checkpoint pruning and queued a follow-up benchmark task.",
			}),
		},
	}

	ledger := projectTaskContextLedger(events, "task-1")
	if len(ledger) != 1 {
		t.Fatalf("ledger entries = %d, want 1: %+v", len(ledger), ledger)
	}
	entry := ledger[0]
	if entry.Kind != "durable_loop_iteration" {
		t.Fatalf("ledger kind = %q, want durable_loop_iteration", entry.Kind)
	}
	if entry.WorkerID != "worker-7" {
		t.Fatalf("ledger worker = %q, want worker-7", entry.WorkerID)
	}
	if !strings.Contains(entry.Summary, "iteration 7:") || !strings.Contains(entry.Summary, "checkpoint pruning") {
		t.Fatalf("ledger summary did not preserve iteration context: %+v", entry)
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
