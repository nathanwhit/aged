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
