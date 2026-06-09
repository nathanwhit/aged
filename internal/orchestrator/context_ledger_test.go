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

func TestContextLedgerFromMemoryEntriesCarriesMetadata(t *testing.T) {
	entries := contextLedgerFromMemoryEntries([]core.MemoryEntry{
		{
			Kind:          "worker_result_digest",
			ProjectID:     "project-1",
			TaskID:        "task-1",
			SourceEventID: 42,
			SourceEvent:   string(core.EventTaskAction),
			WorkerID:      "worker-1",
			Summary:       "decision: keep the validator harness local to the objective",
			Metadata:      core.MustJSON(map[string]any{"nodeId": "node-1"}),
		},
	}, "task-2")

	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Summary != "decision: keep the validator harness local to the objective" {
		t.Fatalf("summary = %q", entries[0].Summary)
	}
	if entries[0].Metadata["nodeId"] != "node-1" {
		t.Fatalf("missing metadata: %+v", entries[0].Metadata)
	}
	if entries[0].Metadata["sourceEventId"] != float64(42) {
		t.Fatalf("source event metadata = %#v", entries[0].Metadata["sourceEventId"])
	}
	if entries[0].Metadata["scope"] != "project" || entries[0].Metadata["sourceTaskId"] != "task-1" || entries[0].Metadata["projectId"] != "project-1" {
		t.Fatalf("missing project memory metadata: %+v", entries[0].Metadata)
	}
}

func TestMergeContextLedgerEntriesPreservesMemoryEntries(t *testing.T) {
	primary := []ContextLedgerEntry{
		{
			Kind:     "worker_result_digest",
			WorkerID: "memory-worker",
			Summary:  "decision: this objective must publish separate coherent PRs",
		},
	}
	var secondary []ContextLedgerEntry
	for index := 0; index < maxContextLedgerEntries*2; index++ {
		secondary = append(secondary, ContextLedgerEntry{
			Kind:     "candidate_result",
			WorkerID: fmt.Sprintf("worker-%02d", index),
			Summary:  fmt.Sprintf("changed result %02d", index),
		})
	}

	merged := mergeContextLedgerEntries(primary, secondary)
	if len(merged) != maxContextLedgerEntries {
		t.Fatalf("merged entries = %d, want %d", len(merged), maxContextLedgerEntries)
	}
	if !ledgerContainsWorker(merged, "memory-worker") {
		t.Fatalf("merged ledger dropped table-backed memory entry: %+v", merged)
	}
	if ledgerContainsWorker(merged, "worker-00") {
		t.Fatalf("merged ledger kept old secondary entry instead of recent tail: %+v", merged)
	}
	if !ledgerContainsWorker(merged, fmt.Sprintf("worker-%02d", maxContextLedgerEntries*2-1)) {
		t.Fatalf("merged ledger dropped recent secondary entry: %+v", merged)
	}
}

func TestMergeContextLedgerEntriesDeduplicatesMemoryAndProjectedEntries(t *testing.T) {
	entry := ContextLedgerEntry{
		Kind:     "worker_result_digest",
		WorkerID: "worker-1",
		Summary:  "baseline binary was built from the wrong commit",
	}

	merged := mergeContextLedgerEntries([]ContextLedgerEntry{entry}, []ContextLedgerEntry{entry})
	if len(merged) != 1 {
		t.Fatalf("merged entries = %d, want 1: %+v", len(merged), merged)
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
