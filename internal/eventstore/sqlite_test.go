package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
)

func openTestSQLiteStore(tb testing.TB, ctx context.Context) *SQLiteStore {
	tb.Helper()

	store, err := OpenSQLite(ctx, filepath.Join(tb.TempDir(), "aged.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Fatal(err)
		}
	})
	return store
}

func appendSQLiteEvents(tb testing.TB, ctx context.Context, store *SQLiteStore, events ...core.Event) {
	tb.Helper()

	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			tb.Fatal(err)
		}
	}
}

func TestSnapshotReplaysMoreThanDefaultEventPage(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "task-1"
	appendSQLiteEvents(t, ctx, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Long task",
			"prompt":   "Generate enough events to cross the default page size.",
			"metadata": map[string]any{},
		}),
	})

	for i := 0; i < 205; i++ {
		appendSQLiteEvents(t, ctx, store, core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"stream": "stdout",
				"text":   "progress",
			}),
		})
	}

	appendSQLiteEvents(t, ctx, store, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	})

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 207 {
		t.Fatalf("events = %d, want 207", len(snapshot.Events))
	}
	if len(snapshot.Tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(snapshot.Tasks))
	}
	if snapshot.Tasks[0].Status != core.TaskSucceeded {
		t.Fatalf("task status = %q, want %q", snapshot.Tasks[0].Status, core.TaskSucceeded)
	}
}

func TestSnapshotMergesDuplicatePullRequestIDsByTaskAndGitHubIdentity(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "task-1"
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Task",
				"prompt": "Prompt",
			}),
		},
		core.Event{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-generated",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"state":  "OPEN",
				"metadata": map[string]any{
					"projectId": "project-1",
				},
			}),
		},
		core.Event{
			Type:   core.EventPRStatusChecked,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":               "pr-generated",
				"checksStatus":     "passing",
				"checksConclusion": "success",
				"mergeStatus":      "CLEAN",
				"mergeable":        "MERGEABLE",
				"reviewStatus":     "APPROVED",
				"metadata": map[string]any{
					"latestPullRequestFeedbackSignature": "2026-05-11T22:01:05Z:conversation:IC_1",
				},
			}),
		},
		core.Event{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "github:owner/repo#7",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"state":  "OPEN",
				"metadata": map[string]any{
					"latestPullRequestFeedbackSignature": "2026-05-11T22:01:05Z:conversation:IC_1",
					"watch":                              true,
				},
			}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	pr := snapshot.PullRequests[0]
	if pr.ID != "pr-generated" || pr.ChecksStatus != "passing" || pr.ChecksConclusion != "success" || pr.MergeStatus != "CLEAN" || pr.Mergeable != "MERGEABLE" || pr.ReviewStatus != "APPROVED" {
		t.Fatalf("merged pr = %+v", pr)
	}
	metadata := string(pr.Metadata)
	if !strings.Contains(metadata, `"projectId":"project-1"`) || !strings.Contains(metadata, `"watch":true`) {
		t.Fatalf("metadata = %s", metadata)
	}
}

func TestSnapshotUpdatesWorkerActivityFromOutput(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	startedAt := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	outputAt := startedAt.Add(10 * time.Minute)
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			At:     startedAt,
			Type:   core.EventTaskCreated,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"title":  "Task",
				"prompt": "Prompt",
			}),
		},
		core.Event{
			At:       startedAt,
			Type:     core.EventExecutionPlanned,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-1",
				"workerId":   "worker-1",
				"workerKind": "codex",
			}),
		},
		core.Event{
			At:       startedAt,
			Type:     core.EventWorkerCreated,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		},
		core.Event{
			At:       startedAt,
			Type:     core.EventWorkerStarted,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload:  core.MustJSON(map[string]any{}),
		},
		core.Event{
			At:       outputAt,
			Type:     core.EventWorkerOutput,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"kind": "log",
				"text": "still working",
			}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Workers) != 1 || !snapshot.Workers[0].UpdatedAt.Equal(outputAt) {
		t.Fatalf("worker updatedAt = %+v, want %s", snapshot.Workers, outputAt)
	}
	if len(snapshot.ExecutionNodes) != 1 || !snapshot.ExecutionNodes[0].UpdatedAt.Equal(outputAt) {
		t.Fatalf("node updatedAt = %+v, want %s", snapshot.ExecutionNodes, outputAt)
	}

	summary, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Events) != 0 {
		t.Fatalf("summary events = %d, want 0", len(summary.Events))
	}
	if len(summary.Workers) != 1 || !summary.Workers[0].UpdatedAt.Equal(snapshot.Workers[0].UpdatedAt) {
		t.Fatalf("summary worker updatedAt = %+v, want %s", summary.Workers, snapshot.Workers[0].UpdatedAt)
	}
	if len(summary.ExecutionNodes) != 1 || !summary.ExecutionNodes[0].UpdatedAt.Equal(snapshot.ExecutionNodes[0].UpdatedAt) {
		t.Fatalf("summary node updatedAt = %+v, want %s", summary.ExecutionNodes, snapshot.ExecutionNodes[0].UpdatedAt)
	}
}

func TestSnapshotSummaryOmitsWorkerOutputEventsAndTracksLastEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "task-summary"
	appendSQLiteEvents(t, ctx, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Summary task",
			"prompt": "Keep initial payload small.",
		}),
	})
	for i := 0; i < 20; i++ {
		appendSQLiteEvents(t, ctx, store, core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: "worker-summary",
			Payload:  core.MustJSON(map[string]any{"text": "verbose output"}),
		})
	}

	summary, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Events) != 0 {
		t.Fatalf("summary events = %d, want 0", len(summary.Events))
	}
	if summary.LastEventID != 21 {
		t.Fatalf("last event id = %d, want 21", summary.LastEventID)
	}
	if len(summary.Tasks) != 1 || summary.Tasks[0].ID != taskID {
		t.Fatalf("tasks = %+v", summary.Tasks)
	}
}

func TestSnapshotProjectionRebuildUsesOnlyLatestWorkerOutput(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	startedAt := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			At:     startedAt,
			Type:   core.EventTaskCreated,
			TaskID: "task-output-rebuild",
			Payload: core.MustJSON(map[string]any{
				"title":  "Output rebuild",
				"prompt": "Keep summary rebuilds bounded by worker count.",
			}),
		},
		core.Event{
			At:       startedAt.Add(time.Second),
			Type:     core.EventWorkerCreated,
			TaskID:   "task-output-rebuild",
			WorkerID: "worker-output-rebuild",
			Payload:  core.MustJSON(map[string]any{"kind": "codex"}),
		},
	)
	for i := 0; i < 50; i++ {
		appendSQLiteEvents(t, ctx, store, core.Event{
			At:       startedAt.Add(time.Duration(i+2) * time.Second),
			Type:     core.EventWorkerOutput,
			TaskID:   "task-output-rebuild",
			WorkerID: "worker-output-rebuild",
			Payload:  core.MustJSON(map[string]any{"text": strings.Repeat("x", 512)}),
		})
	}

	if _, err := store.db.ExecContext(ctx, `DELETE FROM snapshot_projection`); err != nil {
		t.Fatal(err)
	}
	summary, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Workers) != 1 || !summary.Workers[0].UpdatedAt.Equal(startedAt.Add(51*time.Second)) {
		t.Fatalf("worker updatedAt = %+v, want latest output timestamp", summary.Workers)
	}

	events, err := projectionInputEvents(ctx, store.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	var outputCount int
	for _, event := range events {
		if event.Type == core.EventWorkerOutput {
			outputCount++
		}
	}
	if outputCount != 1 {
		t.Fatalf("worker.output projection inputs = %d, want 1", outputCount)
	}
}

func TestSnapshotProjectionMatchesReplayAndTracksNonProjectionEvents(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	startedAt := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			At:     startedAt,
			Type:   core.EventTaskCreated,
			TaskID: "task-projection",
			Payload: core.MustJSON(map[string]any{
				"title":  "Projection",
				"prompt": "Keep projected snapshots current.",
			}),
		},
		core.Event{
			At:       startedAt.Add(time.Second),
			Type:     core.EventExecutionPlanned,
			TaskID:   "task-projection",
			WorkerID: "worker-projection",
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-projection",
				"workerId":   "worker-projection",
				"workerKind": "codex",
			}),
		},
		core.Event{
			At:       startedAt.Add(2 * time.Second),
			Type:     core.EventWorkerCreated,
			TaskID:   "task-projection",
			WorkerID: "worker-projection",
			Payload:  core.MustJSON(map[string]any{"kind": "codex"}),
		},
		core.Event{
			At:       startedAt.Add(3 * time.Second),
			Type:     core.EventWorkerStarted,
			TaskID:   "task-projection",
			WorkerID: "worker-projection",
			Payload:  core.MustJSON(map[string]any{}),
		},
		core.Event{
			At:       startedAt.Add(4 * time.Second),
			Type:     core.EventWorkerOutput,
			TaskID:   "task-projection",
			WorkerID: "worker-projection",
			Payload:  core.MustJSON(map[string]any{"text": strings.Repeat("x", 4096)}),
		},
		core.Event{
			At:     startedAt.Add(5 * time.Second),
			Type:   core.EventTaskPlanned,
			TaskID: "task-projection",
			Payload: core.MustJSON(map[string]any{
				"ignored": true,
			}),
		},
	)

	projected, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events, err := projectionInputEvents(ctx, store.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.snapshotFromEvents(ctx, events, false)
	if err != nil {
		t.Fatal(err)
	}
	if projected.LastEventID != 6 {
		t.Fatalf("projected last event id = %d, want 6", projected.LastEventID)
	}
	assertSnapshotsEqual(t, projected, replayed)
}

func TestSnapshotProjectionWorkerOutputDoesNotRewriteStateBlob(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	startedAt := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			At:     startedAt,
			Type:   core.EventTaskCreated,
			TaskID: "task-output-watermark",
			Payload: core.MustJSON(map[string]any{
				"title":  "Output watermark",
				"prompt": "Avoid rewriting the snapshot blob for output events.",
			}),
		},
		core.Event{
			At:       startedAt.Add(time.Second),
			Type:     core.EventExecutionPlanned,
			TaskID:   "task-output-watermark",
			WorkerID: "worker-output-watermark",
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-output-watermark",
				"workerId":   "worker-output-watermark",
				"workerKind": "codex",
			}),
		},
		core.Event{
			At:       startedAt.Add(2 * time.Second),
			Type:     core.EventWorkerCreated,
			TaskID:   "task-output-watermark",
			WorkerID: "worker-output-watermark",
			Payload:  core.MustJSON(map[string]any{"kind": "codex"}),
		},
		core.Event{
			At:       startedAt.Add(3 * time.Second),
			Type:     core.EventWorkerStarted,
			TaskID:   "task-output-watermark",
			WorkerID: "worker-output-watermark",
			Payload:  core.MustJSON(map[string]any{}),
		},
	)

	var stateBefore string
	if err := store.db.QueryRowContext(ctx, `SELECT state FROM snapshot_projection WHERE id = 1`).Scan(&stateBefore); err != nil {
		t.Fatal(err)
	}

	outputAt := startedAt.Add(4 * time.Second)
	appendSQLiteEvents(t, ctx, store, core.Event{
		At:       outputAt,
		Type:     core.EventWorkerOutput,
		TaskID:   "task-output-watermark",
		WorkerID: "worker-output-watermark",
		Payload:  core.MustJSON(map[string]any{"text": strings.Repeat("x", 4096)}),
	})

	var lastEventID int64
	var stateAfter string
	if err := store.db.QueryRowContext(ctx, `SELECT last_event_id, state FROM snapshot_projection WHERE id = 1`).Scan(&lastEventID, &stateAfter); err != nil {
		t.Fatal(err)
	}
	if lastEventID != 5 {
		t.Fatalf("last event id = %d, want 5", lastEventID)
	}
	if stateAfter != stateBefore {
		t.Fatal("worker output rewrote snapshot projection state")
	}

	snapshot, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Workers) != 1 || !snapshot.Workers[0].UpdatedAt.Equal(outputAt) {
		t.Fatalf("worker updatedAt = %+v, want %s", snapshot.Workers, outputAt)
	}
	if len(snapshot.ExecutionNodes) != 1 || !snapshot.ExecutionNodes[0].UpdatedAt.Equal(outputAt) {
		t.Fatalf("node updatedAt = %+v, want %s", snapshot.ExecutionNodes, outputAt)
	}
}

func TestSnapshotProjectionRecoversWhenMissing(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: "task-recover",
			Payload: core.MustJSON(map[string]any{
				"title":  "Recover",
				"prompt": "Rebuild projection on demand.",
			}),
		},
		core.Event{
			Type:   core.EventTaskStatus,
			TaskID: "task-recover",
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskRunning,
			}),
		},
	)

	if _, err := store.db.ExecContext(ctx, `DELETE FROM snapshot_projection`); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].Status != core.TaskRunning {
		t.Fatalf("snapshot tasks = %+v", snapshot.Tasks)
	}
	var lastEventID int64
	if err := store.db.QueryRowContext(ctx, `SELECT last_event_id FROM snapshot_projection WHERE id = 1`).Scan(&lastEventID); err != nil {
		t.Fatal(err)
	}
	if lastEventID != snapshot.LastEventID {
		t.Fatalf("stored last event id = %d, want %d", lastEventID, snapshot.LastEventID)
	}
}

func TestSnapshotProjectionAppendRebuildsStaleProjection(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	appendSQLiteEvents(t, ctx, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-stale",
		Payload: core.MustJSON(map[string]any{
			"title":  "Stale",
			"prompt": "Repair during append.",
		}),
	})
	if _, err := store.db.ExecContext(ctx, `UPDATE snapshot_projection SET last_event_id = 0, state = ? WHERE id = 1`, string(core.MustJSON(newSnapshotProjectionState()))); err != nil {
		t.Fatal(err)
	}
	appendSQLiteEvents(t, ctx, store, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-stale",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	})

	snapshot, err := store.SnapshotSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LastEventID != 2 {
		t.Fatalf("last event id = %d, want 2", snapshot.LastEventID)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].Status != core.TaskSucceeded {
		t.Fatalf("snapshot tasks = %+v", snapshot.Tasks)
	}
}

func assertSnapshotsEqual(tb testing.TB, got core.Snapshot, want core.Snapshot) {
	tb.Helper()

	gotJSON, err := json.Marshal(got)
	if err != nil {
		tb.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		tb.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) {
		tb.Fatalf("snapshots differ\ngot:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

func TestListTaskEventsLimitsOnlyWorkerOutput(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "task-events"
	appendSQLiteEvents(t, ctx, store,
		core.Event{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Events", "prompt": "Load detail lazily"})},
		core.Event{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: "worker-events", Payload: core.MustJSON(map[string]any{"kind": "mock"})},
	)
	for i := 0; i < 5; i++ {
		appendSQLiteEvents(t, ctx, store, core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: "worker-events",
			Payload:  core.MustJSON(map[string]any{"text": i}),
		})
	}
	appendSQLiteEvents(t, ctx, store, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: "worker-events",
		Payload:  core.MustJSON(map[string]any{"status": core.WorkerSucceeded}),
	})

	limited, err := store.ListTaskEvents(ctx, taskID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 5 {
		t.Fatalf("events = %d, want 5", len(limited))
	}
	var outputCount int
	for _, event := range limited {
		if event.Type == core.EventWorkerOutput {
			outputCount++
		}
	}
	if outputCount != 2 {
		t.Fatalf("worker.output events = %d, want 2; events = %+v", outputCount, limited)
	}
	if limited[len(limited)-1].Type != core.EventWorkerCompleted {
		t.Fatalf("last event type = %q, want worker.completed", limited[len(limited)-1].Type)
	}
}

func TestListTaskLedgerEventsIsTaskScopedAndExcludesWorkerOutput(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "ledger-task"
	appendSQLiteEvents(t, ctx, store,
		core.Event{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Ledger", "prompt": "Bounded query"})},
		core.Event{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: "worker-ledger", Payload: core.MustJSON(map[string]any{"kind": "mock"})},
		core.Event{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: "worker-ledger", Payload: core.MustJSON(map[string]any{"text": strings.Repeat("output-payload", 100)})},
		core.Event{Type: core.EventWorkerCompleted, TaskID: taskID, WorkerID: "worker-ledger", Payload: core.MustJSON(map[string]any{"status": core.WorkerSucceeded, "summary": "decision: keep the bounded ledger query"})},
		core.Event{Type: core.EventTaskMemoryUpdated, TaskID: taskID, Payload: core.MustJSON(core.TaskMemory{Version: 1, Objective: "durable loop memory"})},
		core.Event{Type: core.EventWorkerCompleted, TaskID: "other-task", WorkerID: "other-worker", Payload: core.MustJSON(map[string]any{"status": core.WorkerSucceeded, "summary": "decision: should not leak"})},
	)

	events, err := store.ListTaskLedgerEvents(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3; events = %+v", len(events), events)
	}
	for _, event := range events {
		if event.TaskID != taskID {
			t.Fatalf("event task id = %q, want %q", event.TaskID, taskID)
		}
		if event.Type == core.EventWorkerOutput {
			t.Fatalf("ledger events included worker.output payload: %+v", event)
		}
		if strings.Contains(string(event.Payload), "output-payload") || strings.Contains(string(event.Payload), "should not leak") {
			t.Fatalf("ledger event query included unrelated payload: %+v", event)
		}
	}
	if events[0].Type != core.EventWorkerCreated || events[1].Type != core.EventWorkerCompleted || events[2].Type != core.EventTaskMemoryUpdated {
		t.Fatalf("event types = %q, %q, %q; want worker.created, worker.completed, task.memory_updated", events[0].Type, events[1].Type, events[2].Type)
	}
}

func TestSnapshotProjectsTaskMemoryUpdated(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "memory-task"
	memory := core.TaskMemory{
		Version:   1,
		UpdatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Coverage:  core.TaskMemoryCoverage{FromIteration: 1, ThroughIteration: 20, EventCutoffID: 99},
		Objective: "Keep the long-horizon context available.",
		Decisions: []core.TaskMemoryNote{{
			Text:               "decision: keep synthesis deterministic",
			FirstSeenIteration: 2,
			LastSeenIteration:  10,
			Count:              2,
		}},
	}
	appendSQLiteEvents(t, ctx, store,
		core.Event{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Memory", "prompt": "Remember context"})},
		core.Event{Type: core.EventTaskMemoryUpdated, TaskID: taskID, Payload: core.MustJSON(memory)},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range snapshot.Tasks {
		if task.ID != taskID {
			continue
		}
		if task.Memory == nil {
			t.Fatalf("task memory was not projected: %+v", task)
		}
		if task.Memory.Coverage.ThroughIteration != 20 || len(task.Memory.Decisions) != 1 || task.Memory.Decisions[0].Text != "decision: keep synthesis deterministic" {
			t.Fatalf("task memory = %+v", task.Memory)
		}
		return
	}
	t.Fatalf("task %q not found in snapshot: %+v", taskID, snapshot.Tasks)
}

func BenchmarkSnapshotSummarySkipsWorkerOutput(b *testing.B) {
	ctx := context.Background()
	store := openTestSQLiteStore(b, ctx)

	taskID := "task-bench"
	appendSQLiteEvents(b, ctx, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Benchmark task",
			"prompt": "Measure snapshot payload size.",
		}),
	})
	for i := 0; i < 2000; i++ {
		appendSQLiteEvents(b, ctx, store, core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: "worker-bench",
			Payload:  core.MustJSON(map[string]any{"text": strings.Repeat("x", 512)}),
		})
	}
	appendSQLiteEvents(b, ctx, store, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	})

	b.Run("full", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			snapshot, err := store.Snapshot(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				payload, err := json.Marshal(snapshot)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(payload)), "payload_bytes")
			}
		}
	})
	b.Run("summary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			snapshot, err := store.SnapshotSummary(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				payload, err := json.Marshal(snapshot)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(payload)), "payload_bytes")
			}
		}
	})
}

func BenchmarkSnapshotProjectionLargeHistory(b *testing.B) {
	ctx := context.Background()
	store := openTestSQLiteStore(b, ctx)

	seedLargeSnapshotHistory(b, ctx, store, 100, 5, 40)

	b.Run("replay-summary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			events, err := projectionInputEvents(ctx, store.db, 0)
			if err != nil {
				b.Fatal(err)
			}
			snapshot, err := store.snapshotFromEvents(ctx, events, false)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				payload, err := json.Marshal(snapshot)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(payload)), "payload_bytes")
			}
		}
	})
	b.Run("projected-summary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			snapshot, err := store.SnapshotSummary(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				payload, err := json.Marshal(snapshot)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(payload)), "payload_bytes")
			}
		}
	})
}

func seedLargeSnapshotHistory(tb testing.TB, ctx context.Context, store *SQLiteStore, taskCount int, workersPerTask int, outputsPerWorker int) {
	tb.Helper()

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback()
	insertEvents := func(events ...core.Event) {
		tb.Helper()
		for _, event := range events {
			if event.At.IsZero() {
				event.At = time.Now().UTC()
			}
			if event.Payload == nil {
				event.Payload = json.RawMessage(`{}`)
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO events (at, type, task_id, worker_id, payload)
VALUES (?, ?, ?, ?, ?)`,
				event.At.Format(time.RFC3339Nano),
				string(event.Type),
				event.TaskID,
				event.WorkerID,
				string(event.Payload),
			); err != nil {
				tb.Fatal(err)
			}
		}
	}

	base := time.Date(2026, 5, 13, 9, 0, 0, 0, time.UTC)
	for taskIndex := 0; taskIndex < taskCount; taskIndex++ {
		taskID := fmt.Sprintf("task-%03d", taskIndex)
		insertEvents(core.Event{
			At:     base.Add(time.Duration(taskIndex) * time.Minute),
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  fmt.Sprintf("Task %03d", taskIndex),
				"prompt": "Benchmark projected snapshot reads.",
			}),
		})
		for workerIndex := 0; workerIndex < workersPerTask; workerIndex++ {
			workerID := fmt.Sprintf("%s-worker-%02d", taskID, workerIndex)
			nodeID := fmt.Sprintf("%s-node-%02d", taskID, workerIndex)
			insertEvents(
				core.Event{
					At:       base.Add(time.Duration(taskIndex*workersPerTask+workerIndex) * time.Second),
					Type:     core.EventExecutionPlanned,
					TaskID:   taskID,
					WorkerID: workerID,
					Payload: core.MustJSON(map[string]any{
						"nodeId":     nodeID,
						"workerId":   workerID,
						"workerKind": "codex",
					}),
				},
				core.Event{
					At:       base.Add(time.Duration(taskIndex*workersPerTask+workerIndex)*time.Second + time.Millisecond),
					Type:     core.EventWorkerCreated,
					TaskID:   taskID,
					WorkerID: workerID,
					Payload:  core.MustJSON(map[string]any{"kind": "codex"}),
				},
				core.Event{
					At:       base.Add(time.Duration(taskIndex*workersPerTask+workerIndex)*time.Second + 2*time.Millisecond),
					Type:     core.EventWorkerStarted,
					TaskID:   taskID,
					WorkerID: workerID,
					Payload:  core.MustJSON(map[string]any{}),
				},
			)
			for outputIndex := 0; outputIndex < outputsPerWorker; outputIndex++ {
				insertEvents(core.Event{
					At:       base.Add(time.Duration(taskIndex*workersPerTask+workerIndex)*time.Second + time.Duration(outputIndex+3)*time.Millisecond),
					Type:     core.EventWorkerOutput,
					TaskID:   taskID,
					WorkerID: workerID,
					Payload: core.MustJSON(map[string]any{
						"text": strings.Repeat("x", 512),
					}),
				})
			}
			insertEvents(core.Event{
				At:       base.Add(time.Duration(taskIndex*workersPerTask+workerIndex)*time.Second + time.Duration(outputsPerWorker+3)*time.Millisecond),
				Type:     core.EventWorkerCompleted,
				TaskID:   taskID,
				WorkerID: workerID,
				Payload: core.MustJSON(map[string]any{
					"status": core.WorkerSucceeded,
				}),
			})
		}
		insertEvents(core.Event{
			At:     base.Add(time.Duration(taskIndex)*time.Minute + time.Hour),
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskSucceeded,
			}),
		})
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	if _, _, err := store.rebuildSnapshotProjection(ctx); err != nil {
		tb.Fatal(err)
	}
}

func TestSnapshotCarriesTaskStatusError(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "task-1"
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Publish task",
				"prompt": "Open a PR.",
			}),
		},
		core.Event{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskFailed,
				"error":  "publish completion pull request: patch does not apply",
			}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(snapshot.Tasks))
	}
	if snapshot.Tasks[0].Error != "publish completion pull request: patch does not apply" {
		t.Fatalf("task error = %q", snapshot.Tasks[0].Error)
	}
}

func TestSnapshotProjectsWorkerPrompt(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"title":  "Prompt task",
				"prompt": "Original request",
			}),
		},
		core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"kind":       "codex",
				"command":    []string{"codex", "exec", "-"},
				"prompt":     "line one\n  line two",
				"promptPath": "prompt.txt",
			}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(snapshot.Workers))
	}
	worker := snapshot.Workers[0]
	if worker.Prompt != "line one\n  line two" {
		t.Fatalf("worker prompt = %q", worker.Prompt)
	}
	if worker.PromptPath != "prompt.txt" {
		t.Fatalf("worker prompt path = %q", worker.PromptPath)
	}
}

func TestSnapshotProjectsTaskObjectiveMilestonesAndArtifacts(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	taskID := "task-objective"
	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Resolve issue",
				"prompt": "Open a PR and babysit it.",
			}),
		},
		core.Event{
			Type:   core.EventTaskArtifact,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":   "pr-1",
				"kind": "github_pull_request",
				"name": "owner/repo#12",
				"url":  "https://github.com/owner/repo/pull/12",
				"ref":  "codex/aged-test",
			}),
		},
		core.Event{
			Type:   core.EventTaskMilestone,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"name":    "pr_opened",
				"phase":   "pr_opened",
				"summary": "Pull request opened.",
			}),
		},
		core.Event{
			Type:   core.EventTaskWorkPlan,
			TaskID: taskID,
			Payload: core.MustJSON(core.WorkPlan{
				Summary: "Open the PR and monitor it through merge.",
				Workstreams: []core.WorkPlanItem{{
					ID:       "publish",
					Goal:     "Publish a coherent PR.",
					Status:   "done",
					DoneWhen: "A PR is open.",
				}},
				Validation: []core.WorkPlanItem{{
					ID:       "monitor",
					Goal:     "Monitor checks and review state.",
					Status:   "running",
					DoneWhen: "The PR is merged or no longer needs babysitting.",
				}},
				Risks: []string{"CI may fail after publication."},
			}),
		},
		core.Event{
			Type:   core.EventTaskObjective,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.ObjectiveWaitingExternal,
				"phase":  "pr_opened",
			}),
		},
		core.Event{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := snapshot.Tasks[0]
	if task.Status != core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveWaitingExternal || task.ObjectivePhase != "pr_opened" {
		t.Fatalf("task state = status %q objective %q phase %q", task.Status, task.ObjectiveStatus, task.ObjectivePhase)
	}
	if len(task.Milestones) != 1 || task.Milestones[0].Name != "pr_opened" {
		t.Fatalf("milestones = %+v", task.Milestones)
	}
	if len(task.Artifacts) != 1 || task.Artifacts[0].ID != "pr-1" {
		t.Fatalf("artifacts = %+v", task.Artifacts)
	}
	if task.WorkPlan == nil || task.WorkPlan.Validation[0].ID != "monitor" {
		t.Fatalf("work plan = %+v", task.WorkPlan)
	}
}

func TestSnapshotProjectsExecutionNodes(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"nodeId":        "node-0",
				"workerId":      "worker-1",
				"workerKind":    "codex",
				"planId":        "plan-1",
				"spawnId":       "implementation",
				"role":          "implementer",
				"reason":        "Implement the change.",
				"targetId":      "vm-1",
				"targetKind":    "ssh",
				"remoteSession": "aged-worker",
				"remoteRunDir":  "/runs/worker-1",
				"remoteWorkDir": "/repo",
			}),
		},
		core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   "task-1",
			WorkerID: "worker-2",
			Payload: core.MustJSON(map[string]any{
				"nodeId":       "node-1",
				"workerId":     "worker-2",
				"workerKind":   "claude",
				"planId":       "plan-1",
				"spawnId":      "review",
				"role":         "reviewer",
				"reason":       "Review the implementation.",
				"targetId":     "vm-1",
				"targetKind":   "ssh",
				"dependsOn":    []string{"implementation"},
				"parentNodeId": "node-0",
			}),
		},
		core.Event{
			Type:     core.EventWorkerStarted,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload:  core.MustJSON(map[string]any{}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.ExecutionNodes) != 2 {
		t.Fatalf("execution nodes = %d, want 2", len(snapshot.ExecutionNodes))
	}
	node := snapshot.ExecutionNodes[0]
	if node.ID != "node-0" || node.Status != core.WorkerRunning || node.Role != "implementer" || node.TargetID != "vm-1" || node.RemoteSession != "aged-worker" {
		t.Fatalf("node = %+v", node)
	}
	if len(snapshot.OrchestrationGraphs) != 1 {
		t.Fatalf("graphs = %d, want 1", len(snapshot.OrchestrationGraphs))
	}
	graph := snapshot.OrchestrationGraphs[0]
	if graph.TaskID != "task-1" || graph.Summary.Total != 2 || graph.Summary.Running != 1 {
		t.Fatalf("graph = %+v", graph)
	}
	if len(graph.Edges) != 2 {
		t.Fatalf("graph edges = %+v, want parent and dependency edges", graph.Edges)
	}
}

func TestProjectsPersistInSQLite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "aged.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	project := core.Project{
		ID:            "aged",
		Name:          "aged",
		LocalPath:     "/tmp/aged",
		Repo:          "owner/aged",
		UpstreamRepo:  "upstream/aged",
		HeadRepoOwner: "owner",
		PushRemote:    "fork",
		VCS:           "jj",
		DefaultBase:   "main",
		WorkspaceRoot: ".aged/workspaces",
		TargetLabels:  map[string]string{"pool": "local"},
		Requirements: core.ProjectRequirements{
			MemoryMB:  16_384,
			StorageMB: 100_000,
		},
		RemoteCheckouts: map[string]string{
			"vm-1": "/srv/aged/checkouts/aged",
		},
	}
	if _, err := store.SaveProject(ctx, project, true); err != nil {
		t.Fatal(err)
	}
	store.Close()

	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	projects, defaultID, err := reopened.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if defaultID != "aged" {
		t.Fatalf("default project = %q, want aged", defaultID)
	}
	if len(projects) != 1 {
		t.Fatalf("projects = %d, want 1", len(projects))
	}
	if projects[0].Repo != "owner/aged" || projects[0].UpstreamRepo != "upstream/aged" || projects[0].HeadRepoOwner != "owner" || projects[0].PushRemote != "fork" || projects[0].TargetLabels["pool"] != "local" || projects[0].Requirements.MemoryMB != 16_384 || projects[0].Requirements.StorageMB != 100_000 || projects[0].RemoteCheckouts["vm-1"] != "/srv/aged/checkouts/aged" {
		t.Fatalf("project = %+v", projects[0])
	}
}

func TestProjectsUpdateAndDeleteInSQLite(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	first := core.Project{ID: "a", Name: "A", LocalPath: t.TempDir(), VCS: "auto", DefaultBase: "main"}
	second := core.Project{ID: "b", Name: "B", LocalPath: t.TempDir(), VCS: "auto", DefaultBase: "main"}
	if _, err := store.CreateProject(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateProject(ctx, second); err != nil {
		t.Fatal(err)
	}
	second.Name = "Bee"
	second.DefaultBase = "trunk"
	if _, err := store.SaveProject(ctx, second, false); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteProject(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	projects, defaultID, err := store.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 1 || projects[0].ID != "b" || projects[0].Name != "Bee" || projects[0].DefaultBase != "trunk" {
		t.Fatalf("projects = %+v", projects)
	}
	if defaultID != "b" {
		t.Fatalf("defaultID = %q, want b", defaultID)
	}
}

func TestPluginsPersistInSQLite(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	plugin := core.Plugin{
		ID:           "runner:lint",
		Name:         "Lint Runner",
		Kind:         "runner",
		Protocol:     "aged-runner-v1",
		Enabled:      true,
		Status:       "ready",
		Command:      []string{"aged-lint"},
		Capabilities: []string{"lint"},
		Config:       map[string]string{"restart": "never"},
	}
	if _, err := store.SavePlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	plugins, err := store.ListPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plugins) != 1 || plugins[0].ID != "runner:lint" || plugins[0].Command[0] != "aged-lint" || plugins[0].Config["restart"] != "never" {
		t.Fatalf("plugins = %+v", plugins)
	}
	if err := store.DeletePlugin(ctx, "runner:lint"); err != nil {
		t.Fatal(err)
	}
	plugins, err = store.ListPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plugins) != 0 {
		t.Fatalf("plugins after delete = %+v", plugins)
	}
}

func TestTargetsPersistInSQLite(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	target := core.TargetConfig{
		ID:           "vm-1",
		Kind:         "ssh",
		Host:         "vm.local",
		User:         "aged",
		IdentityFile: "/tmp/id",
		WorkDir:      "/repo",
		WorkRoot:     "/runs",
		Labels:       map[string]string{"location": "remote"},
		Capacity:     core.TargetCapacity{MaxWorkers: 2, CPUWeight: 8, MemoryGB: 32},
	}
	if _, err := store.SaveTarget(ctx, target); err != nil {
		t.Fatal(err)
	}
	targets, err := store.ListTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].ID != "vm-1" || targets[0].Host != "vm.local" || targets[0].Labels["location"] != "remote" || targets[0].Capacity.MaxWorkers != 2 || targets[0].CheckoutRoot != "/repo" {
		t.Fatalf("targets = %+v", targets)
	}
	if err := store.DeleteTarget(ctx, "vm-1"); err != nil {
		t.Fatal(err)
	}
	targets, err = store.ListTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatalf("targets after delete = %+v", targets)
	}
}

func TestSnapshotHidesClearedTasksAndKeepsEvents(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLiteStore(t, ctx)

	appendSQLiteEvents(t, ctx, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"title":  "Finished task",
				"prompt": "Clear me",
			}),
		},
		core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-1",
				"workerId":   "worker-1",
				"workerKind": "mock",
			}),
		},
		core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   "task-1",
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"kind": "mock",
			}),
		},
		core.Event{
			Type:   core.EventTaskStatus,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskSucceeded,
			}),
		},
		core.Event{
			Type:    core.EventTaskCleared,
			TaskID:  "task-1",
			Payload: core.MustJSON(map[string]any{"reason": "test"}),
		},
	)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 0 {
		t.Fatalf("tasks = %d, want 0", len(snapshot.Tasks))
	}
	if len(snapshot.Workers) != 0 {
		t.Fatalf("workers = %d, want 0", len(snapshot.Workers))
	}
	if len(snapshot.ExecutionNodes) != 0 {
		t.Fatalf("execution nodes = %d, want 0", len(snapshot.ExecutionNodes))
	}
	if len(snapshot.Events) != 5 {
		t.Fatalf("events = %d, want 5", len(snapshot.Events))
	}
}
