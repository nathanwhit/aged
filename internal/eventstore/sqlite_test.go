package eventstore

import (
	"context"
	"path/filepath"
	"testing"

	"aged/internal/core"
)

func TestSnapshotReplaysMoreThanDefaultEventPage(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	taskID := "task-1"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Long task",
			"prompt":   "Generate enough events to cross the default page size.",
			"metadata": map[string]any{},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 205; i++ {
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"stream": "stdout",
				"text":   "progress",
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}

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
