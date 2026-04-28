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

func TestSnapshotProjectsExecutionNodes(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-1",
			"workerId":      "worker-1",
			"workerKind":    "codex",
			"planId":        "plan-1",
			"spawnId":       "review",
			"role":          "reviewer",
			"reason":        "Review the implementation.",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/worker-1",
			"remoteWorkDir": "/repo",
			"dependsOn":     []string{"implementation"},
			"parentNodeId":  "node-0",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload:  core.MustJSON(map[string]any{}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("execution nodes = %d, want 1", len(snapshot.ExecutionNodes))
	}
	node := snapshot.ExecutionNodes[0]
	if node.ID != "node-1" || node.Status != core.WorkerRunning || node.Role != "reviewer" || node.DependsOn[0] != "implementation" || node.TargetID != "vm-1" || node.RemoteSession != "aged-worker" {
		t.Fatalf("node = %+v", node)
	}
}

func TestSnapshotHidesClearedTasksAndKeepsEvents(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Finished task",
			"prompt": "Clear me",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   "worker-1",
			"workerKind": "mock",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "mock",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskCleared,
		TaskID:  "task-1",
		Payload: core.MustJSON(map[string]any{"reason": "test"}),
	}); err != nil {
		t.Fatal(err)
	}

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
