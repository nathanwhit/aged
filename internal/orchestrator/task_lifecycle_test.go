package orchestrator

import (
	"context"
	"errors"
	"testing"

	"aged/internal/core"
)

func TestTaskLifecycleIgnoresLateNonTerminalTransitionAfterCancel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	taskID := "task-lifecycle-canceled"
	appendLifecycleTask(t, ctx, store, taskID)
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, nil, t.TempDir())
	if err := service.setTaskStatusWithReason(ctx, taskID, core.TaskCanceled, taskCancelReasonUser); err != nil {
		t.Fatal(err)
	}

	if err := service.setTaskStatus(ctx, taskID, core.TaskRunning); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("task missing")
	}
	if task.Status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", task.Status)
	}
}

func TestTaskLifecycleIgnoresLateFailureAfterCancel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	taskID := "task-lifecycle-fail-after-cancel"
	appendLifecycleTask(t, ctx, store, taskID)
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, nil, t.TempDir())
	if err := service.setTaskStatusWithReason(ctx, taskID, core.TaskCanceled, taskCancelReasonUser); err != nil {
		t.Fatal(err)
	}

	if err := service.failTask(ctx, taskID, errors.New("late worker failed")); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("task missing")
	}
	if task.Status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", task.Status)
	}
	if task.Error != "" {
		t.Fatalf("late failure overwrote canceled task error: %q", task.Error)
	}
}

func TestTaskLifecycleIgnoresLateActiveObjectiveAfterCancel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	taskID := "task-lifecycle-objective-after-cancel"
	appendLifecycleTask(t, ctx, store, taskID)
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, nil, t.TempDir())
	if err := service.setTaskStatusWithReason(ctx, taskID, core.TaskCanceled, taskCancelReasonUser); err != nil {
		t.Fatal(err)
	}

	if err := service.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "continuing", "late PR callback tried to continue"); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("task missing")
	}
	if task.Status != core.TaskCanceled || task.ObjectiveStatus != core.ObjectiveAbandoned {
		t.Fatalf("task state = %s/%s, want canceled/abandoned", task.Status, task.ObjectiveStatus)
	}
}

func TestTaskLifecycleRetryCanReviveTerminalTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	taskID := "task-lifecycle-retry"
	appendLifecycleTask(t, ctx, store, taskID)
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, nil, t.TempDir())
	if err := service.failTask(ctx, taskID, errors.New("initial failure")); err != nil {
		t.Fatal(err)
	}

	if err := service.markTaskRetryPlanning(ctx, taskID); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("task missing")
	}
	if task.Status != core.TaskPlanning {
		t.Fatalf("task status = %q, want planning", task.Status)
	}
	if task.ObjectiveStatus != core.ObjectiveActive || task.ObjectivePhase != "retrying" {
		t.Fatalf("objective = %s/%s, want active/retrying", task.ObjectiveStatus, task.ObjectivePhase)
	}
}

func appendLifecycleTask(t *testing.T, ctx context.Context, store interface {
	Append(context.Context, core.Event) (core.Event, error)
}, taskID string) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  taskID,
			"prompt": "test task lifecycle",
		}),
	}); err != nil {
		t.Fatal(err)
	}
}
