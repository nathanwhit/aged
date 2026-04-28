package orchestrator

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestServiceUsesBrainSelectedWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &recordingRunner{kind: "chosen"}
	workspaces := fakeWorkspaceManager{cwd: t.TempDir()}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "chosen",
		Prompt:     "worker prompt from brain",
		Rationale:  "test brain chose this worker",
		Steps:      []PlanStep{{Title: "Run", Description: "Execute"}},
	}}, map[string]worker.Runner{"chosen": runner}, t.TempDir(), workspaces)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.prompt != "worker prompt from brain" {
		t.Fatalf("runner prompt = %q", runner.prompt)
	}
	if runner.workDir != workspaces.cwd {
		t.Fatalf("runner workDir = %q, want %q", runner.workDir, workspaces.cwd)
	}
	if !hasEvent(snapshot.Events, core.EventTaskPlanned, task.ID, "") {
		t.Fatalf("missing task.planned event")
	}
	if !hasEvent(snapshot.Events, core.EventWorkerWorkspace, task.ID, "") {
		t.Fatalf("missing worker.workspace_prepared event")
	}
	if !hasEvent(snapshot.Events, core.EventWorkerCleanup, task.ID, "") {
		t.Fatalf("missing worker.workspace_cleaned event")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "chosen") {
		t.Fatalf("missing worker.created with chosen kind")
	}
}

func TestServiceFailsCleanlyForUnknownBrainWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "missing",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	if !hasEvent(snapshot.Events, core.EventTaskPlanned, task.ID, "") {
		t.Fatalf("missing task.planned event before failure")
	}
}

type fixedBrain struct {
	plan Plan
	err  error
}

func (b fixedBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, b.err
}

type recordingRunner struct {
	kind    string
	prompt  string
	workDir string
}

func (r *recordingRunner) Kind() string {
	return r.kind
}

func (r *recordingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *recordingRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	return nil
}

type fakeWorkspaceManager struct {
	cwd string
}

func (m fakeWorkspaceManager) Prepare(_ context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	return PreparedWorkspace{
		Root:     m.cwd,
		CWD:      m.cwd,
		Change:   "@ fake",
		Status:   "The working copy has no changes.",
		Mode:     "shared",
		VCSType:  "jj",
		Dirty:    false,
		WorkerID: spec.WorkerID,
		TaskID:   spec.TaskID,
	}, nil
}

func (m fakeWorkspaceManager) Cleanup(_ context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	return WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        workspace.CleanupPolicy,
		Result:        result,
		Reason:        "fake cleanup retained workspace",
	}, nil
}

func openTestStore(t *testing.T) *eventstore.SQLiteStore {
	t.Helper()
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func waitForTaskStatus(t *testing.T, store eventstore.Store, taskID string, status core.TaskStatus) core.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range snapshot.Tasks {
			if task.ID == taskID && task.Status == status {
				return snapshot
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, _ := store.Snapshot(context.Background())
	t.Fatalf("task %s did not reach %s; snapshot = %+v", taskID, status, snapshot.Tasks)
	return core.Snapshot{}
}

func hasEvent(events []core.Event, eventType core.EventType, taskID string, workerID string) bool {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && (workerID == "" || event.WorkerID == workerID) {
			return true
		}
	}
	return false
}

func hasWorkerCreated(events []core.Event, taskID string, kind string) bool {
	for _, event := range events {
		if event.Type != core.EventWorkerCreated || event.TaskID != taskID {
			continue
		}
		if string(event.Payload) == "" {
			continue
		}
		if strings.Contains(string(event.Payload), `"kind":"`+kind+`"`) {
			return true
		}
	}
	return false
}
