package orchestrator

import (
	"context"
	"encoding/json"
	"os"
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

func TestServiceAddsWorkerCompletionSummaryFromResultEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	resultSummary := "implemented the requested change"
	changedFiles := []WorkspaceChangedFile{{Path: "internal/orchestrator/service_test.go", Status: "modified"}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "summary",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"summary": eventRunner{
		kind: "summary",
		events: []worker.Event{
			worker.LogEvent("stdout", "starting work"),
			{
				Kind: worker.EventResult,
				Text: resultSummary,
			},
		},
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: changedFiles,
			DiffStat:     "internal/orchestrator/service_test.go | 1 +",
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	payload := workerCompletedPayload(t, snapshot.Events, task.ID)
	if payload.Summary != resultSummary {
		t.Fatalf("summary = %q", payload.Summary)
	}
	if payload.LogCount != 1 {
		t.Fatalf("logCount = %d", payload.LogCount)
	}
	if len(payload.ChangedFiles) != 1 || payload.ChangedFiles[0] != changedFiles[0] {
		t.Fatalf("changedFiles = %+v", payload.ChangedFiles)
	}
	if !payload.WorkspaceChanges.Dirty {
		t.Fatalf("workspaceChanges.dirty = false")
	}
	if payload.Status != core.WorkerSucceeded {
		t.Fatalf("status = %q", payload.Status)
	}
}

func TestServiceMovesTaskToWaitingWhenWorkerNeedsInput(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "input",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"input": eventRunner{
		kind: "input",
		events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "approve dependency install?",
		}},
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	payload := workerCompletedPayload(t, snapshot.Events, task.ID)
	if payload.Status != core.WorkerWaiting {
		t.Fatalf("status = %q", payload.Status)
	}
	if !payload.NeedsInput {
		t.Fatalf("needsInput = false")
	}
	if hasEvent(snapshot.Events, core.EventWorkerCleanup, task.ID, "") {
		t.Fatalf("waiting worker workspace should be retained")
	}
}

func TestServiceAppliesRetainedWorkerChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	workspaceRoot := t.TempDir()
	changed := WorkspaceChangedFile{Path: "internal/example.txt", Status: "modified"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"writer": fileWritingRunner{
		kind: "writer",
		path: changed.Path,
		body: "worker output\n",
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd:        workspaceRoot,
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	result, err := service.ApplyWorkerChanges(ctx, snapshot.Workers[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.AppliedFiles) != 1 || result.AppliedFiles[0] != changed {
		t.Fatalf("applied files = %+v", result.AppliedFiles)
	}
	if result.Method != "fake_merge" {
		t.Fatalf("method = %q", result.Method)
	}
	appliedSnapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(appliedSnapshot.Events, core.EventWorkerApplied, task.ID, snapshot.Workers[0].ID) {
		t.Fatalf("missing worker.changes_applied event")
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

type eventRunner struct {
	kind   string
	events []worker.Event
}

type fileWritingRunner struct {
	kind string
	path string
	body string
}

func (r eventRunner) Kind() string {
	return r.kind
}

func (r eventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r eventRunner) Run(ctx context.Context, _ worker.Spec, sink worker.Sink) error {
	for _, event := range r.events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r fileWritingRunner) Kind() string {
	return r.kind
}

func (r fileWritingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r fileWritingRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	target := filepath.Join(spec.WorkDir, r.path)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	return os.WriteFile(target, []byte(r.body), 0o644)
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
	cwd        string
	sourceRoot string
	changes    WorkspaceChanges
}

func (m fakeWorkspaceManager) Prepare(_ context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	sourceRoot := m.sourceRoot
	mode := string(WorkspaceModeShared)
	if sourceRoot == "" {
		sourceRoot = m.cwd
	} else if sourceRoot != m.cwd {
		mode = string(WorkspaceModeIsolated)
	}
	return PreparedWorkspace{
		Root:       m.cwd,
		CWD:        m.cwd,
		SourceRoot: sourceRoot,
		Change:     "@ fake",
		Status:     "The working copy has no changes.",
		Mode:       mode,
		VCSType:    "jj",
		Dirty:      false,
		WorkerID:   spec.WorkerID,
		TaskID:     spec.TaskID,
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

func (m fakeWorkspaceManager) DescribeChanges(_ context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	changes := m.changes
	if changes.Root == "" {
		changes.Root = workspace.Root
	}
	if changes.CWD == "" {
		changes.CWD = workspace.CWD
	}
	if changes.WorkspaceName == "" {
		changes.WorkspaceName = workspace.WorkspaceName
	}
	if changes.Mode == "" {
		changes.Mode = workspace.Mode
	}
	if changes.VCSType == "" {
		changes.VCSType = workspace.VCSType
	}
	return changes, nil
}

func (m fakeWorkspaceManager) ApplyChanges(_ context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	return WorkerApplyResult{
		SourceRoot:    workspace.SourceRoot,
		WorkspaceRoot: workspace.Root,
		Method:        "fake_merge",
		AppliedFiles:  changes.ChangedFiles,
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

func workerCompletedPayload(t *testing.T, events []core.Event, taskID string) struct {
	Status           core.WorkerStatus      `json:"status"`
	Summary          string                 `json:"summary"`
	NeedsInput       bool                   `json:"needsInput"`
	LogCount         int                    `json:"logCount"`
	ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
	WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
} {
	t.Helper()
	for _, event := range events {
		if event.Type != core.EventWorkerCompleted || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Status           core.WorkerStatus      `json:"status"`
			Summary          string                 `json:"summary"`
			NeedsInput       bool                   `json:"needsInput"`
			LogCount         int                    `json:"logCount"`
			ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
			WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		return payload
	}
	t.Fatalf("missing worker.completed for task %s", taskID)
	return struct {
		Status           core.WorkerStatus      `json:"status"`
		Summary          string                 `json:"summary"`
		NeedsInput       bool                   `json:"needsInput"`
		LogCount         int                    `json:"logCount"`
		ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
		WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
	}{}
}
