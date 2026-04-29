package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestRecoverRemoteWorkersCancelsStaleLocalWorkers(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-1"
	workerID := "worker-1"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Stale task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   workerID,
			"workerKind": "codex",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[0].Status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", snapshot.Tasks[0].Status)
	}
	if snapshot.Workers[0].Status != core.WorkerCanceled {
		t.Fatalf("worker status = %q, want canceled", snapshot.Workers[0].Status)
	}
	if snapshot.ExecutionNodes[0].Status != core.WorkerCanceled {
		t.Fatalf("node status = %q, want canceled", snapshot.ExecutionNodes[0].Status)
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
	applyCalls := 0
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
		applyCalls: &applyCalls,
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
	if applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", applyCalls)
	}
	if _, err := service.ApplyWorkerChanges(ctx, snapshot.Workers[0].ID); err == nil {
		t.Fatal("second apply succeeded, want error")
	}
	if applyCalls != 1 {
		t.Fatalf("second apply changed apply calls to %d, want 1", applyCalls)
	}
}

func TestServiceRunsSpawnedFollowUpWorkerWithPriorResultContext(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	implementationSummary := "implemented the first refactor slice"
	changed := WorkspaceChangedFile{Path: "internal/refactor.go", Status: "modified"}
	reviewer := &recordingEventRunner{
		kind: "claude",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "reviewed implementation",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Rationale:  "large refactor should start with one bounded implementation turn",
		Steps: []PlanStep{{
			Title:       "Implement slice",
			Description: "Make the first scoped code change.",
		}},
		Spawns: []SpawnRequest{{
			Role:   "reviewer",
			Reason: "Review the implementation output and recommend required follow-up fixes.",
		}},
	}}, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: implementationSummary,
			}},
		},
		"claude": reviewer,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Large refactor",
		Prompt: "Refactor the subsystem and have another worker review it.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 2 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "codex") {
		t.Fatalf("missing initial codex worker")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "claude") {
		t.Fatalf("missing follow-up claude reviewer worker")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}

	prompt := reviewer.promptValue()
	for _, want := range []string{
		"Follow-up role:\nreviewer",
		implementationSummary,
		"modified internal/refactor.go",
		"Review the implementation output",
		"Benchmark Results",
		"Recommended Next Turns",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("follow-up prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestServiceRunsIndependentSpawnedWorkersInParallel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	release := make(chan struct{})
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{
			{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the implementation output.",
				WorkerKind: "left",
			},
			{
				ID:         "test",
				Role:       "tester",
				Reason:     "Validate the implementation output.",
				WorkerKind: "right",
			},
		},
	}}, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: "implemented the first slice",
			}},
		},
		"left":  &blockingEventRunner{kind: "left", started: started, release: release, summary: "left done"},
		"right": &blockingEventRunner{kind: "right", started: started, release: release, summary: "right done"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Parallel review",
		Prompt: "Implement, then review and test in parallel.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(500 * time.Millisecond)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			t.Fatalf("spawned workers did not start in parallel; started = %+v", got)
		}
	}
	close(release)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "left") || !hasWorkerCreated(snapshot.Events, task.ID, "right") {
		t.Fatalf("missing parallel spawned workers")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 3 {
		t.Fatalf("task.planned count = %d, want 3", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
}

func TestServiceHonorsSpawnDependencies(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	firstStarted := make(chan string, 1)
	secondStarted := make(chan string, 1)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	second := &blockingEventRunner{kind: "second", started: secondStarted, release: secondRelease, summary: "second done"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{
			{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the implementation output.",
				WorkerKind: "first",
			},
			{
				ID:         "incorporate",
				Role:       "implementer",
				Reason:     "Incorporate required review feedback.",
				WorkerKind: "second",
				DependsOn:  []string{"review"},
			},
		},
	}}, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: "implemented the first slice",
			}},
		},
		"first":  &blockingEventRunner{kind: "first", started: firstStarted, release: firstRelease, summary: "review summary"},
		"second": second,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent follow-up",
		Prompt: "Implement, review, then incorporate feedback.",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-firstStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first spawned worker did not start")
	}
	select {
	case <-secondStarted:
		t.Fatal("dependent worker started before dependency completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(firstRelease)
	select {
	case <-secondStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dependent worker did not start after dependency completed")
	}
	close(secondRelease)

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(second.promptValue(), "review summary") {
		t.Fatalf("dependent prompt missing dependency summary:\n%s", second.promptValue())
	}
}

func TestServiceDynamicallyReplansAfterFollowUpWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	implementer := &recordingEventRunner{
		kind: "codex",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "implemented the first slice",
		}},
	}
	reviewer := &recordingEventRunner{
		kind: "claude",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "review found a missing edge case",
		}},
	}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement first slice",
			Rationale:  "start with implementation",
			Spawns: []SpawnRequest{{
				Role:   "reviewer",
				Reason: "Review the initial implementation.",
			}},
		},
		decisions: []ReplanDecision{
			{
				Action:    "continue",
				Rationale: "review requested an incorporation turn",
				Plan: &Plan{
					WorkerKind: "codex",
					Prompt:     "incorporate reviewer feedback about the missing edge case",
					Rationale:  "review found a missing edge case",
					Steps: []PlanStep{{
						Title:       "Incorporate feedback",
						Description: "Fix the reviewed edge case.",
					}},
					RequiredApprovals: []ApprovalRequest{},
					Spawns:            []SpawnRequest{},
				},
			},
			{
				Action:    "complete",
				Rationale: "incorporation turn completed",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  implementer,
		"claude": reviewer,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Large refactor",
		Prompt: "Implement, review, then incorporate review feedback.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 3 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 3 {
		t.Fatalf("task.planned count = %d, want 3", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != 2 {
		t.Fatalf("task.replanned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID))
	}
	if !strings.Contains(implementer.promptValue(), "incorporate reviewer feedback") {
		t.Fatalf("last implementer prompt = %q", implementer.promptValue())
	}
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if len(brain.states[0].Results) != 2 {
		t.Fatalf("first replan results = %d, want 2", len(brain.states[0].Results))
	}
	if len(brain.states[1].Results) != 3 {
		t.Fatalf("second replan results = %d, want 3", len(brain.states[1].Results))
	}
}

func TestServiceRunsSpawnedWorkersFromDynamicReplan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	release := make(chan struct{})
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial slice",
		},
		decisions: []ReplanDecision{
			{
				Action: "continue",
				Plan: &Plan{
					WorkerKind: "codex",
					Prompt:     "incorporate the first result",
					Rationale:  "initial result needs review and validation",
					Spawns: []SpawnRequest{
						{
							ID:         "review",
							Role:       "reviewer",
							Reason:     "Review the incorporated result.",
							WorkerKind: "reviewer",
						},
						{
							ID:         "test",
							Role:       "tester",
							Reason:     "Validate the incorporated result.",
							WorkerKind: "tester",
						},
					},
				},
			},
			{
				Action:    "complete",
				Rationale: "implementation and spawned verification completed",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: "codex turn done",
			}},
		},
		"reviewer": &blockingEventRunner{kind: "reviewer", started: started, release: release, summary: "review passed"},
		"tester":   &blockingEventRunner{kind: "tester", started: started, release: release, summary: "tests passed"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dynamic spawn",
		Prompt: "Use dynamic replanning to schedule parallel verification.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(500 * time.Millisecond)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			t.Fatalf("replanned spawned workers did not start in parallel; started = %+v", got)
		}
	}
	close(release)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "reviewer") || !hasWorkerCreated(snapshot.Events, task.ID, "tester") {
		t.Fatalf("missing replanned spawned workers")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 4 {
		t.Fatalf("task.planned count = %d, want 4", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if len(brain.states[1].Results) != 4 {
		t.Fatalf("second replan results = %d, want 4", len(brain.states[1].Results))
	}
}

func TestServiceEmitsExecutionGraphNodes(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement first slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the first slice.",
			WorkerKind: "claude",
		}},
	}}, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"claude": eventRunner{kind: "claude", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Graph", Prompt: "Run graph task."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 2 {
		t.Fatalf("execution nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[0].WorkerKind != "codex" || snapshot.ExecutionNodes[0].Status != core.WorkerSucceeded {
		t.Fatalf("primary node = %+v", snapshot.ExecutionNodes[0])
	}
	if snapshot.ExecutionNodes[1].SpawnID != "review" || snapshot.ExecutionNodes[1].ParentNodeID != snapshot.ExecutionNodes[0].ID {
		t.Fatalf("follow-up node = %+v, primary = %+v", snapshot.ExecutionNodes[1], snapshot.ExecutionNodes[0])
	}
}

func TestServiceDeliversSteeringToRunningWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	gotSteering := make(chan string, 1)
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "steerable",
		Prompt:     "wait for steering",
	}}, map[string]worker.Runner{
		"steerable": steeringRunner{started: started, got: gotSteering},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Steer", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "adjust course"}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-gotSteering:
		if message != "adjust course" {
			t.Fatalf("steering = %q", message)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not receive steering")
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
}

func TestServiceRecommendsManualApplyPolicyForMultipleCandidates(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement baseline",
		Spawns: []SpawnRequest{
			{ID: "opt-a", Role: "optimizer", Reason: "Try optimization A.", WorkerKind: "left"},
			{ID: "opt-b", Role: "optimizer", Reason: "Try optimization B.", WorkerKind: "right"},
		},
	}}, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "baseline"}}},
		"left":  fileWritingRunner{kind: "left", path: "a.txt", body: "a"},
		"right": fileWritingRunner{kind: "right", path: "b.txt", body: "b"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.txt", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Apply policy", Prompt: "Try alternatives."})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	policy, err := service.RecommendApplyPolicy(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Strategy != "manual_select" {
		t.Fatalf("strategy = %q, policy = %+v", policy.Strategy, policy)
	}
	if len(policy.Candidates) < 2 {
		t.Fatalf("candidates = %+v", policy.Candidates)
	}
}

func TestServiceRunsWorkerOnSSHTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Labels:   map[string]string{"role": "remote"},
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote",
		Prompt:     "run remotely",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{
		"remote": buildOnlyRunner{kind: "remote", command: []string{"sh", "-lc", "echo remote output"}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote", Prompt: "Run on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	node := snapshot.ExecutionNodes[0]
	if node.TargetID != "vm-1" || node.TargetKind != "ssh" || node.RemoteSession == "" {
		t.Fatalf("node = %+v", node)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerOutput, task.ID, snapshot.Workers[0].ID) {
		t.Fatalf("missing remote worker output")
	}
}

type fixedBrain struct {
	plan Plan
	err  error
}

func (b fixedBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, b.err
}

type replanningBrain struct {
	plan      Plan
	decisions []ReplanDecision
	states    []OrchestrationState
}

func (b *replanningBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, nil
}

func (b *replanningBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if len(b.decisions) == 0 {
		return ReplanDecision{Action: "complete"}, nil
	}
	decision := b.decisions[0]
	b.decisions = b.decisions[1:]
	return decision, nil
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

type recordingEventRunner struct {
	mu      sync.Mutex
	kind    string
	events  []worker.Event
	prompt  string
	workDir string
}

type blockingEventRunner struct {
	mu      sync.Mutex
	kind    string
	started chan<- string
	release <-chan struct{}
	summary string
	prompt  string
}

type steeringRunner struct {
	started chan<- struct{}
	got     chan<- string
}

type buildOnlyRunner struct {
	kind    string
	command []string
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

func (r *recordingEventRunner) Kind() string {
	return r.kind
}

func (r *recordingEventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *recordingEventRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	r.mu.Unlock()
	for _, event := range r.events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r *recordingEventRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *blockingEventRunner) Kind() string {
	return r.kind
}

func (r *blockingEventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *blockingEventRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.prompt = spec.Prompt
	r.mu.Unlock()
	r.started <- r.kind
	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.summary != "" {
		return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: r.summary})
	}
	return nil
}

func (r *blockingEventRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r steeringRunner) Kind() string {
	return "steerable"
}

func (r steeringRunner) SupportsSteering() bool {
	return true
}

func (r steeringRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r steeringRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	close(r.started)
	select {
	case message := <-spec.Steering:
		r.got <- message
		return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: "received steering"})
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r buildOnlyRunner) Kind() string {
	return r.kind
}

func (r buildOnlyRunner) BuildCommand(worker.Spec) []string {
	return r.command
}

func (r buildOnlyRunner) Run(context.Context, worker.Spec, worker.Sink) error {
	return errors.New("build-only runner should not run locally")
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
	applyCalls *int
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
	if m.applyCalls != nil {
		*m.applyCalls = *m.applyCalls + 1
	}
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

func countEvents(events []core.Event, eventType core.EventType, taskID string) int {
	count := 0
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID {
			count++
		}
	}
	return count
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
