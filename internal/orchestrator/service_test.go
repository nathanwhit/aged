package orchestrator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	if !strings.Contains(runner.prompt, "worker prompt from brain") {
		t.Fatalf("runner prompt = %q", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Run every command from this execution workspace:\n"+workspaces.cwd) {
		t.Fatalf("runner prompt did not include execution workspace: %q", runner.prompt)
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
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(snapshot.Workers))
	}
	if snapshot.Workers[0].Prompt != runner.prompt {
		t.Fatalf("snapshot worker prompt = %q, want runner prompt %q", snapshot.Workers[0].Prompt, runner.prompt)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "prompt", runner.prompt) {
		t.Fatalf("missing worker.created prompt")
	}
}

func TestPlanNormalizeFillsMissingActionReason(t *testing.T) {
	plan := Plan{
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "immediate",
			Inputs: map[string]any{},
		}},
	}
	normalizePlanShape(&plan)
	if plan.Actions[0].Reason == "" {
		t.Fatal("action reason was not filled")
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceRejectsBroadObjectiveFinishWithUnpublishedCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	task := core.Task{
		ID:       "task-unpublished-candidate",
		Title:    "Broad objective",
		Prompt:   "Modernize the UI.",
		Metadata: core.MustJSON(map[string]any{"objectiveMode": "broad"}),
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":    task.Title,
			"prompt":   task.Prompt,
			"metadata": task.Metadata,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	brain := &replanningBrain{decisions: []ReplanDecision{
		{
			Action:    "finish_objective",
			Rationale: "worker completed the UI modernization",
			Message:   "done",
		},
		{
			Action:  "wait",
			Message: "publish or explicitly continue",
		},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.replanLoop(ctx, task, Plan{}, []WorkerTurnResult{{
		WorkerID: "worker-ui",
		Status:   core.WorkerSucceeded,
		Kind:     "codex",
		Summary:  "implemented UI changes",
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "web/src/styles.css", Status: "modified"}},
		},
	}})

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if !hasTaskAction(snapshot.Events, task.ID, "replan_completion_rejected", "rejected") {
		t.Fatalf("missing rejected completion action:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if hasTaskAction(snapshot.Events, task.ID, "finish_objective", "completed") {
		t.Fatalf("finish_objective action should not complete with unpublished candidate:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServiceRejectsBroadObjectiveCompletionWithIncompleteWorkPlan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	task := core.Task{
		ID:       "task-incomplete-work-plan",
		Title:    "Broad objective",
		Prompt:   "Implement the full supervision UI spec.",
		Metadata: core.MustJSON(map[string]any{"objectiveMode": "broad"}),
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":    task.Title,
			"prompt":   task.Prompt,
			"metadata": task.Metadata,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	workPlan := &core.WorkPlan{
		Summary: "Implement the full monitor UI spec.",
		Workstreams: []core.WorkPlanItem{{
			ID:       "manager-summary",
			Goal:     "Expose manager summaries.",
			Status:   "done",
			DoneWhen: "Manager summaries are visible.",
		}, {
			ID:       "monitor-ui",
			Goal:     "Build the actual monitor UI.",
			Status:   "pending",
			DoneWhen: "The user can inspect live sessions and guide workers.",
		}},
		Validation: []core.WorkPlanItem{{
			ID:       "ui-validation",
			Goal:     "Validate the monitor UI end to end.",
			DoneWhen: "Browser checks cover the monitor surface.",
		}},
	}
	brain := &replanningBrain{decisions: []ReplanDecision{
		{
			Action:    "complete",
			Rationale: "manager summary slice is done",
		},
		{
			Action:    "finish_objective",
			Rationale: "manager summary slice is enough",
			Message:   "done",
		},
		{
			Action:  "wait",
			Message: "continue implementing the remaining monitor UI spec",
		},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.replanLoop(ctx, task, Plan{WorkPlan: workPlan}, nil)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != 3 {
		t.Fatalf("replan states = %d, want 3", len(brain.states))
	}
	if countTaskActions(snapshot.Events, task.ID, "replan_completion_rejected", "rejected") != 2 {
		t.Fatalf("rejected completion count = %d, want 2:\n%s", countTaskActions(snapshot.Events, task.ID, "replan_completion_rejected", "rejected"), taskActionPayloads(snapshot.Events, task.ID))
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskAction, task.ID, "monitor-ui") ||
		!eventPayloadContains(snapshot.Events, core.EventTaskAction, task.ID, "ui-validation") {
		t.Fatalf("rejection did not name incomplete work plan items:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if taskStatus(snapshot, task.ID) == core.TaskSucceeded {
		t.Fatalf("task succeeded despite incomplete work plan")
	}
}

func TestServiceProvidesSharedArtifactWorkspaceToLocalWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sharedRoot := filepath.Join(t.TempDir(), "shared-task")
	runner := &recordingEventRunner{
		kind:   "codex",
		events: []worker.Event{{Kind: worker.EventResult, Text: "done"}},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "build a baseline binary",
	}}, map[string]worker.Runner{"codex": runner}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sharedRoot: sharedRoot,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Bench", Prompt: "Save a baseline"})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	spec := runner.specValue()
	if spec.Env["AGED_SHARED_DIR"] != sharedRoot {
		t.Fatalf("AGED_SHARED_DIR = %q, want %q", spec.Env["AGED_SHARED_DIR"], sharedRoot)
	}
	if spec.Env["AGED_SHARED_ARTIFACTS_DIR"] != filepath.Join(sharedRoot, "artifacts") {
		t.Fatalf("AGED_SHARED_ARTIFACTS_DIR = %q", spec.Env["AGED_SHARED_ARTIFACTS_DIR"])
	}
	if !strings.Contains(spec.Prompt, "shared artifact workspace") || !strings.Contains(spec.Prompt, sharedRoot) {
		t.Fatalf("prompt missing shared workspace guidance: %q", spec.Prompt)
	}
	if _, err := os.Stat(filepath.Join(sharedRoot, "workers", shortID(spec.ID))); err != nil {
		t.Fatalf("worker scratch dir was not created: %v", err)
	}
}

func TestCreateTasksActionCreatesGenericChildTasks(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "Run child work.",
		Rationale:  "child task",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "Done."}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	parent := core.Task{
		ID:        "parent-task",
		ProjectID: "default",
	}
	created, err := service.createChildTasksFromAction(ctx, parent, PlanAction{
		Kind: "create_tasks",
		Inputs: map[string]any{
			"tasks": []any{
				map[string]any{
					"title":        "Build benchmark harness",
					"prompt":       "Create a benchmark harness artifact for comparing options.",
					"workstreamId": "benchmark-harness",
				},
				map[string]any{
					"title":        "Ship product optimization",
					"prompt":       "Implement a PR-sized product optimization for the upstream repository.",
					"workstreamId": "product-optimization",
					"dependsOn":    []string{"benchmark-harness"},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 2 {
		t.Fatalf("created %d tasks, want 2", len(created))
	}
	var firstMetadata map[string]any
	if err := json.Unmarshal(created[0].Metadata, &firstMetadata); err != nil {
		t.Fatal(err)
	}
	if firstMetadata["parentTaskId"] != parent.ID {
		t.Fatalf("child parentTaskId = %v, want %s", firstMetadata["parentTaskId"], parent.ID)
	}
	if firstMetadata["workstreamId"] != "benchmark-harness" {
		t.Fatalf("child workstreamId = %v, want benchmark-harness", firstMetadata["workstreamId"])
	}
	var secondMetadata map[string]any
	if err := json.Unmarshal(created[1].Metadata, &secondMetadata); err != nil {
		t.Fatal(err)
	}
	if _, ok := firstMetadata["completionMode"]; ok {
		t.Fatalf("first child metadata = %+v, want no completionMode", firstMetadata)
	}
	if _, ok := secondMetadata["completionMode"]; ok {
		t.Fatalf("second child metadata = %+v, want no completionMode", secondMetadata)
	}
	dependsOn, ok := secondMetadata["dependsOn"].([]any)
	if !ok || len(dependsOn) != 1 || dependsOn[0] != "benchmark-harness" {
		t.Fatalf("child dependsOn = %+v, want benchmark-harness", secondMetadata["dependsOn"])
	}
	found, ok, err := service.FindTaskByExternalID(ctx, "task-child", "parent-task:benchmark-harness")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("child task was not indexed by task-child external id")
	}
	if found.ID != created[0].ID {
		t.Fatalf("found task ID = %s, want %s", found.ID, created[0].ID)
	}
}

func TestSpawnWorkActionRunsObjectiveWorkItems(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	brain := &replanningBrain{
		plan: Plan{
			Rationale: "fan out objective slices",
			Actions: []PlanAction{{
				Kind:   "spawn_work",
				When:   "immediate",
				Reason: "Run independent implementation slices inside this objective.",
				Inputs: map[string]any{
					"items": []any{
						map[string]any{
							"id":         "slice-a",
							"kind":       "objective.slice",
							"reason":     "Port file set A.",
							"prompt":     "port file set A",
							"workerKind": "slice-a",
						},
						map[string]any{
							"id":         "slice-b",
							"kind":       "objective.slice",
							"reason":     "Port file set B.",
							"prompt":     "port file set B",
							"workerKind": "slice-b",
						},
					},
				},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "spawned slices completed",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"slice-a": &blockingEventRunner{kind: "slice-a", started: started, release: releaseA, summary: "slice A done"},
		"slice-b": &blockingEventRunner{kind: "slice-b", started: started, release: releaseB, summary: "slice B done"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Wide port",
		Prompt: "Port several file sets in parallel.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			snapshot, _ := store.Snapshot(ctx)
			t.Fatalf("spawned work items did not start in parallel; started = %+v tasks=%+v workItems=%+v eventCount=%d taskActions=%s", got, snapshot.Tasks, snapshot.WorkItems, len(snapshot.Events), taskActionPayloads(snapshot.Events, task.ID))
		}
	}
	close(releaseA)
	partial := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		first, firstOK := workItemByID(snapshot, "slice-a")
		second, secondOK := workItemByID(snapshot, "slice-b")
		return firstOK && secondOK && first.Status == core.WorkItemSucceeded && second.Status == core.WorkItemRunning
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("spawned work did not reach partial state: %+v", snapshot.WorkItems)
	})
	if taskStatus(partial, task.ID) == core.TaskSucceeded {
		t.Fatalf("task completed before all spawned work drained")
	}
	if len(brain.states) != 0 {
		t.Fatalf("replan states before spawned work drained = %d, want 0", len(brain.states))
	}
	close(releaseB)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	for _, id := range []string{"slice-a", "slice-b"} {
		item, ok := workItemByID(snapshot, id)
		if !ok {
			t.Fatalf("missing work item %s", id)
		}
		if item.Status != core.WorkItemSucceeded || item.TargetKind != "worker" || item.WorkerID == "" {
			t.Fatalf("work item %s = %+v, want succeeded worker item", id, item)
		}
	}
	if !hasTaskAction(snapshot.Events, task.ID, "spawn_work", "") {
		t.Fatalf("missing spawn_work task action")
	}
	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	if len(brain.states[0].Results) != 2 {
		t.Fatalf("replan results = %d, want 2", len(brain.states[0].Results))
	}
}

func TestRecoverRemoteWorkersStartsQueuedSpawnWorkItems(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-wide-recover"
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Wide recover",
				"prompt": "Resume queued slices.",
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskRunning,
			}),
		},
		{
			Type:   core.EventWorkItemQueued,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":         "slice-recover",
				"kind":       "objective.slice",
				"targetKind": "objective",
				"targetId":   taskID,
				"reason":     "Resume a queued slice.",
				"prompt":     "resume queued slice",
				"metadata": map[string]any{
					"sourceAction": "spawn_work",
					"workerKind":   "slice",
				},
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	started := make(chan string, 1)
	release := make(chan struct{})
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"slice": &blockingEventRunner{kind: "slice", started: started, release: release, summary: "recovered slice"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case kind := <-started:
		if kind != "slice" {
			t.Fatalf("started worker kind = %q, want slice", kind)
		}
	case <-time.After(time.Second):
		snapshot, _ := store.Snapshot(ctx)
		t.Fatalf("queued spawned work was not recovered: tasks=%+v workItems=%+v taskActions=%s", snapshot.Tasks, snapshot.WorkItems, taskActionPayloads(snapshot.Events, taskID))
	}
	close(release)
	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		item, ok := workItemByID(snapshot, "slice-recover")
		return ok && item.Status == core.WorkItemSucceeded
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("spawned work item did not complete after recovery: %+v", snapshot.WorkItems)
	})
	if !hasTaskAction(snapshot.Events, taskID, "startup_spawn_work_recovery", "resumed") {
		t.Fatalf("missing startup spawn recovery action: %s", taskActionPayloads(snapshot.Events, taskID))
	}
}

func TestRecordWorkItemLifecycleIgnoresDuplicateTerminalTransitions(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-work-item-idempotence"
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Work item idempotence",
			"prompt": "Do not restart terminal work items.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "slice",
		"kind":       "objective.slice",
		"targetKind": "objective",
		"targetId":   taskID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemStarted(ctx, taskID, "slice", "worker-1"); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemCompleted(ctx, taskID, "slice", core.WorkItemFailed, "worker-1", "failed once"); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemStarted(ctx, taskID, "slice", "worker-2"); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemCompleted(ctx, taskID, "slice", core.WorkItemSucceeded, "worker-2", ""); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := workItemByID(snapshot, "slice")
	if !ok || item.Status != core.WorkItemFailed || item.WorkerID != "worker-1" {
		t.Fatalf("work item = %+v ok=%v, want original failed terminal state", item, ok)
	}
	if got := countEvents(snapshot.Events, core.EventWorkItemStarted, taskID); got != 1 {
		t.Fatalf("work_item.started events = %d, want 1", got)
	}
	if got := countEvents(snapshot.Events, core.EventWorkItemCompleted, taskID); got != 1 {
		t.Fatalf("work_item.completed events = %d, want 1", got)
	}
}

func TestProjectHealthCatchesGitHubGraphQLBadCredentials(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	fakeBin := t.TempDir()
	ghPath := filepath.Join(fakeBin, "gh")
	script := `#!/bin/sh
if [ "$1" = "auth" ] && [ "$2" = "status" ]; then
  echo "github.com"
  exit 0
fi
cat >&2 <<'JSON'
{"message":"Bad credentials","documentation_url":"https://docs.github.com/rest","status":"401"}
JSON
exit 1
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	projectDir := t.TempDir()
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "do it")}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := service.CreateProject(ctx, core.Project{
		ID:          "repo",
		LocalPath:   projectDir,
		Repo:        "fork-owner/repo",
		DefaultBase: "main",
	}); err != nil {
		t.Fatal(err)
	}

	health, err := service.ProjectHealth(ctx, "repo")
	if err != nil {
		t.Fatal(err)
	}
	if health.OK {
		t.Fatalf("health OK = true, want false: %+v", health)
	}
	if health.GitHubStatus != "auth_bad_credentials" {
		t.Fatalf("github status = %q, want auth_bad_credentials; errors=%v", health.GitHubStatus, health.Errors)
	}
	if !strings.Contains(strings.Join(health.Errors, "\n"), "GitHub credentials rejected (401 Bad credentials)") {
		t.Fatalf("health errors = %v", health.Errors)
	}
}

func TestServicePassesReasoningEffortToWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &recordingRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind:      "codex",
		Prompt:          "worker prompt",
		ReasoningEffort: "low",
	}}, map[string]worker.Runner{"codex": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Cheap worker",
		Prompt: "Use a cheap effort level.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.reasoningEffort != "low" {
		t.Fatalf("reasoning effort = %q, want low", runner.reasoningEffort)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "reasoningEffort", "low") {
		t.Fatalf("missing reasoning effort metadata in worker.created")
	}
}

func TestApplyRemotePatchConflictDoesNotDirtySource(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := runTestGit(t, repo, "diff", "--binary", "HEAD", "--", "file.txt")
	runTestGit(t, repo, "checkout", "--", "file.txt")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "source")

	_, err := applyRemotePatch(ctx, core.Project{LocalPath: repo}, PreparedWorkspace{WorkerID: "remote-worker"}, WorkspaceChanges{
		Diff:  patch,
		Dirty: true,
		ChangedFiles: []WorkspaceChangedFile{{
			Path:   "file.txt",
			Status: "modified",
		}},
	})
	if err == nil {
		t.Fatal("applyRemotePatch succeeded; want conflict")
	}
	status := strings.TrimSpace(runTestGit(t, repo, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("source status = %q, want clean after failed remote apply", status)
	}
	contents, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "source\n" {
		t.Fatalf("source file contents = %q, want committed source contents", contents)
	}
}

func TestBaseWorkspaceSpecUsesGitBaseWorkerRecordedBaseChange(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	service := NewServiceWithWorkspaceManager(store, StaticBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   "task",
		WorkerID: "base-worker",
		Payload: core.MustJSON(PreparedWorkspace{
			CWD:        "/tmp/base-worker",
			VCSType:    "git",
			BaseChange: "base-worker-start",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	spec, err := service.baseWorkspaceSpec(ctx, WorkspaceSpec{
		TaskID:       "task",
		WorkerID:     "followup-worker",
		BaseRevision: "current-project-head",
	}, "base-worker")
	if err != nil {
		t.Fatal(err)
	}
	if spec.BaseWorkDir != "/tmp/base-worker" {
		t.Fatalf("base workdir = %q", spec.BaseWorkDir)
	}
	if spec.BaseRevision != "base-worker-start" {
		t.Fatalf("base revision = %q, want recorded base change", spec.BaseRevision)
	}
}

func TestServiceDedupesExternalSourceTasks(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	req := core.CreateTaskRequest{
		Title:      "GitHub issue owner/repo#123",
		Prompt:     "Fix the issue.",
		Source:     "github",
		ExternalID: "owner/repo#123",
		Metadata:   core.MustJSON(map[string]any{"repo": "owner/repo", "issue": 123}),
	}
	first, err := service.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate task id = %s, want %s", second.ID, first.ID)
	}
	snapshot := waitForTaskStatus(t, store, first.ID, core.TaskSucceeded)
	if countEvents(snapshot.Events, core.EventTaskCreated, first.ID) != 1 {
		t.Fatalf("task.created count = %d, want 1", countEvents(snapshot.Events, core.EventTaskCreated, first.ID))
	}
	found, ok, err := service.FindTaskByExternalID(ctx, "github", "owner/repo#123")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found.ID != first.ID {
		t.Fatalf("lookup = %+v ok=%v", found, ok)
	}
}

func TestServiceAssistantRecordsQuestionAndAnswer(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: testWorkItemPlan("mock", "unused")},
		answer:     "Use a worker task for code changes.",
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	response, err := service.Ask(ctx, core.AssistantRequest{Message: "Can you open PRs?"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != brain.answer {
		t.Fatalf("answer = %q", response.Message)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventAssistantAsked, "") != 1 {
		t.Fatalf("assistant.asked count = %d, want 1", countEvents(snapshot.Events, core.EventAssistantAsked, ""))
	}
	if countEvents(snapshot.Events, core.EventAssistantAnswered, "") != 1 {
		t.Fatalf("assistant.answered count = %d, want 1", countEvents(snapshot.Events, core.EventAssistantAnswered, ""))
	}
}

func TestServiceAnswerQuestionTargetsSpecificQuestion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(
		store,
		fixedBrain{plan: Plan{WorkItems: []WorkItemRequest{{
			ID:         "resume",
			Kind:       "objective.implement",
			WorkerKind: "mock",
			Prompt:     "continue from user answer",
		}}}},
		map[string]worker.Runner{"mock": eventRunner{kind: "mock"}},
		t.TempDir(),
		fakeWorkspaceManager{cwd: t.TempDir()},
	)
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-question-answer",
		Payload: core.MustJSON(map[string]any{
			"title":  "Question answer",
			"prompt": "Ask before continuing.",
		}),
	})
	if err := service.waitForUserAction(ctx, "task-question-answer", "worker-1", "missing_input", "Which token should be used?", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Questions) != 1 {
		t.Fatalf("questions = %+v, want one pending question", snapshot.Questions)
	}
	questionID := snapshot.Questions[0].ID
	if err := service.AnswerQuestion(ctx, "task-question-answer", questionID, core.AnswerQuestionRequest{Answer: "Use the session token."}); err != nil {
		t.Fatal(err)
	}
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		for _, question := range snapshot.Questions {
			if question.ID == questionID && question.Decided && question.Answer == "Use the session token." {
				return true
			}
		}
		return false
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("question was not answered; questions = %+v", snapshot.Questions)
	})
	var originalQuestionItem core.WorkItem
	var answeredItem core.WorkItem
	for _, item := range snapshot.WorkItems {
		if item.Kind == "user.question" {
			originalQuestionItem = item
		}
		if item.Kind == "user.question_answered" && item.TargetKind == "question" && item.TargetID == questionID {
			answeredItem = item
		}
	}
	if originalQuestionItem.Status != core.WorkItemSucceeded {
		t.Fatalf("user.question work item = %+v, want succeeded", originalQuestionItem)
	}
	if answeredItem.Status != core.WorkItemSucceeded {
		t.Fatalf("user.question_answered work item = %+v, want succeeded", answeredItem)
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalDecided, "task-question-answer", `"questionId":"`+questionID+`"`) {
		t.Fatalf("missing questionId in approval.decided event")
	}
}

func TestServiceResumesAssistantProviderSession(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	assistant := &recordingAssistant{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "unused")}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetAssistant(assistant)

	first, err := service.Ask(ctx, core.AssistantRequest{ConversationID: "c1", Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ProviderSessionID != "session-1" {
		t.Fatalf("first session = %q", first.ProviderSessionID)
	}
	second, err := service.Ask(ctx, core.AssistantRequest{ConversationID: "c1", Message: "again"})
	if err != nil {
		t.Fatal(err)
	}
	if second.ProviderSessionID != "session-1" {
		t.Fatalf("second session = %q", second.ProviderSessionID)
	}
	if len(assistant.requests) != 2 || assistant.requests[1].ProviderSessionID != "session-1" {
		t.Fatalf("assistant requests = %+v", assistant.requests)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventAssistantAnswered, "") != 2 {
		t.Fatalf("assistant.answered count = %d, want 2", countEvents(snapshot.Events, core.EventAssistantAnswered, ""))
	}
}

func TestServiceGeneratesMissingTaskTitle(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetTitleGenerator(fakeTitleGenerator{title: "Generated Parser Title"})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Prompt: "implement parser retries when the upstream endpoint times out",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.Title != "Generated Parser Title" {
		t.Fatalf("title = %q", task.Title)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].Title != "Generated Parser Title" {
		t.Fatalf("snapshot title = %q", snapshot.Tasks[0].Title)
	}
	var metadata map[string]any
	if err := json.Unmarshal(snapshot.Tasks[0].Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["titleGenerated"] != true {
		t.Fatalf("metadata = %+v", metadata)
	}
}

func TestServiceFallsBackWhenTitleGeneratorFails(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetTitleGenerator(fakeTitleGenerator{err: errors.New("model unavailable")})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Prompt: "implement parser retries when upstream endpoint times out",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.Title != "implement parser retries when upstream endpoint" {
		t.Fatalf("title = %q", task.Title)
	}
}

func TestNormalizeCreateTaskRequestDoesNotInferCompletionMode(t *testing.T) {
	tests := []struct {
		name     string
		prompt   string
		metadata map[string]any
	}{
		{name: "default work prompt", prompt: "Fix the bug and open a PR"},
		{name: "explicit no pr", prompt: "Fix the bug, no PR needed"},
		{name: "broad objective", prompt: "Find multiple improvements", metadata: map[string]any{"objectiveMode": "broad"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := core.CreateTaskRequest{Prompt: test.prompt}
			if test.metadata != nil {
				req.Metadata = core.MustJSON(test.metadata)
			}
			normalized, err := NormalizeCreateTaskRequest(req)
			if err != nil {
				t.Fatal(err)
			}
			metadata := map[string]any{}
			if err := json.Unmarshal(normalized.Metadata, &metadata); err != nil {
				t.Fatal(err)
			}
			if _, ok := metadata["completionMode"]; ok {
				t.Fatalf("metadata = %+v, want no completionMode", metadata)
			}
			if _, ok := metadata["completionModeInferred"]; ok {
				t.Fatalf("metadata = %+v, want no inferred completion mode", metadata)
			}
		})
	}
}

func TestServiceLocalNoChangeTaskWaitsWhenReplannerUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "mock",
			Prompt:     "inspect the requested PR mention",
		},
		err: errors.New("codex replan command failed: exec: \"codex\": executable file not found in $PATH"),
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed PR and found no code changes needed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Review PR mention",
		Prompt: "Review the PR mention and leave a comment if needed.",
		Metadata: core.MustJSON(map[string]any{
			"source": "github-mention",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if !hasEvent(snapshot.Events, core.EventTaskReplanned, task.ID, "") {
		t.Fatalf("missing fallback replan event")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval-needed event after replanner failure")
	}
}

func TestServicePublishesPullRequestAfterApplyingSingleWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	applyCalls := 0
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: testWorkItemPlan("change", "make change")},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
		applyCalls: &applyCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)

	pr, err := service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{
		Repo: "owner/repo",
		Base: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if applyCalls != 0 {
		t.Fatalf("apply calls = %d, want 0", applyCalls)
	}
	if pr.URL == "" || pr.Repo != "owner/repo" {
		t.Fatalf("pr = %+v", pr)
	}
	if publisher.published.WorkerID == "" {
		t.Fatalf("publisher worker id was empty")
	}

	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	if hasEvent(snapshot.Events, core.EventWorkerApplied, task.ID, publisher.published.WorkerID) {
		t.Fatalf("worker was applied during PR publish")
	}
	if !hasEvent(snapshot.Events, core.EventPRPublished, task.ID, "") {
		t.Fatalf("missing pr published event")
	}
}

func TestPullRequestBodyWithIssueClosingReferenceDoesNotDuplicateExistingClosingKeyword(t *testing.T) {
	task := core.Task{
		Metadata: core.MustJSON(map[string]any{
			"source":     "github-issue",
			"externalId": "owner/repo#12",
		}),
	}
	tests := []struct {
		name        string
		body        string
		publishRepo string
		want        string
	}{
		{
			name:        "same repo shorthand fix",
			body:        "## Summary\n- Fixed it.\n\nFixes #12",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed it.\n\nFixes #12",
		},
		{
			name:        "qualified closes",
			body:        "## Summary\n- Fixed it.\n\nCloses owner/repo#12",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed it.\n\nCloses owner/repo#12",
		},
		{
			name:        "issue url resolves",
			body:        "## Summary\n- Fixed it.\n\nResolves https://github.com/owner/repo/issues/12",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed it.\n\nResolves https://github.com/owner/repo/issues/12",
		},
		{
			name:        "plain issue mention is not enough",
			body:        "## Summary\n- See #12 for context.",
			publishRepo: "owner/repo",
			want:        "## Summary\n- See #12 for context.\n\nCloses owner/repo#12",
		},
		{
			name:        "different issue number is not enough",
			body:        "## Summary\n- Fixed another issue.\n\nFixes owner/repo#120",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed another issue.\n\nFixes owner/repo#120\n\nCloses owner/repo#12",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pullRequestBodyWithIssueClosingReference(tc.body, task, tc.publishRepo)
			if got != tc.want {
				t.Fatalf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestServiceCanceledFollowUpDoesNotPublishPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 1)
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "review",
			Kind:       "objective.validate",
			Reason:     "Review before publishing.",
			Prompt:     "Review before publishing.",
			TargetKind: "objective",
			WorkerKind: "review",
			Metadata: map[string]any{
				"role": "reviewer",
			},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"review": &blockingEventRunner{kind: "review", started: started},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Implement feature",
		Prompt: "Do it.",
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reviewWorkerID := ""
	for _, candidate := range snapshot.Workers {
		if candidate.TaskID == task.ID && candidate.Kind == "review" && candidate.Status == core.WorkerRunning {
			reviewWorkerID = candidate.ID
			break
		}
	}
	if reviewWorkerID == "" {
		t.Fatalf("missing running review worker: %+v", snapshot.Workers)
	}
	if err := service.CancelWorker(ctx, reviewWorkerID); err != nil {
		t.Fatal(err)
	}
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskCanceled)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
}

func TestValidatesBlockedCandidateRequiresLineage(t *testing.T) {
	results := []WorkerTurnResult{
		{
			WorkerID: "blocked-impl",
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
			},
		},
		{
			WorkerID: "unrelated-validation",
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
		{
			WorkerID:     "related-validation",
			BaseWorkerID: "blocked-impl",
			Status:       core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
	}

	if validatesBlockedCandidate(results, "unrelated-validation", "blocked-impl") {
		t.Fatalf("unrelated no-change worker validated blocked candidate without lineage")
	}
	if !validatesBlockedCandidate(results, "related-validation", "blocked-impl") {
		t.Fatalf("related no-change worker did not validate blocked candidate through BaseWorkerID lineage")
	}
}

func TestServicePlanActionPublishesIntermediatePullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "open a PR so review can happen while the objective continues",
				Inputs: map[string]any{
					"repo": "owner/repo",
					"base": "main",
					"body": "## Summary\n- Implement feature.\n\n## Validation\n- Worker completed successfully.",
				},
			}},
		}},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it, open a PR, and babysit it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForEvent(t, store, core.EventTaskArtifact, task.ID)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveWaitingExternal {
		t.Fatalf("task status = %q objective = %q", task.Status, task.ObjectiveStatus)
	}
	if !hasEvent(snapshot.Events, core.EventTaskAction, task.ID, "") {
		t.Fatalf("missing task action event")
	}
	if publisher.published.WorkerID == "" || publisher.published.WorkDir != taskWorkspaceCWD(snapshot, task.ID) {
		t.Fatalf("published from wrong worker workspace: %+v", publisher.published)
	}
	if !strings.Contains(publisher.published.Body, "Implement feature.") {
		t.Fatalf("publish action body was not forwarded: %+v", publisher.published)
	}
}

func TestServicePlanActionPublishesCandidateWithPublishDiffOnly(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: Plan{
			WorkerKind: "change",
			Prompt:     "make remote change",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "publish the remote cumulative patch",
				Inputs: map[string]any{
					"repo":  "owner/repo",
					"base":  "main",
					"title": "perf(node): optimize ServerResponse.end()",
					"body":  "## Summary\n- Optimize ServerResponse.end().\n\n## Validation\n- Release-lite checks passed.",
				},
			}},
		}},
		changes: WorkspaceChanges{
			PublishDiff: "diff --git a/ext/node/polyfills/_http_outgoing.ts b/ext/node/polyfills/_http_outgoing.ts\n",
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote publish diff", Prompt: "Publish the remote cumulative patch."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "started") {
		t.Fatalf("missing started publish action")
	}
	if hasEventPayloadValue(snapshot.Events, core.EventTaskStatus, task.ID, "status", string(core.TaskFailed)) {
		t.Fatalf("task failed despite publishDiff candidate")
	}
}

func TestServicePlanActionPublishesLogicalWorkerID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: Plan{
			Actions: []PlanAction{{
				Kind:     "publish_pull_request",
				When:     "after_success",
				Reason:   "publish the implementation worker after it succeeds",
				WorkerID: "implement_adaptive_decomposition",
				Inputs: map[string]any{
					"repo": "owner/repo",
					"base": "main",
					"body": "## Summary\n- Implement adaptive decomposition.\n\n## Validation\n- Focused tests passed.",
				},
			}},
			WorkItems: []WorkItemRequest{{
				ID:         "implement_adaptive_decomposition",
				Kind:       "objective.implement",
				TargetKind: "objective",
				Reason:     "make the change",
				WorkerKind: "change",
				Prompt:     "make change",
				Metadata: map[string]any{
					"role": "implementer",
				},
			}},
		}},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Adaptive decomposition", Prompt: "Do it and open a PR."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	if publisher.published.WorkerID == "" || publisher.published.WorkerID == "implement_adaptive_decomposition" {
		t.Fatalf("published worker id = %q, want resolved runtime worker id", publisher.published.WorkerID)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "started") {
		t.Fatalf("missing started publish action")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskAction, task.ID, `"workerId":"`+publisher.published.WorkerID+`"`) {
		t.Fatalf("publish action did not record resolved runtime worker id")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskStatus, task.ID, "selected unknown worker") {
		t.Fatalf("task failed with unknown worker: %+v", snapshot.Events)
	}
}

func TestServiceExplicitPublishActionRunsBeforeFollowUpSpawns(t *testing.T) {
	t.Skip("legacy inline worker graph ordering was removed by the durable work item scheduler")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: Plan{
			Workers: []WorkerRequest{{
				ID:         "implement_optimization",
				Role:       "implementer",
				Reason:     "prepare the first optimization",
				WorkerKind: "change",
				Prompt:     "make change",
			}},
			Spawns: []SpawnRequest{{
				ID:         "next_optimization_planner",
				Role:       "planner",
				Reason:     "after the first optimization is published, choose the next PR-sized optimization",
				WorkerKind: "planner",
			}},
			Actions: []PlanAction{{
				Kind:     "publish_pull_request",
				When:     "after_success",
				Reason:   "publish the first optimization before planning the next one",
				WorkerID: "implement_optimization",
				Inputs: map[string]any{
					"repo":                 "owner/repo",
					"title":                "perf(node): optimize ServerResponse.end()",
					"body":                 "## Summary\n- Optimize ServerResponse.end().\n\n## Validation\n- Focused tests passed.",
					"continueAfterPublish": true,
				},
			}},
		}},
		runners: map[string]worker.Runner{
			"change":  eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented first optimization"}}},
			"planner": eventRunner{kind: "planner", events: []worker.Event{{Kind: worker.EventResult, Text: "planned next optimization"}}},
		},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Perf research", Prompt: "Publish each successful optimization and continue."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "spawnID", "next_optimization_planner")
	}, func(snapshot core.Snapshot) string {
		return "missing next optimization planner"
	})
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	publishedAt := firstEventID(snapshot.Events, core.EventPRPublished, task.ID, "")
	plannerCreatedAt := firstEventIDWithPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "spawnID", "next_optimization_planner")
	if publishedAt == 0 || plannerCreatedAt == 0 {
		t.Fatalf("missing event ordering evidence: published=%d planner=%d", publishedAt, plannerCreatedAt)
	}
	if publishedAt > plannerCreatedAt {
		t.Fatalf("follow-up planner was spawned before publish action: publish event %d, planner event %d", publishedAt, plannerCreatedAt)
	}
}

func TestServicePlanActionPublishWithoutCandidateWaitsForUser(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "inspect",
		Prompt:     "inspect the workspace",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish a fix if the worker changed code",
			Inputs: map[string]any{
				"repo":  "denoland/deno",
				"base":  "main",
				"title": "fix(dx): preserve skill dotfiles",
				"body":  "Fixes denoland/deno#33922.",
			},
		}},
	}}, map[string]worker.Runner{
		"inspect": eventRunner{kind: "inspect", events: []worker.Event{{Kind: worker.EventResult, Text: "The execution workspace is nathanwhit/aged, not denoland/deno. No Deno sources are present."}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix Deno Issue 33922",
		Prompt: "reproduce and fix denoland/deno#33922",
		Metadata: core.MustJSON(map[string]any{
			"projectId": "default",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveWaitingUser || task.ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q/%q, want waiting user approval", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want none without candidate", publisher.publishCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "waiting") {
		t.Fatalf("missing waiting publish_pull_request action")
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, task.ID, "missing_publish_candidate") {
		t.Fatalf("missing actionable approval-needed event")
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, task.ID, "not denoland/deno") {
		t.Fatalf("approval-needed event did not include worker blocker summary")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskStatus, task.ID, `"status":"failed"`) {
		t.Fatalf("task failed instead of waiting for user action")
	}
}

func TestServicePlanActionAdoptsWorkerCreatedPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		Repo:         "owner/repo",
		Number:       61,
		URL:          "https://github.com/owner/repo/pull/61",
		Branch:       "codex/ssh-checkout-root-health",
		Base:         "main",
		Title:        "Avoid SSH targets with invalid checkout roots",
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "open a PR so review can happen while the objective continues",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Adopt the worker-created pull request."},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{
			worker.LogEvent("stdout", "https://github.com/owner/repo/pull/61"),
			{Kind: worker.EventResult, Text: "implemented and opened https://github.com/owner/repo/pull/61"},
		}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/ssh_target.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Avoid invalid SSH roots", Prompt: "Do it and open a PR."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "")
	}, func(snapshot core.Snapshot) string {
		return "missing completed publish action"
	})
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want worker-created PR to be adopted", publisher.publishCalls)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	pr := snapshot.PullRequests[0]
	if pr.ID != "github:owner/repo#61" || pr.Number != 61 || pr.Branch != "codex/ssh-checkout-root-health" {
		t.Fatalf("adopted pr = %+v", pr)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "") {
		t.Fatalf("missing completed publish action")
	}
	if len(pr.Metadata) == 0 || !strings.Contains(string(pr.Metadata), `"workerCreated":true`) {
		t.Fatalf("pr metadata = %s", pr.Metadata)
	}
}

func TestServiceRetriesExplicitPublishPullRequestActionAfterRecoverableSigningFailure(t *testing.T) {
	t.Skip("legacy publish-action retry path was removed with final-candidate publication recovery")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		errOnce: errors.New(strings.Join([]string{
			"push jj bookmark: exit status 255",
			"sign_and_send_pubkey: signing failed for ED25519",
			"failed to fill whole buffer",
		}, "\n")),
	}
	brain := &sequenceBrain{plans: []Plan{{
		WorkerKind: "change",
		Prompt:     "make change",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "open a PR so review can happen while the objective continues",
			Inputs: map[string]any{
				"repo":   "owner/repo",
				"base":   "release",
				"branch": "aged/retry-explicit-publish",
				"draft":  true,
				"title":  "Retry explicit publish",
				"body":   "Retry explicit publish.",
			},
		}},
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it, open a PR, and babysit it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	snapshot = waitForEventCount(t, store, core.EventTaskAction, task.ID, 2)
	if publisher.publishCalls != 1 {
		t.Fatalf("initial publish calls = %d, want 1", publisher.publishCalls)
	}
	originalSpec := publisher.published
	if originalSpec.WorkerID == "" {
		t.Fatalf("initial publish did not retain worker id: %+v", originalSpec)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "waiting") {
		t.Fatalf("missing waiting publish_pull_request action")
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v, want none before retry", snapshot.PullRequests)
	}

	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "SSH signing agent is fixed; retry publication."}); err != nil {
		t.Fatal(err)
	}
	snapshot = waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "")
	}, func(snapshot core.Snapshot) string {
		return "missing completed publish_pull_request action"
	})
	if publisher.publishCalls != 2 {
		t.Fatalf("publish calls = %d, want retry publish", publisher.publishCalls)
	}
	retrySpec := publisher.published
	if retrySpec.WorkerID != originalSpec.WorkerID {
		t.Fatalf("retried worker = %q, want retained action candidate %q", retrySpec.WorkerID, originalSpec.WorkerID)
	}
	if retrySpec.Repo != originalSpec.Repo || retrySpec.Base != originalSpec.Base || retrySpec.Branch != originalSpec.Branch || retrySpec.Title != originalSpec.Title || retrySpec.Draft != originalSpec.Draft {
		t.Fatalf("retried publish did not preserve action inputs:\ninitial=%+v\nretry=%+v", originalSpec, retrySpec)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 1 {
		t.Fatalf("feedback reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 1 {
		t.Fatalf("feedback replanned task; task.planned count = %d", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if !hasEvent(snapshot.Events, core.EventApprovalDecided, task.ID, "") {
		t.Fatalf("missing approval.decided event")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "") {
		t.Fatalf("missing completed publish_pull_request action")
	}
}

func TestServiceRetriesFailedPublishPullRequestAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-failed-publish-action"
	publishWorkerID := "worker-publishable"
	actionInputs := map[string]any{
		"repo":  "owner/repo",
		"base":  "main",
		"title": "perf(node): optimize ServerResponse.end()",
		"body":  "## Summary\n- Optimize ServerResponse.end().\n\n## Validation\n- Release-lite checks passed.",
	}
	events := []core.Event{{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Optimize node:http throughput",
			"prompt": "Find and publish useful optimization PRs.",
		}),
	}, {
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "change", Prompt: "make change"}),
	}, {
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: publishWorkerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "change",
			"metadata": map[string]any{"nodeID": "node-publishable"},
		}),
	}, {
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: publishWorkerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:       "/remote/run",
			CWD:        "/remote/repo",
			SourceRoot: "/remote/repo",
			VCSType:    "ssh",
			TaskID:     taskID,
			WorkerID:   publishWorkerID,
		}),
	}, {
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: publishWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented node:http fast path",
			"workspaceChanges": WorkspaceChanges{
				PublishDiff: "diff --git a/ext/node/polyfills/_http_outgoing.ts b/ext/node/polyfills/_http_outgoing.ts\n",
			},
		}),
	}, {
		Type:   core.EventTaskAction,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "publish_pull_request",
			"when":     "after_success",
			"reason":   "publish the useful intermediate candidate",
			"inputs":   actionInputs,
			"workerId": publishWorkerID,
			"status":   "started",
		}),
	}, {
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
			"error":  errPullRequestWorkerNotPublishable.Error(),
		}),
	}}
	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:   fixedBrain{plan: testWorkItemPlan("change", "unused")},
		changes: WorkspaceChanges{PublishDiff: "diff --git a/ext/node/polyfills/_http_outgoing.ts b/ext/node/polyfills/_http_outgoing.ts\n"},
	})
	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, taskID, 1)
	snapshot = waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want retry publish only", publisher.publishCalls)
	}
	if publisher.published.WorkerID != publishWorkerID {
		t.Fatalf("published worker = %q, want failed action worker %q", publisher.published.WorkerID, publishWorkerID)
	}
	if publisher.published.Title != "perf(node): optimize ServerResponse.end" {
		t.Fatalf("published title = %q", publisher.published.Title)
	}
	if !hasTaskAction(snapshot.Events, taskID, "publish_pull_request", "") {
		t.Fatalf("missing completed publish_pull_request action")
	}
}

func TestServiceContinueAfterPublishStartsIndependentWorkItemFromSource(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			Rationale: "ship the first slice",
			WorkItems: []WorkItemRequest{{
				ID:         "first-slice",
				Kind:       "objective.implement",
				Reason:     "implement the first independent slice",
				Prompt:     "make the first change",
				TargetKind: "objective",
				WorkerKind: "change",
			}},
			Actions: []PlanAction{{
				Kind:     "publish_pull_request",
				When:     "after_success",
				Reason:   "publish the first slice and continue the broader objective",
				WorkerID: "first-slice",
				Inputs: map[string]any{
					"repo":                 "owner/repo",
					"title":                "Implement first slice",
					"body":                 "## Summary\n- Implement the first slice.\n\n## Validation\n- Worker completed.",
					"continueAfterPublish": true,
				},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "start the next independent slice",
			Plan: &Plan{
				Rationale: "plan the next slice independently",
				WorkItems: []WorkItemRequest{{
					ID:         "next-slice",
					Kind:       "objective.implement",
					Reason:     "implement the next independent slice",
					Prompt:     "make the next change from the current source checkout",
					TargetKind: "objective",
					WorkerKind: "change",
				}},
			},
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "web/src/main.tsx", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Broad UI objective",
		Prompt: "Publish independent UI slices and keep going.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return eventPayloadContains(snapshot.Events, core.EventWorkerCreated, task.ID, "next-slice")
	}, func(snapshot core.Snapshot) string {
		return "missing next independent slice worker:\n" + taskEventSummary(snapshot.Events, task.ID)
	})
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want first intermediate PR only", publisher.publishCalls)
	}
	for _, event := range snapshot.Events {
		if event.Type != core.EventWorkerCreated || event.TaskID != task.ID || !strings.Contains(string(event.Payload), "next-slice") {
			continue
		}
		if strings.Contains(string(event.Payload), `"baseWorkerID"`) || strings.Contains(string(event.Payload), `"baseWorkspaceCWD"`) {
			t.Fatalf("next independent slice inherited previous worker workspace: %s", event.Payload)
		}
		return
	}
	t.Fatal("missing next independent slice worker.created event")
}

func TestServicePlanActionCanPublishPullRequestAndContinue(t *testing.T) {
	t.Skip("legacy publish-and-continue assertions were replaced by explicit durable work item actions")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "find the first optimization",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "ship the first optimization and keep researching",
				Inputs: map[string]any{"repo": "owner/repo", "continueAfterPublish": true, "body": "Ship the first optimization."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "continue looking for the next optimization",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "find the second optimization",
				Metadata:   map[string]any{"baseWorkerID": "source"},
				Actions: []PlanAction{{
					Kind:   "publish_pull_request",
					When:   "after_success",
					Reason: "ship the second optimization and keep the task owning both PRs",
					Inputs: map[string]any{"repo": "owner/repo", "body": "Ship the second optimization."},
				}},
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "optimized"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Perf research",
		Prompt: "Keep producing optimization PRs.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 2)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	snapshot = waitForEventCount(t, store, core.EventTaskAction, task.ID, 4)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveWaitingExternal || task.ObjectivePhase != "pr_opened" {
		t.Fatalf("objective = %q phase %q", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if len(publisher.publishedSpecs) != 2 {
		t.Fatalf("published specs = %d, want 2", len(publisher.publishedSpecs))
	}
	if !boolMetadata(publisher.publishedSpecs[0].Metadata, "continueAfterPublish") {
		t.Fatalf("first publish spec continueAfterPublish = false, want intermediate PR metadata")
	}
	if boolMetadata(publisher.publishedSpecs[1].Metadata, "continueAfterPublish") {
		t.Fatalf("second publish spec continueAfterPublish = true, want terminal PR metadata")
	}
	if publisher.publishedSpecs[0].Title == task.Title {
		t.Fatalf("intermediate PR title fell back to broad task title %q", task.Title)
	}
	if countTaskActionEventsExcludingKind(snapshot.Events, task.ID, "worker_result_digest") != 4 {
		t.Fatalf("task action events = %d, want 4", countTaskActionEventsExcludingKind(snapshot.Events, task.ID, "worker_result_digest"))
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != 1 {
		t.Fatalf("task.replanned events = %d, want 1", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID))
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "baseWorkerID", "source") {
		t.Fatalf("missing source-base worker metadata")
	}
}

func TestValidatePullRequestPublicationRequestRejectsImplicitBroadCompletionPR(t *testing.T) {
	task := core.Task{
		ID:    "task-broad",
		Title: "Trim Heavy Deno Dependencies",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{}); err == nil || !strings.Contains(err.Error(), "explicit title") {
		t.Fatalf("missing title error = %v", err)
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{Title: "Replace open dependency"}); err == nil || !strings.Contains(err.Error(), "explicit body") {
		t.Fatalf("missing body error = %v", err)
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{
		Title: "Replace open dependency",
		Body:  "## Summary\n- Replace open.\n\n## Validation\n- go test ./...",
	}); err != nil {
		t.Fatalf("explicit broad completion publish rejected: %v", err)
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{ContinueAfterPublish: true}); err == nil || !strings.Contains(err.Error(), "explicit title") {
		t.Fatalf("implicit intermediate broad publish error = %v", err)
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{
		ContinueAfterPublish: true,
		Title:                "refactor(fetch): remove tower-http decompression",
		Body:                 "## Summary\n- Remove tower-http decompression from deno_fetch.\n\n## Validation\n- cargo test -p deno_fetch",
	}); err != nil {
		t.Fatalf("explicit broad intermediate publish rejected: %v", err)
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{
		Title: "refactor(fetch): remove tower-http decompression",
		Body:  "## Summary\n- Remove tower-http decompression.\n\n## Validation\n- cargo test -p deno_fetch\n\n## Recommended Next Turns\n- Run broader CI.",
	}); err == nil || !strings.Contains(err.Error(), "worker-report section") {
		t.Fatalf("worker report body error = %v", err)
	}
	if err := validatePullRequestPublicationRequest(task, core.PublishPullRequestRequest{
		Title: "refactor(fetch): remove tower-http decompression",
		Body:  "## Summary\n- Remove tower-http decompression.\n\n## Validation\n- Binary size not measured.",
	}); err == nil || !strings.Contains(err.Error(), "missing validation") {
		t.Fatalf("missing validation body error = %v", err)
	}
}

func TestServiceReplanPublishesReadyCandidateBeforeNextWorkers(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			Rationale: "produce one candidate before replanning",
			WorkItems: []WorkItemRequest{{
				ID:         "candidate",
				Kind:       "objective.slice",
				TargetKind: "objective",
				Reason:     "Produce the first reviewable slice.",
				WorkerKind: "change",
				Prompt:     "implement first slice",
				Metadata: map[string]any{
					"role": "candidate",
				},
			}},
		},
		decisions: []ReplanDecision{
			{
				Action:    "continue",
				Rationale: "publish the first slice and continue",
				Plan: &Plan{
					Rationale: "the candidate is ready to publish before starting the next slice",
					Actions: []PlanAction{{
						Kind:     "publish_pull_request",
						When:     "after_success",
						Reason:   "publish the first slice before continuing",
						WorkerID: "candidate",
						Inputs: map[string]any{
							"repo":                 "owner/repo",
							"title":                "Ship first slice",
							"body":                 "Ship the first slice.",
							"continueAfterPublish": true,
						},
					}},
					WorkItems: []WorkItemRequest{{
						ID:         "next_slice",
						Kind:       "objective.slice",
						TargetKind: "objective",
						Reason:     "Continue with the next slice after publication.",
						WorkerKind: "change",
						Prompt:     "implement next slice",
						Metadata: map[string]any{
							"role": "implementer",
						},
					}},
				},
			},
			{
				Action:  "wait",
				Message: "pause after next slice",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "changed code"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "Cargo.toml", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Broad objective",
		Prompt: "Publish slices as they become ready.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEventCount(t, store, core.EventTaskReplanned, task.ID, 2)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want ready candidate published before next work", publisher.publishCalls)
	}
	publishEventID := firstEventIDWithPayloadValue(snapshot.Events, core.EventTaskAction, task.ID, "kind", "publish_pull_request")
	if publishEventID == 0 {
		t.Fatalf("missing publish action; actions:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	nextWorkerEventID := firstEventIDWithPayloadValue(snapshot.Events, core.EventExecutionPlanned, task.ID, "spawnID", "next_slice")
	if nextWorkerEventID == 0 {
		t.Fatalf("missing next_slice worker; events = %s", taskEventSummary(snapshot.Events, task.ID))
	}
	if publishEventID > nextWorkerEventID {
		t.Fatalf("publish action event %d happened after next worker event %d", publishEventID, nextWorkerEventID)
	}
}

func TestServicePlanWorkItemActionRunsBeforeDependentWorkItem(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		Rationale: "publish a first slice while continuing the objective",
		Actions: []PlanAction{{
			Kind:     "publish_pull_request",
			When:     "after_success",
			Reason:   "publish the first slice before planning the next slice",
			WorkerID: "first_slice",
			Inputs: map[string]any{
				"repo":                 "owner/repo",
				"title":                "Ship first UI slice",
				"body":                 "Ship the first UI slice.",
				"continueAfterPublish": true,
			},
		}},
		WorkItems: []WorkItemRequest{{
			ID:         "first_slice",
			Kind:       "objective.implement",
			TargetKind: "objective",
			Reason:     "Produce a reviewable first slice.",
			WorkerKind: "change",
			Prompt:     "implement first slice",
		}, {
			ID:              "next_slice_plan",
			Kind:            "objective.compose",
			TargetKind:      "objective",
			Reason:          "Plan the next slice after the first slice is published.",
			WorkerKind:      "change",
			Prompt:          "plan next slice",
			DependsOn:       []string{"first_slice"},
			ReasoningEffort: "medium",
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "changed code"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "web/src/main.tsx", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Broad UI objective",
		Prompt: "Publish the first slice and continue.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEventCount(t, store, core.EventExecutionPlanned, task.ID, 2)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want first slice published before dependent work", publisher.publishCalls)
	}
	publishEventID := firstEventIDWithPayloadValue(snapshot.Events, core.EventTaskAction, task.ID, "kind", "publish_pull_request")
	if publishEventID == 0 {
		t.Fatalf("missing publish action; actions:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	nextWorkerEventID := firstEventIDWithPayloadValue(snapshot.Events, core.EventExecutionPlanned, task.ID, "spawnID", "next_slice_plan")
	if nextWorkerEventID == 0 {
		t.Fatalf("missing next_slice_plan worker; events = %s", taskEventSummary(snapshot.Events, task.ID))
	}
	if publishEventID > nextWorkerEventID {
		t.Fatalf("publish action event %d happened after dependent worker event %d", publishEventID, nextWorkerEventID)
	}
}

func TestServiceIntermediatePullRequestKeepsObjectiveRunning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	secondStarted := make(chan string, 1)
	releaseSecond := make(chan struct{})
	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "first",
			Prompt:     "produce first slice",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "ship the first slice and continue the objective",
				Inputs: map[string]any{"repo": "owner/repo", "continueAfterPublish": true, "body": "Ship the first slice."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "keep working after the intermediate PR",
			Plan: &Plan{
				WorkerKind: "second",
				Prompt:     "produce second slice",
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"first":  eventRunner{kind: "first", events: []worker.Event{{Kind: worker.EventResult, Text: "first slice"}}},
		"second": &blockingEventRunner{kind: "second", started: secondStarted, release: releaseSecond, summary: "second slice"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "slice.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Broad objective",
		Prompt: "Produce multiple reviewable slices.",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForPullRequests(t, store, task.ID, 1)
	<-secondStarted
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskRunning || task.ObjectiveStatus != core.ObjectiveActive {
		t.Fatalf("task status = %q objective = %q/%q, want running active", task.Status, task.ObjectiveStatus, task.ObjectivePhase)
	}
	if task.ObjectivePhase != "continuing_after_pr" {
		t.Fatalf("objective phase = %q, want continuing_after_pr", task.ObjectivePhase)
	}
	if !hasEvent(snapshot.Events, core.EventPRBabysitter, task.ID, "") {
		t.Fatalf("missing PR babysitter event")
	}
	if len(publisher.publishedSpecs) != 1 || !boolMetadata(publisher.publishedSpecs[0].Metadata, "continueAfterPublish") {
		t.Fatalf("published specs = %+v, want one intermediate PR", publisher.publishedSpecs)
	}

	close(releaseSecond)
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
}

func TestServiceIntermediatePublishConflictContinuesToReplan(t *testing.T) {
	t.Skip("legacy final-candidate publish conflict replanning was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{errCount: 1}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "find the first optimization",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "ship the first optimization and keep researching",
				Inputs: map[string]any{
					"repo":                 "owner/repo",
					"title":                "perf: reduce binary size",
					"body":                 "## Summary\n- Reduce binary size.\n\n## Validation\n- Focused tests passed.",
					"continueAfterPublish": true,
				},
			}},
		},
		decisions: []ReplanDecision{{
			Action:  "wait",
			Message: "try a different PR-sized candidate",
		}},
	}
	service, _ := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     brain,
		publisher: publisher,
		runners: map[string]worker.Runner{
			"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "optimized"}}},
		},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Perf research", Prompt: "Keep producing optimization PRs."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "failed") {
		t.Fatalf("publish action was recorded as terminal failure")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "continued") {
		t.Fatalf("missing continued publish_pull_request action")
	}
	if len(brain.states) == 0 {
		t.Fatalf("intermediate publish conflict did not re-enter dynamic replanning")
	}
	if len(brain.states[0].Results) == 0 || !strings.Contains(brain.states[0].Results[0].Error, "intermediate publish failed") {
		t.Fatalf("replan state did not include blocked publish candidate: %+v", brain.states[0].Results)
	}
}

func TestServicePublishPullRequestActionSanitizesTrackedPullRequestTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service, _ := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     fixedBrain{},
		publisher: publisher,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "ext/ffi/dlfcn.rs", Status: "modified"}},
			Diff:         "diff --git a/ext/ffi/dlfcn.rs b/ext/ffi/dlfcn.rs\n",
		},
	})
	projectDir := t.TempDir()
	if _, err := service.CreateProject(ctx, core.Project{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   projectDir,
		Repo:        "owner/repo",
		DefaultBase: "main",
	}); err != nil {
		t.Fatal(err)
	}

	task := core.Task{ID: "task-multi-pr", ProjectID: "repo", Title: "Reduce dependency footprint"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"projectId": task.ProjectID,
			"title":     task.Title,
			"prompt":    "Produce multiple independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-existing",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 41,
		URL:    "https://github.com/owner/repo/pull/41",
		Branch: "codex/existing-slice",
		Base:   "main",
		Title:  "refactor(cli): remove zip crate",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish": true,
			"publicationPhase":     "intermediate",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskPlanned,
			TaskID: task.ID,
			Payload: core.MustJSON(Plan{
				WorkerKind: "change",
				Prompt:     "remove serde-value from FFI",
			}),
		},
		{
			Type:     core.EventExecutionPlanned,
			TaskID:   task.ID,
			WorkerID: "worker-serde-value",
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-serde-value",
				"workerId":   "worker-serde-value",
				"workerKind": "change",
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   task.ID,
			WorkerID: "worker-serde-value",
			Payload: core.MustJSON(map[string]any{
				"kind":    "change",
				"command": []string{"change"},
			}),
		},
		{
			Type:     core.EventWorkerWorkspace,
			TaskID:   task.ID,
			WorkerID: "worker-serde-value",
			Payload: core.MustJSON(PreparedWorkspace{
				Root:       "/remote/work/root",
				CWD:        "/remote/work/root/repo",
				SourceRoot: projectDir,
				VCSType:    "ssh",
				TaskID:     task.ID,
				WorkerID:   "worker-serde-value",
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: "worker-serde-value",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "removed serde-value from FFI",
				"workspaceChanges": WorkspaceChanges{
					Root:         "/remote/work/root",
					CWD:          "/remote/work/root/repo",
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "ext/ffi/dlfcn.rs", Status: "modified"}},
					Diff:         "diff --git a/ext/ffi/dlfcn.rs b/ext/ffi/dlfcn.rs\n",
				},
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	results := []WorkerTurnResult{{
		WorkerID: "worker-serde-value",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "ext/ffi/dlfcn.rs", Status: "modified"}},
			Diff:         "diff --git a/ext/ffi/dlfcn.rs b/ext/ffi/dlfcn.rs\n",
		},
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "publish_pull_request",
		When:     "after_success",
		WorkerID: "worker-serde-value",
		Reason:   "publish the next independent dependency cleanup",
		Inputs: map[string]any{
			"id":                   "pr-existing",
			"pullRequestId":        "pr-existing",
			"repo":                 "owner/repo",
			"number":               41,
			"url":                  "https://github.com/owner/repo/pull/41",
			"branch":               "codex/existing-slice",
			"base":                 "main",
			"title":                "refactor(ffi): remove serde-value dependency",
			"body":                 "## Summary\n- Remove serde-value from FFI parsing.\n\n## Validation\n- cargo test -p deno_ffi",
			"continueAfterPublish": true,
		},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("intermediate publish should keep objective planning active")
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	if publisher.published.Branch != "" {
		t.Fatalf("publish reused existing PR branch %q", publisher.published.Branch)
	}
	if publisher.published.Title != "refactor(ffi): remove serde-value dependency" {
		t.Fatalf("publish title = %q", publisher.published.Title)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_target_sanitized", "applied") {
		t.Fatalf("missing publish target sanitization action")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "started") {
		t.Fatalf("missing publish started action")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskAction, task.ID, `"branch":"codex/existing-slice"`) {
		t.Fatalf("sanitized publish action still recorded existing branch target")
	}
}

func TestRecoverablePublishConflictIncludesNonFastForwardPush(t *testing.T) {
	err := errors.New(`push git branch: exit status 1: To https://github.com/nathanwhitbot/deno.git
 ! [rejected]              codex/aged-f48cc51b-f43 -> codex/aged-f48cc51b-f43 (non-fast-forward)
error: failed to push some refs to 'https://github.com/nathanwhitbot/deno.git'`)
	if !isRecoverablePublishConflict(err) {
		t.Fatalf("non-fast-forward push rejection should be recoverable")
	}
}

func TestServicePlanActionDoesNotPublishAfterBlockingReviewFinding(t *testing.T) {
	t.Skip("legacy completion-review gate on implicit publication was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "tighten the signing-agent classifier",
			Spawns: []SpawnRequest{{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review whether the change is ready to publish.",
				WorkerKind: "reviewer",
			}},
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "publish the classifier fix",
				Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish the classifier fix."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:  "wait",
			Message: "review feedback needs an implementation follow-up",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented classifier changes"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: `## Findings
- Medium issue: internal/orchestrator/service.go still misclassifies signing-agent failures.

## Recommended Next Turns
- Tighten the signing-agent classifier before publishing.`}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix signing-agent classification",
		Prompt: "Implement, review, and publish only when ready.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want blocking review finding to suppress publish", publisher.publishCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_blocked_by_follow_up", "rejected") {
		t.Fatalf("missing blocked publication action")
	}
	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	if len(brain.states[0].Results) != 2 {
		t.Fatalf("replan results = %d, want implementation plus review", len(brain.states[0].Results))
	}
}

func TestPublicationBlockerIgnoresFindingsBeforeCurrentCandidate(t *testing.T) {
	results := []WorkerTurnResult{{
		WorkerID: "old-candidate",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "old.ts", Status: "modified"}},
		},
	}, {
		WorkerID: "old-review",
		SpawnID:  "validate_old_candidate",
		Role:     "independent validator",
		Status:   core.WorkerSucceeded,
		Summary: `## Findings

Medium issue: reject this candidate before publishing.

## Recommended Next Turns

Fix it before publishing.`,
	}, {
		WorkerID: "new-candidate",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "new.ts", Status: "modified"}},
		},
	}, {
		WorkerID: "new-review",
		SpawnID:  "validate_new_candidate",
		Role:     "independent validator",
		Status:   core.WorkerSucceeded,
		Summary: `## Findings

No findings.

## Recommended Next Turns

Publish the pull request.`,
	}}

	if blocker, ok := publicationBlockedByFollowUpFinding(results, "new-candidate"); ok {
		t.Fatalf("new candidate blocked by stale finding: %+v", blocker)
	}
	if blocker, ok := publicationBlockedByFollowUpFinding(results, "old-candidate"); !ok || blocker.WorkerID != "old-review" {
		t.Fatalf("old candidate blocker = %+v ok=%v, want old-review", blocker, ok)
	}
}

func TestServicePlanActionSkipsTerminalPullRequestUpdate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(&fakePullRequestPublisher{})

	task := core.Task{ID: "task-closed-pr", Title: "Broad objective"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Keep producing independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-closed",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 42,
		URL:    "https://github.com/owner/repo/pull/42",
		Branch: "codex/old",
		Base:   "main",
		Title:  "Old attempt",
		State:  "CLOSED",
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "worker-new",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
		},
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "worker-new",
		Reason:   "repair the old PR",
		Inputs:   map[string]any{"repo": "owner/repo", "number": 42},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("terminal PR update should be skipped without stopping the plan")
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request", "skipped") {
		t.Fatalf("missing skipped update_pull_request action")
	}
}

func TestServicePlanActionRefreshesPullRequestBeforeUpdatePush(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		ID:     "pr-stale-open",
		TaskID: "task-stale-open-pr",
		Repo:   "owner/repo",
		Number: 42,
		URL:    "https://github.com/owner/repo/pull/42",
		Branch: "codex/old",
		Base:   "main",
		Title:  "Old attempt",
		State:  "MERGED",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "repair.go", Status: "modified"}},
			Diff:         "diff --git a/repair.go b/repair.go\n",
		},
	})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-stale-open-pr", Title: "Broad objective"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Keep producing independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-stale-open",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 42,
		URL:    "https://github.com/owner/repo/pull/42",
		Branch: "codex/old",
		Base:   "main",
		Title:  "Old attempt",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish": true,
			"publicationPhase":     "intermediate",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "worker-new",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "repair.go", Status: "modified"}},
			Diff:         "diff --git a/repair.go b/repair.go\n",
		},
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "worker-new",
		Reason:   "repair the old PR",
		Inputs:   map[string]any{"repo": "owner/repo", "number": 42},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("terminal remote PR update should be skipped without stopping the plan")
	}
	if publisher.inspectCalls != 1 {
		t.Fatalf("inspect calls = %d, want 1", publisher.inspectCalls)
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0", publisher.updateCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.PullRequests[0].State; got != "MERGED" {
		t.Fatalf("pull request state = %q, want MERGED", got)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request", "skipped") {
		t.Fatalf("missing skipped update_pull_request action")
	}
}

func TestServicePlanMetadataOnlyPullRequestUpdateSkipsWorkerChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "later-change.go", Status: "modified"}},
			Diff:         "diff --git a/later-change.go b/later-change.go\n",
		},
	})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-metadata-pr", Title: "Reduce dependency footprint"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Produce multiple independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-open",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/slice",
		Base:   "main",
		Title:  "Generic title",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish":                        true,
			"publicationPhase":                            "intermediate",
			"latestPullRequestFeedbackSignature":          "sig-description",
			"latestPullRequestFeedbackTriggeredSignature": "sig-old",
			"latestPullRequestFeedbackBody":               "improve the description",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "metadata-repair",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "later-change.go", Status: "modified"}},
			Diff:         "diff --git a/later-change.go b/later-change.go\n",
		},
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "metadata-repair",
		Reason:   "fix PR title and body",
		Inputs: map[string]any{
			"repo":         "owner/repo",
			"number":       7,
			"title":        "refactor: remove os_pipe dependency",
			"body":         "## Summary\n- Replace os_pipe.\n\n## Validation\n- Not run.",
			"comment":      "Updated the PR title and description to describe the os_pipe removal.",
			"metadataOnly": true,
		},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("metadata-only PR update should not stop the plan")
	}
	if publisher.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", publisher.updateCalls)
	}
	if !publisher.updated.MetadataOnly {
		t.Fatalf("metadata-only update sent MetadataOnly=false: %+v", publisher.updated)
	}
	if publisher.updated.Patch != "" || publisher.updated.PatchFromBase {
		t.Fatalf("metadata-only update included worker patch: %+v", publisher.updated)
	}
	if publisher.commentCalls != 1 {
		t.Fatalf("comment calls = %d, want 1", publisher.commentCalls)
	}
	if publisher.commentSpec.Body != "Updated the PR title and description to describe the os_pipe removal." {
		t.Fatalf("comment body = %q", publisher.commentSpec.Body)
	}
}

func TestServicePlanPullRequestUpdateWithMetadataPushesWorkerChangesByDefault(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "repair.go", Status: "modified"}},
			Diff:         "diff --git a/repair.go b/repair.go\n",
		},
	})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-pr-code-and-metadata", Title: "Repair PR"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Repair the pull request.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-open",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/slice",
		Base:   "main",
		Title:  "Generic title",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"latestPullRequestFeedbackSignature":          "sig-review",
			"latestPullRequestFeedbackTriggeredSignature": "sig-old",
			"latestPullRequestFeedbackBody":               "please fix the regression in the PR",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []core.Event{
		{
			Type:     core.EventWorkerCreated,
			TaskID:   task.ID,
			WorkerID: "repair-worker",
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		},
		{
			Type:     core.EventWorkerWorkspace,
			TaskID:   task.ID,
			WorkerID: "repair-worker",
			Payload: core.MustJSON(PreparedWorkspace{
				Root:       "/repo",
				CWD:        "/repo",
				SourceRoot: "/repo",
				VCSType:    "jj",
				TaskID:     task.ID,
				WorkerID:   "repair-worker",
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: "repair-worker",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "Add targeted repair for behavior",
				"workspaceChanges": WorkspaceChanges{
					Root:         "/repo",
					CWD:          "/repo",
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "repair.go", Status: "modified"}},
					Diff:         "diff --git a/repair.go b/repair.go\n",
				},
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	results := []WorkerTurnResult{{
		WorkerID: "repair-worker",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "repair.go", Status: "modified"}},
			Diff:         "diff --git a/repair.go b/repair.go\n",
		},
	}}
	_, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "repair-worker",
		Reason:   "fix PR code and title",
		Inputs: map[string]any{
			"repo":    "owner/repo",
			"number":  7,
			"title":   "fix: repair behavior",
			"body":    "## Summary\n- Repair behavior.\n\n## Validation\n- go test.",
			"comment": "Pushed a targeted repair for the regression and kept the PR description current.",
		},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", publisher.updateCalls)
	}
	if publisher.updated.MetadataOnly {
		t.Fatalf("update unexpectedly metadata-only: %+v", publisher.updated)
	}
	if publisher.updated.WorkerID != "repair-worker" {
		t.Fatalf("worker id = %q, want repair-worker", publisher.updated.WorkerID)
	}
	if publisher.updated.CommitMessage != "Add targeted repair for behavior" {
		t.Fatalf("commit message = %q, want worker summary", publisher.updated.CommitMessage)
	}
	if publisher.commentCalls != 1 {
		t.Fatalf("comment calls = %d, want 1", publisher.commentCalls)
	}
	if publisher.commentSpec.Body != "Pushed a targeted repair for the regression and kept the PR description current." {
		t.Fatalf("comment body = %q", publisher.commentSpec.Body)
	}
}

func TestTaskHasActiveObjectiveWorkersIgnoresBackgroundPullRequestFollowUp(t *testing.T) {
	snapshot := core.Snapshot{
		Workers: []core.Worker{{
			ID:     "pr-worker",
			TaskID: "task-1",
			Status: core.WorkerRunning,
		}},
		ExecutionNodes: []core.ExecutionNode{{
			TaskID:   "task-1",
			WorkerID: "pr-worker",
			Status:   core.WorkerRunning,
			Role:     "github_pr_followup",
			Metadata: core.MustJSON(map[string]any{
				"backgroundPullRequestFollowUp": true,
			}),
		}},
	}
	if !taskHasActiveWorkers(snapshot, "task-1") {
		t.Fatal("background follow-up should still count as an active worker for cancellation/status")
	}
	if taskHasActiveObjectiveWorkers(snapshot, "task-1") {
		t.Fatal("background pull request follow-up should not block objective recovery")
	}

	snapshot.Workers = append(snapshot.Workers, core.Worker{
		ID:     "objective-worker",
		TaskID: "task-1",
		Status: core.WorkerRunning,
	})
	snapshot.ExecutionNodes = append(snapshot.ExecutionNodes, core.ExecutionNode{
		TaskID:   "task-1",
		WorkerID: "objective-worker",
		Status:   core.WorkerRunning,
		Role:     "implementer",
	})
	if !taskHasActiveObjectiveWorkers(snapshot, "task-1") {
		t.Fatal("regular running worker should block objective recovery")
	}
}

func TestServicePlanPullRequestUpdateWithMetadataFallsBackWhenWorkerHasNoChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
	})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-pr-metadata-fallback", Title: "Repair PR metadata"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Repair the pull request metadata.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-open",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/slice",
		Base:   "main",
		Title:  "Generic title",
		State:  "OPEN",
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "metadata-worker",
		Status:   core.WorkerSucceeded,
	}}
	_, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "metadata-worker",
		Reason:   "fix PR title",
		Inputs: map[string]any{
			"repo":   "owner/repo",
			"number": 7,
			"title":  "fix: clearer title",
		},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", publisher.updateCalls)
	}
	if !publisher.updated.MetadataOnly {
		t.Fatalf("metadata fallback sent MetadataOnly=false: %+v", publisher.updated)
	}
}

func TestServicePullRequestUpdateFailsWithoutExplicitAfterSuccessWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "other-pr.go", Status: "modified"}},
			Diff:         "diff --git a/other-pr.go b/other-pr.go\n",
		},
	})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-wide-objective", Title: "Optimize broadly"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Produce multiple independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-a",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 41,
		URL:    "https://github.com/owner/repo/pull/41",
		Branch: "codex/pr-a",
		Base:   "main",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish": true,
			"publicationPhase":     "intermediate",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "other-pr-worker",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "other-pr.go", Status: "modified"}},
			Diff:         "diff --git a/other-pr.go b/other-pr.go\n",
		},
	}, {
		WorkerID: "comment-only-follow-up",
		Status:   core.WorkerSucceeded,
		Summary:  "Posted requested benchmark numbers on PR 41.",
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:   "update_pull_request",
		When:   "after_success",
		Reason: "Return PR 41 to monitoring after handling feedback.",
		Inputs: map[string]any{
			"repo":   "owner/repo",
			"number": 41,
			"branch": "codex/pr-a",
		},
	}, results)
	if err == nil {
		t.Fatal("missing explicit update worker should fail required after-success PR update")
	}
	if keepGoing {
		t.Fatal("missing explicit update worker should stop required after-success action")
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0; stale worker %q would have overwritten PR", publisher.updateCalls, publisher.updated.WorkerID)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request", "skipped") {
		t.Fatalf("missing skipped update action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if !strings.Contains(taskActionPayloads(snapshot.Events, task.ID), "requires an explicit workerId") {
		t.Fatalf("missing explicit workerId skip reason; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServiceUpdatePullRequestReadinessRejectsIncoherentPatch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &publicationReviewBrain{
		reviews: []PublicationReview{{
			Ready:  false,
			Reason: "worker patch adds an unrelated dependency slice that belongs in a separate PR",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, nil, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-pr-update-readiness", Title: "Trim dependencies broadly"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Produce several focused dependency cleanup PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-geometry",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 41,
		URL:    "https://github.com/owner/repo/pull/41",
		Title:  "refactor: replace geometry dependency",
		Branch: "codex/geometry",
		Base:   "main",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish": true,
			"publicationPhase":     "intermediate",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "repair-worker",
		Status:   core.WorkerSucceeded,
		Summary:  "Also changed the CLI keyring dependency.",
		Changes: WorkspaceChanges{
			Dirty: true,
			ChangedFiles: []WorkspaceChangedFile{
				{Path: "cli/Cargo.toml", Status: "modified"},
				{Path: "Cargo.lock", Status: "modified"},
			},
			Diff: "diff --git a/cli/Cargo.toml b/cli/Cargo.toml\n",
		},
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "repair-worker",
		Reason:   "Address feedback on PR 41.",
		Inputs: map[string]any{
			"repo":   "owner/repo",
			"number": 41,
			"branch": "codex/geometry",
		},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("rejected intermediate PR update should let objective continue")
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want semantic readiness rejection to prevent push", publisher.updateCalls)
	}
	if brain.reviewCalls != 1 {
		t.Fatalf("review calls = %d, want 1", brain.reviewCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request_readiness_rejected", "rejected") {
		t.Fatalf("missing rejected update readiness action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if !strings.Contains(taskActionPayloads(snapshot.Events, task.ID), "separate PR") {
		t.Fatalf("missing readiness rejection reason; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServiceUpdatePullRequestRejectsTargetMismatchBeforePush(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-pr-target-mismatch", Title: "Maintain focused intermediate PRs"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Keep each intermediate PR on its own branch.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	pr := core.PullRequest{
		ID:     "pr-branch",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Title:  "Focused change",
		Branch: "codex/right",
		Base:   "main",
		State:  "OPEN",
	}

	_, err := service.UpdateTaskPullRequest(ctx, task.ID, pr, core.PublishPullRequestRequest{
		Repo:         "owner/repo",
		Base:         "main",
		Branch:       "codex/wrong",
		Title:        "Retitle",
		Body:         "Body",
		MetadataOnly: true,
	})
	if !errors.Is(err, errPullRequestTargetMismatch) {
		t.Fatalf("error = %v, want target mismatch", err)
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0", publisher.updateCalls)
	}
}

func TestServiceUpdatePullRequestRejectsStaleWorkerBeforePush(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{
		cwd:          t.TempDir(),
		baseRevision: "old-pr-head",
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	taskID := "task-pr-stale-worker"
	workerID := "worker-stale"
	appendTestEvents(t, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Maintain focused intermediate PRs",
				"prompt": "Keep each intermediate PR on its own branch.",
			}),
		},
		core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload:  core.MustJSON(map[string]any{"kind": "mock"}),
		},
		core.Event{
			Type:     core.EventWorkerWorkspace,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(PreparedWorkspace{
				Root:       "/repo",
				CWD:        "/repo",
				SourceRoot: "/repo",
				BaseChange: "old-pr-head",
				VCSType:    "git",
				WorkerID:   workerID,
				TaskID:     taskID,
			}),
		},
		core.Event{
			Type:     core.EventWorkerStarted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload:  core.MustJSON(map[string]any{}),
		},
		core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status": core.WorkerSucceeded,
				"workspaceChanges": WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
				},
			}),
		},
	)
	pr := core.PullRequest{
		ID:       "pr-stale",
		TaskID:   taskID,
		Repo:     "owner/repo",
		Number:   7,
		URL:      "https://github.com/owner/repo/pull/7",
		Title:    "Focused change",
		Branch:   "codex/right",
		Base:     "main",
		State:    "OPEN",
		Metadata: core.MustJSON(map[string]any{"headRefOid": "new-pr-head"}),
	}
	publisher.status = pr

	_, err := service.UpdateTaskPullRequest(ctx, taskID, pr, core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		Branch:   "codex/right",
		Title:    "Retitle",
		Body:     "Body",
		WorkerID: workerID,
	})
	if !errors.Is(err, errPullRequestHeadMismatch) {
		t.Fatalf("error = %v, want PR head mismatch", err)
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0", publisher.updateCalls)
	}
}

func TestServicePlanActionSkipsPullRequestTargetMismatch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-pr-action-target-mismatch", Title: "Maintain focused intermediate PRs"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Keep each intermediate PR on its own branch.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-branch",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Title:  "Focused change",
		Branch: "codex/right",
		Base:   "main",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish": true,
			"publicationPhase":     "intermediate",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:   "update_pull_request",
		When:   "after_success",
		Reason: "Refresh PR metadata without changing its branch.",
		Inputs: map[string]any{
			"id":           "pr-branch",
			"branch":       "codex/wrong",
			"title":        "Retitle",
			"body":         "Body",
			"metadataOnly": true,
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("target mismatch on intermediate PR update should let objective continue")
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0", publisher.updateCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request", "skipped") {
		t.Fatalf("missing skipped update action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if !strings.Contains(taskActionPayloads(snapshot.Events, task.ID), "requested branch") {
		t.Fatalf("missing target mismatch reason; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServicePlanActionFinishObjectiveCompletesWithoutPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	publisher := &fakePullRequestPublisher{}
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-finish-objective", Title: "Broad objective"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Finish the objective after all useful PRs are already handled.",
			"metadata": map[string]any{
				"objectiveMode": "broad",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:   "finish_objective",
		When:   "after_success",
		Reason: "All useful slices have landed or been abandoned.",
		Inputs: map[string]any{
			"summary": "Objective is done; no additional PR should be published.",
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if keepGoing {
		t.Fatal("finish_objective should stop the current plan")
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	found, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if found.ObjectiveStatus != core.ObjectiveSatisfied || found.ObjectivePhase != "satisfied" {
		t.Fatalf("objective = %q/%q, want satisfied", found.ObjectiveStatus, found.ObjectivePhase)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "finish_objective", "completed") {
		t.Fatalf("missing finish_objective action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestAnnotatePullRequestFollowUpPlanDisablesLatestCandidateInheritance(t *testing.T) {
	plan := annotatePullRequestFollowUpPlan(Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "repair",
			Kind:       "pr.followup",
			Reason:     "Fix the failing PR.",
			Prompt:     "Fix the failing PR.",
			TargetKind: "pull_request",
			WorkerKind: "codex",
		}},
	}, core.PullRequest{
		ID:     "pr-1",
		TaskID: "task-1",
		Repo:   "owner/repo",
		Number: 7,
		Branch: "codex/slice",
		Base:   "main",
		State:  "OPEN",
	})

	if got := stringMetadata(plan.Metadata, "workspaceBaseRef"); got != "refs/pull/7/head" {
		t.Fatalf("workspaceBaseRef = %q, want fetchable PR head ref", got)
	}
	if got := stringMetadata(plan.Metadata, "workspaceBaseRefKind"); got != "pull_request_head" {
		t.Fatalf("workspaceBaseRefKind = %q, want pull_request_head", got)
	}
	if shouldInheritLatestCandidate(plan.Metadata) {
		t.Fatalf("PR follow-up plan should not inherit latest broad-objective candidate: %+v", plan.Metadata)
	}
}

func TestNormalizePullRequestFollowUpPlanBindsImplicitUpdateToSingleWorkItem(t *testing.T) {
	plan := normalizePullRequestFollowUpPlan(Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "repair-pr",
			Kind:       "pr.followup",
			TargetKind: "pull_request",
			Reason:     "Address queued PR feedback.",
			Prompt:     "Repair PR.",
			WorkerKind: "codex",
		}},
		Actions: []PlanAction{{
			Kind:   "update_pull_request",
			When:   "after_success",
			Reason: "apply repair",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}, {
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return to monitoring",
		}},
	})

	if len(plan.Actions) != 2 {
		t.Fatalf("actions = %+v, want update then watch", plan.Actions)
	}
	if plan.Actions[0].Kind != "update_pull_request" {
		t.Fatalf("first action kind = %q, want update_pull_request", plan.Actions[0].Kind)
	}
	if plan.Actions[0].WorkerID != "repair-pr" {
		t.Fatalf("update workerId = %q, want repair-pr", plan.Actions[0].WorkerID)
	}
}

func TestCanonicalPullRequestFollowUpPlanRewritesStaleTargets(t *testing.T) {
	plan := canonicalizePullRequestFollowUpPlan(Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "repair",
			Kind:       "pr.followup",
			Reason:     "Repair PR 34321.",
			Prompt:     "Repair PR 34321.",
			TargetKind: "pull_request",
			WorkerKind: "codex",
		}},
		Metadata: map[string]any{
			"pullRequestID":        "github:owner/repo#34323",
			"pullRequestRepo":      "owner/repo",
			"pullRequestNumber":    34323,
			"pullRequestBranch":    "stale-branch",
			"workspaceBaseRef":     "refs/pull/34323/head",
			"workspaceBaseRefKind": "pull_request_head",
		},
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish repair",
			Inputs: map[string]any{
				"repo":   "owner/repo",
				"number": 34321,
				"branch": "wrong-branch",
				"title":  "fix: repair PR",
			},
		}, {
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "watch repair",
			Inputs: map[string]any{
				"repo":   "owner/repo",
				"number": 34321,
				"branch": "wrong-branch",
			},
		}},
	}, core.PullRequest{
		ID:     "pr-live",
		TaskID: "task-1",
		Repo:   "owner/repo",
		Number: 34408,
		URL:    "https://github.com/owner/repo/pull/34408",
		Branch: "codex/aged-live",
		Base:   "main",
		State:  "OPEN",
	})

	if got := intMetadata(plan.Metadata, "pullRequestNumber"); got != 34408 {
		t.Fatalf("metadata pullRequestNumber = %d, want 34408", got)
	}
	if got := stringMetadata(plan.Metadata, "workspaceBaseRef"); got != "refs/pull/34408/head" {
		t.Fatalf("workspaceBaseRef = %q, want PR head", got)
	}
	if len(plan.Actions) != 2 {
		t.Fatalf("actions = %d, want 2", len(plan.Actions))
	}
	if plan.Actions[0].Kind != "update_pull_request" {
		t.Fatalf("first action kind = %q, want update_pull_request", plan.Actions[0].Kind)
	}
	for _, action := range plan.Actions {
		if got := intMetadata(action.Inputs, "number"); got != 34408 {
			t.Fatalf("%s action number = %d, want 34408", action.Kind, got)
		}
		if got := stringMetadata(action.Inputs, "branch"); got != "codex/aged-live" {
			t.Fatalf("%s action branch = %q, want codex/aged-live", action.Kind, got)
		}
	}
	if len(plan.WorkItems) != 1 {
		t.Fatalf("workItems = %+v, want one repair item", plan.WorkItems)
	}
	if !strings.Contains(plan.WorkItems[0].Prompt, "Do not use aged-publish-pr for existing PR follow-up work") {
		t.Fatalf("missing existing-PR publish guard in prompt: %s", plan.WorkItems[0].Prompt)
	}
	if !strings.Contains(plan.WorkItems[0].Prompt, "provide inputs.commitMessage as a short subject") {
		t.Fatalf("missing PR update commit message guidance in prompt: %s", plan.WorkItems[0].Prompt)
	}
}

func TestPullRequestFollowUpForPlanRejectsUnqueuedExplicitTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-target-rejection"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix queued PR feedback.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-live",
			"repo":   "owner/repo",
			"number": 34408,
			"url":    "https://github.com/owner/repo/pull/34408",
			"branch": "codex/aged-live",
			"base":   "main",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-live",
			"repo":    "owner/repo",
			"number":  34408,
			"url":     "https://github.com/owner/repo/pull/34408",
			"branch":  "codex/aged-live",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pr, mismatch, ok := pullRequestFollowUpForPlan(snapshot, taskID, Plan{
		Actions: []PlanAction{{
			Kind: "update_pull_request",
			When: "after_success",
			Inputs: map[string]any{
				"repo":   "owner/repo",
				"number": 34321,
			},
		}},
	})
	if !ok {
		t.Fatal("expected queued PR feedback")
	}
	if mismatch == "" {
		t.Fatal("expected explicit non-queued PR target to be rejected")
	}
	if pr.Number != 34408 {
		t.Fatalf("selected PR = %d, want queued PR 34408", pr.Number)
	}
}

func TestPullRequestFollowUpForPlanAcceptsMatchingActionDespiteStaleMetadata(t *testing.T) {
	snapshot := core.Snapshot{
		PullRequests: []core.PullRequest{{
			ID:     "pr-live",
			TaskID: "task-1",
			Repo:   "owner/repo",
			Number: 34408,
			URL:    "https://github.com/owner/repo/pull/34408",
			Branch: "codex/aged-live",
			State:  "OPEN",
		}},
		Events: []core.Event{{
			ID:     1,
			Type:   core.EventPRFollowUp,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"id":      "pr-live",
				"repo":    "owner/repo",
				"number":  34408,
				"url":     "https://github.com/owner/repo/pull/34408",
				"branch":  "codex/aged-live",
				"attempt": 1,
				"reason":  "pull_request_needs_work",
			}),
		}},
	}
	pr, mismatch, ok := pullRequestFollowUpForPlan(snapshot, "task-1", Plan{
		Metadata: map[string]any{
			"pullRequestRepo":   "owner/repo",
			"pullRequestNumber": 34323,
		},
		Actions: []PlanAction{{
			Kind: "update_pull_request",
			When: "after_success",
			Inputs: map[string]any{
				"repo":   "owner/repo",
				"number": 34408,
			},
		}},
	})
	if !ok || mismatch != "" || pr.Number != 34408 {
		t.Fatalf("selection = pr %d mismatch %q ok %v, want queued PR without mismatch", pr.Number, mismatch, ok)
	}
}

func TestPendingPullRequestFeedbackSkipsUntrackedAndTerminalPullRequests(t *testing.T) {
	snapshot := core.Snapshot{
		PullRequests: []core.PullRequest{{
			ID:     "pr-open",
			TaskID: "task-1",
			Repo:   "owner/repo",
			Number: 7,
			URL:    "https://github.com/owner/repo/pull/7",
			Branch: "codex/open",
			State:  "OPEN",
		}, {
			ID:     "pr-closed",
			TaskID: "task-1",
			Repo:   "owner/repo",
			Number: 8,
			URL:    "https://github.com/owner/repo/pull/8",
			Branch: "codex/closed",
			State:  "CLOSED",
		}},
		Events: []core.Event{{
			ID:     1,
			Type:   core.EventPRFollowUp,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"id":     "github:owner/repo#9",
				"repo":   "owner/repo",
				"number": 9,
				"url":    "https://github.com/owner/repo/pull/9",
				"branch": "other-branch",
				"reason": "pull_request_needs_work",
			}),
		}, {
			ID:     2,
			Type:   core.EventPRFollowUp,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-closed",
				"repo":   "owner/repo",
				"number": 8,
				"url":    "https://github.com/owner/repo/pull/8",
				"branch": "codex/closed",
				"reason": "pull_request_needs_work",
			}),
		}, {
			ID:     3,
			Type:   core.EventPRFollowUp,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"id":     "github:owner/repo#7",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"branch": "codex/open",
				"reason": "pull_request_needs_work",
			}),
		}},
	}

	pending := pendingPullRequestFeedback(snapshot, "task-1")
	if len(pending) != 1 {
		t.Fatalf("pending feedback = %+v, want only tracked open PR", pending)
	}
	if pending[0].PullRequestID != "pr-open" || pending[0].Number != 7 {
		t.Fatalf("pending feedback = %+v, want canonical tracked PR", pending[0])
	}
}

func TestQueuePlanWorkItemsSkipsTerminalPullRequestFollowUp(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-terminal-pr-followup"
	appendTrackedPullRequest(t, ctx, store, taskID, "", core.TaskRunning)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":    "pr-1",
			"state": "MERGED",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})

	queued, err := service.queuePlanWorkItems(ctx, task, Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "repair-pr",
			Kind:       "pr.followup",
			Reason:     "Repair review feedback.",
			WorkerKind: "codex",
			Metadata: map[string]any{
				"pullRequestId": "pr-1",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 0 {
		t.Fatalf("queued work items = %+v, want none for merged PR", queued)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvents(snapshot.Events, core.EventWorkItemQueued, taskID); got != 0 {
		t.Fatalf("work item queued events = %d, want 0", got)
	}
	if !hasTaskAction(snapshot.Events, taskID, "terminal_pull_request_followup_skipped", "skipped") {
		t.Fatalf("missing terminal follow-up skipped action:\n%s", taskActionPayloads(snapshot.Events, taskID))
	}
}

func TestRunSpawnedWorkItemSkipsTerminalPullRequestFollowUp(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-terminal-pr-race"
	appendTrackedPullRequest(t, ctx, store, taskID, "", core.TaskRunning)
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "repair-pr",
		"kind":       "pr.followup",
		"targetKind": "pull_request",
		"targetId":   "pr-1",
		"reason":     "Repair review feedback.",
		"metadata": map[string]any{
			"sourceAction":      "plan",
			"pullRequestID":     "pr-1",
			"workerKind":        "codex",
			"feedbackSignature": "sig-1",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":    "pr-1",
			"state": "MERGED",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service.runSpawnedWorkItem(ctx, taskID, "repair-pr")

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := workItemByIDFromSnapshot(snapshot, taskID, "repair-pr")
	if !ok || item.Status != core.WorkItemSucceeded || item.WorkerID != "" {
		t.Fatalf("work item = %+v ok=%v, want succeeded without worker", item, ok)
	}
	if got := countEvents(snapshot.Events, core.EventWorkerCreated, taskID); got != 0 {
		t.Fatalf("worker created events = %d, want 0", got)
	}
	if !hasTaskAction(snapshot.Events, taskID, "terminal_pull_request_followup_skipped", "skipped") {
		t.Fatalf("missing terminal follow-up skipped action:\n%s", taskActionPayloads(snapshot.Events, taskID))
	}
}

func TestServicePlanActionSkipsMissingPullRequestUpdateTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-missing-pr", Title: "Broad objective"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Keep producing independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	results := []WorkerTurnResult{{
		WorkerID: "repair-worker",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "fix.go", Status: "modified"}},
			Diff:         "diff --git a/fix.go b/fix.go\n",
		},
	}}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:     "update_pull_request",
		When:     "after_success",
		WorkerID: "repair-worker",
		Reason:   "repair stale PR feedback",
		Inputs: map[string]any{
			"repo":   "owner/repo",
			"number": 404,
			"url":    "https://github.com/owner/repo/pull/404",
		},
	}, results)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("missing PR update target should not stop the plan")
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want 0", publisher.updateCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request", "skipped") {
		t.Fatalf("missing skipped update action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if !strings.Contains(taskActionPayloads(snapshot.Events, task.ID), "not tracked by this task") {
		t.Fatalf("missing not tracked skip reason; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServiceIntermediatePullRequestUpdateFailureContinuesObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{errCount: 1}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task := core.Task{ID: "task-intermediate-update-failed", Title: "Broad objective"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Keep producing independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-open",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/slice",
		Base:   "main",
		Title:  "Generic title",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"continueAfterPublish": true,
			"publicationPhase":     "intermediate",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:   "update_pull_request",
		When:   "after_success",
		Reason: "fix PR title and body",
		Inputs: map[string]any{
			"repo":   "owner/repo",
			"number": 7,
			"title":  "refactor: remove os_pipe dependency",
			"body":   "## Summary\n- Replace os_pipe.\n\n## Validation\n- Not run.",
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("intermediate PR update failure should continue the plan")
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskFailed {
		t.Fatalf("task failed after intermediate PR update error: %+v", task)
	}
	if task.ObjectiveStatus != core.ObjectiveActive || task.ObjectivePhase != "intermediate_pr_update_failed" {
		t.Fatalf("objective = %q/%q, want active/intermediate_pr_update_failed", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "update_pull_request", "continued") {
		t.Fatalf("missing continued update_pull_request action")
	}
}

func TestServiceProjectReviewPolicyBlocksIntermediatePublication(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectDir := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:        "reviewed",
		LocalPath: projectDir,
		ReviewPolicy: core.ReviewPolicy{
			Enabled:              true,
			BeforeIntermediatePR: true,
			BlockingSeverities:   []string{"P1"},
			ReviewerKinds:        []string{"reviewer"},
			MaxAttempts:          1,
			Instructions:         "Block lifecycle regressions.",
		},
	}}, "reviewed")
	if err != nil {
		t.Fatal(err)
	}
	reviewer := &recordingEventRunner{
		kind: "reviewer",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "Decision: request_changes\nFindings:\n- P1: missing regression coverage for the publication lifecycle.\nCommands Run:\n- Not run\nResidual Risk:\n- Publication should wait.",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"reviewer": reviewer,
	}, projectDir, fakeWorkspaceManager{
		cwd:        projectDir,
		sourceRoot: projectDir,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})
	service.SetProjects(projects)

	taskID := "task-review-policy"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":     "Publish candidate",
			"prompt":    "Implement and publish after review.",
			"projectId": "reviewed",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: "candidate-1",
		Payload: core.MustJSON(PreparedWorkspace{
			Root:       projectDir,
			CWD:        projectDir,
			SourceRoot: projectDir,
			Mode:       string(WorkspaceModeShared),
			VCSType:    "jj",
			WorkerID:   "candidate-1",
			TaskID:     taskID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	candidate := WorkerTurnResult{
		WorkerID: "candidate-1",
		NodeID:   "candidate-node",
		Kind:     "codex",
		Status:   core.WorkerSucceeded,
		Summary:  "implemented lifecycle change",
		Changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	}
	review, err := service.reviewCandidateBeforePullRequest(ctx, taskID, []WorkerTurnResult{candidate}, candidate.WorkerID, "intermediate")
	if err != nil {
		t.Fatal(err)
	}
	if review.Ready {
		t.Fatalf("review ready = true, want blocking review to stop publication")
	}
	if len(review.Results) != 2 || review.ReviewWorkerID == "" {
		t.Fatalf("review result = %+v", review)
	}
	if !strings.Contains(reviewer.promptValue(), "Block lifecycle regressions.") {
		t.Fatalf("review prompt did not include project instructions:\n%s", reviewer.promptValue())
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, taskID, "code_review_gate", "blocked") {
		t.Fatalf("missing blocked code_review_gate action")
	}
}

func TestServiceWorkerSteeringRetriesIntermediateReviewGateAndPublishes(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectDir := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:        "reviewed",
		LocalPath: projectDir,
		ReviewPolicy: core.ReviewPolicy{
			Enabled:              true,
			BeforeIntermediatePR: true,
			ReviewerKinds:        []string{"claude"},
			MaxAttempts:          2,
		},
	}}, "reviewed")
	if err != nil {
		t.Fatal(err)
	}
	reviewer := &recordingEventRunner{
		kind: "codex",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "Decision: approve\nFindings:\n- No P0/P1 issues.\nCommands Run:\n- Not run\nResidual Risk:\n- Low.",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{},
		runners: map[string]worker.Runner{
			"codex": reviewer,
		},
		workDir:    projectDir,
		cwd:        projectDir,
		sourceRoot: projectDir,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "cli/main.rs", Status: "modified"}},
		},
	})
	service.SetProjects(projects)

	taskID := "task-review-steering"
	candidateID := "candidate-1"
	reviewWorkerID := "review-old"
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":     "Publish candidate",
				"prompt":    "Implement and publish after review.",
				"projectId": "reviewed",
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
		{
			Type:     core.EventWorkerWorkspace,
			TaskID:   taskID,
			WorkerID: candidateID,
			Payload: core.MustJSON(PreparedWorkspace{
				Root:       projectDir,
				CWD:        projectDir,
				SourceRoot: projectDir,
				Mode:       string(WorkspaceModeShared),
				VCSType:    "git",
				WorkerID:   candidateID,
				TaskID:     taskID,
			}),
		},
		{
			Type:   core.EventTaskPlanned,
			TaskID: taskID,
			Payload: core.MustJSON(Plan{
				WorkerKind: "codex",
				Prompt:     "implement the dependency reduction",
				Actions: []PlanAction{{
					Kind:     "publish_pull_request",
					When:     "after_success",
					Reason:   "publish the approved intermediate candidate",
					WorkerID: candidateID,
					Inputs: map[string]any{
						"repo":                 "owner/repo",
						"base":                 "main",
						"body":                 "## Summary\n- Remove a dependency.\n\n## Validation\n- Checked by review gate.",
						"continueAfterPublish": true,
					},
				}},
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: candidateID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
				"metadata": map[string]any{
					"spawnID":   "implement",
					"spawnRole": "implementer",
				},
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: candidateID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "removed the dependency",
				"workspaceChanges": WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "cli/main.rs", Status: "modified"}},
				},
			}),
		},
		{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: reviewWorkerID,
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "review-node-old",
				"workerId":   reviewWorkerID,
				"workerKind": "claude",
				"role":       "review",
				"spawnId":    "code-review-gate",
				"metadata": map[string]any{
					"candidateWorkerID": candidateID,
					"reviewPhase":       "intermediate",
				},
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: reviewWorkerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "claude",
			}),
		},
		{
			Type:     core.EventWorkerSteered,
			TaskID:   taskID,
			WorkerID: reviewWorkerID,
			Payload: core.MustJSON(map[string]any{
				"workerId": reviewWorkerID,
				"message":  "Use codex for this review gate.",
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	if !service.resumeCodeReviewGateSteering(ctx, task, snapshot) {
		t.Fatal("code review gate steering was not handled")
	}

	snapshot = waitForPullRequests(t, store, taskID, 1)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	if publisher.published.WorkerID != candidateID {
		t.Fatalf("published worker = %q, want %q", publisher.published.WorkerID, candidateID)
	}
	if reviewer.callsValue() != 1 {
		t.Fatalf("reviewer calls = %d, want 1", reviewer.callsValue())
	}
	if !hasTaskAction(snapshot.Events, taskID, "code_review_gate", "passed") {
		t.Fatalf("missing passed code_review_gate action")
	}
	if !hasTaskAction(snapshot.Events, taskID, "publish_pull_request", "started") {
		t.Fatalf("missing resumed publish action")
	}
}

func TestCodeReviewBlocksPublicationHonorsApproveDecision(t *testing.T) {
	result := WorkerTurnResult{
		Status: core.WorkerSucceeded,
		Summary: "Decision: approve\n" +
			"Findings:\n" +
			"- No P0/P1 issues. Missing dedicated coverage is not blocking.\n" +
			"Commands Run:\n" +
			"- go test ./...\n" +
			"Residual Risk:\n" +
			"- Low.",
	}
	policy := core.ReviewPolicy{BlockingSeverities: []string{"P0", "P1"}}
	if codeReviewBlocksPublication(result, policy) {
		t.Fatal("approval decision with negated severity mentions blocked publication")
	}
}

func TestCodeReviewBlocksPublicationHonorsRequestChangesDecision(t *testing.T) {
	result := WorkerTurnResult{
		Status:  core.WorkerSucceeded,
		Summary: "Decision: request_changes\nFindings:\n- Needs more validation before publishing.",
	}
	if !codeReviewBlocksPublication(result, core.ReviewPolicy{}) {
		t.Fatal("request_changes decision did not block publication")
	}
}

func TestCodeReviewBlocksPublicationFallsBackToBlockingSeverity(t *testing.T) {
	result := WorkerTurnResult{
		Status:  core.WorkerSucceeded,
		Summary: "Findings:\n- P1: publication would skip the required review gate.",
	}
	policy := core.ReviewPolicy{BlockingSeverities: []string{"P1"}}
	if !codeReviewBlocksPublication(result, policy) {
		t.Fatal("blocking severity without a decision line did not block publication")
	}
}

func TestServicePlanActionPublishesAfterCleanReviewFinding(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "change",
			Kind:       "objective.implement",
			Reason:     "Tighten the signing-agent classifier.",
			Prompt:     "tighten the signing-agent classifier",
			TargetKind: "objective",
			WorkerKind: "change",
		}},
		Actions: []PlanAction{{
			Kind:     "publish_pull_request",
			When:     "after_success",
			Reason:   "publish the classifier fix",
			WorkerID: "change",
			Inputs:   map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish the classifier fix."},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented classifier changes"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: `## Findings
- No findings.

## Recommended Next Turns
- Publish the pull request.`}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix signing-agent classification",
		Prompt: "Implement, review, and publish when ready.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want clean review to allow publish", publisher.publishCalls)
	}
	if hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_blocked_by_follow_up", "rejected") {
		t.Fatalf("clean review should not block publication")
	}
}

func TestServicePlanActionDoesNotPublishRejectedCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	baseBrain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "find and implement a real throughput optimization",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "publish a useful optimization PR when one is ready",
				Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish the optimization when ready."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "The worker correctly reported this is not ready to publish yet.",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "continue until there is an actual task-relevant optimization",
			},
		}, {
			Action:  "wait",
			Message: "continuing broader investigation",
		}},
	}
	brain := &publicationReviewBrain{
		BrainProvider:  baseBrain,
		ReplanProvider: baseBrain,
		reviews: []PublicationReview{{
			Ready:  false,
			Reason: "worker result says the requested optimization is not done and only produced setup",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "I added setup notes, but the requested throughput optimization is not done yet."}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "bench/throughput.md", Status: "added"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Improve Deno Serve Throughput",
		Prompt: "Keep working until you find real throughput optimizations and open PRs as useful complete units become ready.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want rejected candidate to stay unpublished", publisher.publishCalls)
	}
	if brain.reviewCalls != 1 {
		t.Fatalf("publication review calls = %d, want 1", brain.reviewCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_readiness_rejected", "rejected") {
		t.Fatalf("missing publication readiness rejection event")
	}
	if len(baseBrain.states) == 0 {
		t.Fatalf("replanner was not given a chance to continue after rejected publication")
	}
}

func TestServiceImmediatePlanActionWatchesExistingPullRequests(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "no worker should run",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "immediate",
			Reason: "standalone PR babysitting task",
			Inputs: map[string]any{"repo": "owner/repo", "number": 42},
		}},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "should not run"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Babysit PR", Prompt: "Watch owner/repo#42 until merged."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	waitForEvent(t, store, core.EventTaskArtifact, task.ID)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskWaiting || task.ObjectivePhase != "watching_pull_requests" {
		t.Fatalf("task status = %q phase = %q", task.Status, task.ObjectivePhase)
	}
	if len(snapshot.Workers) != 0 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 42 {
		t.Fatalf("list spec = %+v", publisher.listSpec)
	}
	if !hasMilestone(task.Milestones, "pull_requests_watched") || len(task.Artifacts) != 1 {
		t.Fatalf("milestones=%+v artifacts=%+v", task.Milestones, task.Artifacts)
	}
}

func TestServicePlanActionWatchesIntermediatePullRequestsWithoutBlockingBroadObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	taskID := "task-broad-watch"
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Reduce Deno dependencies",
			"prompt": "Keep producing focused dependency-reduction PRs.",
			"metadata": map[string]any{
				"objectiveMode": "broad",
			},
		}),
	}, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-intermediate",
			"repo":   "owner/repo",
			"number": 42,
			"url":    "https://github.com/owner/repo/pull/42",
			"branch": "codex/intermediate",
			"base":   "main",
			"title":  "refactor(fetch): remove tower-http decompression",
			"state":  "OPEN",
			"metadata": map[string]any{
				"continueAfterPublish": true,
				"publicationPhase":     "intermediate",
			},
		}),
	})
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}

	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:   "watch_pull_requests",
		When:   "after_success",
		Reason: "return intermediate PR to monitoring",
		Inputs: map[string]any{"repo": "owner/repo"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("watching intermediate PR stopped broad objective")
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok = findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveActive || task.ObjectivePhase != "watching_intermediate_pull_requests" {
		t.Fatalf("task status=%q objective=%s/%s, want active non-waiting", task.Status, task.ObjectiveStatus, task.ObjectivePhase)
	}
	if !hasTaskAction(snapshot.Events, taskID, "watch_pull_requests", "") {
		t.Fatalf("missing completed watch action; payloads:\n%s", taskActionPayloads(snapshot.Events, taskID))
	}
}

func TestServiceExplicitWatchPreservesIntermediatePRMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		list: []core.PullRequest{{
			Repo:   "owner/repo",
			Number: 42,
			URL:    "https://github.com/owner/repo/pull/42",
			Branch: "codex/intermediate",
			Base:   "main",
			Title:  "refactor(fetch): remove tower-http decompression",
			State:  "OPEN",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	taskID := "task-explicit-watch"
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Reduce Deno dependencies",
			"prompt": "Keep producing focused dependency-reduction PRs.",
			"metadata": map[string]any{
				"objectiveMode": "broad",
			},
		}),
	}, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-intermediate",
			"repo":   "owner/repo",
			"number": 42,
			"url":    "https://github.com/owner/repo/pull/42",
			"branch": "codex/intermediate",
			"base":   "main",
			"title":  "refactor(fetch): remove tower-http decompression",
			"state":  "OPEN",
			"metadata": map[string]any{
				"continueAfterPublish": true,
				"publicationPhase":     "intermediate",
			},
		}),
	})

	prs, err := service.WatchPullRequests(ctx, taskID, core.WatchPullRequestsRequest{Repo: "owner/repo", Number: 42})
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 1 || !pullRequestContinuesTask(prs[0]) {
		t.Fatalf("watched PRs = %+v, want preserved continueAfterPublish metadata", prs)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveActive {
		t.Fatalf("task = %+v, want explicit intermediate watch to stay active", task)
	}
}

func TestRefreshIntermediatePullRequestDoesNotBlockBroadObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		status: core.PullRequest{
			ID:               "pr-intermediate",
			Repo:             "owner/repo",
			Number:           42,
			URL:              "https://github.com/owner/repo/pull/42",
			Branch:           "codex/intermediate",
			Base:             "main",
			Title:            "refactor(fetch): remove tower-http decompression",
			State:            "OPEN",
			ChecksStatus:     "passing",
			ChecksConclusion: "SUCCESS",
			MergeStatus:      "MERGEABLE",
			Mergeable:        "MERGEABLE",
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	taskID := "task-refresh-intermediate"
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Reduce Deno dependencies",
			"prompt": "Keep producing focused dependency-reduction PRs.",
			"metadata": map[string]any{
				"objectiveMode": "broad",
			},
		}),
	}, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}, core.Event{
		Type:   core.EventTaskObjective,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.ObjectiveActive,
			"phase":   "working",
			"summary": "Objective work is active.",
		}),
	}, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-intermediate",
			"repo":   "owner/repo",
			"number": 42,
			"url":    "https://github.com/owner/repo/pull/42",
			"branch": "codex/intermediate",
			"base":   "main",
			"title":  "refactor(fetch): remove tower-http decompression",
			"state":  "OPEN",
			"metadata": map[string]any{
				"continueAfterPublish": true,
				"publicationPhase":     "intermediate",
			},
		}),
	})

	pr, err := service.RefreshPullRequest(ctx, "pr-intermediate")
	if err != nil {
		t.Fatal(err)
	}
	if !pullRequestContinuesTask(pr) {
		t.Fatalf("refreshed PR lost intermediate metadata: %+v", pr)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveActive || task.ObjectivePhase != "intermediate_pr_open" {
		t.Fatalf("task status=%q objective=%s/%s, want active intermediate PR state", task.Status, task.ObjectiveStatus, task.ObjectivePhase)
	}
}

func TestWatchPullRequestsWithoutExplicitTargetDoesNotAdoptRepoPullRequests(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Broad objective", Prompt: "Keep reducing dependencies."})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.WatchPullRequests(ctx, task.ID, core.WatchPullRequestsRequest{Repo: "owner/repo"})
	if !errors.Is(err, errNoPullRequestsToWatch) {
		t.Fatalf("WatchPullRequests error = %v, want errNoPullRequestsToWatch", err)
	}
	if publisher.listCalls != 0 {
		t.Fatalf("list calls = %d, want no broad repo listing", publisher.listCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v, want none adopted", snapshot.PullRequests)
	}
}

func TestWatchPullRequestsExplicitTerminalTargetDoesNotEnterWaiting(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{list: []core.PullRequest{{
		ID:     "github:owner/repo#42",
		Repo:   "owner/repo",
		Number: 42,
		URL:    "https://github.com/owner/repo/pull/42",
		State:  "CLOSED",
	}}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Babysit PR", Prompt: "Watch owner/repo#42."})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.WatchPullRequests(ctx, task.ID, core.WatchPullRequestsRequest{Repo: "owner/repo", Number: 42})
	if !errors.Is(err, errNoPullRequestsToWatch) {
		t.Fatalf("WatchPullRequests error = %v, want errNoPullRequestsToWatch", err)
	}
	if publisher.listCalls != 1 {
		t.Fatalf("list calls = %d, want explicit target lookup", publisher.listCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v, want terminal target ignored", snapshot.PullRequests)
	}
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskWaiting || task.ObjectiveStatus == core.ObjectiveWaitingExternal {
		t.Fatalf("task = %+v, want no waiting transition", task)
	}
}

func TestServicePlanActionSkipsWatchWhenNoTaskOwnedPullRequestsRemain(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(&fakePullRequestPublisher{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Broad objective", Prompt: "Keep going."})
	if err != nil {
		t.Fatal(err)
	}
	keepGoing, _, err := service.executePlanAction(ctx, task, PlanAction{
		Kind:   "watch_pull_requests",
		When:   "after_success",
		Reason: "return to monitoring after stale follow-up",
		Inputs: map[string]any{"repo": "owner/repo"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !keepGoing {
		t.Fatal("empty watch should continue replanning")
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "watch_pull_requests", "skipped") {
		t.Fatalf("missing skipped watch action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskWaiting || task.ObjectiveStatus == core.ObjectiveWaitingExternal {
		t.Fatalf("task = %+v, want no waiting transition", task)
	}
}

func TestServicePullRequestFollowUpSuppressesPlanSpawnsWhenReturningToWatch(t *testing.T) {
	t.Skip("legacy spawn suppression was removed; PR follow-up work must be explicit work items")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-followup"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix the pull request and keep watching it.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "repair dirty PR branch",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
		Spawns: []SpawnRequest{{
			ID:         "review-after-repair",
			Role:       "post-repair reviewer",
			Reason:     "review repaired PR",
			WorkerKind: "reviewer",
		}},
	}}, map[string]worker.Runner{
		"change":   eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if hasWorkerCreated(snapshot.Events, taskID, "reviewer") {
		t.Fatalf("pull request follow-up should not run plan spawns before returning to watch")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, `"spawnsSuppressedReason":"pull_request_followup_returns_to_github_monitor"`) {
		t.Fatalf("missing suppressed spawn metadata")
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 7 {
		t.Fatalf("list spec = %+v", publisher.listSpec)
	}
}

func TestServiceRetryPullRequestFollowUpRunsPersistedRepairPlan(t *testing.T) {
	t.Skip("legacy persisted PR follow-up retry path was replaced by durable pr work items")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-pr-followup"
	initialWorkerID := "worker-initial"
	followUpPlan := Plan{
		Workers: []WorkerRequest{{
			ID:         "repair_pr_followup",
			Role:       "repair PR",
			Reason:     "CI failed on the open pull request.",
			WorkerKind: "codex",
			Prompt:     "Fix the failing CI and keep watching the PR.",
		}},
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
		Metadata: map[string]any{
			"pullRequestID":        "pr-1",
			"workspaceBaseRef":     "codex/aged-test",
			"workspaceBaseRefKind": "pull_request_head",
		},
	}
	events := []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":    "Repair PR",
				"prompt":   "Fix the pull request and keep watching it.",
				"metadata": map[string]any{},
			}),
		},
		{
			Type:   core.EventTaskPlanned,
			TaskID: taskID,
			Payload: core.MustJSON(Plan{
				WorkerKind: "codex",
				Prompt:     "Implement the original change.",
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: initialWorkerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: initialWorkerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "implemented original change",
				"workspaceChanges": WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "mod.ts", Status: "modified"}},
				},
			}),
		},
		{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-1",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"branch": "codex/aged-test",
				"base":   "main",
				"state":  "OPEN",
			}),
		},
		{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":      "pr-1",
				"attempt": 1,
				"reason":  "pull_request_needs_work",
			}),
		},
		{
			Type:    core.EventTaskPlanned,
			TaskID:  taskID,
			Payload: core.MustJSON(followUpPlan),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskFailed,
				"error":  `unknown worker kind ""`,
			}),
		},
	}
	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	runner := &recordingEventRunner{
		kind:   "codex",
		events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}},
	}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if runner.callsValue() != 1 {
		t.Fatalf("follow-up runner calls = %d, want 1", runner.callsValue())
	}
	if publisher.publishCalls != 0 || publisher.updateCalls != 0 {
		t.Fatalf("publish calls = %d update calls = %d, want retry to run follow-up worker only", publisher.publishCalls, publisher.updateCalls)
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 7 {
		t.Fatalf("list spec = %+v", publisher.listSpec)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, `"repair_pr_followup"`) {
		t.Fatalf("missing retried pull request follow-up plan")
	}
}

func TestServicePullRequestFollowUpStartsWorkspaceFromPullRequestHead(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")
	runTestGit(t, repo, "checkout", "-b", "codex/aged-test")
	if err := os.WriteFile(filepath.Join(repo, "fix.txt"), []byte("pr head\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "fix.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "pr head")
	prHead := strings.TrimSpace(runTestGit(t, repo, "rev-parse", "HEAD"))
	runTestGit(t, repo, "push", "-u", "origin", "codex/aged-test")
	runTestGit(t, remote, "update-ref", "refs/pull/7/head", prHead)
	runTestGit(t, repo, "checkout", "main")

	taskID := "task-pr-head-followup"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix the pull request and keep watching it.",
			"metadata": map[string]any{
				"projectId": "repo",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   repo,
		Repo:        "owner/repo",
		DefaultBase: "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "repair dirty PR branch",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}}},
	}, repo, workspace)
	service.SetProjects(projects)
	service.SetPullRequestPublisher(&fakePullRequestPublisher{})

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if workspace.baseRevision != "refs/remotes/origin/pull/7/head" {
		t.Fatalf("workspace base revision = %q, want PR head", workspace.baseRevision)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, `"workspaceBaseRef":"refs/pull/7/head"`) {
		t.Fatalf("missing PR head workspace metadata")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, "Do not post PR status comments") {
		t.Fatalf("missing PR status comment guard")
	}
}

func TestServicePullRequestFollowUpUpdatesExistingPullRequestBeforeWatching(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-followup-update"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix the pull request and keep watching it.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Repair PR",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	changed := WorkspaceChangedFile{Path: "internal/orchestrator/pull_request.go", Status: "modified"}
	workspace := &recordingWorkspaceManager{changes: WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: []WorkspaceChangedFile{changed},
	}}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "repair dirty PR branch",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}}},
	}, t.TempDir(), workspace)
	service.SetPullRequestPublisher(publisher)

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if publisher.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", publisher.updateCalls)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want existing PR update only", publisher.publishCalls)
	}
	if publisher.updatedPR.ID != "pr-1" || publisher.updated.Branch != "codex/aged-test" || publisher.updated.Base != "main" {
		t.Fatalf("updated PR=%+v spec=%+v", publisher.updatedPR, publisher.updated)
	}
	if publisher.updated.WorkerID == "" {
		t.Fatalf("update worker id was empty")
	}
	if !hasEvent(snapshot.Events, core.EventPRUpdated, taskID, "") {
		t.Fatalf("missing pull_request.updated event")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskAction, taskID, `"kind":"update_pull_request"`) {
		t.Fatalf("missing deterministic update_pull_request action")
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 7 {
		t.Fatalf("watch list spec = %+v", publisher.listSpec)
	}
}

func TestServicePullRequestFollowUpNoChangeReturnsToWatch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-followup-no-change"
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Check PR",
				"prompt": "Inspect the pull request and keep watching it.",
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
		{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-1",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"branch": "codex/aged-test",
				"base":   "main",
				"title":  "Check PR",
				"state":  "OPEN",
			}),
		},
		{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":      "pr-1",
				"attempt": 1,
				"reason":  "pull_request_needs_work",
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkItems: []WorkItemRequest{{
			ID:         "inspect_pr",
			Kind:       "pr.followup",
			Reason:     "determine whether the PR needs changes",
			Prompt:     "inspect PR state and report no code changes are needed",
			TargetKind: "pull_request",
			TargetID:   "pr-1",
			WorkerKind: "change",
		}},
		Actions: []PlanAction{
			{
				Kind:     "update_pull_request",
				When:     "after_success",
				Reason:   "apply repair changes if any were needed",
				WorkerID: "inspect_pr",
				Inputs:   map[string]any{"repo": "owner/repo", "number": 7},
			},
			{
				Kind:     "watch_pull_requests",
				When:     "after_success",
				Reason:   "return PR to monitor",
				WorkerID: "inspect_pr",
				Inputs:   map[string]any{"repo": "owner/repo", "number": 7},
			},
		},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "PR is already green; no code change needed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want no update for no-change follow-up", publisher.updateCalls)
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 7 {
		t.Fatalf("watch list spec = %+v", publisher.listSpec)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskAction, taskID, `"kind":"update_pull_request"`) ||
		!eventPayloadContains(snapshot.Events, core.EventTaskAction, taskID, `"status":"skipped"`) ||
		!eventPayloadContains(snapshot.Events, core.EventTaskAction, taskID, "no candidate changes") {
		t.Fatalf("missing skipped no-change update action")
	}
}

func TestServiceRefreshPullRequestCompletesLegacyBabysitterTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		ID:           "github:owner/repo#7",
		Repo:         "owner/repo",
		Number:       7,
		URL:          "https://github.com/owner/repo/pull/7",
		Branch:       "codex/aged-test",
		Base:         "main",
		Title:        "Task",
		State:        "MERGED",
		ChecksStatus: "success",
		MergeStatus:  "UNKNOWN",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	for _, task := range []struct {
		id       string
		metadata map[string]any
	}{
		{id: "task-1"},
		{id: "babysitter-1", metadata: map[string]any{"pullRequestId": "github:owner/repo#7", "repo": "owner/repo", "number": 7}},
	} {
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskCreated,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"title":    "Task",
				"prompt":   "Prompt",
				"metadata": task.metadata,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "github:owner/repo#7",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := service.RefreshPullRequest(ctx, "github:owner/repo#7"); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, "babysitter-1", core.TaskSucceeded)
	babysitter, ok := findTask(snapshot, "babysitter-1")
	if !ok {
		t.Fatal("missing babysitter task")
	}
	if babysitter.ObjectiveStatus != core.ObjectiveSatisfied || babysitter.ObjectivePhase != "merged" {
		t.Fatalf("babysitter objective = %q phase %q", babysitter.ObjectiveStatus, babysitter.ObjectivePhase)
	}
	if !hasMilestone(babysitter.Milestones, "pr_merged") {
		t.Fatalf("babysitter milestones = %+v", babysitter.Milestones)
	}
}

func TestServiceReconcilesTerminalPullRequestLegacyBabysitterTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	for _, task := range []struct {
		id       string
		metadata map[string]any
	}{
		{id: "task-1"},
		{id: "babysitter-1", metadata: map[string]any{"pullRequestId": "github:owner/repo#7", "repo": "owner/repo", "number": 7}},
	} {
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskCreated,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"title":    "Task",
				"prompt":   "Prompt",
				"metadata": task.metadata,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "github:owner/repo#7",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "MERGED",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.ReconcilePullRequestTerminalTasks(ctx, "github:owner/repo#7"); err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, "babysitter-1", core.TaskSucceeded)
}

func TestServiceRoutesTaskToConfiguredProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectA := t.TempDir()
	projectB := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "a", Name: "A", LocalPath: projectA, Repo: "owner/a", DefaultBase: "main"},
		{ID: "b", Name: "B", LocalPath: projectB, Repo: "owner/b", DefaultBase: "trunk"},
	}, "a")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	runner := &recordingRunner{kind: "chosen"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "chosen",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"chosen": runner}, projectA, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		ProjectID: "b",
		Title:     "Project routed",
		Prompt:    "Run in project B.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "b" {
		t.Fatalf("task project = %q, want b", task.ProjectID)
	}
	if workspace.workDir != projectB {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, projectB)
	}
	if runner.workDir != projectB {
		t.Fatalf("runner workDir = %q, want %q", runner.workDir, projectB)
	}
	if snapshot.Tasks[0].ProjectID == "" {
		t.Fatalf("snapshot task missing project id: %+v", snapshot.Tasks[0])
	}
}

func TestServiceStartsNewTaskWorkspaceFromProjectDefaultBase(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	mainCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", "HEAD"))
	upstream := t.TempDir()
	runTestGit(t, upstream, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "upstream", upstream)
	runTestGit(t, repo, "push", "-u", "upstream", "main")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "checkout", "-b", "feature")
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "unrelated feature")

	projects, err := NewProjectRegistry([]core.Project{{
		ID:           "repo",
		Name:         "Repo",
		LocalPath:    repo,
		Repo:         "fork/repo",
		UpstreamRepo: "owner/repo",
		DefaultBase:  "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, repo, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "New task",
		Prompt: "Do unrelated work.",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if workspace.baseRevision != "refs/remotes/upstream/main" {
		t.Fatalf("workspace base revision = %q, want upstream default base", workspace.baseRevision)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", workspace.baseRevision))
	if gotCommit != mainCommit {
		t.Fatalf("workspace base revision commit = %q, want %q", gotCommit, mainCommit)
	}
}

func TestServiceStartsNewTaskWorkspaceFromFetchedProjectDefaultBase(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	fixture := prepareStaleOriginMainFixture(t, "remote update")
	repo := fixture.repo
	fixture.assertLocalOriginMainStale(t)
	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   repo,
		Repo:        "owner/repo",
		DefaultBase: "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, repo, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "New task",
		Prompt: "Do unrelated work.",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if workspace.baseRevision != "refs/remotes/origin/main" {
		t.Fatalf("workspace base revision = %q, want origin default base", workspace.baseRevision)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", workspace.baseRevision))
	if gotCommit != fixture.remoteCommit {
		t.Fatalf("workspace base revision commit = %q, want %q", gotCommit, fixture.remoteCommit)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFetchesStaleBase(t *testing.T) {
	ctx := context.Background()
	fixture := prepareStaleOriginMainFixture(t, "remote update")
	repo := fixture.repo
	fixture.assertLocalOriginMainStale(t)
	ref, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref != "refs/remotes/origin/main" {
		t.Fatalf("synced base ref = %q, want refs/remotes/origin/main", ref)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", ref))
	if gotCommit != fixture.remoteCommit {
		t.Fatalf("synced base commit = %q, want %q", gotCommit, fixture.remoteCommit)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionUsesOriginFallbackWithoutBranchUpstream(t *testing.T) {
	ctx := context.Background()
	fixture := prepareStaleOriginMainFixture(t, "origin fallback update")
	repo := fixture.repo
	runTestGit(t, repo, "config", "--unset", "branch.main.remote")
	runTestGit(t, repo, "config", "--unset", "branch.main.merge")
	fixture.assertLocalOriginMainStale(t)
	ref, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref != "refs/remotes/origin/main" {
		t.Fatalf("synced base ref = %q, want refs/remotes/origin/main", ref)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", ref))
	if gotCommit != fixture.remoteCommit {
		t.Fatalf("synced base commit = %q, want %q", gotCommit, fixture.remoteCommit)
	}
}

type staleOriginMainFixture struct {
	repo         string
	remoteCommit string
	remoteRef    string
}

func prepareStaleOriginMainFixture(t *testing.T, updateMessage string) staleOriginMainFixture {
	t.Helper()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")
	runTestGit(t, remote, "symbolic-ref", "HEAD", "refs/heads/main")

	updaterParent := t.TempDir()
	updater := filepath.Join(updaterParent, "updater")
	runTestGit(t, updaterParent, "clone", remote, updater)
	runTestGit(t, updater, "config", "user.name", "aged-test")
	runTestGit(t, updater, "config", "user.email", "aged-test@example.invalid")
	runTestGit(t, updater, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(updater, "file.txt"), []byte(updateMessage+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, updater, "add", "file.txt")
	runTestGit(t, updater, "commit", "-m", updateMessage)
	runTestGit(t, updater, "push", "origin", "main")

	return staleOriginMainFixture{
		repo:         repo,
		remoteCommit: strings.TrimSpace(runTestGit(t, updater, "rev-parse", "HEAD")),
		remoteRef:    "refs/remotes/origin/main",
	}
}

func (f staleOriginMainFixture) assertLocalOriginMainStale(t *testing.T) {
	t.Helper()

	staleCommit := strings.TrimSpace(runTestGit(t, f.repo, "rev-parse", f.remoteRef))
	if staleCommit == f.remoteCommit {
		t.Fatalf("test setup failed: local origin/main is already current")
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFailsWithoutUpstream(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")

	_, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err == nil {
		t.Fatal("syncedProjectWorkspaceBaseRevision succeeded; want missing upstream error")
	}
	if !strings.Contains(err.Error(), "upstream tracking branch is not configured") {
		t.Fatalf("error = %v, want upstream tracking blocker", err)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFailsWithoutResolvableRemoteTrackingFallback(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)

	_, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err == nil {
		t.Fatal("syncedProjectWorkspaceBaseRevision succeeded; want missing upstream error")
	}
	if !strings.Contains(err.Error(), "upstream tracking branch is not configured") {
		t.Fatalf("error = %v, want upstream tracking blocker", err)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFailsWithUnsupportedBranchUpstream(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")
	runTestGit(t, repo, "config", "branch.main.merge", "refs/tags/main")

	_, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err == nil {
		t.Fatal("syncedProjectWorkspaceBaseRevision succeeded; want unsupported upstream error")
	}
	if !strings.Contains(err.Error(), "unsupported upstream merge ref") {
		t.Fatalf("error = %v, want unsupported upstream blocker", err)
	}
}

func TestServicePublishedPRContainsWorkerChangesNotDaemonBranch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	mainCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", "HEAD"))
	upstream := t.TempDir()
	runTestGit(t, upstream, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "upstream", upstream)
	runTestGit(t, repo, "push", "-u", "upstream", "main")
	runTestGit(t, repo, "checkout", "-b", "daemon-feature")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("do not publish\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "unrelated.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "unrelated daemon branch work")

	projects, err := NewProjectRegistry([]core.Project{{
		ID:           "repo",
		Name:         "Repo",
		LocalPath:    repo,
		Repo:         "fork/repo",
		UpstreamRepo: "owner/repo",
		DefaultBase:  "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	publisher := LocalPullRequestPublisher{
		exec: func(ctx context.Context, dir string, name string, args ...string) (string, error) {
			switch {
			case name == "git" && len(args) > 0 && args[0] == "push":
				return "", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "create":
				return "https://github.com/owner/repo/pull/22", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "view":
				return `{"number":22,"url":"https://github.com/owner/repo/pull/22","state":"OPEN","title":"CI","isDraft":false,"headRefName":"ci-branch","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":""}`, nil
			default:
				return runCommand(ctx, dir, name, args...)
			}
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "add workflow",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish CI workflow",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "branch": "ci-branch", "title": "CI", "body": "Body"},
		}},
	}}, map[string]worker.Runner{"writer": fileWritingRunner{
		kind: "writer",
		path: ".github/workflows/ci.yml",
		body: "name: CI\n",
	}}, repo, NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain))
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Add CI",
		Prompt: "Implement CI that checks formatting and runs all the tests.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	if contents := runTestGit(t, repo, "show", "ci-branch:.github/workflows/ci.yml"); contents != "name: CI\n" {
		t.Fatalf("published branch missing worker workflow: %q", contents)
	}
	if _, err := runCommand(ctx, repo, "git", "cat-file", "-e", "ci-branch:unrelated.txt"); err == nil {
		t.Fatalf("published branch included unrelated daemon branch file")
	}
	if base := strings.TrimSpace(runTestGit(t, repo, "merge-base", "ci-branch", "refs/remotes/upstream/main")); base != mainCommit {
		t.Fatalf("branch merge-base = %q, want upstream main %q", base, mainCommit)
	}
}

func TestServiceLoadsProjectsFromSQLiteBeforeSeed(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	seedDir := t.TempDir()
	seed, err := NewProjectRegistry([]core.Project{{ID: "seed", Name: "Seed", LocalPath: seedDir}}, "seed")
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), seedDir)
	if err := service.LoadProjects(ctx, seed); err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	if _, err := service.CreateProject(ctx, core.Project{ID: "api", Name: "API", LocalPath: projectDir, Repo: "owner/api"}); err != nil {
		t.Fatal(err)
	}

	restarted := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), seedDir)
	if err := restarted.LoadProjects(ctx, seed); err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Projects) != 2 {
		t.Fatalf("projects = %+v, want seed and api", snapshot.Projects)
	}
	if project, ok := restarted.projects.Get("api"); !ok || project.Repo != "owner/api" {
		t.Fatalf("loaded project = %+v, ok = %v", project, ok)
	}
}

func TestServiceDisablingRunnerPluginRemovesRuntimeRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, map[string]worker.Runner{}, t.TempDir())
	plugin := core.Plugin{
		ID:       "runner:lint",
		Name:     "Lint",
		Kind:     "runner",
		Enabled:  true,
		Protocol: "aged-runner-v1",
		Command:  []string{"aged-lint"},
	}
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if runner := service.runners["lint"]; runner == nil {
		t.Fatalf("runner plugin was not registered: %+v", service.runners)
	}

	plugin.Enabled = false
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if runner, ok := service.runners["lint"]; ok {
		t.Fatalf("disabled runner plugin left stale runner: %+v", runner)
	}
}

func TestServiceClearingRunnerPluginCommandRemovesRuntimeRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, map[string]worker.Runner{}, t.TempDir())
	plugin := core.Plugin{
		ID:       "runner:lint",
		Name:     "Lint",
		Kind:     "runner",
		Enabled:  true,
		Protocol: "aged-runner-v1",
		Command:  []string{"aged-lint"},
	}
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if _, ok := service.runners["lint"]; !ok {
		t.Fatalf("runner plugin was not registered: %+v", service.runners)
	}

	plugin.Command = nil
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if _, ok := service.runners["lint"]; ok {
		t.Fatalf("runner plugin with cleared command left stale runner: %+v", service.runners)
	}
}

func TestServiceRunnerPluginProtocolChangeRestoresStaticRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	static := buildOnlyRunner{kind: "lint", command: []string{"static-lint"}}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, map[string]worker.Runner{"lint": static}, t.TempDir())
	plugin := core.Plugin{
		ID:       "runner:lint",
		Name:     "Lint",
		Kind:     "runner",
		Enabled:  true,
		Protocol: "aged-runner-v1",
		Command:  []string{"aged-lint"},
	}
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(service.runners["lint"].BuildCommand(workerSpec("w1")), " "); got != "aged-lint run" {
		t.Fatalf("registered runner command = %q", got)
	}

	plugin.Protocol = "aged-plugin-v1"
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	runner, ok := service.runners["lint"]
	if !ok {
		t.Fatalf("static runner was not restored after protocol change: %+v", service.runners)
	}
	if got := strings.Join(runner.BuildCommand(workerSpec("w1")), " "); got != "static-lint" {
		t.Fatalf("static runner was not restored, command = %q", got)
	}
}

func TestServiceMapsExternalRepoToProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectA := t.TempDir()
	projectB := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "a", Name: "A", LocalPath: projectA, Repo: "owner/a"},
		{ID: "b", Name: "B", LocalPath: projectB, Repo: "owner/b"},
	}, "a")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, projectA, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "GitHub issue owner/b#1",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"repo": "owner/b"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "b" {
		t.Fatalf("task project = %q, want b", task.ProjectID)
	}
	if workspace.workDir != projectB {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, projectB)
	}
}

func TestServiceRoutesGitHubIssueToExplicitUpstreamProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	upstreamCheckout := t.TempDir()
	forkCheckout := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "upstream", Name: "Upstream", LocalPath: upstreamCheckout, Repo: "owner/repo"},
		{ID: "fork", Name: "Fork", LocalPath: forkCheckout, Repo: "fork-owner/repo", UpstreamRepo: "owner/repo"},
	}, "upstream")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, upstreamCheckout, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "GitHub issue owner/repo#1",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"source": "github-issue", "repo": "owner/repo"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "fork" {
		t.Fatalf("task project = %q, want fork", task.ProjectID)
	}
	if workspace.workDir != forkCheckout {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, forkCheckout)
	}
}

func TestServiceRoutesGitHubIssueRepoDeterministically(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	firstForkCheckout := t.TempDir()
	secondForkCheckout := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "z-fork", Name: "Second Fork", LocalPath: secondForkCheckout, Repo: "second/repo", UpstreamRepo: "owner/repo"},
		{ID: "a-fork", Name: "First Fork", LocalPath: firstForkCheckout, Repo: "first/repo", UpstreamRepo: "owner/repo"},
	}, "z-fork")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, secondForkCheckout, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "GitHub issue owner/repo#1",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"source": "github-issue", "repo": "owner/repo"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "a-fork" {
		t.Fatalf("task project = %q, want a-fork", task.ProjectID)
	}
	if workspace.workDir != firstForkCheckout {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, firstForkCheckout)
	}
}

func TestServiceKeepsLocalRepoLookupWhenNotGitHubIssue(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	upstreamCheckout := t.TempDir()
	forkCheckout := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "upstream", Name: "Upstream", LocalPath: upstreamCheckout, Repo: "owner/repo"},
		{ID: "fork", Name: "Fork", LocalPath: forkCheckout, Repo: "fork-owner/repo", UpstreamRepo: "owner/repo"},
	}, "fork")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, forkCheckout, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Local repo task",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"repo": "owner/repo"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "upstream" {
		t.Fatalf("task project = %q, want upstream", task.ProjectID)
	}
	if workspace.workDir != upstreamCheckout {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, upstreamCheckout)
	}
}

func TestServicePublishesPullRequestUsingProjectDefaults(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   projectRoot,
		Repo:        "owner/repo",
		DefaultBase: "trunk",
		PullRequestPolicy: core.PullRequestPolicy{
			BranchPrefix: "aged/custom-",
			Draft:        true,
			AllowMerge:   true,
			AutoMerge:    false,
		},
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, projectRoot, fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: projectRoot,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "repo", Title: "Implement feature", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if _, err := service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{}); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q", publisher.published.Repo)
	}
	if publisher.published.Base != "trunk" {
		t.Fatalf("published base = %q", publisher.published.Base)
	}
	if publisher.published.BranchPrefix != "aged/custom-" {
		t.Fatalf("published branch prefix = %q", publisher.published.BranchPrefix)
	}
	if !publisher.published.Draft {
		t.Fatalf("published draft = false, want project policy draft")
	}
	if publisher.published.WorkDir != taskWorkspaceCWD(snapshot, task.ID) {
		t.Fatalf("published workDir = %q, want worker workspace", publisher.published.WorkDir)
	}
	if publisher.published.HeadRepoOwner != "" || publisher.published.PushRemote != "" {
		t.Fatalf("non-fork publish spec had fork fields: %+v", publisher.published)
	}
}

func TestServicePublishesForkPullRequestUsingProjectConfig(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:            "fork",
		Name:          "Fork",
		LocalPath:     projectRoot,
		Repo:          "fork-owner/repo",
		UpstreamRepo:  "owner/repo",
		HeadRepoOwner: "fork-owner",
		PushRemote:    "fork",
		DefaultBase:   "trunk",
	}}, "fork")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, projectRoot, fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: projectRoot,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "fork", Title: "Implement feature", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if _, err := service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{}); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q, want owner/repo", publisher.published.Repo)
	}
	if publisher.published.HeadRepoOwner != "fork-owner" {
		t.Fatalf("published head owner = %q, want fork-owner", publisher.published.HeadRepoOwner)
	}
	if publisher.published.PushRemote != "fork" {
		t.Fatalf("published push remote = %q, want fork", publisher.published.PushRemote)
	}
	if publisher.published.Base != "trunk" {
		t.Fatalf("published base = %q, want trunk", publisher.published.Base)
	}
}

func TestServiceRefreshesPullRequestStatus(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		status: core.PullRequest{
			State:            "OPEN",
			ChecksConclusion: "SUCCESS",
			Mergeable:        "MERGEABLE",
			ReviewStatus:     "APPROVED",
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "run")}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	pr, err := service.RefreshPullRequest(ctx, "pr-1")
	if err != nil {
		t.Fatal(err)
	}
	if pr.ChecksStatus != "passing" || pr.ChecksConclusion != "SUCCESS" || pr.MergeStatus != "MERGEABLE" || pr.Mergeable != "MERGEABLE" || pr.ReviewStatus != "APPROVED" {
		t.Fatalf("refreshed pr = %+v", pr)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PullRequests[0].ChecksStatus != "passing" || snapshot.PullRequests[0].ChecksConclusion != "SUCCESS" || snapshot.PullRequests[0].MergeStatus != "MERGEABLE" || snapshot.PullRequests[0].Mergeable != "MERGEABLE" {
		t.Fatalf("snapshot pr = %+v", snapshot.PullRequests[0])
	}
	if snapshot.Tasks[0].ObjectivePhase != "ready_to_merge" {
		t.Fatalf("objective phase = %q, want ready_to_merge", snapshot.Tasks[0].ObjectivePhase)
	}
}

func TestServiceAttachesPullRequestBabysittingToSourceTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "babysit",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	task, err := service.StartPullRequestBabysitter(ctx, "pr-1")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if task.ID != "task-1" {
		t.Fatalf("babysitter task = %q, want source task", task.ID)
	}
	if !hasEvent(snapshot.Events, core.EventPRBabysitter, "task-1", "") {
		t.Fatalf("missing pr babysitter event")
	}
	var found bool
	for _, pr := range snapshot.PullRequests {
		if pr.ID == "pr-1" && pr.BabysitterTaskID == task.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("pull request did not point at babysitter task: %+v", snapshot.PullRequests)
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

func TestServiceRunsDurableLoopModeWithoutBrainPlanning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventResult, Text: "LEDGER_FACT: loop learned the repo uses generated fixtures"}},
			{{Kind: worker.EventNeedsInput, Text: "need user input"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 0,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if calls := runner.callsValue(); calls != 2 {
		t.Fatalf("runner calls = %d, want 2", calls)
	}
	if count := countEvents(snapshot.Events, core.EventTaskPlanned, task.ID); count != 2 {
		t.Fatalf("task.planned count = %d, want 2", count)
	}
	if !strings.Contains(runner.promptValue(), "# Durable Agent Loop") {
		t.Fatalf("runner prompt missing loop context:\n%s", runner.promptValue())
	}
	assertDurableLoopPlaybookGuidance(t, runner.promptValue())
	if !strings.Contains(runner.promptValue(), "# Continuation Context") {
		t.Fatalf("runner prompt missing loop continuation context:\n%s", runner.promptValue())
	}
	if strings.Contains(runner.promptValue(), "previously failed or canceled") {
		t.Fatalf("runner prompt used retry wording for loop continuation:\n%s", runner.promptValue())
	}
	if !strings.Contains(runner.promptValue(), "# Context Ledger") || !strings.Contains(runner.promptValue(), "generated fixtures") {
		t.Fatalf("runner prompt missing durable context ledger:\n%s", runner.promptValue())
	}
	if !hasTaskAction(snapshot.Events, task.ID, "durable_loop", "waiting_for_input") {
		t.Fatalf("missing durable loop waiting action")
	}
	if hasTaskAction(snapshot.Events, task.ID, "durable_loop", "paused") {
		t.Fatalf("loop should only stop on worker input or cancelation")
	}
}

func TestServiceRecoveredRemoteWorkerContinuesDurableLoop(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "need user input"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not replan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	taskID := "loop-task"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep looking for bugs.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 0,
			},
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
		Type:   core.EventTaskPlanned,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"workerKind": "loop",
			"prompt":     "iteration 1",
			"metadata": map[string]any{
				"executionMode":  "loop",
				"loopIteration":  1,
				"loopWorkerKind": "loop",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   "worker-1",
			"workerKind": "loop",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "loop",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "iteration 1 complete",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service.resumeRecoveredRemoteTask(ctx, taskID)

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if runner.callsValue() != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.callsValue())
	}
	if !strings.Contains(runner.promptValue(), "iteration 2") {
		t.Fatalf("runner prompt did not resume at iteration 2:\n%s", runner.promptValue())
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, taskID) != 0 {
		t.Fatalf("loop recovery should not enter normal replan")
	}
	if countEvents(snapshot.Events, core.EventTaskStatus, taskID) == 0 || snapshot.Tasks[0].Status != core.TaskWaiting {
		t.Fatalf("task = %+v", snapshot.Tasks)
	}
	if !hasTaskAction(snapshot.Events, taskID, "durable_loop", "waiting_for_input") {
		t.Fatalf("missing durable loop waiting action")
	}
}

func TestServiceRetriesSucceededDurableLoopTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "need user input"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not replan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	taskID := "loop-task"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep looking for bugs.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 0,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"workerKind": "loop",
			"prompt":     "iteration 1",
			"metadata": map[string]any{
				"executionMode":  "loop",
				"loopIteration":  1,
				"loopWorkerKind": "loop",
			},
		}),
	}); err != nil {
		t.Fatal(err)
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

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if runner.callsValue() != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.callsValue())
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, taskID) != 0 {
		t.Fatalf("loop retry should not enter normal replan")
	}
	if !hasTaskAction(snapshot.Events, taskID, "durable_loop", "waiting_for_input") {
		t.Fatalf("missing durable loop waiting action")
	}
}

func TestServiceUpdatesDurableLoopInterval(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "pause after first iteration"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 300,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)

	updated, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{LoopIntervalSeconds: ptrInt(30)})
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if intMetadata(metadata, "loopIntervalSeconds") != 30 {
		t.Fatalf("loopIntervalSeconds = %v, want 30", metadata["loopIntervalSeconds"])
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "loop_config_updated", "updated") {
		t.Fatalf("missing loop config update action")
	}
}

func TestServiceUpdatesDurableLoopPrompt(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "pause after first iteration"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Original durable objective.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 300,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)

	updated, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{LoopPrompt: ptrString("  Updated standing loop objective.  ")})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Prompt != "Original durable objective." {
		t.Fatalf("task prompt = %q, want original prompt preserved", updated.Prompt)
	}
	var metadata map[string]any
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(metadata["loopPrompt"]); got != "Updated standing loop objective." {
		t.Fatalf("loopPrompt = %q, want updated standing objective", got)
	}
	config := durableLoopConfigFromTask(updated, service.runners)
	if config.Prompt != "Updated standing loop objective." {
		t.Fatalf("config prompt = %q, want updated loop prompt", config.Prompt)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "loop_config_updated", "updated") {
		t.Fatalf("missing loop config update action")
	}
}

func TestServiceUpdatesDurableLoopRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "pause after first iteration"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 300,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)

	updated, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{RequiredTargetID: ptrString("  vm-fast  ")})
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(metadata["requiredTargetID"]); got != "vm-fast" {
		t.Fatalf("requiredTargetID = %q, want vm-fast", got)
	}

	updated, err = service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{RequiredTargetID: ptrString("")})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(metadata["requiredTargetID"]); got != "" {
		t.Fatalf("requiredTargetID after clear = %q, want empty", got)
	}
}

func TestDurableLoopUsesUpdatedConfigForNextIteration(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventResult, Text: "iteration 1 done"}},
			{{Kind: worker.EventNeedsInput, Text: "pause after iteration 2"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Original loop objective.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 10,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasIterationCompletedAction(snapshot.Events, task.ID, 1)
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("iteration 1 did not complete; events = %+v", snapshot.Events)
	})

	if _, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{
		LoopPrompt: ptrString("Updated loop objective for next iteration."),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{
		LoopIntervalSeconds: ptrInt(0),
	}); err != nil {
		t.Fatal(err)
	}

	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if runner.callsValue() != 2 {
		t.Fatalf("runner calls = %d, want 2", runner.callsValue())
	}
	if !strings.Contains(runner.promptValue(), "Updated loop objective for next iteration.") {
		t.Fatalf("iteration 2 prompt did not use updated loopPrompt:\n%s", runner.promptValue())
	}
}

func hasIterationCompletedAction(events []core.Event, taskID string, iteration int) bool {
	for _, event := range events {
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind      string `json:"kind"`
			Status    string `json:"status"`
			Iteration int    `json:"iteration"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == "durable_loop" && payload.Status == "iteration_completed" && payload.Iteration == iteration {
			return true
		}
	}
	return false
}

func TestDurableLoopIntervalWaitObservesConfigUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sqliteStore := openTestStore(t)
	store := &snapshotCountingStore{Store: sqliteStore}
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"loop": eventRunner{kind: "loop"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	taskID := "loop-task"
	if _, err := service.append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep making bounded progress.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 30,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		done <- service.waitDurableLoopInterval(ctx, taskID, 30*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := service.append(ctx, core.Event{
		Type:   core.EventTaskUpdated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"metadataPatch": map[string]any{
				"loopIntervalSeconds": 0,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("wait returned after %s, want it to observe the interval update promptly", elapsed)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := store.snapshotCount(); got != 0 {
		t.Fatalf("Snapshot calls = %d, want 0", got)
	}
}

func TestDurableLoopIntervalWaitStopsOnTerminalTaskStatusWithoutSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sqliteStore := openTestStore(t)
	store := &snapshotCountingStore{Store: sqliteStore}
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"loop": eventRunner{kind: "loop"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	taskID := "loop-task"
	if _, err := service.append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep making bounded progress.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 30,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- service.waitDurableLoopInterval(ctx, taskID, 30*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	if err := service.setTaskStatus(ctx, taskID, core.TaskCanceled); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errDurableLoopTaskTerminal) {
			t.Fatalf("error = %v, want terminal task error", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := store.snapshotCount(); got != 0 {
		t.Fatalf("Snapshot calls = %d, want 0", got)
	}
}

func TestServiceRejectsLoopIntervalUpdateForNonLoopTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "do it",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "One shot", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{LoopIntervalSeconds: ptrInt(30)})
	if err == nil || !strings.Contains(err.Error(), "not a durable loop") {
		t.Fatalf("error = %v, want durable loop rejection", err)
	}
}

func TestServiceFailsDurableLoopWithMissingExplicitRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "should not run"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "codex",
			"loopIntervalSeconds": 0,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	failed, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatalf("missing task %s", task.ID)
	}
	if !strings.Contains(failed.Error, `loop worker kind "codex" is not configured`) {
		t.Fatalf("task error = %q", failed.Error)
	}
	if count := countEvents(snapshot.Events, core.EventWorkerCreated, task.ID); count != 0 {
		t.Fatalf("worker.created count = %d, want 0", count)
	}
}

func TestServiceRetriesFailedTaskFromPersistedPlan(t *testing.T) {
	t.Skip("legacy persisted plan retry reconstruction was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &flakyRunner{kind: "retryable"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "retryable",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"retryable": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskFailed)

	retried, err := service.RetryTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != task.ID {
		t.Fatalf("retry returned task %q, want %q", retried.ID, task.ID)
	}
	if retried.Error != "" || retried.ObjectiveStatus != core.ObjectiveActive || retried.ObjectivePhase != "retrying" {
		t.Fatalf("retried task did not reset failed objective state: %+v", retried)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.callsValue() != 2 {
		t.Fatalf("runner calls = %d, want 2", runner.callsValue())
	}
	if countEvents(snapshot.Events, core.EventTaskCreated, task.ID) != 1 {
		t.Fatalf("retry created a new task")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 2 {
		t.Fatalf("worker.created count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
}

func TestServiceRetryFailsWhenExplicitTaskProjectWasDeleted(t *testing.T) {
	t.Skip("legacy persisted plan retry reconstruction was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	defaultProject := core.Project{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"}
	deletedProject := core.Project{ID: "deleted", Name: "Deleted", LocalPath: t.TempDir(), DefaultBase: "main"}
	if _, err := store.SaveProject(ctx, defaultProject, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveProject(ctx, deletedProject, false); err != nil {
		t.Fatal(err)
	}
	projects, err := NewProjectRegistry([]core.Project{defaultProject, deletedProject}, defaultProject.ID)
	if err != nil {
		t.Fatal(err)
	}
	runner := &flakyRunner{kind: "retryable"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "retryable",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"retryable": runner}, defaultProject.LocalPath, fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		ProjectID: deletedProject.ID,
		Title:     "Do project work",
		Prompt:    "User request",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	if err := service.DeleteProject(ctx, deletedProject.ID); err != nil {
		t.Fatal(err)
	}

	_, err = service.RetryTask(ctx, task.ID)
	if err == nil || !strings.Contains(err.Error(), `unknown projectId "deleted"`) {
		t.Fatalf("retry err = %v, want missing explicit project", err)
	}
	if runner.callsValue() != 1 {
		t.Fatalf("runner calls = %d, want retry to stop before rerun", runner.callsValue())
	}
	if _, err := service.projectForTaskID(ctx, task.ID); err == nil || !strings.Contains(err.Error(), `unknown projectId "deleted"`) {
		t.Fatalf("projectForTaskID err = %v, want missing explicit project", err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 1 {
		t.Fatalf("worker.created count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
}

func TestServiceRetriesCanceledTaskFromPersistedPlan(t *testing.T) {
	t.Skip("legacy canceled worker retry reconstruction was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-canceled"
	plan := Plan{WorkerKind: "retryable", Prompt: "resume canceled work"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Canceled work",
			"prompt": "Pick up where the canceled worker left off.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	runner := eventRunner{kind: "retryable", events: []worker.Event{{Kind: worker.EventResult, Text: "resumed"}}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{"retryable": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	retried, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != taskID || retried.Status != core.TaskPlanning {
		t.Fatalf("retried = %+v", retried)
	}
	if retried.ObjectiveStatus != core.ObjectiveActive || retried.ObjectivePhase != "retrying" {
		t.Fatalf("retried objective = %q/%q, want active/retrying", retried.ObjectiveStatus, retried.ObjectivePhase)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if countEvents(snapshot.Events, core.EventTaskPlanned, taskID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, taskID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("worker.created count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestRecoverRemoteWorkersRetriesStartupCanceledLocalTask(t *testing.T) {
	t.Skip("legacy startup retry reconstruction was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-startup-canceled"
	plan := Plan{WorkerKind: "retryable", Prompt: "resume startup-canceled work"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Startup canceled work",
			"prompt": "Pick up where the worker left off.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: "old-worker",
		Payload: core.MustJSON(map[string]any{
			"kind":   "retryable",
			"prompt": "old attempt",
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

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{
		"retryable": eventRunner{kind: "retryable", events: []worker.Event{{Kind: worker.EventResult, Text: "resumed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if !hasTaskAction(snapshot.Events, taskID, "startup_auto_retry", "retrying") {
		t.Fatalf("missing startup auto-retry action")
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 2 {
		t.Fatalf("worker.created count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCompleted, taskID) != 2 {
		t.Fatalf("worker.completed count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCompleted, taskID))
	}
}

func TestRecoverRemoteWorkersDoesNotRetryManualCanceledTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-manual-canceled"
	plan := Plan{WorkerKind: "retryable", Prompt: "manual retry only"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Manual canceled work",
			"prompt": "Do not retry automatically.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{
		"retryable": eventRunner{kind: "retryable", events: []worker.Event{{Kind: worker.EventResult, Text: "should not run"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatalf("missing task %s", taskID)
	}
	if task.Status != core.TaskCanceled {
		t.Fatalf("task status = %s, want canceled", task.Status)
	}
	if hasTaskAction(snapshot.Events, taskID, "startup_auto_retry", "retrying") {
		t.Fatalf("manual cancellation was auto-retried")
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 0 {
		t.Fatalf("worker.created count = %d, want 0", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestServiceRetriesDynamicReplanFailureFromCompletedGraph(t *testing.T) {
	tests := []struct {
		name  string
		error string
	}{
		{
			name:  "decode failure",
			error: "dynamic replan failed: decode codex replan decision: invalid character '}' after top-level value",
		},
		{
			name:  "unknown spawn dependency",
			error: `spawn "next_hotspot_planner" depends on unknown spawn "review_header_path"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := openTestStore(t)
			defer store.Close()

			taskID := "task-retry-graph"
			workerID := "worker-done"
			initial := Plan{WorkerKind: "codex", Prompt: "implement the change"}
			if _, err := store.Append(ctx, core.Event{
				Type:   core.EventTaskCreated,
				TaskID: taskID,
				Payload: core.MustJSON(map[string]any{
					"title":  "Graph retry",
					"prompt": "Retry only replan.",
				}),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, core.Event{
				Type:    core.EventTaskPlanned,
				TaskID:  taskID,
				Payload: core.MustJSON(initial),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, core.Event{
				Type:     core.EventWorkerCreated,
				TaskID:   taskID,
				WorkerID: workerID,
				Payload: core.MustJSON(map[string]any{
					"kind": "codex",
					"metadata": map[string]any{
						"nodeID": "node-1",
					},
				}),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, core.Event{
				Type:     core.EventWorkerCompleted,
				TaskID:   taskID,
				WorkerID: workerID,
				Payload: core.MustJSON(map[string]any{
					"status":  core.WorkerSucceeded,
					"summary": "implemented",
					"workspaceChanges": WorkspaceChanges{
						Dirty:        true,
						ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
					},
				}),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, core.Event{
				Type:   core.EventTaskStatus,
				TaskID: taskID,
				Payload: core.MustJSON(map[string]any{
					"status": core.TaskFailed,
					"error":  tt.error,
				}),
			}); err != nil {
				t.Fatal(err)
			}

			service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
			retried, err := service.RetryTask(ctx, taskID)
			if err != nil {
				t.Fatal(err)
			}
			if retried.Status != core.TaskPlanning {
				t.Fatalf("retry status = %q", retried.Status)
			}
			snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
			if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
				t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
			}
		})
	}
}

func TestServiceRetriesGraphDependencyFailureWithoutCandidateChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-graph-no-candidate"
	workerID := "worker-validate"
	initial := Plan{
		WorkerKind: "codex",
		Prompt:     "Validate the http compressible size slice.",
	}
	invalidLatestPlan := Plan{
		Workers: []WorkerRequest{{
			ID:         "source_next_opportunity_scout",
			WorkerKind: "codex",
			Prompt:     "Find the next opportunity.",
			DependsOn:  []string{"validate_http_compressible_size_slice"},
		}},
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Broad graph retry",
			"prompt": "Retry only replan.",
			"metadata": map[string]any{
				"objectiveMode": "broad",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(initial)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
			"metadata": map[string]any{
				"nodeID": "validate_http_compressible_size_slice",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":           core.WorkerSucceeded,
			"summary":          "validated the slice and found no publishable candidate yet",
			"workspaceChanges": WorkspaceChanges{},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(invalidLatestPlan)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
			"error":  `worker "source_next_opportunity_scout" depends on unknown worker "validate_http_compressible_size_slice"`,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:    "wait",
		Rationale: "graph dependency retry should ask the replanner",
		Message:   "waiting after graph retry",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	retried, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != core.TaskPlanning || retried.ObjectiveStatus != core.ObjectiveActive || retried.ObjectivePhase != "retrying" {
		t.Fatalf("retried = %+v", retried)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if len(brain.states) != 1 {
		t.Fatalf("replan calls = %d, want 1", len(brain.states))
	}
	if got := len(brain.states[0].Results); got != 1 {
		t.Fatalf("replan result count = %d, want 1", got)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
	if countTaskStatusErrors(snapshot.Events, taskID, core.TaskFailed, "depends on unknown worker") != 1 {
		t.Fatalf("retry reran the invalid latest plan")
	}
}

func TestServiceRetriesFollowUpFailureFromCompletedGraph(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-follow-up"
	workerID := "worker-impl"
	reviewID := "worker-review"
	initial := Plan{WorkerKind: "codex", Prompt: "implement the change"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Follow-up failure retry",
			"prompt": "Retry orchestration after review failure.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(initial)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"kind": "codex", "metadata": map[string]any{"nodeID": "node-1"}}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: reviewID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "claude",
			"metadata": map[string]any{"nodeID": "node-2", "baseWorkerID": workerID, "spawnRole": "review"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: reviewID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerFailed,
			"error":  "worker command failed: exit status 1",
			"workspaceChanges": WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	_, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 2 {
		t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestServiceRetryReusesCanceledWorkerWorkspaceAndSession(t *testing.T) {
	t.Skip("legacy canceled worker session retry reconstruction was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-resume"
	previousWorkerID := "worker-old"
	workspaceRoot := t.TempDir()
	sourceRoot := t.TempDir()
	freshWorkspaceRoot := t.TempDir()
	plan := Plan{WorkerKind: "codex", Prompt: "continue the partial implementation"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Resume canceled work",
			"prompt": "Continue the task.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-old",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          workspaceRoot,
			CWD:           workspaceRoot,
			SourceRoot:    sourceRoot,
			WorkspaceName: "aged-old",
			Mode:          string(WorkspaceModeIsolated),
			VCSType:       "jj",
			TaskID:        taskID,
			WorkerID:      previousWorkerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(worker.Event{
			Kind:   worker.EventLog,
			Stream: "stdout",
			Text:   `{"type":"thread.started","thread_id":"thread-1"}`,
			Raw:    json.RawMessage(`{"type":"thread.started","thread_id":"thread-1"}`),
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	runner := &recordingRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{"codex": runner}, sourceRoot, fakeWorkspaceManager{
		cwd:        freshWorkspaceRoot,
		sourceRoot: sourceRoot,
	})

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if runner.workDir != workspaceRoot {
		t.Fatalf("runner workDir = %q, want retained workspace %q", runner.workDir, workspaceRoot)
	}
	if runner.resumeSessionID != "thread-1" {
		t.Fatalf("resume session = %q, want thread-1", runner.resumeSessionID)
	}
	if !strings.Contains(runner.prompt, "Previous worker ID: "+previousWorkerID) {
		t.Fatalf("runner prompt missing retry context:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Run every command from this execution workspace:\n"+workspaceRoot) {
		t.Fatalf("runner prompt missing retained workspace:\n%s", runner.prompt)
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, taskID, `"retryWorkspaceReused":true`) {
		t.Fatalf("missing retry workspace reuse metadata")
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, taskID, "retryResumeSessionID", "thread-1") {
		t.Fatalf("missing retry session metadata")
	}
}

func TestServiceGuardsWorkerPromptWithPreparedWorkspace(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := filepath.Join(t.TempDir(), "source")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "Inspect " + sourceRoot + " and make the requested edit.",
	}}, map[string]worker.Runner{"codex": runner}, sourceRoot, fakeWorkspaceManager{cwd: workspaceRoot, sourceRoot: sourceRoot})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do isolated work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.workDir != workspaceRoot {
		t.Fatalf("runner workDir = %q, want %q", runner.workDir, workspaceRoot)
	}
	if !strings.Contains(runner.prompt, "Run every command from this execution workspace:\n"+workspaceRoot) {
		t.Fatalf("worker prompt did not name prepared workspace first:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Do not edit the source checkout directly:\n"+sourceRoot) {
		t.Fatalf("worker prompt did not guard source checkout:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Inspect "+sourceRoot+" and make the requested edit.") {
		t.Fatalf("worker prompt dropped original task:\n%s", runner.prompt)
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

func TestRecoverRemoteWorkersResumesRunningTaskWithTerminalGraph(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-orphan-running-graph"
	workerID := "worker-impl"
	initial := Plan{WorkerKind: "codex", Prompt: "implement the cleanup"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Orphaned running graph",
			"prompt": "Recover after follow-up setup failure.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(initial)}); err != nil {
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
			"nodeId":     "node-impl",
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
			"kind":     "codex",
			"metadata": map[string]any{"nodeID": "node-impl"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/cleanup.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	followUp := Plan{
		WorkerKind: "codex",
		Prompt:     "review the cleanup",
		Metadata: map[string]any{
			"nodeID":       "node-review",
			"spawnID":      "review",
			"spawnRole":    "reviewer",
			"baseWorkerID": workerID,
		},
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(followUp)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventExecutionStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"nodeId": "node-review",
			"status": core.WorkerFailed,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:    "complete",
		Rationale: "primary worker already produced the candidate",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if !hasTaskAction(snapshot.Events, taskID, "startup_running_recovery", "resumed") {
		t.Fatalf("missing startup running recovery action")
	}
	if len(brain.states) != 1 || len(brain.states[0].Results) != 1 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("recovery reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestRecoverRemoteWorkersRetriesOrphanedPullRequestFollowUpPlan(t *testing.T) {
	t.Skip("legacy orphaned PR follow-up plan retry was replaced by durable pr work items")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-orphan-pr-followup"
	initialWorkerID := "worker-original"
	followUpWorkerID := "worker-followup"
	followUpPlan := Plan{
		Workers: []WorkerRequest{{
			ID:         "respond_review",
			Role:       "repair or respond",
			Reason:     "Address the latest PR review thread.",
			WorkerKind: "codex",
			Prompt:     "Inspect the PR thread, fix code if needed, otherwise reply.",
		}},
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return PR to monitoring",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
		Metadata: map[string]any{
			"pullRequestID":        "pr-1",
			"workspaceBaseRef":     "codex/aged-test",
			"workspaceBaseRefKind": "pull_request_head",
		},
	}
	events := []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":    "Repair PR",
				"prompt":   "Fix the pull request and keep watching it.",
				"metadata": map[string]any{},
			}),
		},
		{
			Type:    core.EventTaskPlanned,
			TaskID:  taskID,
			Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "Implement the original change."}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskRunning,
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: initialWorkerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: initialWorkerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "implemented original change",
				"workspaceChanges": WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "mod.ts", Status: "modified"}},
				},
			}),
		},
		{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-1",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"branch": "codex/aged-test",
				"base":   "main",
				"state":  "OPEN",
			}),
		},
		{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":      "pr-1",
				"attempt": 1,
				"reason":  "pull_request_needs_work",
			}),
		},
		{
			Type:    core.EventTaskPlanned,
			TaskID:  taskID,
			Payload: core.MustJSON(followUpPlan),
		},
		{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: followUpWorkerID,
			Payload: core.MustJSON(map[string]any{
				"workerId":   followUpWorkerID,
				"workerKind": "codex",
				"nodeId":     "node-followup",
				"targetId":   "local",
				"targetKind": "local",
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: followUpWorkerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
				"metadata": map[string]any{
					"nodeID":  "node-followup",
					"spawnID": "respond_review",
				},
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: followUpWorkerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerCanceled,
				"summary": "Worker was canceled from persisted daemon state.",
				"error":   "worker did not have a live local cancellation handle",
			}),
		},
	}
	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	runner := &recordingEventRunner{
		kind:   "codex",
		events: []worker.Event{{Kind: worker.EventResult, Text: "rechecked review thread"}},
	}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if runner.callsValue() != 1 {
		t.Fatalf("follow-up runner calls = %d, want 1", runner.callsValue())
	}
	if publisher.publishCalls != 0 || publisher.updateCalls != 0 {
		t.Fatalf("publish calls = %d update calls = %d, want no implicit PR publication", publisher.publishCalls, publisher.updateCalls)
	}
	if !hasTaskAction(snapshot.Events, taskID, "startup_running_recovery", "resumed") {
		t.Fatalf("missing startup running recovery action")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, `"retryFromWorkerID":"`+followUpWorkerID+`"`) {
		t.Fatalf("missing follow-up retry metadata")
	}
}

func TestRecoverRemoteWorkersResumesOrphanedPullRequestFollowUpPlanning(t *testing.T) {
	t.Skip("legacy orphaned PR follow-up planning recovery was replaced by durable pr work items")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-dirty-pr"
	appendInterruptedPullRequestFollowUpPlanning(t, ctx, store, taskID)

	brain := &sequenceBrain{plans: []Plan{{
		WorkerKind: "repair",
		Prompt:     "repair dirty PR",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"repair": eventRunner{kind: "repair", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "repaired dirty PR branch",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/repair.go", Status: "modified"}},
		},
	})

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForEvent(t, store, core.EventWorkerCreated, taskID)
	if !hasTaskAction(snapshot.Events, taskID, "startup_planning_recovery", "resumed") {
		t.Fatalf("missing startup planning recovery action")
	}
	if !hasWorkerCreated(snapshot.Events, taskID, "repair") {
		t.Fatalf("missing recovered repair worker")
	}
	if got := strings.Join(brain.steering, "\n"); !strings.Contains(got, "Merge status: DIRTY") {
		t.Fatalf("recovered planning did not preserve PR steering: %q", got)
	}
}

func TestRecoverRemoteWorkersRestartsGenericOrphanedPlanningTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-orphan-planning"
	oldWorkItemID := "old-objective-plan"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Interrupted planning",
			"prompt": "Plan was interrupted by daemon restart.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskPlanning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventWorkItemQueued,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":         oldWorkItemID,
			"kind":       "objective.plan",
			"targetKind": "objective",
			"targetId":   taskID,
			"reason":     "interrupted initial planning",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventWorkItemStarted,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":         oldWorkItemID,
			"leaseOwner": "daemon:local",
			"leaseUntil": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		}),
	}); err != nil {
		t.Fatal(err)
	}

	runner := &recordingEventRunner{
		kind:   "mock",
		events: []worker.Event{{Kind: worker.EventResult, Text: "planned after restart"}},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "continue after interrupted planning",
	}}, map[string]worker.Runner{"mock": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return runner.callsValue() == 1
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("runner calls = %d, want 1; events = %s", runner.callsValue(), taskEventSummary(snapshot.Events, taskID))
	})
	if !hasTaskAction(snapshot.Events, taskID, "startup_planning_recovery", "resumed") {
		t.Fatalf("missing startup planning recovery action")
	}
	if hasEvent(snapshot.Events, core.EventApprovalNeeded, taskID, "") {
		t.Fatalf("planning recovery should not ask for user input")
	}
	oldWorkItem, ok := workItemByIDFromSnapshot(snapshot, taskID, oldWorkItemID)
	if !ok {
		t.Fatalf("missing old planning work item")
	}
	if oldWorkItem.Status != core.WorkItemFailed {
		t.Fatalf("old planning work item status = %q, want failed", oldWorkItem.Status)
	}
	if !strings.Contains(oldWorkItem.Error, "superseded by startup planning recovery") {
		t.Fatalf("old planning work item error = %q", oldWorkItem.Error)
	}
}

func TestRecoverRemoteWorkersResumesOrphanedGraphReplanning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-orphan-graph-replanning"
	workerID := "worker-done"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Interrupted graph replanning",
			"prompt": "Replan was interrupted by daemon restart.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "initial"}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "codex",
			"metadata": map[string]any{"nodeID": "initial-scout"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "found next step",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskPlanning,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:    "wait",
		Rationale: "graph replanning resumed after restart",
		Message:   "waiting from resumed graph replan",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if len(brain.states) != 1 {
		t.Fatalf("replan calls = %d, want 1", len(brain.states))
	}
	if got := len(brain.states[0].Results); got != 1 {
		t.Fatalf("replan results = %d, want 1", got)
	}
	if !hasTaskAction(snapshot.Events, taskID, "startup_planning_recovery", "resumed") {
		t.Fatalf("missing startup planning recovery action")
	}
	if eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, taskID, "Planning was interrupted") {
		t.Fatalf("planning recovery should not ask for user input")
	}
}

func TestCancelTaskCancelsPersistedActiveWorkers(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-replayed"
	workerIDs := []string{"worker-running", "worker-queued"}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Replayed task",
			"prompt": "Was active before daemon restart",
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
	for _, workerID := range workerIDs {
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-" + workerID,
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
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: "worker-running",
		Payload:  core.MustJSON(map[string]any{}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "work-queued",
		"kind":       "user.question",
		"targetKind": "task",
		"targetId":   taskID,
		"reason":     "waiting for input",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "work-running",
		"kind":       "pr.followup",
		"targetKind": "pull_request",
		"targetId":   "pr-1",
		"reason":     "handling PR feedback",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemStarted(ctx, taskID, "work-running", "worker-running"); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("taskHasActiveWorkers before cancel = false, want true")
	}

	if err := service.CancelTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("taskHasActiveWorkers after cancel = true, want false")
	}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID && worker.Status != core.WorkerCanceled {
			t.Fatalf("worker %s status = %q, want canceled", worker.ID, worker.Status)
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.Status != core.WorkerCanceled {
			t.Fatalf("node %s status = %q, want canceled", node.ID, node.Status)
		}
	}
	for _, item := range snapshot.WorkItems {
		if item.TaskID == taskID && item.Status != core.WorkItemCanceled {
			t.Fatalf("work item %s status = %q, want canceled", item.ID, item.Status)
		}
	}
	if status := taskStatus(snapshot, taskID); status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", status)
	}
}

func TestCancelWorkItemCancelsQueuedItem(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	taskID := "task-cancel-work-item"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Cancelable work",
			"prompt": "Run queued work.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "queued-work",
		"kind":       "objective.slice",
		"targetKind": "slice",
		"targetId":   "slice-a",
		"reason":     "queued slice",
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.CancelWorkItem(ctx, taskID, "queued-work"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := workItemByID(snapshot, "queued-work")
	if !ok || item.Status != core.WorkItemCanceled {
		t.Fatalf("work item = %+v ok=%v, want canceled", item, ok)
	}
}

func TestCancelWorkItemDecidesUserQuestion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	taskID := "task-cancel-user-question"
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Cancelable question",
			"prompt": "Ask before continuing.",
		}),
	})
	if err := service.waitForUserAction(ctx, taskID, "", "dynamic_replan_limit", "Provide steering.", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Questions) != 1 {
		t.Fatalf("questions = %+v, want one pending question", snapshot.Questions)
	}
	questionID := snapshot.Questions[0].ID
	approvalEventID, ok := approvalEventIDFromQuestionID(questionID)
	if !ok {
		t.Fatalf("question id = %q, want approval event id", questionID)
	}
	workItemID := userQuestionWorkItemID(approvalEventID)

	if err := service.CancelWorkItem(ctx, taskID, workItemID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	question := questionByID(snapshot, questionID)
	if !question.Decided || question.Approved == nil || *question.Approved {
		t.Fatalf("question = %+v, want decided with rejected approval", question)
	}
	if question.Answer != "work item canceled by user request" {
		t.Fatalf("question answer = %q, want cancel reason", question.Answer)
	}
	item, ok := workItemByID(snapshot, workItemID)
	if !ok || item.Status != core.WorkItemCanceled {
		t.Fatalf("work item = %+v ok=%v, want canceled", item, ok)
	}
}

func TestCancelWorkItemCancelsRunningWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	taskID := "task-cancel-running-work-item"
	workerID := "worker-running-work-item"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Cancelable worker work",
			"prompt": "Run worker-backed work.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "running-work",
		"kind":       "objective.slice",
		"targetKind": "worker",
		"targetId":   workerID,
		"reason":     "running slice",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemStarted(ctx, taskID, "running-work", workerID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	startedItem, ok := workItemByID(snapshot, "running-work")
	if !ok || startedItem.Status != core.WorkItemRunning || startedItem.LeaseOwner != "worker:"+workerID || startedItem.LeaseUntil == nil || startedItem.Attempt != 1 {
		t.Fatalf("started work item = %+v ok=%v, want worker lease", startedItem, ok)
	}
	canceled := false
	service.mu.Lock()
	service.cancels[workerID] = func() { canceled = true }
	service.tasks[workerID] = taskID
	service.mu.Unlock()

	if err := service.CancelWorkItem(ctx, taskID, "running-work"); err != nil {
		t.Fatal(err)
	}
	if !canceled {
		t.Fatal("worker cancel func was not called")
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := workItemByID(snapshot, "running-work")
	if !ok || item.Status != core.WorkItemCanceled || item.WorkerID != workerID {
		t.Fatalf("work item = %+v ok=%v, want canceled with worker", item, ok)
	}
	if item.LeaseOwner != "" || item.LeaseUntil != nil || item.Attempt != 1 {
		t.Fatalf("canceled work item lease = owner %q until %v attempt %d; want cleared lease and preserved attempt", item.LeaseOwner, item.LeaseUntil, item.Attempt)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerCompleted, taskID, workerID) {
		t.Fatal("missing worker.completed cancellation event")
	}
}

func TestCancelTaskAfterRestartReconstructsWorkersFromSnapshot(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-restarted"
	remoteWorkerID := "worker-recovered-remote"
	persistedWorkerID := "worker-persisted-local"
	remoteTarget := TargetConfig{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 1},
	}
	targets := NewTargetRegistry([]TargetConfig{remoteTarget})
	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run after restart",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Restarted task",
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
	for _, spec := range []struct {
		workerID     string
		nodeID       string
		targetID     string
		kind         string
		workerEvents bool
		payload      map[string]any
	}{
		{
			workerID:     remoteWorkerID,
			nodeID:       "node-recovered-remote",
			targetID:     "vm-1",
			kind:         "ssh",
			workerEvents: true,
			payload: map[string]any{
				"remoteSession": "aged-recovered",
				"remoteRunDir":  "/runs/aged-recovered",
				"remoteWorkDir": "/repo",
			},
		},
		{
			workerID: persistedWorkerID,
			nodeID:   "node-persisted-local",
			targetID: "local",
			kind:     "local",
		},
	} {
		payload := map[string]any{
			"nodeId":     spec.nodeID,
			"workerId":   spec.workerID,
			"workerKind": "codex",
			"targetId":   spec.targetID,
			"targetKind": spec.kind,
		}
		for key, value := range spec.payload {
			payload[key] = value
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: spec.workerID,
			Payload:  core.MustJSON(payload),
		}); err != nil {
			t.Fatal(err)
		}
		if !spec.workerEvents {
			continue
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: spec.workerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerStarted,
			TaskID:   taskID,
			WorkerID: spec.workerID,
			Payload:  core.MustJSON(map[string]any{}),
		}); err != nil {
			t.Fatal(err)
		}
	}

	liveCancelCalled := false
	service.cancels[remoteWorkerID] = func() {
		liveCancelCalled = true
	}
	service.remoteRuns[remoteWorkerID] = remoteRun{
		Target:   remoteTarget,
		Session:  "aged-recovered",
		RunDir:   "/runs/aged-recovered",
		WorkDir:  "/repo",
		TaskID:   taskID,
		WorkerID: remoteWorkerID,
		Status:   "running",
	}
	delete(service.tasks, remoteWorkerID)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("taskHasActiveWorkers before cancel = false, want true")
	}
	workerIDs := activeTaskWorkerIDs(snapshot, taskID)
	if !reflect.DeepEqual(workerIDs, []string{persistedWorkerID, remoteWorkerID}) {
		t.Fatalf("activeTaskWorkerIDs before cancel = %+v, want persisted and remote worker IDs", workerIDs)
	}
	for _, worker := range snapshot.Workers {
		if worker.ID == persistedWorkerID {
			t.Fatalf("persisted worker unexpectedly has worker row before cancel: %+v", worker)
		}
	}

	if err := service.CancelTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	if !liveCancelCalled {
		t.Fatalf("recovered remote worker cancel func was not called")
	}
	foundKill := false
	for _, command := range executor.commands {
		joined := strings.Join(command, " ")
		if strings.Contains(joined, "kill-session") && strings.Contains(joined, "aged-recovered") {
			foundKill = true
			break
		}
	}
	if !foundKill {
		t.Fatalf("expected remote tmux kill command, got %+v", executor.commands)
	}

	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID && worker.Status != core.WorkerCanceled {
			t.Fatalf("worker %s status = %q, want canceled", worker.ID, worker.Status)
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.Status != core.WorkerCanceled {
			t.Fatalf("node %s status = %q, want canceled", node.ID, node.Status)
		}
	}
	if status := taskStatus(snapshot, taskID); status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", status)
	}
}

func TestRecoveredRemoteWorkerCompletesPlanWorkItem(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-recovered-work-item"
	workerID := "worker-recovered-work-item"
	workItemID := "objective_worker_node-recovered-work-item"
	remoteTarget := TargetConfig{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
	}
	targets := NewTargetRegistry([]TargetConfig{remoteTarget})
	service := NewServiceWithWorkspaceManagerAndTargets(
		store,
		fixedBrain{plan: testWorkItemPlan("mock", "noop")},
		map[string]worker.Runner{"mock": eventRunner{kind: "mock"}},
		t.TempDir(),
		fakeWorkspaceManager{cwd: t.TempDir()},
		targets,
		SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond},
	)

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Recovered work item",
			"prompt": "Finish after restart",
			"metadata": map[string]any{
				"objectiveMode": "broad",
			},
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
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":     workItemID,
		"kind":   "objective.validate",
		"reason": "validate recovered worker output",
		"metadata": map[string]any{
			"sourceAction": "plan",
			"planID":       "plan-recovered",
			"workItemKind": "objective.validate",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemStarted(ctx, taskID, workItemID, workerID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-recovered-work-item",
			"workerId":      workerID,
			"workerKind":    "mock",
			"planId":        "plan-recovered",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-recovered-work-item",
			"remoteRunDir":  "/runs/aged-recovered-work-item",
			"remoteWorkDir": "/repo",
			"metadata": map[string]any{
				"sourceAction": "plan",
				"planID":       "plan-recovered",
				"workItemKind": "objective.validate",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "mock",
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

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		item, ok := workItemByID(snapshot, workItemID)
		return ok && item.Status == core.WorkItemSucceeded && item.WorkerID == workerID
	}, func(snapshot core.Snapshot) string {
		item, ok := workItemByID(snapshot, workItemID)
		return fmt.Sprintf("recovered work item = %+v ok=%v, want succeeded for worker", item, ok)
	})
	item, ok := workItemByID(snapshot, workItemID)
	if !ok || item.Status != core.WorkItemSucceeded || item.WorkerID != workerID {
		t.Fatalf("recovered work item = %+v ok=%v, want succeeded for worker", item, ok)
	}
	if item.Attempt != 1 {
		t.Fatalf("recovered work item attempt = %d, want original attempt preserved", item.Attempt)
	}
}

func TestCancelTaskUnknownTaskReturnsNotFoundWithoutCanceling(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	taskCanceled := false
	workerCanceled := false
	service.taskCancels["missing-task"] = func() {
		taskCanceled = true
	}
	service.cancels["worker-orphan"] = func() {
		workerCanceled = true
	}
	service.tasks["worker-orphan"] = "missing-task"

	err := service.CancelTask(ctx, "missing-task")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("CancelTask error = %v, want ErrNotFound", err)
	}
	if taskCanceled {
		t.Fatalf("task cancel func was called for missing task")
	}
	if workerCanceled {
		t.Fatalf("worker cancel func was called for missing task")
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskStatus, "missing-task") != 0 {
		t.Fatalf("missing task has %d task status events, want 0", countEvents(snapshot.Events, core.EventTaskStatus, "missing-task"))
	}
}

func TestCancelWorkerFallsBackToPersistedRemoteRun(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-remote"
	workerID := "worker-remote"
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
	}})
	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
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
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
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
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.CancelWorker(ctx, workerID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Workers[0].Status != core.WorkerCanceled {
		t.Fatalf("worker status = %q, want canceled", snapshot.Workers[0].Status)
	}
	foundKill := false
	for _, command := range executor.commands {
		joined := strings.Join(command, " ")
		if strings.Contains(joined, "kill-session") && strings.Contains(joined, "aged-worker") {
			foundKill = true
			break
		}
	}
	if !foundKill {
		t.Fatalf("expected remote tmux kill command, got %+v", executor.commands)
	}
}

func TestRemoteWorkerCallbackCreatesTaskThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	callbackID := "create-task.20260511T000000Z.1.0"
	callbackOutput := "AGED-CALLBACK-FILE:" + callbackID + ".json\n" +
		`{"type":"create_task","promptBase64":"` + base64.StdEncoding.EncodeToString([]byte("follow up from remote")) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte("Remote follow-up")) + `","parentTaskIdBase64":"` + base64.StdEncoding.EncodeToString([]byte("task-parent")) + `","parentWorkerIdBase64":"` + base64.StdEncoding.EncodeToString([]byte("worker-parent")) + `"}` + "\n" +
		"AGED-CALLBACK-END\n"
	executor := &fakeRemoteExecutor{callbackOutput: callbackOutput}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"},
		{ID: "deno", Name: "Deno", LocalPath: t.TempDir(), DefaultBase: "main"},
	}, "default")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "deno", Title: "Parent", Prompt: "Run remote parent."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found core.Task
	for _, candidate := range snapshot.Tasks {
		if candidate.Title == "Remote follow-up" {
			found = candidate
			break
		}
	}
	if found.ID == "" || found.Prompt != "follow up from remote" {
		t.Fatalf("missing created follow-up task: %+v", snapshot.Tasks)
	}
	source, externalID := taskExternalRef(found)
	if source != "remote-worker" || !strings.Contains(externalID, callbackID) {
		t.Fatalf("external ref = %q %q", source, externalID)
	}
	var metadata map[string]any
	if err := json.Unmarshal(found.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if _, ok := metadata["completionMode"]; ok {
		t.Fatalf("metadata = %+v, want no completionMode", metadata)
	}
	if found.ProjectID != "deno" || metadata["projectId"] != "deno" {
		t.Fatalf("follow-up project = %q metadata %+v, want inherited deno", found.ProjectID, metadata)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "remote worker queued follow-up task") {
		t.Fatalf("missing parent worker callback event")
	}
}

func TestRemoteCreateTaskCallbackForTrackedPullRequestIsIgnored(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	taskID := "task-parent"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Broad objective",
			"prompt": "Keep producing independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-34636",
		TaskID: taskID,
		Repo:   "denoland/deno",
		Number: 34636,
		URL:    "https://github.com/denoland/deno/pull/34636",
		Branch: "codex/aged-9fb21b99-31c",
		Base:   "main",
		Title:  "perf(http): reduce overhead",
		State:  "OPEN",
	}); err != nil {
		t.Fatal(err)
	}

	err := service.handleRemoteCreateTaskCallback(ctx, remoteRun{
		TaskID:   taskID,
		WorkerID: "worker-parent",
		Target:   TargetConfig{ID: "bigboi"},
		Session:  "aged-session",
	}, RemoteWorkerCallback{
		ID:     "create-task.pr-followup.json",
		Type:   "create_task",
		Title:  "Recheck Deno PR CI",
		Prompt: "Follow up on existing PR denoland/deno#34636. Do not open a new PR; update the existing PR branch if needed.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range snapshot.Tasks {
		if task.Title == "Recheck Deno PR CI" {
			t.Fatalf("tracked PR create-task callback created child task: %+v", task)
		}
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "ignored remote create-task callback for tracked pull request denoland/deno#34636") {
		t.Fatalf("missing ignored tracked PR callback event")
	}
}

func TestRemotePullRequestFollowUpIgnoresCreateTaskCallback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	callbackID := "create-task.20260511T000000Z.1.0"
	callbackOutput := "AGED-CALLBACK-FILE:" + callbackID + ".json\n" +
		`{"type":"create_task","promptBase64":"` + base64.StdEncoding.EncodeToString([]byte("check this PR again later")) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte("PR follow-up child")) + `"}` + "\n" +
		"AGED-CALLBACK-END\n"
	executor := &fakeRemoteExecutor{callbackOutput: callbackOutput}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "repair the existing PR",
		Metadata: map[string]any{
			"backgroundPullRequestFollowUp": true,
			"scheduler":                     "pull_request_monitor",
			"pullRequestID":                 "pr-1",
		},
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"},
		{ID: "deno", Name: "Deno", LocalPath: t.TempDir(), DefaultBase: "main"},
	}, "default")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "deno", Title: "Parent", Prompt: "Run remote parent."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range snapshot.Tasks {
		if candidate.Title == "PR follow-up child" {
			t.Fatalf("pull request follow-up create-task callback created child task: %+v", candidate)
		}
	}
	if eventContains(snapshot.Events, core.EventWorkerCreated, "aged-create-task") {
		t.Fatalf("pull request follow-up worker prompt advertised aged-create-task")
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "ignored remote create-task callback from pull request follow-up worker") {
		t.Fatalf("missing ignored callback event")
	}
}

func TestLocalWorkerCallbackCreatesTaskThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &localCallbackRunner{kind: "callback"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "callback",
		Prompt:     "parent task creates a follow-up",
	}}, map[string]worker.Runner{"callback": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"},
		{ID: "deno", Name: "Deno", LocalPath: t.TempDir(), DefaultBase: "main"},
	}, "default")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "deno", Title: "Parent", Prompt: "Run local parent."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if runner.parentWorkerID == "" {
		t.Fatalf("callback runner did not run; prompt:\n%s", runner.prompt)
	}
	var found core.Task
	for _, candidate := range snapshot.Tasks {
		if candidate.Title == "Local follow-up" {
			found = candidate
			break
		}
	}
	if found.ID == "" || found.Prompt != "follow up from local" {
		t.Fatalf("missing created follow-up task: %+v", snapshot.Tasks)
	}
	source, externalID := taskExternalRef(found)
	if source != "local-worker" || !strings.Contains(externalID, runner.parentWorkerID) {
		t.Fatalf("external ref = %q %q", source, externalID)
	}
	var metadata map[string]any
	if err := json.Unmarshal(found.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["parentTaskId"] != task.ID || metadata["parentWorkerId"] != runner.parentWorkerID {
		t.Fatalf("metadata = %+v, want parent ids %q %q", metadata, task.ID, runner.parentWorkerID)
	}
	if found.ProjectID != "deno" || metadata["projectId"] != "deno" {
		t.Fatalf("follow-up project = %q metadata %+v, want inherited deno", found.ProjectID, metadata)
	}
	if !strings.Contains(runner.prompt, "aged-create-task") {
		t.Fatalf("runner prompt missing task helper instructions:\n%s", runner.prompt)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "local worker queued follow-up task") {
		t.Fatalf("missing parent worker callback event")
	}
}

func TestLocalPullRequestFollowUpIgnoresCreateTaskCallback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &localCallbackRunner{kind: "callback"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "callback",
		Prompt:     "repair the existing PR",
		Metadata: map[string]any{
			"backgroundPullRequestFollowUp": true,
			"scheduler":                     "pull_request_monitor",
			"pullRequestID":                 "pr-1",
		},
	}}, map[string]worker.Runner{"callback": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"},
		{ID: "deno", Name: "Deno", LocalPath: t.TempDir(), DefaultBase: "main"},
	}, "default")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "deno", Title: "Parent", Prompt: "Run local parent."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range snapshot.Tasks {
		if candidate.Title == "Local follow-up" {
			t.Fatalf("pull request follow-up create-task callback created child task: %+v", candidate)
		}
	}
	if strings.Contains(runner.prompt, "aged-create-task") {
		t.Fatalf("pull request follow-up prompt advertised task helper:\n%s", runner.prompt)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "ignored local create-task callback from pull request follow-up worker") {
		t.Fatalf("missing ignored callback event")
	}
}

func TestLocalCreateTaskCallbackForTrackedPullRequestIsIgnored(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	taskID := "task-parent"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Broad objective",
			"prompt": "Keep producing independent PRs.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-34636",
		TaskID: taskID,
		Repo:   "denoland/deno",
		Number: 34636,
		URL:    "https://github.com/denoland/deno/pull/34636",
		Branch: "codex/aged-9fb21b99-31c",
		Base:   "main",
		Title:  "perf(http): reduce overhead",
		State:  "OPEN",
	}); err != nil {
		t.Fatal(err)
	}

	err := service.handleLocalCreateTaskCallback(ctx, taskID, "worker-parent", RemoteWorkerCallback{
		ID:     "create-task.pr-followup.json",
		Type:   "create_task",
		Title:  "Update PR Benchmark Results",
		Prompt: "Continue follow-up on PR 34636 and post the requested benchmark numbers.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range snapshot.Tasks {
		if task.Title == "Update PR Benchmark Results" {
			t.Fatalf("tracked PR create-task callback created child task: %+v", task)
		}
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "ignored local create-task callback for tracked pull request denoland/deno#34636") {
		t.Fatalf("missing ignored tracked PR callback event")
	}
}

func TestLocalWorkerCallbackPublishesPullRequestThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	runner := &localPublishPRCallbackRunner{kind: "callback"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "callback",
		Prompt:     "publish an intermediate pull request",
	}}, map[string]worker.Runner{"callback": runner}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "loop.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Parent",
		Prompt: "Run local parent.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return eventContains(snapshot.Events, core.EventWorkerOutput, "local worker published pull request")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record local worker PR publication; events = %+v", task.ID, snapshot.Events)
	})
	if runner.parentWorkerID == "" {
		t.Fatalf("callback runner did not run; prompt:\n%s", runner.prompt)
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	if publisher.published.WorkerID != runner.parentWorkerID {
		t.Fatalf("published worker = %q, want %q", publisher.published.WorkerID, runner.parentWorkerID)
	}
	if publisher.published.Title != "Local callback PR" || publisher.published.Body != "Callback PR body" || publisher.published.Repo != "owner/repo" {
		t.Fatalf("published spec = %+v", publisher.published)
	}
	if !strings.Contains(runner.prompt, "aged-publish-pr") {
		t.Fatalf("runner prompt missing publish helper instructions:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "suitable for a PR title and commit subject") || !strings.Contains(runner.prompt, "never use status narration") {
		t.Fatalf("runner prompt missing publish title quality instructions:\n%s", runner.prompt)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "local worker published pull request") {
		t.Fatalf("missing parent worker publish event")
	}
}

func TestWorkerCallbackUpdatesPullRequestMetadataThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-metadata-callback"
	workerID := "worker-pr-metadata-callback"
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Follow up PR",
			"prompt": "Improve the PR description.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordPullRequestPublished(ctx, core.PullRequest{
		ID:     "pr-1",
		TaskID: taskID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/aged-test",
		Base:   "main",
		Title:  "Generic PR",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"latestPullRequestFeedbackSignature":          "sig-description",
			"latestPullRequestFeedbackTriggeredSignature": "sig-old",
			"latestPullRequestFeedbackBody":               "improve the description",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	err := service.handleWorkerUpdatePullRequestCallback(ctx, taskID, workerID, RemoteWorkerCallback{
		ID:      "update-pr.local",
		Type:    "update_pull_request",
		Title:   "docs: clarify cache behavior",
		Body:    "## Summary\n- Clarify the cache behavior change.\n\n## Validation\n- Not run; metadata-only update.",
		Comment: "Updated the PR description to clarify the cache behavior change.",
		Repo:    "owner/repo",
		Number:  7,
	}, "local")
	if err != nil {
		t.Fatal(err)
	}

	if publisher.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", publisher.updateCalls)
	}
	if !publisher.updated.MetadataOnly {
		t.Fatalf("callback update sent MetadataOnly=false: %+v", publisher.updated)
	}
	if publisher.updated.Title != "docs: clarify cache behavior" || !strings.Contains(publisher.updated.Body, "Clarify the cache behavior change") {
		t.Fatalf("updated metadata = title %q body %q", publisher.updated.Title, publisher.updated.Body)
	}
	if publisher.commentCalls != 1 {
		t.Fatalf("comment calls = %d, want 1", publisher.commentCalls)
	}
	if publisher.commentSpec.Body != "Updated the PR description to clarify the cache behavior change." {
		t.Fatalf("comment body = %q", publisher.commentSpec.Body)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "local worker updated pull request metadata") {
		t.Fatalf("missing worker output for metadata update")
	}
	if len(snapshot.PullRequests) != 1 || pullRequestHasUntriggeredFeedback(snapshot.PullRequests[0]) {
		t.Fatalf("pull request feedback still untriggered: %+v", snapshot.PullRequests)
	}
}

func TestServicePullRequestFeedbackCommentDedupesStaleOriginalMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{})
	task := core.Task{ID: "task-pr-comment-dedupe", Title: "Follow up PR"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": "Handle PR feedback.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	pr := core.PullRequest{
		ID:     "pr-1",
		TaskID: task.ID,
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/aged-test",
		Base:   "main",
		Title:  "refactor(cron): remove saffron dependency",
		State:  "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"latestPullRequestFeedbackSignature":          "sig-weekday-comment",
			"latestPullRequestFeedbackTriggeredSignature": "sig-old",
			"latestPullRequestFeedbackBody":               "The numeric weekday mapping is offset relative to the named one.",
		}),
	}
	if err := service.recordPullRequestPublished(ctx, pr); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: "repair-worker",
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "ext/cron/cron.rs", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.commentPullRequestFeedbackAddressed(ctx, task, pr, pr, core.PublishPullRequestRequest{WorkerID: "repair-worker"}, "repair-worker"); err != nil {
		t.Fatal(err)
	}
	if publisher.commentCalls != 0 {
		t.Fatalf("comment calls without worker-authored comment = %d, want 0", publisher.commentCalls)
	}

	req := core.PublishPullRequestRequest{
		WorkerID:        "repair-worker",
		FeedbackComment: "Pushed a comment documenting the saffron-compatible numeric weekday quirk.",
	}
	if err := service.commentPullRequestFeedbackAddressed(ctx, task, pr, pr, req, "repair-worker"); err != nil {
		t.Fatal(err)
	}
	if err := service.commentPullRequestFeedbackAddressed(ctx, task, pr, pr, req, "repair-worker"); err != nil {
		t.Fatal(err)
	}
	if publisher.commentCalls != 1 {
		t.Fatalf("comment calls = %d, want 1", publisher.commentCalls)
	}
	if publisher.commentSpec.Body != "Pushed a comment documenting the saffron-compatible numeric weekday quirk." {
		t.Fatalf("comment body = %q", publisher.commentSpec.Body)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	metadata := pullRequestMetadataMap(snapshot.PullRequests[0].Metadata)
	if got := stringMetadataValue(metadata["latestPullRequestFeedbackCommentedSignature"]); got != "sig-weekday-comment" {
		t.Fatalf("commented signature = %q, want sig-weekday-comment", got)
	}
}

func TestRemoteWorkerPublishPullRequestCallbackWithoutCandidateIsSkipped(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-callback-no-candidate"
	workerID := "worker-callback-no-candidate"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote parent",
			"prompt": "try to publish",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "inspected the workspace but found no changes to publish",
			"workspaceChanges": WorkspaceChanges{
				Status: "The working copy has no changes.",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	err := service.handleRemoteWorkerCallbacks(ctx, remoteRun{
		TaskID:   taskID,
		WorkerID: workerID,
	}, []RemoteWorkerCallback{{
		ID:             "publish-pr.test",
		Type:           "publish_pull_request",
		ParentWorkerID: workerID,
		Title:          "Remote callback PR",
		Body:           "Callback PR body",
		Repo:           "owner/repo",
	}})
	if err != nil {
		t.Fatalf("handle remote callbacks returned error: %v", err)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want none without candidate changes", publisher.publishCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, taskID, "publish_pull_request", "skipped") {
		t.Fatalf("missing skipped publish_pull_request callback action")
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "remote worker skipped pull request publication") {
		t.Fatalf("missing worker output explaining skipped PR callback")
	}
	if eventPayloadContains(snapshot.Events, core.EventWorkerOutput, taskID, "failed to drain terminal remote worker callbacks") {
		t.Fatalf("callback was reported as a terminal drain failure")
	}
}

func TestLocalWorkerCallbackSkipsBroadWorkerReportPullRequestBody(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	runner := &localPublishPRCallbackRunner{
		kind:  "callback",
		title: "Remove tower-http decompression from deno_fetch",
		body:  "## Summary\n- Remove tower-http decompression from deno_fetch.\n\n## Validation\n- cargo test -p deno_fetch\n\n## Recommended Next Turns\n- Run broader CI.\n",
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "callback",
		Prompt:     "publish an intermediate pull request",
	}}, map[string]worker.Runner{"callback": runner}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "ext/fetch/lib.rs", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Trim Deno dependency graph",
		Prompt: "Run broad objective work.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return eventContains(snapshot.Events, core.EventWorkerOutput, "local worker skipped pull request publication")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not skip local worker PR publication; events = %+v", task.ID, snapshot.Events)
	})
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want none for invalid broad worker-report body", publisher.publishCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "skipped") {
		t.Fatalf("missing skipped publish_pull_request callback action")
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerOutput, task.ID, "worker-report section") {
		t.Fatalf("missing worker-report rejection reason")
	}
}

func TestServiceRunsRemoteWorkerInPerWorkerGitWorktree(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/remote/checkouts",
		WorkRoot: "/remote/runs",
		Capacity: TargetCapacity{MaxWorkers: 4, CPUWeight: 100},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})
	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "deno",
		Name:        "Deno",
		LocalPath:   t.TempDir(),
		Repo:        "denoland/deno",
		DefaultBase: "main",
	}}, "deno")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "deno", Title: "Remote worktree", Prompt: "Run remote work."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	var workspace PreparedWorkspace
	var workerID string
	for _, event := range snapshot.Events {
		if event.Type != core.EventWorkerWorkspace || event.TaskID != task.ID {
			continue
		}
		workerID = event.WorkerID
		if err := json.Unmarshal(event.Payload, &workspace); err != nil {
			t.Fatal(err)
		}
	}
	if workerID == "" {
		t.Fatalf("missing worker workspace event")
	}
	wantRunDir := "/remote/runs/" + workerID
	wantWorkDir := wantRunDir + "/repo"
	if workspace.Root != wantRunDir || workspace.CWD != wantWorkDir || workspace.SourceRoot != "/remote/checkouts/deno" {
		t.Fatalf("workspace = %+v, want root %q cwd %q source root /remote/checkouts/deno", workspace, wantRunDir, wantWorkDir)
	}
	if !eventPayloadContains(snapshot.Events, core.EventExecutionPlanned, task.ID, `"remoteWorkDir":"`+wantWorkDir+`"`) {
		t.Fatalf("missing per-worker remoteWorkDir %q in execution plan", wantWorkDir)
	}
	joinedCommands := strings.Join(flattenCommands(executor.commands), "\n")
	if !strings.Contains(joinedCommands, "/remote/checkouts/deno") {
		t.Fatalf("remote checkout source was not prepared:\n%s", joinedCommands)
	}
	if !strings.Contains(joinedCommands, wantWorkDir) || !strings.Contains(joinedCommands, `git -C "$source_dir" worktree add --detach "$worktree_dir" HEAD`) {
		t.Fatalf("remote per-worker worktree was not prepared:\n%s", joinedCommands)
	}
}

func TestRecoverRemoteWorkerResumesTaskAfterCompletion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-recover-remote"
	workerID := "worker-recover-remote"
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "run remotely"}),
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
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
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
			"metadata": map[string]any{
				"nodeID": "node-remote",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
}

func TestRecoverRemoteWorkerCancelDoesNotCancelTaskWithOtherActiveWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-recover-remote"
	canceledWorkerID := "worker-canceled"
	activeWorkerID := "worker-active"
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 1},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
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
	for _, workerID := range []string{canceledWorkerID, activeWorkerID} {
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"nodeId":        "node-" + workerID,
				"workerId":      workerID,
				"workerKind":    "codex",
				"targetId":      "vm-1",
				"targetKind":    "ssh",
				"remoteSession": "aged-" + workerID,
				"remoteRunDir":  "/runs/aged-" + workerID,
				"remoteWorkDir": "/repo",
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
			Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-" + workerID}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: canceledWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service.resumeRecoveredRemoteTask(ctx, taskID)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[0].Status != core.TaskRunning {
		t.Fatalf("task status = %q, want running while another worker remains active", snapshot.Tasks[0].Status)
	}
}

func TestRecoverRemoteWorkersReservesTargetCapacityDuringRecovery(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-reserve-capacity"
	workerID := "worker-reserve-capacity"
	targets := NewTargetRegistry([]TargetConfig{
		{
			ID:       "vm-1",
			Kind:     TargetKindSSH,
			Host:     "vm",
			WorkDir:  "/repo",
			WorkRoot: "/runs",
			Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
		},
		defaultLocalTargetConfig(),
	})
	executor := &gatedPollExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "run remotely"}),
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
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "codex",
			"metadata": map[string]any{"nodeID": "node-remote"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	waitForTargetRunning(t, targets, "vm-1", 1)
	if err := targets.Delete("vm-1"); err == nil || !strings.Contains(err.Error(), "running workers") {
		t.Fatalf("Delete during recovery err = %v, want \"running workers\"", err)
	}

	executor.complete()
	waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	waitForTargetRunning(t, targets, "vm-1", 0)
	if err := targets.Delete("vm-1"); err != nil {
		t.Fatalf("Delete after recovery err = %v, want nil", err)
	}
}

type gatedPollExecutor struct {
	mu   sync.Mutex
	done bool
}

func (e *gatedPollExecutor) complete() {
	e.mu.Lock()
	e.done = true
	e.mu.Unlock()
}

func (e *gatedPollExecutor) isDone() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.done
}

func (e *gatedPollExecutor) Run(_ context.Context, argv []string) (string, error) {
	joined := strings.Join(argv, " ")
	switch {
	case strings.Contains(joined, "status.json"):
		if e.isDone() {
			return `{"status":"succeeded","exit":0}`, nil
		}
		return `{"status":"running"}`, nil
	case strings.Contains(joined, "vcs.txt"):
		return "git\n", nil
	case strings.Contains(joined, "root.txt"):
		return "/repo\n", nil
	default:
		return "", nil
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

func TestServiceRetriesTransientFailedWorkerCompletedAppendFailure(t *testing.T) {
	ctx := context.Background()
	baseStore := openTestStore(t)
	defer baseStore.Close()
	store := &transientAppendErrorStore{
		Store:        baseStore,
		eventType:    core.EventWorkerCompleted,
		failuresLeft: 1,
		err:          errors.New("temporary sqlite write failure"),
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "completion-retry",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"completion-retry": eventThenFailRunner{
		kind: "completion-retry",
		events: []worker.Event{{
			Kind:   worker.EventLog,
			Stream: "stderr",
			Text:   "cargo test failed: assertion mismatch",
		}},
		err: errors.New("assertion mismatch"),
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	if got := store.failureCount(); got != 1 {
		t.Fatalf("transient append failures = %d, want 1", got)
	}
	if countEvents(snapshot.Events, core.EventWorkerCompleted, task.ID) != 1 {
		t.Fatalf("worker.completed count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCompleted, task.ID))
	}
	payload := workerCompletedPayload(t, snapshot.Events, task.ID)
	if payload.Status != core.WorkerFailed || payload.LogCount != 1 || !strings.Contains(payload.Error, "assertion mismatch") {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestServiceFallsBackToAlternateProviderWhenUsageExhausted(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "claude",
		Prompt:     "do the work",
	}}, map[string]worker.Runner{
		"claude": eventThenFailRunner{
			kind: "claude",
			events: []worker.Event{{
				Kind: worker.EventError,
				Text: "Claude usage limit reached. Your limit resets at 3pm.",
			}},
			err: errors.New("worker command failed: exit status 1"),
		},
		"codex": eventRunner{kind: "codex", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "completed with codex",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 2 {
		t.Fatalf("workers = %+v, want claude failure and codex fallback", snapshot.Workers)
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "claude") || !hasWorkerCreated(snapshot.Events, task.ID, "codex") {
		t.Fatalf("missing provider worker creation events:\n%s", taskEventSummary(snapshot.Events, task.ID))
	}
	if !hasTaskAction(snapshot.Events, task.ID, "provider_usage_fallback", "started") || !hasTaskAction(snapshot.Events, task.ID, "provider_usage_fallback", "completed") {
		t.Fatalf("missing provider usage fallback actions:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServiceWaitsForProviderCapacityWhenUsageExhaustedWithoutFallback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "claude",
		Prompt:     "do the work",
	}}, map[string]worker.Runner{
		"claude": eventThenFailRunner{
			kind: "claude",
			events: []worker.Event{{
				Kind: worker.EventNeedsInput,
				Text: "Claude usage limit reached. Please wait until your usage limits reset.",
			}},
			err: nil,
		},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingExternal || snapshot.Tasks[0].ObjectivePhase != "provider_usage_exhausted" {
		t.Fatalf("objective = %q phase %q", snapshot.Tasks[0].ObjectiveStatus, snapshot.Tasks[0].ObjectivePhase)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "provider_usage_exhausted", "waiting_external") {
		t.Fatalf("missing provider usage exhausted action:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
	if countEvents(snapshot.Events, core.EventApprovalNeeded, task.ID) != 0 {
		t.Fatalf("usage exhaustion should not request user action:\n%s", taskEventSummary(snapshot.Events, task.ID))
	}
}

func TestServiceRemoteWorkerStartFailureCompletesWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &startFailRemoteExecutor{err: errors.New("tmux launch failed")}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:           "vm-1",
		Kind:         TargetKindSSH,
		Host:         "vm",
		CheckoutRoot: "/repo",
		WorkRoot:     "/runs",
		Capacity:     TargetCapacity{MaxWorkers: 1, CPUWeight: 100},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote-start",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"remote-start": eventRunner{kind: "remote-start"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Remote work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	if countEvents(snapshot.Events, core.EventWorkerStarted, task.ID) != 1 {
		t.Fatalf("worker.started count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerStarted, task.ID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCompleted, task.ID) != 1 {
		t.Fatalf("worker.completed count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCompleted, task.ID))
	}
	payload := workerCompletedPayload(t, snapshot.Events, task.ID)
	if payload.Status != core.WorkerFailed || !strings.Contains(payload.Error, "tmux launch failed") {
		t.Fatalf("payload = %+v", payload)
	}
	if len(snapshot.Workers) != 1 || snapshot.Workers[0].Status != core.WorkerFailed {
		t.Fatalf("workers = %+v, want failed worker", snapshot.Workers)
	}
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].Status != core.WorkerFailed {
		t.Fatalf("execution nodes = %+v, want failed node", snapshot.ExecutionNodes)
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
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser || snapshot.Tasks[0].ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q phase %q", snapshot.Tasks[0].ObjectiveStatus, snapshot.Tasks[0].ObjectivePhase)
	}
	if hasEvent(snapshot.Events, core.EventWorkerCleanup, task.ID, "") {
		t.Fatalf("waiting worker workspace should be retained")
	}
}

func TestServiceUserFeedbackResumeClearsWaitingObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 1)
	release := make(chan struct{})
	brain := &sequenceBrain{plans: []Plan{
		{WorkerKind: "ask", Prompt: "ask for input"},
		{WorkerKind: "answer", Prompt: "continue with user answer"},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Which dependency should I use?",
		}}},
		"answer": &blockingEventRunner{
			kind:    "answer",
			started: started,
			release: release,
			summary: "continued after user answer",
		},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser {
		t.Fatalf("objective status before resume = %q, want waiting_user", snapshot.Tasks[0].ObjectiveStatus)
	}

	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "Use the existing dependency."}); err != nil {
		t.Fatal(err)
	}
	select {
	case kind := <-started:
		if kind != "answer" {
			t.Fatalf("started runner = %q, want answer", kind)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resume worker did not start")
	}
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		task, ok := findTask(snapshot, task.ID)
		return ok && task.Status == core.TaskRunning && task.ObjectiveStatus == core.ObjectiveActive
	}, func(snapshot core.Snapshot) string {
		task, _ := findTask(snapshot, task.ID)
		return fmt.Sprintf("task did not clear waiting objective after resume: %+v", task)
	})
	resumed, _ := findTask(snapshot, task.ID)
	if resumed.ObjectivePhase != "replanning" {
		t.Fatalf("objective phase = %q, want replanning", resumed.ObjectivePhase)
	}

	close(release)
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
}

func TestServiceAutonomouslyContinuesWhenReplannerAnswersWorkerQuestion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "ask",
			Prompt:     "ask for input",
		},
		decisions: []ReplanDecision{{
			Action:  "continue",
			Message: "Use the existing dependency.",
			Plan: &Plan{
				WorkerKind: "answer",
				Prompt:     "continue with autonomous answer",
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Which dependency should I use?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after orchestrator answer",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval.needed event")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalDecided, task.ID, "") {
		t.Fatalf("missing approval.decided event")
	}
	if item, ok := workItemByKind(snapshot, "user.question"); !ok || item.Status != core.WorkItemSucceeded {
		t.Fatalf("question work item = %+v ok=%v, want succeeded", item, ok)
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing continuation worker")
	}
	if len(brain.states) == 0 || !ledgerContainsSummary(brain.states[0].ContextLedger, "Which dependency should I use?") {
		t.Fatalf("worker-question replan state missing context ledger: %+v", brain.states)
	}
}

func TestServiceAutonomousQuestionContinuationRunsPlannedFollowUps(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "ask",
			Prompt:     "ask for input",
		},
		decisions: []ReplanDecision{{
			Action:  "continue",
			Message: "Use the existing dependency.",
			Plan: &Plan{
				WorkerKind: "answer",
				Prompt:     "continue with autonomous answer",
				Spawns: []SpawnRequest{{
					ID:         "review",
					Role:       "reviewer",
					Reason:     "Review the continuation output.",
					WorkerKind: "reviewer",
				}},
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Which dependency should I use?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after orchestrator answer",
		}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "reviewed continuation",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing continuation worker")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "reviewer") {
		t.Fatalf("missing planned follow-up worker")
	}
}

func TestServiceResumesWaitingTaskWhenSteered(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &sequenceBrain{plans: []Plan{
		{WorkerKind: "ask", Prompt: "ask for input"},
		{WorkerKind: "answer", Prompt: "continue after feedback"},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Should I install a dependency?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after user feedback",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if item, ok := workItemByKind(waiting, "user.question"); !ok || item.Status != core.WorkItemQueued {
		t.Fatalf("waiting question work item = %+v ok=%v, want queued", item, ok)
	}
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "Use the existing package only."}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasEvent(snapshot.Events, core.EventApprovalDecided, task.ID, "") {
		t.Fatalf("missing approval.decided event")
	}
	if item, ok := workItemByKind(snapshot, "user.question"); !ok || item.Status != core.WorkItemSucceeded {
		t.Fatalf("answered question work item = %+v ok=%v, want succeeded", item, ok)
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing resumed worker")
	}
	if got := strings.Join(brain.steering, "\n"); !strings.Contains(got, "Should I install a dependency?") || !strings.Contains(got, "Use the existing package only.") {
		t.Fatalf("resume steering = %q", got)
	}
}

func TestServiceResumeWaitingTaskRunsPlannedFollowUps(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &sequenceBrain{plans: []Plan{
		{WorkerKind: "ask", Prompt: "ask for input"},
		{
			WorkerKind: "answer",
			Prompt:     "continue after feedback",
			Spawns: []SpawnRequest{{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the resumed output.",
				WorkerKind: "reviewer",
			}},
		},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Should I install a dependency?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after user feedback",
		}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "reviewed resumed work",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "Use the existing package only."}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing resumed worker")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "reviewer") {
		t.Fatalf("missing planned follow-up worker")
	}
}

func TestServiceResumeWaitingTaskRunsWorkerGraphPlan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &sequenceBrain{plans: []Plan{
		{WorkerKind: "ask", Prompt: "ask for input"},
		{
			Rationale: "repair the existing pull request",
			Workers: []WorkerRequest{{
				ID:              "repair_pr_followup",
				Role:            "repairer",
				Reason:          "The PR needs one focused repair turn.",
				WorkerKind:      "codex",
				Prompt:          "inspect and repair the PR",
				ReasoningEffort: "medium",
			}},
		},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "PR needs follow-up work.",
		}}},
		"codex": eventRunner{kind: "codex", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "repaired PR metadata",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Babysit PR", Prompt: "Open and monitor the PR."})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "GitHub pull request owner/repo#7 needs follow-up work."}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "codex") {
		t.Fatalf("missing graph worker from resumed plan")
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 2 {
		t.Fatalf("worker.created count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
}

func TestServiceAskUserActionMovesTaskToWaiting(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "noop",
		Prompt:     "confirm profiling setup",
		Actions: []PlanAction{{
			Kind:   "ask_user",
			When:   "after_success",
			Reason: "perf setup is missing",
			Inputs: map[string]any{
				"question":   "Please install perf on the VM.",
				"summary":    "Profiling setup required.",
				"target":     "vm-a",
				"commands":   []any{"sudo apt-get install linux-perf"},
				"resumeHint": "Reply when perf works.",
			},
		}},
	}}, map[string]worker.Runner{"noop": eventRunner{
		kind: "noop",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "ready to profile",
		}},
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Profile", Prompt: "Run profiling"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser {
		t.Fatalf("objective = %q", snapshot.Tasks[0].ObjectiveStatus)
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "ask_user" || payload["target"] != "vm-a" {
		t.Fatalf("approval payload = %+v", payload)
	}
	if commands, ok := payload["commands"].([]any); !ok || len(commands) != 1 {
		t.Fatalf("commands = %+v", payload["commands"])
	}
}

func TestServiceTreatsRecoverableWorkerFailureAsUserAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "fail",
		Prompt:     "run perf",
	}}, map[string]worker.Runner{"fail": failingRunner{
		kind: "fail",
		err:  errors.New("perf: command not found"),
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Profile", Prompt: "Run perf"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser {
		t.Fatalf("objective = %q", snapshot.Tasks[0].ObjectiveStatus)
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "missing_tool" {
		t.Fatalf("reason = %v", payload["reason"])
	}
	if question, _ := payload["question"].(string); !strings.Contains(question, "perf: command not found") {
		t.Fatalf("question = %q", question)
	}
}

func TestServiceTreatsRecoverableDynamicReplanWorkerFailureAsUserAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "produce initial candidate",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 10,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial candidate"}}},
		"follow": failingRunner{
			kind: "follow",
			err:  errors.New("unexpected status 401 Unauthorized: Missing bearer or basic authentication in header, url: https://api.openai.com/v1/responses"),
		},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Recover dynamic auth", Prompt: "Keep improving."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "worker_auth_required" {
		t.Fatalf("reason = %v", payload["reason"])
	}
	if hasTaskAction(snapshot.Events, task.ID, "worker_failure_recovery", "continued") {
		t.Fatalf("unexpected continued worker failure recovery")
	}
}

func TestServiceTreatsWorkflowScopePushRejectionAsRecoverable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		errOnce: errors.New("push git branch: refusing to allow an OAuth App to create or update workflow `.github/workflows/ci.yml` without `workflow` scope"),
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "add CI workflow",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish CI workflow",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish CI workflow."},
		}},
	}}, map[string]worker.Runner{"writer": eventRunner{
		kind:   "writer",
		events: []worker.Event{{Kind: worker.EventResult, Text: "added CI workflow"}},
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: ".github/workflows/ci.yml", Status: "added"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Add Formatting and Test CI",
		Prompt: "Add GitHub Actions CI.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser || snapshot.Tasks[0].ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q/%q, want user approval needed", snapshot.Tasks[0].ObjectiveStatus, snapshot.Tasks[0].ObjectivePhase)
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "github_workflow_scope_required" {
		t.Fatalf("approval reason = %v", payload["reason"])
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want one blocked publish attempt", publisher.publishCalls)
	}
}

func TestServiceAppliesRetainedWorkerChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	workspaceRoot := t.TempDir()
	changed := WorkspaceChangedFile{Path: "internal/example.txt", Status: "modified"}
	applyCalls := 0
	diffCalls := 0
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
		diff:       "diff --git a/internal/example.txt b/internal/example.txt\n",
		diffCalls:  &diffCalls,
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
	review, err := service.ReviewWorkerChanges(ctx, snapshot.Workers[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if review.Changes.Diff == "" {
		t.Fatal("review diff is empty")
	}
	if diffCalls != 1 {
		t.Fatalf("diff calls = %d, want 1", diffCalls)
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
	if diffCalls != 1 {
		t.Fatalf("apply should not reread diff; diff calls = %d, want 1", diffCalls)
	}
	if _, err := service.ApplyWorkerChanges(ctx, snapshot.Workers[0].ID); err == nil {
		t.Fatal("second apply succeeded, want error")
	}
	if applyCalls != 1 {
		t.Fatalf("second apply changed apply calls to %d, want 1", applyCalls)
	}
}

func TestServiceRemoteApplyFailsWhenExplicitTaskProjectWasDeleted(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	defaultProject := core.Project{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"}
	deletedProject := core.Project{ID: "deleted", Name: "Deleted", LocalPath: t.TempDir(), DefaultBase: "main"}
	if _, err := store.SaveProject(ctx, defaultProject, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveProject(ctx, deletedProject, false); err != nil {
		t.Fatal(err)
	}
	projects, err := NewProjectRegistry([]core.Project{defaultProject, deletedProject}, defaultProject.ID)
	if err != nil {
		t.Fatal(err)
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, defaultProject.LocalPath, fakeWorkspaceManager{})
	service.SetProjects(projects)

	taskID := "task-deleted-project"
	workerID := "worker-remote"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"projectId": deletedProject.ID,
			"title":     "Remote changes",
			"prompt":    "Apply remote patch.",
			"metadata":  map[string]any{"projectId": deletedProject.ID},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:       "/runs/remote",
			CWD:        "/checkouts/deleted",
			SourceRoot: "/checkouts/deleted",
			Mode:       "remote",
			VCSType:    "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Root:         "/runs/remote",
				CWD:          "/checkouts/deleted",
				Diff:         newFilePatch("remote.txt", "remote\n"),
				ChangedFiles: []WorkspaceChangedFile{{Path: "remote.txt", Status: "added"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
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
	remoteApplyCalls := 0
	service.remoteApply = func(context.Context, core.Project, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error) {
		remoteApplyCalls++
		return WorkerApplyResult{}, nil
	}
	if err := service.DeleteProject(ctx, deletedProject.ID); err != nil {
		t.Fatal(err)
	}

	_, err = service.ApplyWorkerChanges(ctx, workerID)
	if err == nil || !strings.Contains(err.Error(), `unknown projectId "deleted"`) {
		t.Fatalf("apply err = %v, want missing explicit project", err)
	}
	if remoteApplyCalls != 0 {
		t.Fatalf("remote apply calls = %d, want 0", remoteApplyCalls)
	}
}

func TestServiceAppliesExplicitWorkerChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

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
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
		applyCalls: &applyCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	var workerID string
	for _, worker := range snapshot.Workers {
		if worker.Kind == "writer" {
			workerID = worker.ID
			break
		}
	}
	if workerID == "" {
		t.Fatalf("missing writer worker: %+v", snapshot.Workers)
	}
	result, err := service.ApplyWorkerChanges(ctx, workerID)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkerID != workerID {
		t.Fatalf("applied worker = %q, want explicit worker %q", result.WorkerID, workerID)
	}
	applied, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Tasks[0].AppliedWorkerID != workerID {
		t.Fatalf("applied worker id = %q, want %q", applied.Tasks[0].AppliedWorkerID, workerID)
	}
	if applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", applyCalls)
	}
}

func TestServiceAppliesRemoteWorkerPatchArtifact(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := t.TempDir()
	taskID := "task-remote"
	workerID := "worker-remote"
	changed := WorkspaceChangedFile{Path: "main.go", Status: "modified"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Remote work",
			"prompt":   "Apply remote patch",
			"metadata": map[string]any{},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/" + workerID,
			CWD:           "/repo",
			SourceRoot:    "/repo",
			WorkspaceName: "aged-remote",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      workerID,
			TargetID:      "vm-1",
			TargetKind:    "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Root:         "/runs/" + workerID,
				CWD:          "/repo",
				Mode:         "remote",
				VCSType:      "git",
				Dirty:        true,
				Diff:         "diff --git a/main.go b/main.go\n",
				ChangedFiles: []WorkspaceChangedFile{changed},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, sourceRoot, fakeWorkspaceManager{})
	applied := 0
	service.SetRemotePatchApplier(func(_ context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
		applied++
		if project.LocalPath != sourceRoot {
			t.Fatalf("project local path = %q, want %q", project.LocalPath, sourceRoot)
		}
		if workspace.VCSType != "ssh" || changes.Diff == "" || len(changes.ChangedFiles) != 1 {
			t.Fatalf("workspace=%+v changes=%+v", workspace, changes)
		}
		result := baseWorkerApplyResult(workspace, "remote_patch_apply")
		result.SourceRoot = project.LocalPath
		result.AppliedFiles = changes.ChangedFiles
		return result, nil
	})

	review, err := service.ReviewWorkerChanges(ctx, workerID)
	if err != nil {
		t.Fatal(err)
	}
	if review.Changes.Diff == "" || review.Changes.ChangedFiles[0] != changed {
		t.Fatalf("review changes = %+v", review.Changes)
	}
	result, err := service.ApplyWorkerChanges(ctx, workerID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Method != "remote_patch_apply" || result.SourceRoot != sourceRoot || len(result.AppliedFiles) != 1 {
		t.Fatalf("result = %+v", result)
	}
	if applied != 1 {
		t.Fatalf("applied calls = %d, want 1", applied)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerApplied, taskID, workerID) {
		t.Fatal("missing worker.changes_applied event")
	}
}

func TestServicePublishesRemoteWorkerPullRequestFromWorkerPatch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := t.TempDir()
	taskID := "task-remote-pr"
	workerID := "worker-remote-pr"
	changed := WorkspaceChangedFile{Path: "main.go", Status: "modified"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote PR",
			"prompt": "Publish remote patch",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/" + workerID,
			CWD:           "/repo",
			SourceRoot:    "/repo",
			WorkspaceName: "aged-remote",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      workerID,
			TargetID:      "vm-1",
			TargetKind:    "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Root:         "/runs/" + workerID,
				CWD:          "/repo",
				Mode:         "remote",
				VCSType:      "git",
				Dirty:        true,
				Diff:         "diff --git a/main.go b/main.go\n",
				ChangedFiles: []WorkspaceChangedFile{changed},
			},
		}),
	}); err != nil {
		t.Fatal(err)
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

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, sourceRoot, fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)
	service.SetRemotePatchApplier(func(_ context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
		t.Fatalf("remote patch applier should not run while publishing an SSH worker PR: project=%+v workspace=%+v changes=%+v", project, workspace, changes)
		return WorkerApplyResult{}, nil
	})

	published, err := service.PublishTaskPullRequest(ctx, taskID, core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		WorkerID: workerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.published.WorkDir != sourceRoot {
		t.Fatalf("published workDir = %q, want local source root %q", publisher.published.WorkDir, sourceRoot)
	}
	if !publisher.published.PatchFromBase {
		t.Fatal("published spec did not request patch-from-base publication")
	}
	if publisher.published.Patch != "diff --git a/main.go b/main.go\n" {
		t.Fatalf("published patch = %q", publisher.published.Patch)
	}
	publishedMetadata := map[string]any{}
	if err := json.Unmarshal(published.Metadata, &publishedMetadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(publishedMetadata["ownerWorkerId"]); got != workerID {
		t.Fatalf("ownerWorkerId = %q, want %q; metadata=%+v", got, workerID, publishedMetadata)
	}
	if got := stringMetadataValue(publishedMetadata["ownerWorkspaceCwd"]); got != "/repo" {
		t.Fatalf("ownerWorkspaceCwd = %q, want /repo; metadata=%+v", got, publishedMetadata)
	}
	if got := stringMetadataValue(publishedMetadata["ownerTargetId"]); got != "vm-1" {
		t.Fatalf("ownerTargetId = %q, want vm-1; metadata=%+v", got, publishedMetadata)
	}
	updated, err := service.UpdateTaskPullRequest(ctx, taskID, published, core.PublishPullRequestRequest{
		Repo:         published.Repo,
		Base:         published.Base,
		Branch:       published.Branch,
		Title:        "Updated remote PR",
		Body:         "Updated body",
		WorkerID:     workerID,
		MetadataOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	updatedMetadata := map[string]any{}
	if err := json.Unmarshal(updated.Metadata, &updatedMetadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(updatedMetadata["ownerWorkerId"]); got != workerID {
		t.Fatalf("ownerWorkerId after update = %q, want %q; metadata=%+v", got, workerID, updatedMetadata)
	}
	if got := stringMetadataValue(updatedMetadata["lastUpdateWorkerId"]); got != workerID {
		t.Fatalf("lastUpdateWorkerId = %q, want %q; metadata=%+v", got, workerID, updatedMetadata)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hasEvent(snapshot.Events, core.EventWorkerApplied, taskID, workerID) {
		t.Fatal("unexpected worker.changes_applied event")
	}
	if !hasEvent(snapshot.Events, core.EventPRPublished, taskID, "") {
		t.Fatal("missing pr.published event")
	}
}

func TestServiceSeparateTopLevelRemotePullRequestsStartFromProjectBase(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := initGitTestRepo(t)
	runTestGit(t, sourceRoot, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, sourceRoot, "remote", "add", "origin", remote)
	runTestGit(t, sourceRoot, "push", "-u", "origin", "main")

	createCalls := 0
	publisher := LocalPullRequestPublisher{
		exec: func(ctx context.Context, dir string, name string, args ...string) (string, error) {
			switch {
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "create":
				createCalls++
				if strings.Contains(strings.Join(args, " "), "first-pr") {
					return "https://github.com/owner/repo/pull/31", nil
				}
				return "https://github.com/owner/repo/pull/32", nil
			case name == "gh" && len(args) >= 3 && args[0] == "pr" && args[1] == "view":
				if strings.Contains(args[2], "/31") {
					return `{"number":31,"url":"https://github.com/owner/repo/pull/31","state":"OPEN","title":"First","isDraft":false,"headRefName":"first-pr","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":""}`, nil
				}
				return `{"number":32,"url":"https://github.com/owner/repo/pull/32","state":"OPEN","title":"Second","isDraft":false,"headRefName":"second-pr","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":""}`, nil
			default:
				return runCommand(ctx, dir, name, args...)
			}
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, sourceRoot, fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	appendRemotePublishCandidate := func(taskID string, workerID string, title string, diff string, changedPath string) {
		t.Helper()
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  title,
				"prompt": title,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerWorkspace,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(PreparedWorkspace{
				Root:          "/runs/" + workerID,
				CWD:           "/repo",
				SourceRoot:    "/repo",
				WorkspaceName: "aged-remote",
				Mode:          "remote",
				VCSType:       "ssh",
				TaskID:        taskID,
				WorkerID:      workerID,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status": core.WorkerSucceeded,
				"workspaceChanges": WorkspaceChanges{
					Root:    "/runs/" + workerID,
					CWD:     "/repo",
					Mode:    "remote",
					VCSType: "git",
					Dirty:   true,
					Diff:    diff,
					ChangedFiles: []WorkspaceChangedFile{{
						Path:   changedPath,
						Status: "added",
					}},
				},
			}),
		}); err != nil {
			t.Fatal(err)
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
	}

	appendRemotePublishCandidate("task-one", "worker-one", "First task", newFilePatch("first.txt", "first\n"), "first.txt")
	if _, err := service.PublishTaskPullRequest(ctx, "task-one", core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		Branch:   "first-pr",
		WorkerID: "worker-one",
	}); err != nil {
		t.Fatal(err)
	}
	if contents := runTestGit(t, sourceRoot, "show", "first-pr:first.txt"); contents != "first\n" {
		t.Fatalf("first branch content = %q", contents)
	}
	if _, err := runCommand(ctx, sourceRoot, "git", "cat-file", "-e", "HEAD:first.txt"); err == nil {
		t.Fatal("source checkout still contains first task change after publishing")
	}

	appendRemotePublishCandidate("task-two", "worker-two", "Second task", newFilePatch("second.txt", "second\n"), "second.txt")
	if _, err := service.PublishTaskPullRequest(ctx, "task-two", core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		Branch:   "second-pr",
		WorkerID: "worker-two",
	}); err != nil {
		t.Fatal(err)
	}
	if createCalls != 2 {
		t.Fatalf("gh pr create calls = %d, want 2", createCalls)
	}
	if contents := runTestGit(t, sourceRoot, "show", "second-pr:second.txt"); contents != "second\n" {
		t.Fatalf("second branch content = %q", contents)
	}
	if _, err := runCommand(ctx, sourceRoot, "git", "cat-file", "-e", "second-pr:first.txt"); err == nil {
		t.Fatal("second top-level PR branch included the first task change")
	}
}

func newFilePatch(path string, body string) string {
	var builder strings.Builder
	builder.WriteString("diff --git a/")
	builder.WriteString(path)
	builder.WriteString(" b/")
	builder.WriteString(path)
	builder.WriteString("\nnew file mode 100644\n--- /dev/null\n+++ b/")
	builder.WriteString(path)
	builder.WriteString("\n@@ -0,0 +1 @@\n+")
	builder.WriteString(strings.TrimSuffix(body, "\n"))
	builder.WriteString("\n")
	return builder.String()
}

func TestServiceRecordsBenchmarkResultArtifact(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "benchmark_compare",
		Prompt:     "baseline: 10\ncandidate: 12\nthreshold_percent: 5\nhigher_is_better: true",
	}}, map[string]worker.Runner{
		"benchmark_compare": worker.BenchmarkCompareRunner{},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Bench", Prompt: "Compare benchmark result."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	task = snapshot.Tasks[0]
	if len(task.Artifacts) != 1 || task.Artifacts[0].Kind != "benchmark_report" {
		t.Fatalf("artifacts = %+v", task.Artifacts)
	}
	if !strings.Contains(string(task.Artifacts[0].Metadata), "deltaPercent") {
		t.Fatalf("artifact metadata = %s", task.Artifacts[0].Metadata)
	}
}

func TestServiceRunsSpawnedFollowUpWorkerWithPriorResultContext(t *testing.T) {
	t.Skip("obsolete: follow-up workers are now durable workItems, not inline spawn graph turns")
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

func TestServiceContinuesAfterFailedFollowUpWorker(t *testing.T) {
	t.Skip("obsolete: failed durable workItems are replanned from queue drain, not inline follow-up graph results")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	reviewer := &flakyRunner{kind: "reviewer"}
	brain := &replanningBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the implementation output.",
			WorkerKind: "reviewer",
		}},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": reviewer,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover failed review",
		Prompt: "Implement, then review the candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != 1 || len(brain.states[0].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	if brain.states[0].Results[1].Status != core.WorkerFailed {
		t.Fatalf("follow-up status = %q, want failed", brain.states[0].Results[1].Status)
	}
	if reviewer.callsValue() != 1 {
		t.Fatalf("reviewer calls = %d, want 1", reviewer.callsValue())
	}
}

func TestServiceContinuesAfterFollowUpSetupError(t *testing.T) {
	t.Skip("obsolete: setup failures now belong to durable workItem drain/replan state")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	prepareCalls := 0
	workspaceErr := errors.New("apply base worker patch in local workspace: corrupt patch")
	brain := &replanningBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the implementation output.",
			WorkerKind: "reviewer",
		}},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:              t.TempDir(),
		prepareCalls:     &prepareCalls,
		failPrepareAfter: 1,
		prepareErr:       workspaceErr,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover failed setup",
		Prompt: "Implement, then review the candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != 1 || len(brain.states[0].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	followUp := brain.states[0].Results[1]
	if followUp.Status != core.WorkerFailed || !strings.Contains(followUp.Error, "corrupt patch") {
		t.Fatalf("follow-up result = %+v", followUp)
	}
	if countEvents(snapshot.Events, core.EventTaskStatus, task.ID) == 0 {
		t.Fatalf("missing task status events")
	}
}

func TestServiceReplansAfterInitialWorkerSetupError(t *testing.T) {
	t.Skip("obsolete: initial worker setup recovery now goes through durable workItems")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	prepareCalls := 0
	workspaceErr := errors.New("prepare workspace: apply base worker patch in local workspace: corrupt patch")
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the first attempt",
		},
		decisions: []ReplanDecision{{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "codex",
				Prompt:     "retry with a repaired workspace handoff",
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented after recovery"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:              t.TempDir(),
		prepareCalls:     &prepareCalls,
		failPrepareUntil: 1,
		prepareErr:       workspaceErr,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/recovered.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover primary setup",
		Prompt: "Implement despite a setup failure.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	firstFailure := brain.states[0].Results[0]
	if firstFailure.Status != core.WorkerFailed || !strings.Contains(firstFailure.Error, "corrupt patch") {
		t.Fatalf("first failure = %+v", firstFailure)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "worker_failure_recovery", "started") {
		t.Fatalf("missing worker failure recovery action")
	}
}

func TestServiceRecoversDynamicReplanGraphSetupErrorWithoutWorkerID(t *testing.T) {
	t.Skip("obsolete: dynamic graph setup recovery was removed with the inline worker graph")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	prepareCalls := 0
	workspaceErr := errors.New("apply base worker patch on remote target: repository lacks the necessary blob to perform 3-way merge")
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the first slice",
			Spawns: []SpawnRequest{{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the implementation output.",
				WorkerKind: "reviewer",
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "review found a repairable issue",
			Plan: &Plan{
				Rationale: "repair the implementation with an initial worker graph",
				Workers: []WorkerRequest{{
					ID:         "repair",
					Role:       "repairer",
					Reason:     "Repair the rejected candidate.",
					WorkerKind: "codex",
					Prompt:     "Repair the candidate.",
				}},
			},
		}, {
			Action:    "wait",
			Rationale: "repair setup failed and should be visible to the replanner",
			Message:   "retry after the setup issue is repaired",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "review found a repairable issue"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:              t.TempDir(),
		prepareCalls:     &prepareCalls,
		failPrepareAfter: 2,
		prepareErr:       workspaceErr,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/recovered.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover dynamic graph setup",
		Prompt: "Implement, review, then repair from a graph worker.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].Status != core.TaskWaiting {
		t.Fatalf("task status = %q, want waiting", snapshot.Tasks[0].Status)
	}
	if len(brain.states) < 2 {
		t.Fatalf("replan states = %+v, want setup failure to trigger another replan turn", brain.states)
	}
	if !replanStatesContainResultError(brain.states, "necessary blob") {
		t.Fatalf("replan states missing setup error: %+v", brain.states)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "worker_failure_recovery", "continued") {
		t.Fatalf("missing continued worker failure recovery action:\n%s", taskEventSummary(snapshot.Events, task.ID))
	}
	for _, event := range snapshot.Events {
		if event.Type == core.EventTaskStatus && event.TaskID == task.ID && strings.Contains(string(event.Payload), string(core.TaskFailed)) {
			t.Fatalf("task should not have been terminal-failed after graph setup error: %s", event.Payload)
		}
	}
}

func TestServiceBasesFollowUpWorkspaceOnLatestCandidate(t *testing.T) {
	t.Skip("obsolete: implicit follow-up base handoff was replaced by explicit workItem dependencies")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	workspace := &recordingWorkspaceManager{
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the implementation output.",
			WorkerKind: "reviewer",
		}},
	}}, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), workspace)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Candidate review",
		Prompt: "Implement, then review the candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if workspace.baseWorkDir == "" || workspace.baseRevision != "shared@" {
		t.Fatalf("follow-up base workdir=%q baseRevision=%q, want candidate workspace base", workspace.baseWorkDir, workspace.baseRevision)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "baseWorkerID", snapshot.Workers[0].ID) {
		t.Fatalf("missing baseWorkerID metadata on follow-up worker")
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
	deadline := time.After(3 * time.Second)
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
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) < 3 {
		t.Fatalf("task.planned count = %d, want at least 3", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
}

func TestServiceRunsInitialWorkersInParallel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	release := make(chan struct{})
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		Rationale: "independent initial investigation can run in parallel",
		Workers: []WorkerRequest{
			{
				ID:              "audit",
				Role:            "auditor",
				Reason:          "Inspect one side of the task.",
				WorkerKind:      "left",
				Prompt:          "Audit the left side.",
				ReasoningEffort: "low",
			},
			{
				ID:              "test",
				Role:            "tester",
				Reason:          "Inspect another side of the task.",
				WorkerKind:      "right",
				Prompt:          "Audit the right side.",
				ReasoningEffort: "low",
			},
		},
	}}, map[string]worker.Runner{
		"left":  &blockingEventRunner{kind: "left", started: started, release: release, summary: "left done"},
		"right": &blockingEventRunner{kind: "right", started: started, release: release, summary: "right done"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Parallel initial work",
		Prompt: "Run independent audits in parallel.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			t.Fatalf("initial workers did not start in parallel; started = %+v", got)
		}
	}
	close(release)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "left") || !hasWorkerCreated(snapshot.Events, task.ID, "right") {
		t.Fatalf("missing initial workers")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) < 1 {
		t.Fatalf("task.planned count = %d, want at least 1", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	for _, node := range snapshot.ExecutionNodes {
		if !payloadValueMatchesRef(node.SpawnID, "audit") && !payloadValueMatchesRef(node.SpawnID, "test") {
			t.Fatalf("unexpected initial worker node spawn id: %+v", node)
		}
	}
}

func TestServiceHonorsInitialWorkerDependencies(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	firstStarted := make(chan string, 1)
	secondStarted := make(chan string, 1)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	second := &blockingEventRunner{kind: "second", started: secondStarted, release: secondRelease, summary: "second done"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		Rationale: "initial worker graph has a dependency",
		Workers: []WorkerRequest{
			{
				ID:         "inspect",
				Role:       "inspector",
				Reason:     "Inspect the current implementation.",
				WorkerKind: "first",
				Prompt:     "Inspect first.",
			},
			{
				ID:         "repair",
				Role:       "implementer",
				Reason:     "Repair issues found by inspection.",
				WorkerKind: "second",
				Prompt:     "Repair after inspection.",
				DependsOn:  []string{"inspect"},
			},
		},
	}}, map[string]worker.Runner{
		"first":  &blockingEventRunner{kind: "first", started: firstStarted, release: firstRelease, summary: "inspection summary"},
		"second": second,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent initial graph",
		Prompt: "Inspect, then repair.",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first initial worker did not start")
	}
	select {
	case <-secondStarted:
		t.Fatal("dependent initial worker started before dependency completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(firstRelease)
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("dependent initial worker did not start after dependency completed")
	}
	close(secondRelease)

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(second.promptValue(), "inspection summary") {
		t.Fatalf("dependent prompt missing dependency summary:\n%s", second.promptValue())
	}
}

func TestServiceReplansInitialWorkerGraphAfterErroredDeferredSuccess(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	secondStarted := make(chan string, 1)
	secondRelease := make(chan struct{})
	brain := &replanningBrain{
		plan: Plan{
			Rationale: "initial worker graph has a dependency",
			Workers: []WorkerRequest{
				{
					ID:         "baseline",
					Role:       "baseline collector",
					Reason:     "Collect the baseline.",
					WorkerKind: "first",
					Prompt:     "Collect baseline.",
				},
				{
					ID:         "implement",
					Role:       "implementer",
					Reason:     "Implement after the baseline.",
					WorkerKind: "second",
					Prompt:     "Implement after baseline.",
					DependsOn:  []string{"baseline"},
				},
			},
		},
		decisions: []ReplanDecision{{
			Action:    "wait",
			Rationale: "baseline worker did not finish",
			Message:   "retry baseline collection",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"first": eventRunner{kind: "first", events: []worker.Event{
			{Kind: worker.EventError, Text: "tool use failed while checking build progress"},
			{Kind: worker.EventResult, Text: "Waiting for the cargo build to finish; harness will re-invoke when it completes."},
		}},
		"second": &blockingEventRunner{kind: "second", started: secondStarted, release: secondRelease, summary: "implemented"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent initial graph",
		Prompt: "Collect baseline, then implement.",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-secondStarted:
		close(secondRelease)
		t.Fatal("dependent initial worker started after failed dependency")
	case <-time.After(100 * time.Millisecond):
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != 1 || len(brain.states[0].Results) != 1 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	result := brain.states[0].Results[0]
	if result.Status != core.WorkerFailed || !strings.Contains(result.Summary, "Waiting for the cargo build") || !strings.Contains(result.Error, "deferring completion") {
		t.Fatalf("dependency result = %+v, want failed deferred-success result", result)
	}
}

func TestServiceReplansInitialWorkerGraphValidatorRejectionBeforePublishAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			Rationale: "initial worker graph validates a candidate before publication",
			Workers: []WorkerRequest{
				{
					ID:         "implement_first_candidate",
					Role:       "implementer",
					Reason:     "Implement the first candidate.",
					WorkerKind: "implementer",
					Prompt:     "Implement one candidate.",
				},
				{
					ID:         "validate_first_candidate",
					Role:       "independent validator",
					Reason:     "Validate the candidate.",
					WorkerKind: "validator",
					Prompt:     "Reject bad candidates.",
					DependsOn:  []string{"implement_first_candidate"},
				},
			},
			Actions: []PlanAction{{
				Kind:     "publish_pull_request",
				When:     "after_success",
				Reason:   "Publish only after validation succeeds.",
				WorkerID: "validate_first_candidate",
				Inputs: map[string]any{
					"repo":                 "owner/repo",
					"title":                "Reduce Deno binary size",
					"body":                 "## Summary\n- Reduce binary size.\n\n## Validation\n- release-lite build.",
					"continueAfterPublish": true,
				},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "wait",
			Rationale: "validator rejected the candidate",
			Message:   "move on to another candidate",
		}},
	}
	service, _ := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     brain,
		publisher: publisher,
		runners: map[string]worker.Runner{
			"implementer": eventRunner{kind: "implementer", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented a candidate"}}},
			"validator": eventThenFailRunner{
				kind:   "validator",
				events: []worker.Event{{Kind: worker.EventResult, Text: "Rejecting the candidate because it does not reduce shipped binary size. Recommended next turns: try another candidate."}},
				err:    errors.New("validator rejected candidate"),
			},
		},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "Cargo.toml", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Shrink Deno Binary Size",
		Prompt: "Find multiple reviewable binary size reductions.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want rejected validator candidate not published", publisher.publishCalls)
	}
	if hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "started") {
		t.Fatalf("publish action ran after validator rejected candidate")
	}
	if len(brain.states) != 1 || len(brain.states[0].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	failed := brain.states[0].Results[1]
	if failed.Status != core.WorkerFailed || !strings.Contains(failed.Summary, "Rejecting the candidate") || !strings.Contains(failed.Error, "validator rejected candidate") {
		t.Fatalf("validator result = %+v, want failed rejection in replan context", failed)
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
	case <-time.After(2 * time.Second):
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
	case <-time.After(2 * time.Second):
		t.Fatal("dependent worker did not start after dependency completed")
	}
	close(secondRelease)

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(second.promptValue(), "review summary") {
		t.Fatalf("dependent prompt missing dependency summary:\n%s", second.promptValue())
	}
}

func TestServiceReplansFollowUpGraphAfterFailedDependency(t *testing.T) {
	t.Skip("obsolete: follow-up graph failure replanning was replaced by durable workItem dependency handling")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	secondStarted := make(chan string, 1)
	secondRelease := make(chan struct{})
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the first bounded slice",
			Spawns: []SpawnRequest{
				{
					ID:         "review",
					Role:       "reviewer",
					Reason:     "Review the implementation.",
					WorkerKind: "first",
				},
				{
					ID:         "repair",
					Role:       "repairer",
					Reason:     "Repair review findings.",
					WorkerKind: "second",
					DependsOn:  []string{"review"},
				},
			},
		},
		decisions: []ReplanDecision{{
			Action:    "wait",
			Rationale: "review worker failed",
			Message:   "retry review",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"first": eventThenFailRunner{
			kind: "first",
			err:  errors.New("review command failed"),
		},
		"second": &blockingEventRunner{kind: "second", started: secondStarted, release: secondRelease, summary: "repaired"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent follow-up graph",
		Prompt: "Implement, review, then repair.",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-secondStarted:
		close(secondRelease)
		t.Fatal("dependent follow-up worker started after failed dependency")
	case <-time.After(100 * time.Millisecond):
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != 1 || len(brain.states[0].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	failed := brain.states[0].Results[1]
	if failed.Status != core.WorkerFailed || !strings.Contains(failed.Error, "review command failed") {
		t.Fatalf("follow-up dependency result = %+v", failed)
	}
}

func TestServiceDynamicallyReplansAfterFollowUpWorker(t *testing.T) {
	t.Skip("obsolete: dynamic replanning no longer runs inline follow-up worker graphs")
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
			WorkPlan: &core.WorkPlan{
				Summary: "Implement a first slice, review it, then incorporate feedback.",
				Workstreams: []core.WorkPlanItem{{
					ID:       "implement",
					Goal:     "Implement the first slice.",
					Status:   "running",
					DoneWhen: "The first slice is implemented.",
				}, {
					ID:        "review",
					Goal:      "Review the first slice.",
					Status:    "pending",
					DoneWhen:  "Review findings are reported.",
					DependsOn: []string{"implement"},
				}},
				Validation: []core.WorkPlanItem{{
					ID:        "validate",
					Goal:      "Validate the incorporated result.",
					Status:    "pending",
					DoneWhen:  "Validation is reported.",
					DependsOn: []string{"review"},
				}},
				Risks: []string{"Review may find a missing edge case."},
			},
			Spawns: []SpawnRequest{{
				Role:   "reviewer",
				Reason: "Review the initial implementation.",
			}},
		},
		decisions: []ReplanDecision{
			{
				Action:    "continue",
				Rationale: "review requested an incorporation turn",
				WorkPlan: &core.WorkPlan{
					Summary: "Implementation and review are done; feedback incorporation is running.",
					Workstreams: []core.WorkPlanItem{{
						ID:       "implement",
						Goal:     "Implement the first slice.",
						Status:   "done",
						DoneWhen: "The first slice is implemented.",
					}, {
						ID:       "review",
						Goal:     "Review the first slice.",
						Status:   "done",
						DoneWhen: "Review findings are reported.",
					}, {
						ID:        "incorporate",
						Goal:      "Incorporate the reviewed edge case.",
						Status:    "running",
						DoneWhen:  "The reviewed edge case is fixed.",
						DependsOn: []string{"review"},
					}},
					Validation: []core.WorkPlanItem{{
						ID:        "validate",
						Goal:      "Validate the incorporated result.",
						Status:    "pending",
						DoneWhen:  "Validation is reported.",
						DependsOn: []string{"incorporate"},
					}},
					Risks: []string{"The incorporation turn may uncover more feedback."},
				},
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
				WorkPlan: &core.WorkPlan{
					Summary: "Implementation, review, and feedback incorporation are complete.",
					Workstreams: []core.WorkPlanItem{{
						ID:       "implement",
						Goal:     "Implement the first slice.",
						Status:   "done",
						DoneWhen: "The first slice is implemented.",
					}, {
						ID:       "review",
						Goal:     "Review the first slice.",
						Status:   "done",
						DoneWhen: "Review findings are reported.",
					}, {
						ID:       "incorporate",
						Goal:     "Incorporate the reviewed edge case.",
						Status:   "done",
						DoneWhen: "The reviewed edge case is fixed.",
					}},
					Validation: []core.WorkPlanItem{{
						ID:       "validate",
						Goal:     "Validate the incorporated result.",
						Status:   "done",
						DoneWhen: "Validation is reported.",
					}},
					Risks: []string{},
				},
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
	if countEvents(snapshot.Events, core.EventTaskWorkPlan, task.ID) != 3 {
		t.Fatalf("task.work_plan_updated count = %d, want 3", countEvents(snapshot.Events, core.EventTaskWorkPlan, task.ID))
	}
	if snapshot.Tasks[0].WorkPlan == nil || snapshot.Tasks[0].WorkPlan.Workstreams[2].Status != "done" {
		t.Fatalf("final work plan = %+v", snapshot.Tasks[0].WorkPlan)
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
	if !ledgerContainsWorker(brain.states[0].ContextLedger, brain.states[0].Results[0].WorkerID) {
		t.Fatalf("first replan state missing projected context ledger: %+v", brain.states[0].ContextLedger)
	}
	if len(brain.states[1].Results) != 3 {
		t.Fatalf("second replan results = %d, want 3", len(brain.states[1].Results))
	}
	if brain.states[0].WorkPlan == nil || brain.states[0].WorkPlan.Workstreams[0].Status != "running" {
		t.Fatalf("first replan work plan = %+v", brain.states[0].WorkPlan)
	}
	if brain.states[1].WorkPlan == nil || brain.states[1].WorkPlan.Workstreams[2].Status != "running" {
		t.Fatalf("second replan work plan = %+v", brain.states[1].WorkPlan)
	}
}

func TestServiceRecordsCompactWorkerResultDigestLedgerEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	largeContent := strings.Repeat("large artifact content ", 1000)
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "write result",
	}}, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "benchmark result improved by 12%"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			Diff:         strings.Repeat("raw diff", 1000),
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			Artifacts: []WorkspaceArtifact{{
				ID:      "bench-log",
				Kind:    "log",
				Name:    "benchmark.log",
				Content: largeContent,
			}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Digest artifact", Prompt: "Run work."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	var digest core.Event
	for _, event := range snapshot.Events {
		if event.TaskID == task.ID && event.Type == core.EventTaskAction && eventPayloadContains([]core.Event{event}, core.EventTaskAction, task.ID, "worker_result_digest") {
			digest = event
			break
		}
	}
	if digest.ID == 0 {
		t.Fatalf("missing worker_result_digest event: %+v", snapshot.Events)
	}
	if strings.Contains(string(digest.Payload), largeContent) || strings.Contains(string(digest.Payload), "raw diffraw diff") {
		t.Fatalf("digest event retained raw artifact content or diff: %s", digest.Payload)
	}
	if !strings.Contains(string(digest.Payload), "benchmark result improved") {
		t.Fatalf("digest event missing compact summary: %s", digest.Payload)
	}
	ledger := projectTaskContextLedger(snapshot.Events, task.ID)
	if !ledgerContainsSummary(ledger, "benchmark result improved") {
		t.Fatalf("digest was not projected into context ledger: %+v", ledger)
	}
}

func TestServiceStopsDynamicReplanWhenTaskBecomesTerminal(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 1)
	release := make(chan struct{})
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "initial implementation",
		},
		decisions: []ReplanDecision{
			{
				Action: "continue",
				Plan: &Plan{
					WorkerKind: "follow",
					Prompt:     "continue after initial result",
				},
			},
			{
				Action:    "complete",
				Rationale: "should not run after cancellation",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial"}}},
		"follow": &blockingEventRunner{kind: "follow", started: started, release: release, summary: "follow-up finished"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Terminal replan", Prompt: "Keep working."})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-started:
		if got != "follow" {
			t.Fatalf("started worker kind = %q, want follow", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("follow-up worker did not start")
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
			"reason": "test_terminal_status",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	snapshot := waitForEventCount(t, store, core.EventWorkerCompleted, task.ID, 2)
	time.Sleep(100 * time.Millisecond)
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if taskStatus(snapshot, task.ID) != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", taskStatus(snapshot, task.ID))
	}
	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, "should not run after cancellation") {
		t.Fatalf("dynamic replan continued after terminal task status")
	}
}

func TestServiceDynamicReplanFollowUpHandsOffLocalBaseToRemoteTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	localRunner := &recordingEventRunner{
		kind: "codex",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "local candidate ready",
		}},
	}
	brain := &baseHandoffReplanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "produce the local candidate",
			Metadata: map[string]any{
				"retryTargetID": "local",
			},
		},
		followUp: Plan{
			WorkerKind: "codex",
			Prompt:     "validate the local candidate",
		},
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-fast", Kind: TargetKindSSH, Host: "vm-fast", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})
	remoteExecutor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, brain, map[string]worker.Runner{
		"codex": localRunner,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.go", Status: "modified"}},
		},
		diff: "diff --git a/candidate.go b/candidate.go\n--- a/candidate.go\n+++ b/candidate.go\n@@ -1 +1 @@\n-old\n+new\n",
	}, targets, SSHRunner{Executor: remoteExecutor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent target inheritance",
		Prompt: "Run a dependent follow-up after a local candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 2 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[0].TargetID != "local" || snapshot.ExecutionNodes[0].TargetKind != "local" {
		t.Fatalf("first node should run locally; nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[1].TargetID != "vm-fast" || snapshot.ExecutionNodes[1].TargetKind != "ssh" {
		t.Fatalf("dependent follow-up should move to remote target; nodes = %+v", snapshot.ExecutionNodes)
	}
	joinedCommands := strings.Join(flattenCommands(remoteExecutor.commands), "\n")
	if !strings.Contains(joinedCommands, "base.patch") || !strings.Contains(joinedCommands, "git apply") {
		t.Fatalf("remote handoff did not upload/apply base patch: %+v", remoteExecutor.commands)
	}
	if remoteExecutor.input == "" || !strings.Contains(remoteExecutor.input, "candidate.go") {
		t.Fatalf("uploaded base patch = %q", remoteExecutor.input)
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, task.ID, `"baseHandoff":"patch"`) {
		t.Fatalf("missing base handoff metadata")
	}
}

func TestServiceWaitsWhenReplannerErrorsAfterWorkerResult(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the change",
		},
		err: errors.New("decode codex replan decision: invalid character '}' after top-level value"),
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Fallback complete", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != 1 {
		t.Fatalf("task.replanned count = %d, want 1", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID))
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback wait event")
	}
}

func TestServiceWaitsInsteadOfFallbackCompletionWhenReplannerExceedsContextWindow(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the change",
		},
		err: errors.New("codex replan command failed: Codex ran out of room in the model's context window"),
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Context wait",
		Prompt:   "Do it.",
		Metadata: core.MustJSON(map[string]any{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback replanned event")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"action":"complete"`) {
		t.Fatalf("context-window error used fallback completion")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, "context window") {
		t.Fatalf("missing context-window fallback reason")
	}
}

func TestServiceWaitsWhenReplannerErrorsWithAmbiguousCandidates(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement baseline",
			Spawns: []SpawnRequest{
				{ID: "left", Role: "left", Reason: "Try A.", WorkerKind: "left"},
				{ID: "right", Role: "right", Reason: "Try B.", WorkerKind: "right"},
			},
		},
		err: errors.New("decode codex replan decision: invalid character '}' after top-level value"),
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
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

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Fallback wait", Prompt: "Try alternatives."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback wait event")
	}
}

func TestServiceDoesNotExhaustTurnLimitWhileReplannerMakesProgress(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial slice",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 1,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial"}}},
		"follow": eventRunner{kind: "follow", events: []worker.Event{{Kind: worker.EventResult, Text: "follow-up patch"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Replan limit", Prompt: "Keep improving the candidate."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns+2 {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns+2)
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != maxConsecutiveUnproductiveReplanTurns+2 {
		t.Fatalf("task.replanned count = %d, want %d", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID), maxConsecutiveUnproductiveReplanTurns+2)
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("unexpected fallback replanned event")
	}
	var sawFollow bool
	for _, worker := range snapshot.Workers {
		if worker.Kind == "follow" {
			sawFollow = true
		}
	}
	if !sawFollow {
		t.Fatalf("missing dynamic follow worker: %+v", snapshot.Workers)
	}
}

func TestServiceContinuesDynamicReplanningUntilObjectiveDecision(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial candidate",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 10,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial implementation"}}},
		"follow": failingRunner{kind: "follow", err: errors.New("no useful follow-up progress")},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Stalled replan", Prompt: "Keep trying follow-ups."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatusWithin(t, store, task.ID, core.TaskSucceeded, 15*time.Second)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns+11 {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns+11)
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("unexpected fallback replanned event")
	}
}

func TestServiceBroadGitHubObjectiveWaitsOnReplanErrorInsteadOfFallbackCompletion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "produce first slice",
		},
		err: errors.New("codex provider temporarily unavailable"),
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "cli/main.rs", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Trim Heavy Deno Dependencies",
		Prompt: "Find several dependency-reduction PRs.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want no fallback PR publication", publisher.publishCalls)
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v, want none", snapshot.PullRequests)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback wait event")
	}
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser || snapshot.Tasks[0].ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q/%q, want user approval needed", snapshot.Tasks[0].ObjectiveStatus, snapshot.Tasks[0].ObjectivePhase)
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, task.ID, "dynamic_replan_error") {
		t.Fatalf("missing dynamic replan error approval")
	}
}

func TestServiceReplanErrorWaitsForCoveredPullRequestFeedback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	taskID := "task-covered-pr-feedback-error"
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":         "Covered PR feedback",
			"prompt":        "Keep working while PR feedback is handled.",
			"objectiveMode": "broad",
		}),
	})
	seedRunningPullRequestFollowUp(t, ctx, service, store, taskID)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}

	service.recoverReplanError(ctx, task, 1, nil, errors.New("custom codex replan prompt failed"), replanLoopOptions{})
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return eventPayloadContains(snapshot.Events, core.EventTaskReplanned, taskID, `"internalQueueWait":true`)
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("missing internal queue wait; events = %+v", snapshot.Events)
	})
	if hasEvent(snapshot.Events, core.EventApprovalNeeded, taskID, "") {
		t.Fatalf("unexpected approval needed while PR follow-up is already running")
	}
	task, ok = findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskRunning || task.ObjectiveStatus != core.ObjectiveActive || task.ObjectivePhase != "waiting_followup" {
		t.Fatalf("task = status %q objective %q/%q, want running active/waiting_followup", task.Status, task.ObjectiveStatus, task.ObjectivePhase)
	}
}

func TestServiceReplanCompleteWaitsForCoveredPullRequestFeedback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{}
	service := NewServiceWithWorkspaceManager(store, brain, nil, t.TempDir(), fakeWorkspaceManager{})
	taskID := "task-covered-pr-feedback-complete"
	appendTestEvents(t, store, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":         "Covered PR feedback",
			"prompt":        "Keep working while PR feedback is handled.",
			"objectiveMode": "broad",
		}),
	})
	seedRunningPullRequestFollowUp(t, ctx, service, store, taskID)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}

	ok, completionReason, _ := service.replanLoop(ctx, task, Plan{}, nil)
	if ok {
		t.Fatalf("replanLoop ok = true, want wait for covered PR feedback")
	}
	if completionReason != "" {
		t.Fatalf("completion reason = %q, want empty", completionReason)
	}
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return eventPayloadContains(snapshot.Events, core.EventTaskAction, taskID, `"kind":"replan_completion_rejected"`) &&
			eventPayloadContains(snapshot.Events, core.EventTaskReplanned, taskID, `"internalQueueWait":true`)
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("missing completion rejection/internal wait; events = %+v", snapshot.Events)
	})
	if hasEvent(snapshot.Events, core.EventApprovalNeeded, taskID, "") {
		t.Fatalf("unexpected approval needed while PR follow-up is already running")
	}
	if len(brain.states) != 1 {
		t.Fatalf("replan calls = %d, want one before internal wait", len(brain.states))
	}
}

func TestServiceRetryBroadGitHubTaskWithNewSteeringIgnoresStaleCompletionState(t *testing.T) {
	t.Skip("legacy stale final-candidate retry path was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-broad-stale-final"
	workerID := "worker-stale-final"
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":     "Trim Heavy Deno Dependencies",
				"prompt":    "Find several dependency-reduction PRs.",
				"metadata":  core.MustJSON(map[string]any{"objectiveMode": "broad"}),
				"projectId": "default",
			}),
		},
		{
			Type:   core.EventTaskPlanned,
			TaskID: taskID,
			Payload: core.MustJSON(Plan{
				WorkerKind: "change",
				Prompt:     "produce a dependency-reduction slice",
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload:  core.MustJSON(map[string]any{"kind": "change"}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "validated one dependency-reduction slice",
				"workspaceChanges": WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "Cargo.toml", Status: "modified"}},
				},
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskCanceled,
			}),
		},
		{
			Type:   core.EventTaskSteered,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"message": "Discard the closed PR artifact and continue with focused intermediate PRs.",
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	brain := &replanningBrain{
		decisions: []ReplanDecision{{
			Action:  "wait",
			Message: "continue broad objective via explicit intermediate PR planning",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "Cargo.toml", Status: "modified"}},
		},
	})

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want stale completion candidate bypassed", publisher.publishCalls)
	}
	if len(brain.states) != 1 {
		t.Fatalf("replan calls = %d, want graph retry through replanner", len(brain.states))
	}
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser {
		t.Fatalf("objective status = %q, want waiting_user", snapshot.Tasks[0].ObjectiveStatus)
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, taskID, "continue broad objective") {
		t.Fatalf("missing replan wait approval event")
	}
}

func TestServiceDoesNotApplyDynamicReplanLimitToBroadObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial candidate",
		},
		continueTurns:   maxConsecutiveUnproductiveReplanTurns + 1,
		finishObjective: true,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial implementation"}}},
		"follow": eventRunner{kind: "follow", events: []worker.Event{{Kind: worker.EventResult, Text: "follow-up made no publishable changes"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Broad stalled replan",
		Prompt: "Keep trying follow-ups.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatusWithin(t, store, task.ID, core.TaskSucceeded, 10*time.Second)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns+2 {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns+2)
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("unexpected fallback replanned event for broad objective")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "finish_objective", "completed") {
		t.Fatalf("missing finish_objective task action; payloads:\n%s", taskActionPayloads(snapshot.Events, task.ID))
	}
}

func TestServiceWaitsWhenDynamicReplanningStallsWithoutCandidate(t *testing.T) {
	t.Skip("legacy dynamic replan terminal limit was removed for durable objective work")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "attempt initial implementation",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 10,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  failingRunner{kind: "codex", err: errors.New("missing API token")},
		"follow": failingRunner{kind: "follow", err: errors.New("cannot run worker as root")},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Stalled no candidate", Prompt: "Keep trying until fixed."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback replanned event")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval-needed event")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"action":"complete"`) {
		t.Fatalf("unexpected fallback completion")
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
					Rationale: "initial result needs review and validation",
					Workers: []WorkerRequest{{
						ID:         "incorporate",
						Role:       "implementer",
						Reason:     "Incorporate the first result.",
						WorkerKind: "codex",
						Prompt:     "incorporate the first result",
					}},
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
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) < 4 {
		t.Fatalf("task.planned count = %d, want at least 4", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if len(brain.states[1].Results) != 4 {
		t.Fatalf("second replan results = %d, want 4", len(brain.states[1].Results))
	}
	completedValidationItems := 0
	for _, item := range snapshot.WorkItems {
		if item.Kind == "objective.validate" && item.Status == core.WorkItemSucceeded {
			completedValidationItems++
		}
	}
	if completedValidationItems != 2 {
		t.Fatalf("completed objective.validate work items = %d, want 2; items=%+v", completedValidationItems, snapshot.WorkItems)
	}
}

func TestServiceRunsReplannedSpawnDependingOnWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "establish baseline",
		},
		decisions: []ReplanDecision{
			{
				Action: "continue",
				Plan: &Plan{
					Rationale: "try one candidate then plan the next slice",
					Workers: []WorkerRequest{
						{
							ID:         "implement_header_path",
							Role:       "implementer",
							Reason:     "Implement the header path candidate.",
							WorkerKind: "implementer",
							Prompt:     "implement",
						},
						{
							ID:         "review_header_path",
							Role:       "reviewer",
							Reason:     "Review the header path candidate.",
							WorkerKind: "reviewer",
							Prompt:     "review",
							DependsOn:  []string{"implement_header_path"},
						},
					},
					Spawns: []SpawnRequest{{
						ID:         "next_hotspot_planner",
						Role:       "planner",
						Reason:     "Plan the next hotspot after review.",
						WorkerKind: "planner",
						DependsOn:  []string{"review_header_path"},
					}},
				},
			},
			{
				Action:    "complete",
				Rationale: "all replanned work completed",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":       eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "baseline"}}},
		"implementer": eventRunner{kind: "implementer", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer":    eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
		"planner":     eventRunner{kind: "planner", events: []worker.Event{{Kind: worker.EventResult, Text: "planned next"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Worker dependency spawn", Prompt: "Continue after review."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "planner") {
		t.Fatalf("dependent spawn did not run")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskStatus, task.ID, `depends on unknown spawn "review_header_path"`) {
		t.Fatalf("task recorded unknown-spawn failure")
	}
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if len(brain.states[1].Results) != 4 {
		t.Fatalf("second replan results = %d, want initial plus worker graph and dependent spawn", len(brain.states[1].Results))
	}
}

func TestServiceRunsReplannedWorkerDependingOnPriorWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		Rationale: "work items can depend on earlier work items",
		WorkItems: []WorkItemRequest{{
			ID:         "validate_http_compressible_size_slice",
			Kind:       "objective.validate",
			Reason:     "Validate the current candidate.",
			WorkerKind: "validate",
			Prompt:     "validate",
		}, {
			ID:         "source_next_opportunity_scout",
			Kind:       "objective.scout",
			Reason:     "Scout the next opportunity after validation.",
			WorkerKind: "scout",
			Prompt:     "scout",
			DependsOn:  []string{"validate_http_compressible_size_slice"},
		}},
	}}, map[string]worker.Runner{
		"validate": eventRunner{kind: "validate", events: []worker.Event{{Kind: worker.EventResult, Text: "validated"}}},
		"scout":    eventRunner{kind: "scout", events: []worker.Event{{Kind: worker.EventResult, Text: "scouted"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Cross-turn dependency", Prompt: "Validate, then scout next."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "validate") {
		t.Fatalf("validation worker did not run")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "scout") {
		t.Fatalf("dependent scout worker did not run")
	}
}

func TestServiceCompletesObjectiveWhenWorkerCreatedDuringDynamicReplan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueThenSelectLatestBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement initial slice",
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial"}}},
		"follow": eventRunner{kind: "follow", events: []worker.Event{{Kind: worker.EventResult, Text: "follow-up patch"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Dynamic final result", Prompt: "Patch then select the patch."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	var followWorkerID string
	for _, worker := range snapshot.Workers {
		if worker.Kind == "follow" {
			followWorkerID = worker.ID
			break
		}
	}
	if followWorkerID == "" {
		t.Fatalf("missing follow-up worker: %+v", snapshot.Workers)
	}
	if len(brain.states) != 2 || len(brain.states[1].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
}

func TestServiceEmitsExecutionGraphNodes(t *testing.T) {
	t.Skip("legacy execution graph shape was replaced by session and work-item execution records")
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

func TestServiceSteerTaskMissingTaskReturnsNotFoundWithoutEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	err := service.SteerTask(ctx, "missing-task", core.SteeringRequest{Message: "adjust course"})
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("SteerTask error = %v, want ErrNotFound", err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskSteered, "missing-task") != 0 {
		t.Fatalf("task.steered events = %d, want 0", countEvents(snapshot.Events, core.EventTaskSteered, "missing-task"))
	}
}

func TestNormalizeSteeringTargetKindAliases(t *testing.T) {
	cases := map[string]string{
		"":             "",
		"task":         "task",
		"  Task ":      "task",
		"objective":    "task",
		"OBJECTIVE":    "task",
		"worker":       "worker",
		"Worker":       "worker",
		"session":      "session",
		"  Session  ":  "session",
		"work_item":    "work_item",
		"work-item":    "work_item",
		"workitem":     "work_item",
		"item":         "work_item",
		"pull_request": "pull_request",
		"pull-request": "pull_request",
		"pullrequest":  "pull_request",
		"PR":           "pull_request",
		"mystery":      "mystery",
	}
	for in, want := range cases {
		if got := normalizeSteeringTargetKind(in); got != want {
			t.Errorf("normalizeSteeringTargetKind(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestServiceSteerTaskRejectsTargetWithoutTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	for _, kind := range []string{"worker", "session", "work_item", "pull_request"} {
		err := service.SteerTask(ctx, "task-x", core.SteeringRequest{Message: "go", TargetKind: kind})
		if err == nil || !strings.Contains(err.Error(), "targetId is required") {
			t.Fatalf("SteerTask(%q) err = %v, want targetId is required", kind, err)
		}
	}
}

func TestServiceSteerTaskRejectsUnsupportedTargetKind(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	err := service.SteerTask(ctx, "task-x", core.SteeringRequest{Message: "go", TargetKind: "mystery", TargetID: "x"})
	if err == nil || !strings.Contains(err.Error(), "unsupported steering target kind") {
		t.Fatalf("SteerTask err = %v, want unsupported steering target kind", err)
	}
}

func TestServiceSteerTaskRoutesWorkerTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	seedSteerableWorkerGraph(t, ctx, store, "task-routed-worker", "worker-1")

	if err := service.SteerTask(ctx, "task-routed-worker", core.SteeringRequest{
		Message:    "Bigger benchmarks please.",
		TargetKind: "worker",
		TargetID:   "worker-1",
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerSteered, "task-routed-worker", "worker-1") {
		t.Fatalf("worker.steered event missing after SteerTask(worker): %+v", snapshot.Events)
	}
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-routed-worker", "") {
		t.Fatalf("SteerTask(worker) should not emit task.steered: %+v", snapshot.Events)
	}
}

func TestServiceSteerTaskRoutesSessionTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	taskID := "task-routed-session"
	workerID := "worker-routed-session"
	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Route session", "prompt": "Route session"})},
		{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(Plan{WorkerKind: "mock", Prompt: "work"})},
		{Type: core.EventExecutionPlanned, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"nodeId": "node-routed-session", "workerId": workerID, "workerKind": "mock"})},
		{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock"})},
		{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	if err := service.SteerTask(ctx, taskID, core.SteeringRequest{
		Message:    "focus the session",
		TargetKind: "session",
		TargetID:   workerID,
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerSteered, taskID, workerID) {
		t.Fatalf("SteerTask(session) should reach worker via SteerSession: %+v", snapshot.Events)
	}
	if hasEvent(snapshot.Events, core.EventTaskSteered, taskID, "") {
		t.Fatalf("SteerTask(session) should not emit task.steered: %+v", snapshot.Events)
	}
}

func TestServiceSteerWorkItemRecordsTargetedSteering(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	taskID := "task-work-item-steering"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Targeted steering",
			"prompt": "Run queued work.",
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
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "slice-a",
		"kind":       "objective.slice",
		"targetKind": "slice",
		"targetId":   "slice-a",
		"reason":     "queued slice",
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.SteerTask(ctx, taskID, core.SteeringRequest{
		Message:    "Narrow this slice to parser files.",
		TargetKind: "work_item",
		TargetID:   "slice-a",
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var targeted core.SteeringItem
	for _, item := range snapshot.Steering {
		if item.TargetKind == "work_item" && item.TargetID == "slice-a" {
			targeted = item
			break
		}
	}
	if targeted.ID == "" || targeted.Message != "Narrow this slice to parser files." {
		t.Fatalf("targeted steering = %+v", targeted)
	}
	item, ok := workItemByKind(snapshot, "user.steering")
	if !ok || item.Status != core.WorkItemSucceeded || item.TargetKind != "work_item" || item.TargetID != "slice-a" {
		t.Fatalf("steering work item = %+v ok=%v, want succeeded targeted item", item, ok)
	}
}

func TestServiceSteerPullRequestQueuesFollowUpWorkItem(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, nil, t.TempDir(), fakeWorkspaceManager{})
	appendTrackedPullRequest(t, ctx, store, "task-pr-steering", "", core.TaskRunning)

	if err := service.SteerTask(ctx, "task-pr-steering", core.SteeringRequest{
		Message:    "Answer the reviewer with benchmark numbers.",
		TargetKind: "pull_request",
		TargetID:   "pr-1",
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var targeted core.SteeringItem
	for _, item := range snapshot.Steering {
		if item.TargetKind == "pull_request" && item.TargetID == "pr-1" {
			targeted = item
			break
		}
	}
	if targeted.ID == "" || targeted.Message != "Answer the reviewer with benchmark numbers." {
		t.Fatalf("targeted PR steering = %+v", targeted)
	}
	item, ok := pullRequestFollowUpWorkItemByTarget(snapshot, "task-pr-steering", "pr-1")
	if !ok || item.Status != core.WorkItemQueued || !strings.Contains(item.Prompt, "Answer the reviewer with benchmark numbers.") {
		t.Fatalf("PR follow-up work item = %+v ok=%v", item, ok)
	}
}

func TestServiceTaskSteeringRestartsActiveNonSteerableWorkersWithoutCancelingObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	cancelSeen := make(chan struct{}, 1)
	runner := &restartOnSteeringRunner{started: started, firstCancelSeen: cancelSeen}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Steer running objective", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "leave wasm_dep_analyzer alone"}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-cancelSeen:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task steering did not cancel the active worker")
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if calls := runner.callsValue(); calls < 2 {
		t.Fatalf("runner calls = %d, want restarted worker", calls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "steering_restart", "resumed") {
		t.Fatal("missing resumed steering restart action")
	}
	item, ok := workItemByKind(snapshot, "user.steering")
	if !ok || item.Status != core.WorkItemSucceeded || item.TargetKind != "objective" || item.TargetID != task.ID {
		t.Fatalf("objective steering work item = %+v ok=%v", item, ok)
	}
}

func TestServiceRestartsNonSteerableRunningWorkerWithSteering(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	runner := &restartOnSteeringRunner{started: started}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Steer restart", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "adjust course"}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if calls := runner.callsValue(); calls < 2 {
		t.Fatalf("runner calls = %d, want at least 2", calls)
	}
	prompt := runner.promptValue()
	if !strings.Contains(prompt, "Apply this user steering on the resumed turn") || !strings.Contains(prompt, "adjust course") {
		t.Fatalf("retry prompt did not include steering: %q", prompt)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "steering_restart", "started") {
		t.Fatalf("missing started steering restart action")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "steering_restart", "resumed") {
		t.Fatalf("missing resumed steering restart action")
	}
}

func TestWaitForTaskWorkersStoppedAllowsRunningTaskAfterWorkerCanceled(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-1"
	workerID := "worker-1"
	for _, event := range []core.Event{{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Steering restart",
			"prompt": "Cancel and resume the active worker.",
		}),
	}, {
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}, {
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
		}),
	}, {
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	snapshot, err := service.waitForTaskWorkersStopped(ctx, taskID, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForTaskWorkersStopped returned error: %v", err)
	}
	if status := taskStatus(snapshot, taskID); status != core.TaskRunning {
		t.Fatalf("task status = %q, want running", status)
	}
	if taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("task still has active workers")
	}
}

func TestRetryWorkerExecutionPromptDeduplicatesSteering(t *testing.T) {
	prompt := retryWorkerExecutionPrompt("continue", "worker-1", "thread-1", []string{
		"You need to use release-lite builds of deno. Debug is not accurate",
		"You need to use release-lite builds of deno. Debug is not accurate",
		"  You need to use release-lite builds of deno. Debug is not accurate  ",
	}, "")
	if got := strings.Count(prompt, "You need to use release-lite builds of deno. Debug is not accurate"); got != 1 {
		t.Fatalf("steering occurrence count = %d, want 1:\n%s", got, prompt)
	}
}

func TestRetryPlanForTaskNarrowsInitialWorkerGraphToCanceledWorker(t *testing.T) {
	t.Skip("legacy initial worker graph retry narrowing was removed")
	taskID := "task-1"
	initial := Plan{
		Rationale: "run a broad graph",
		Workers: []WorkerRequest{
			{
				ID:         "scout",
				Role:       "scout",
				Reason:     "Find options.",
				WorkerKind: "claude",
				Prompt:     "Scout options.",
			},
			{
				ID:         "implement",
				Role:       "implementer",
				Reason:     "Implement selected option.",
				WorkerKind: "codex",
				Prompt:     "Implement.",
			},
			{
				ID:         "validate",
				Role:       "validator",
				Reason:     "Validate implementation.",
				WorkerKind: "claude",
				Prompt:     "Validate.",
				DependsOn:  []string{"implement"},
			},
		},
		Actions: []PlanAction{{
			Kind:     "publish_pull_request",
			When:     "after_success",
			WorkerID: "validate",
		}},
	}
	snapshot := core.Snapshot{
		ExecutionNodes: []core.ExecutionNode{{
			ID:         "node-validate",
			TaskID:     taskID,
			WorkerID:   "worker-validate",
			WorkerKind: "claude",
			Status:     core.WorkerCanceled,
			SpawnID:    "validate",
			Role:       "validator",
		}},
		Events: []core.Event{{
			Type:    core.EventTaskPlanned,
			TaskID:  taskID,
			Payload: core.MustJSON(initial),
		}, {
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: "worker-implement",
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
				"metadata": map[string]any{
					"spawnID":   "implement",
					"spawnRole": "implementer",
				},
			}),
		}, {
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: "worker-implement",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "implemented candidate",
				"workspaceChanges": WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
				},
			}),
		}, {
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: "worker-validate",
			Payload: core.MustJSON(map[string]any{
				"workerId":   "worker-validate",
				"workerKind": "claude",
				"nodeId":     "node-validate",
				"spawnId":    "validate",
				"role":       "validator",
			}),
		}, {
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: "worker-validate",
			Payload: core.MustJSON(map[string]any{
				"status": core.WorkerCanceled,
			}),
		}},
	}

	retry, err := retryPlanForTask(snapshot, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry.Workers) != 0 {
		t.Fatalf("retry workers = %+v, want direct single-worker retry", retry.Workers)
	}
	if retry.WorkerKind != "claude" || retry.Prompt == "Scout options." || !strings.Contains(retry.Prompt, "Validate.") {
		t.Fatalf("retry plan = %+v", retry)
	}
	if !strings.Contains(retry.Prompt, "implemented candidate") {
		t.Fatalf("retry prompt missing dependency result:\n%s", retry.Prompt)
	}
	if got := stringMetadata(retry.Metadata, "retryFromWorkerID"); got != "worker-validate" {
		t.Fatalf("retryFromWorkerID = %q, want worker-validate; metadata = %+v", got, retry.Metadata)
	}
	if got := stringMetadata(retry.Metadata, "spawnID"); got != "validate" {
		t.Fatalf("spawnID = %q, want validate; metadata = %+v", got, retry.Metadata)
	}
	if len(retry.Actions) != 1 || retry.Actions[0].WorkerID != "validate" {
		t.Fatalf("retry actions = %+v", retry.Actions)
	}
}

func TestWorkerRunStateFailsEmptyRetainedRetrySuccess(t *testing.T) {
	state := &workerRunState{}
	state.observe(worker.Event{Kind: worker.EventLog, Text: "did some work but produced no final answer"})
	status, err := state.normalizeCompletionStatus(Plan{
		Metadata: map[string]any{"retryFromWorkerID": "worker-old"},
	}, core.WorkerSucceeded, nil, WorkspaceChanges{})
	if status != core.WorkerFailed || err == nil || !strings.Contains(err.Error(), "without a final summary") {
		t.Fatalf("status = %q err = %v, want failed empty retry success", status, err)
	}
}

func TestWorkerRunStateFailsSuccessThatDefersNextValidation(t *testing.T) {
	state := &workerRunState{}
	state.observe(worker.Event{Kind: worker.EventError, Text: "command failed: exit status 1"})
	state.observe(worker.Event{Kind: worker.EventResult, Text: "The exact FFI-symbol search returned no matches. I'm running the focused Node zlib test next."})
	status, err := state.normalizeCompletionStatus(Plan{}, core.WorkerSucceeded, nil, WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
	})
	if status != core.WorkerFailed || err == nil || !strings.Contains(err.Error(), "deferring completion") {
		t.Fatalf("status = %q err = %v, want failed deferred validation success", status, err)
	}
}

func TestWorkerRunStateFailsSuccessWithPriorErrorAndProgressOnlySummary(t *testing.T) {
	state := &workerRunState{}
	state.observe(worker.Event{Kind: worker.EventError, Text: "cargo test failed: exit status 101"})
	state.observe(worker.Event{Kind: worker.EventResult, Text: "The full check has reached `deno_runtime` and CLI support crates. It's long, but still no errors."})
	status, err := state.normalizeCompletionStatus(Plan{}, core.WorkerSucceeded, nil, WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: []WorkspaceChangedFile{{Path: "libs/cache_dir/npm.rs", Status: "modified"}},
	})
	if status != core.WorkerFailed || err == nil || !strings.Contains(err.Error(), "deferring completion") {
		t.Fatalf("status = %q err = %v, want failed progress-only success", status, err)
	}
}

func TestWorkerRunStateOmitsRecoveredToolErrorFromSuccessfulResult(t *testing.T) {
	state := &workerRunState{}
	state.observe(worker.Event{Kind: worker.EventError, Text: "format check failed: exit status 1"})
	state.observe(worker.Event{Kind: worker.EventResult, Text: "Fixed the formatting issue. Validation passed."})
	changes := WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: []WorkspaceChangedFile{{Path: "Cargo.toml", Status: "modified"}},
	}
	status, err := state.normalizeCompletionStatus(Plan{}, core.WorkerSucceeded, nil, changes)
	if status != core.WorkerSucceeded || err != nil {
		t.Fatalf("status = %q err = %v, want successful recovered worker", status, err)
	}
	payload := state.completionPayload(status, err, changes)
	if payload["error"] != nil {
		t.Fatalf("successful completion payload retained recovered error: %+v", payload)
	}
	result := state.turnResult("worker-format-fix", Plan{WorkerKind: "codex"}, status, err, changes)
	if result.Error != "" {
		t.Fatalf("successful worker result retained recovered error: %+v", result)
	}
}

func TestTaskSteeringDeduplicatesRepeatedMessages(t *testing.T) {
	snapshot := core.Snapshot{Events: []core.Event{
		{
			Type:   core.EventTaskSteered,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"message": "Use release-lite builds.",
			}),
		},
		{
			Type:   core.EventTaskSteered,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"message": " Use release-lite builds. ",
			}),
		},
		{
			Type:   core.EventTaskSteered,
			TaskID: "task-1",
			Payload: core.MustJSON(map[string]any{
				"message": "Also compare against main.",
			}),
		},
	}}
	steering := taskSteering(snapshot, "task-1")
	if got, want := strings.Join(steering, "\n"), "Use release-lite builds.\nAlso compare against main."; got != want {
		t.Fatalf("steering = %q, want %q", got, want)
	}
}

func TestServiceWorkerSteeringQueuesReplanState(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:  "wait",
		Message: "pause after worker steering",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	seedSteerableWorkerGraph(t, ctx, store, "task-1", "worker-1")

	if err := service.SteerWorker(ctx, "worker-1", core.SteeringRequest{Message: "Use release-lite builds."}); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventTaskReplanned, "task-1")
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("worker steering leaked into task steering")
	}
	if len(brain.states) == 0 {
		t.Fatalf("brain did not receive replan state")
	}
	pending := brain.states[0].PendingWorkerSteering
	if len(pending) != 1 || pending[0].WorkerID != "worker-1" || pending[0].Message != "Use release-lite builds." {
		t.Fatalf("pending worker steering = %+v", pending)
	}
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		item, ok := workItemByKindTarget(snapshot, "user.worker_steering", "worker", "worker-1")
		return ok && item.Status == core.WorkItemSucceeded
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("worker steering work item was not completed: %+v", snapshot.WorkItems)
	})
	item, ok := workItemByKindTarget(snapshot, "user.worker_steering", "worker", "worker-1")
	if !ok || item.WorkerID != "worker-1" || item.Prompt != "Use release-lite builds." {
		t.Fatalf("worker steering work item = %+v ok=%v", item, ok)
	}
}

func TestServiceSessionTailReturnsWorkerEventsAndCurrentAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	taskID := "task-session-tail"
	workerID := "worker-session-tail"
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Tail", "prompt": "Tail worker output"})}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock"})}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventExecutionPlanned, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{
		"nodeId":        "node-session-tail",
		"workerId":      workerID,
		"workerKind":    "mock",
		"role":          "implementation",
		"targetId":      "vultr-vm",
		"targetKind":    "ssh",
		"remoteSession": "aged-worker-tail",
		"remoteWorkDir": "/work/repo",
	})}); err != nil {
		t.Fatal(err)
	}
	started, err := store.Append(ctx, core.Event{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: "other-worker", Payload: core.MustJSON(map[string]any{"text": "ignore"})}); err != nil {
		t.Fatal(err)
	}
	output, err := store.Append(ctx, core.Event{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "tool", "text": "go test ./..."})})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := store.Append(ctx, core.Event{Type: core.EventWorkerCompleted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{
		"status":  core.WorkerSucceeded,
		"summary": "tail done",
		"workspaceChanges": map[string]any{
			"changedFiles": []map[string]any{{"path": "internal/orchestrator/service.go", "status": "modified"}},
		},
	})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventPRPublished, TaskID: taskID, Payload: core.MustJSON(map[string]any{
		"id":     "pr-session-tail",
		"repo":   "owner/repo",
		"number": 9,
		"url":    "https://github.com/owner/repo/pull/9",
		"branch": "session-tail",
		"title":  "Session tail",
		"metadata": map[string]any{
			"workerId": workerID,
		},
	})}); err != nil {
		t.Fatal(err)
	}

	tail, err := service.SessionTail(ctx, workerID, started.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if tail.SessionID != workerID || tail.WorkerID != workerID || tail.TaskID != taskID {
		t.Fatalf("tail identity = %+v", tail)
	}
	if tail.LastEventID != completed.ID {
		t.Fatalf("lastEventId = %d, want %d", tail.LastEventID, completed.ID)
	}
	if len(tail.Events) != 2 || tail.Events[0].ID != output.ID || tail.Events[1].ID != completed.ID {
		t.Fatalf("events = %+v, want output/completed events %d/%d", tail.Events, output.ID, completed.ID)
	}
	if tail.CurrentAction == nil || !strings.Contains(tail.CurrentAction.Text, "go test") || tail.CurrentAction.EventID != output.ID {
		t.Fatalf("current action = %+v", tail.CurrentAction)
	}
	if tail.Session == nil || tail.Session.RemoteSession != "aged-worker-tail" || tail.Session.RemoteWorkDir != "/work/repo" {
		t.Fatalf("session context = %+v", tail.Session)
	}
	if tail.Worker == nil || tail.Worker.Kind != "mock" {
		t.Fatalf("worker context = %+v", tail.Worker)
	}
	if tail.Node == nil || tail.Node.ID != "node-session-tail" || tail.Node.TargetID != "vultr-vm" {
		t.Fatalf("node context = %+v", tail.Node)
	}
	if len(tail.PullRequests) != 1 || tail.PullRequests[0].ID != "pr-session-tail" {
		t.Fatalf("pull requests = %+v", tail.PullRequests)
	}
	if tail.Completion == nil || tail.Completion.EventID != completed.ID || len(tail.ChangedFiles) != 1 || tail.ChangedFiles[0].Path != "internal/orchestrator/service.go" {
		t.Fatalf("completion = %+v changedFiles = %+v", tail.Completion, tail.ChangedFiles)
	}
}

func TestServiceSessionTailInitialReturnsLatestWindow(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	taskID := "task-session-tail-latest"
	workerID := "worker-session-tail-latest"
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Tail", "prompt": "Tail latest"})}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock"})}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})}); err != nil {
		t.Fatal(err)
	}
	var outputs []core.Event
	for i := 0; i < 8; i++ {
		event, err := store.Append(ctx, core.Event{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "tool", "text": fmt.Sprintf("line-%d", i)})})
		if err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, event)
	}

	tail, err := service.SessionTail(ctx, workerID, 0, 3, core.EventWorkerOutput)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Events) != 3 {
		t.Fatalf("initial tail event count = %d, want 3 (events=%+v)", len(tail.Events), tail.Events)
	}
	wantIDs := []int64{outputs[5].ID, outputs[6].ID, outputs[7].ID}
	for i, event := range tail.Events {
		if event.ID != wantIDs[i] {
			t.Fatalf("initial tail events[%d].ID = %d, want %d (events=%+v)", i, event.ID, wantIDs[i], tail.Events)
		}
	}
	if tail.LastEventID != outputs[7].ID {
		t.Fatalf("LastEventID = %d, want %d", tail.LastEventID, outputs[7].ID)
	}

	incremental, err := service.SessionTail(ctx, workerID, outputs[5].ID, 10, core.EventWorkerOutput)
	if err != nil {
		t.Fatal(err)
	}
	if len(incremental.Events) != 2 {
		t.Fatalf("incremental tail event count = %d, want 2 (events=%+v)", len(incremental.Events), incremental.Events)
	}
	if incremental.Events[0].ID != outputs[6].ID || incremental.Events[1].ID != outputs[7].ID {
		t.Fatalf("incremental tail events = %+v, want %d/%d", incremental.Events, outputs[6].ID, outputs[7].ID)
	}
	if incremental.LastEventID != outputs[7].ID {
		t.Fatalf("incremental LastEventID = %d, want %d", incremental.LastEventID, outputs[7].ID)
	}
}

func TestServiceSessionControlDelegatesToWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	taskID := "task-session-control"
	workerID := "worker-session-control"
	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Control", "prompt": "Control session"})},
		{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(Plan{WorkerKind: "mock", Prompt: "work"})},
		{Type: core.EventExecutionPlanned, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"nodeId": "node-session-control", "workerId": workerID, "workerKind": "mock"})},
		{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock"})},
		{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	if err := service.SteerSession(ctx, workerID, core.SteeringRequest{Message: "focus the session"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerSteered, taskID, workerID) {
		t.Fatalf("missing worker steering event: %+v", snapshot.Events)
	}

	if err := service.CancelSession(ctx, workerID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	worker, ok := findWorker(snapshot, workerID)
	if !ok || worker.Status != core.WorkerCanceled {
		t.Fatalf("worker = %+v ok=%v, want canceled", worker, ok)
	}
}

func TestServiceTaskSteeringQueuesReplanState(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:  "wait",
		Message: "pause after task steering",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	seedSteerableWorkerGraph(t, ctx, store, "task-1", "worker-1")

	if err := service.SteerTask(ctx, "task-1", core.SteeringRequest{Message: "Go for bolder changes."}); err != nil {
		t.Fatal(err)
	}

	task := core.Task{ID: "task-1", Title: "Task", Prompt: "Prompt"}
	service.replanLoop(ctx, task, Plan{WorkerKind: "mock", Prompt: "initial worker"}, []WorkerTurnResult{{
		WorkerID: "worker-1",
		Status:   core.WorkerSucceeded,
		Kind:     "mock",
		Summary:  "finished benchmark",
	}})

	if len(brain.states) == 0 {
		t.Fatalf("brain did not receive replan state")
	}
	got := strings.Join(brain.states[0].TaskSteering, "\n")
	if got != "Go for bolder changes." {
		t.Fatalf("task steering = %q", got)
	}
	if len(brain.states[0].PendingWorkerSteering) != 0 {
		t.Fatalf("task steering leaked into pending worker steering: %+v", brain.states[0].PendingWorkerSteering)
	}
}

func TestReplanLoopBuildsTurnStateWithSingleSnapshot(t *testing.T) {
	ctx := context.Background()
	baseStore := openTestStore(t)
	defer baseStore.Close()

	task := core.Task{ID: "task-1", Title: "Task", Prompt: "Prompt"}
	if _, err := baseStore.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"title":  task.Title,
			"prompt": task.Prompt,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	store := &resettableSnapshotCountingStore{Store: baseStore}
	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:  "wait",
		Message: "pause after one replan turn",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})

	store.resetSnapshotCalls()
	service.replanLoop(ctx, task, Plan{WorkerKind: "mock", Prompt: "initial worker"}, []WorkerTurnResult{{
		WorkerID: "worker-1",
		Status:   core.WorkerSucceeded,
		Kind:     "mock",
		Summary:  "finished benchmark",
	}})

	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	if calls := store.snapshotCalls(); calls != 1 {
		t.Fatalf("Snapshot calls during one replan turn = %d, want 1", calls)
	}
}

func TestServiceWorkerSteeringAnnotatesContinuePlan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{decisions: []ReplanDecision{
		{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "mock",
				Prompt:     "rerun the benchmark",
			},
		},
		{
			Action:  "wait",
			Message: "pause after retry",
		},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "retried with release-lite"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	seedSteerableWorkerGraph(t, ctx, store, "task-1", "worker-1")

	if err := service.SteerWorker(ctx, "worker-1", core.SteeringRequest{Message: "Use release-lite builds."}); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEventCount(t, store, core.EventTaskPlanned, "task-1", 2)
	var retryPlan Plan
	for _, event := range snapshot.Events {
		if event.Type == core.EventTaskPlanned && event.TaskID == "task-1" {
			if err := json.Unmarshal(event.Payload, &retryPlan); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := stringMetadata(retryPlan.Metadata, "retryFromWorkerID"); got != "worker-1" {
		t.Fatalf("retryFromWorkerID = %q, want worker-1; metadata = %+v", got, retryPlan.Metadata)
	}
	if got := strings.Join(stringSliceMetadata(retryPlan.Metadata, "retrySteering"), "\n"); got != "Use release-lite builds." {
		t.Fatalf("retrySteering = %q", got)
	}
}

func TestServiceWorkerSteeringPreventsFallbackCompletionWhenReplannerUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{err: errors.New("codex replan command failed: exec: \"codex\": executable file not found in $PATH")}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	seedSteerableWorkerGraph(t, ctx, store, "task-1", "worker-1")

	if err := service.SteerWorker(ctx, "worker-1", core.SteeringRequest{Message: "Use release-lite builds."}); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return taskStatus(snapshot, "task-1") == core.TaskWaiting &&
			hasEvent(snapshot.Events, core.EventApprovalNeeded, "task-1", "") &&
			eventPayloadContains(snapshot.Events, core.EventTaskReplanned, "task-1", "queued worker steering must be handled")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task did not reach queued steering approval state; status = %q events = %+v", taskStatus(snapshot, "task-1"), snapshot.Events)
	})
	if taskStatus(snapshot, "task-1") != core.TaskWaiting {
		t.Fatalf("task status = %q, want waiting", taskStatus(snapshot, "task-1"))
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, "task-1", "queued worker steering must be handled") {
		t.Fatalf("missing queued steering fallback reason")
	}
}

func TestServiceDeduplicatesConcurrentNonSteerableSteeringRestarts(t *testing.T) {
	t.Skip("legacy non-steerable worker restart dedupe is being replaced by targeted work-item steering")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	retryStarted := make(chan struct{}, 2)
	retryRelease := make(chan struct{})
	runner := &restartOnSteeringRunner{
		started:      started,
		retryStarted: retryStarted,
		retryRelease: retryRelease,
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Duplicate steer restart", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "continue"})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-retryStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry worker did not start")
	}
	select {
	case <-retryStarted:
		t.Fatal("duplicate steering restart launched a second retry worker")
	case <-time.After(100 * time.Millisecond):
	}

	close(retryRelease)
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if calls := runner.callsValue(); calls != 2 {
		t.Fatalf("runner calls = %d, want 2", calls)
	}
	if created := countEvents(snapshot.Events, core.EventWorkerCreated, task.ID); created != 2 {
		t.Fatalf("worker.created count = %d, want 2", created)
	}
	if countTaskActions(snapshot.Events, task.ID, "steering_restart", "started") != 1 {
		t.Fatalf("steering restart started actions = %d, want 1", countTaskActions(snapshot.Events, task.ID, "steering_restart", "started"))
	}
	if countTaskActions(snapshot.Events, task.ID, "steering_restart", "skipped") != 1 {
		t.Fatalf("steering restart skipped actions = %d, want 1", countTaskActions(snapshot.Events, task.ID, "steering_restart", "skipped"))
	}
}

func TestServiceCancelTaskStopsPendingSteeringRestart(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	firstCancelSeen := make(chan struct{})
	firstCancelRelease := make(chan struct{})
	retryStarted := make(chan struct{}, 1)
	runner := &restartOnSteeringRunner{
		started:            started,
		firstCancelSeen:    firstCancelSeen,
		firstCancelRelease: firstCancelRelease,
		retryStarted:       retryStarted,
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Cancel steering restart", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "continue"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstCancelSeen:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("initial worker was not canceled")
	}
	if err := service.CancelTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	close(firstCancelRelease)

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, task.ID, "steering_restart", "skipped")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("steering restart did not skip after task cancel; events = %+v", snapshot.Events)
	})
	if status := taskStatus(snapshot, task.ID); status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", status)
	}
	if created := countEvents(snapshot.Events, core.EventWorkerCreated, task.ID); created != 1 {
		t.Fatalf("worker.created count = %d, want 1", created)
	}
	select {
	case <-retryStarted:
		t.Fatal("steering restart launched a retry worker after task cancel")
	default:
	}
}

func TestServiceRecommendsManualApplyPolicyForCompetingCandidates(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &finalSelectingBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement baseline",
		Spawns: []SpawnRequest{
			{ID: "opt-a", Role: "optimizer", Reason: "Try optimization A.", WorkerKind: "left"},
			{ID: "opt-b", Role: "optimizer", Reason: "Try optimization B.", WorkerKind: "right"},
		},
	}, role: "optimizer"}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
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

func TestServiceRecommendApplyPolicyMissingTaskDoesNotRecordEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	_, err := service.RecommendApplyPolicy(ctx, "missing-task")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hasEvent(snapshot.Events, core.EventApplyPolicy, "missing-task", "") {
		t.Fatalf("recorded apply-policy event for missing task")
	}
}

func TestServiceCompletesWithAmbiguousCandidatesForExplicitSelection(t *testing.T) {
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

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Ambiguous candidates", Prompt: "Try alternatives."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	policy, err := service.RecommendApplyPolicy(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Strategy != "manual_select" {
		t.Fatalf("strategy = %q, policy = %+v", policy.Strategy, policy)
	}
}

func TestServiceIgnoresLegacyExplicitReplanCompletionForCompetingBranches(t *testing.T) {
	t.Skip("legacy explicit completion candidate handling was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &finalSelectingBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement baseline",
		Spawns: []SpawnRequest{
			{ID: "opt-a", Role: "left", Reason: "Try optimization A.", WorkerKind: "left"},
			{ID: "opt-b", Role: "right", Reason: "Try optimization B.", WorkerKind: "right"},
		},
	}, role: "right"}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
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

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Selected candidate", Prompt: "Try alternatives and choose one."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) == 0 || len(brain.states[0].Results) < 3 {
		t.Fatalf("replan states = %+v", brain.states)
	}
}

func TestServiceGithubCompletionWithSelectedNoChangeWorkerDoesNotPublishAncestor(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-no-change-final"
	implID := "worker-impl"
	finalID := "worker-no-change"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Already fixed",
			"prompt":   "Confirm whether issue 107 needs any new code changes.",
			"metadata": map[string]any{},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskReplanned,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"turn": 2,
			"decision": map[string]any{
				"action":          "complete",
				"workerId":        finalID,
				"pullRequestBody": "No new changes are needed; the fix is already present.",
				"rationale":       "The follow-up worker found a clean workspace.",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	err := service.completeTask(ctx, taskID, []WorkerTurnResult{
		{
			WorkerID: implID,
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/plugins.go", Status: "modified"}},
			},
		},
		{
			WorkerID:     finalID,
			Status:       core.WorkerSucceeded,
			BaseWorkerID: implID,
			Summary:      "The intended fix is already present in HEAD and the final worktree diff is empty.",
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
	}, finalID, "The follow-up worker found a clean workspace.")
	if err != nil {
		t.Fatal(err)
	}

	waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want no PR for selected no-change completion worker", publisher.publishCalls)
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
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	remoteWorker := snapshot.Workers[0]
	if !strings.Contains(remoteWorker.Prompt, "Run every command from this execution workspace:\n/runs/"+remoteWorker.ID+"/repo") {
		t.Fatalf("worker prompt missing per-worker execution workspace:\n%s", remoteWorker.Prompt)
	}
	if !strings.Contains(remoteWorker.Prompt, "Do not edit the source checkout directly:\n/repo/default") {
		t.Fatalf("worker prompt missing source-checkout warning:\n%s", remoteWorker.Prompt)
	}
	if !strings.Contains(remoteWorker.Prompt, "do not ask the follow-up task to open a draft pull request unless the user explicitly requested a draft PR") {
		t.Fatalf("worker prompt missing draft PR guard:\n%s", remoteWorker.Prompt)
	}
	if remoteWorker.PromptPath != "/runs/"+remoteWorker.ID+"/prompt.txt" {
		t.Fatalf("worker prompt path = %q", remoteWorker.PromptPath)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerOutput, task.ID, remoteWorker.ID) {
		t.Fatalf("missing remote worker output")
	}
}

func TestServiceRemoteClaudeWorkerUploadsPromptForPrintStdin(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "claude",
		Prompt:     "review remotely",
	}}, map[string]worker.Runner{
		"claude": worker.DefaultRunners()["claude"],
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote Claude", Prompt: "Run Claude on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if got, want := executor.input, snapshot.Workers[0].Prompt; got != want {
		t.Fatalf("uploaded prompt = %q, want worker prompt %q", got, want)
	}
	if !strings.Contains(executor.input, "review remotely") {
		t.Fatalf("uploaded prompt missing plan prompt: %q", executor.input)
	}
}

func TestServiceRemotePluginWorkerUploadsRunnerSpecStdin(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind:      "review-plugin",
		Prompt:          "review remotely",
		ReasoningEffort: "high",
	}}, map[string]worker.Runner{
		"review-plugin": worker.NewPluginRunner("review-plugin", []string{"aged-review-plugin"}),
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote plugin", Prompt: "Run plugin on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 1 || len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("workers = %+v nodes = %+v", snapshot.Workers, snapshot.ExecutionNodes)
	}
	remoteWorker := snapshot.Workers[0]
	node := snapshot.ExecutionNodes[0]
	expected, err := worker.PluginRunnerStdin(worker.Spec{
		ID:              remoteWorker.ID,
		TaskID:          task.ID,
		Kind:            "review-plugin",
		Prompt:          remoteWorker.Prompt,
		WorkDir:         node.RemoteWorkDir,
		ReasoningEffort: "high",
	})
	if err != nil {
		t.Fatal(err)
	}
	if executor.input != expected {
		t.Fatalf("uploaded plugin stdin = %q, want %q", executor.input, expected)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(executor.input), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["prompt"] != remoteWorker.Prompt || !strings.Contains(remoteWorker.Prompt, "review remotely") {
		t.Fatalf("plugin stdin prompt = %v, worker prompt = %q", payload["prompt"], remoteWorker.Prompt)
	}
	if payload["workDir"] != node.RemoteWorkDir {
		t.Fatalf("plugin stdin workDir = %v, want %q", payload["workDir"], node.RemoteWorkDir)
	}
	joinedCommands := strings.Join(flattenCommands(executor.commands), "\n")
	if !strings.Contains(joinedCommands, "launcher.sh") || !strings.Contains(executor.launcherInput, "aged-review-plugin") || !strings.Contains(executor.launcherInput, "run") {
		t.Fatalf("remote launcher did not start plugin runner: commands=%+v launcher=%s", executor.commands, executor.launcherInput)
	}
}

func TestServiceRemoteWorkerUsesProjectCheckoutOverride(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo-root",
		WorkRoot: "/runs",
		Labels:   map[string]string{"role": "remote"},
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote",
		Prompt:     "run remotely",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{
		"remote": buildOnlyRunner{kind: "remote", command: []string{"sh", "-lc", "echo remote output"}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := service.CreateProject(ctx, core.Project{
		ID:              "node",
		LocalPath:       t.TempDir(),
		Repo:            "owner/node",
		RemoteCheckouts: map[string]string{"vm-1": "/custom/node"},
		TargetLabels:    map[string]string{"role": "remote"},
	}); err != nil {
		t.Fatal(err)
	}
	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "node", Title: "Remote", Prompt: "Run on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	node := snapshot.ExecutionNodes[0]
	wantWorkDir := "/runs/" + node.WorkerID + "/repo"
	if node.RemoteWorkDir != wantWorkDir {
		t.Fatalf("remote workdir = %q, want per-worker worktree %q", node.RemoteWorkDir, wantWorkDir)
	}
	joinedCommands := strings.Join(flattenCommands(executor.commands), "\n")
	if !strings.Contains(joinedCommands, "/custom/node") || !strings.Contains(joinedCommands, wantWorkDir) {
		t.Fatalf("remote commands did not use checkout override: %+v", executor.commands)
	}
}

func TestServiceRetryReusesRemoteWorkerTargetWorkspaceAndSession(t *testing.T) {
	t.Skip("legacy remote worker retry reconstruction was removed")
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-remote-retry"
	previousWorkerID := "worker-remote-old"
	plan := Plan{WorkerKind: "codex", Prompt: "continue remote work"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote retry",
			"prompt": "Continue the remote task.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":      previousWorkerID,
			"workerKind":    "codex",
			"nodeId":        "node-remote-old",
			"targetId":      "vm-old",
			"targetKind":    "ssh",
			"remoteSession": "aged-old",
			"remoteRunDir":  "/runs/old",
			"remoteWorkDir": "/repo-old",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/old",
			CWD:           "/repo-old",
			SourceRoot:    "/repo-old",
			WorkspaceName: "aged-old",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      previousWorkerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(worker.Event{
			Kind:   worker.EventLog,
			Stream: "stdout",
			Text:   `{"type":"thread.started","thread_id":"thread-remote"}`,
			Raw:    json.RawMessage(`{"type":"thread.started","thread_id":"thread-remote"}`),
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-new", Kind: TargetKindSSH, Host: "new-vm", WorkDir: "/repo-new", WorkRoot: "/runs", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 10}},
		{ID: "vm-old", Kind: TargetKindSSH, Host: "old-vm", WorkDir: "/repo-default", WorkRoot: "/runs", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	runner := &recordingBuildRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: plan}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if runner.spec.WorkDir != "/repo-old" {
		t.Fatalf("remote retry work dir = %q, want /repo-old", runner.spec.WorkDir)
	}
	if runner.spec.ResumeSessionID != "thread-remote" {
		t.Fatalf("remote retry session = %q, want thread-remote", runner.spec.ResumeSessionID)
	}
	if !strings.Contains(runner.spec.Prompt, "Previous worker ID: "+previousWorkerID) {
		t.Fatalf("remote retry prompt missing context:\n%s", runner.spec.Prompt)
	}
	newNode := latestExecutionNodeForTask(snapshot, taskID, previousWorkerID)
	if newNode.TargetID != "vm-old" || newNode.RemoteWorkDir != "/repo-old" {
		t.Fatalf("new execution node = %+v", newNode)
	}
	if newNode.RemoteRunDir == "/runs/old" || newNode.RemoteSession == "aged-old" {
		t.Fatalf("remote retry should allocate a fresh run/session for logs: %+v", newNode)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, taskID, "retryResumeSessionID", "thread-remote") {
		t.Fatalf("missing remote retry session metadata")
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, taskID, `"retryWorkspaceReused":true`) {
		t.Fatalf("missing remote retry workspace reuse metadata")
	}
}

func TestServiceRetryBackgroundPullRequestFollowUpFailureDoesNotFailObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-followup-retry"
	initialPlan := Plan{WorkerKind: "mock", Prompt: "continue objective"}
	followUpPlan := Plan{
		WorkerKind: "mock",
		Prompt:     "address pull request feedback",
		Metadata: map[string]any{
			"backgroundPullRequestFollowUp": true,
			"pullRequestID":                 "pr-1",
			"pullRequestNumber":             7,
			"pullRequestURL":                "https://github.com/owner/repo/pull/7",
		},
	}
	appendTestEvents(t, store,
		core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":         "Broad objective",
				"prompt":        "Keep improving this project.",
				"objectiveMode": "broad",
			}),
		},
		core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  taskID,
			Payload: core.MustJSON(initialPlan),
		},
		core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: "objective-worker",
			Payload: core.MustJSON(map[string]any{
				"kind": "mock",
				"metadata": map[string]any{
					"workerKind": "mock",
				},
			}),
		},
		core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: "objective-worker",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "objective made useful progress",
				"workspaceChanges": WorkspaceChanges{
					ChangedFiles: []WorkspaceChangedFile{{Path: "src/lib.rs", Status: "modified"}},
					Dirty:        true,
				},
			}),
		},
		core.Event{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-1",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"branch": "codex/aged-test",
				"base":   "main",
				"title":  "Task",
				"state":  "OPEN",
			}),
		},
		core.Event{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-1",
				"status": "queued",
			}),
		},
		core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  taskID,
			Payload: core.MustJSON(followUpPlan),
		},
		core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: "old-followup-worker",
			Payload: core.MustJSON(map[string]any{
				"kind":     "mock",
				"metadata": followUpPlan.Metadata,
			}),
		},
		core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: "old-followup-worker",
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerFailed,
				"summary": "The bounded wait is still running; I will collect the final poll output.",
				"error":   "worker reported success after an unresolved tool or runtime error while deferring completion",
			}),
		},
		core.Event{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskFailed,
				"error":  "worker command failed",
			}),
		},
	)

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:  "wait",
		Message: "continue after operator steering",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": failingRunner{kind: "mock", err: errors.New("worker reported success after an unresolved tool or runtime error while deferring completion")},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, taskID, "pull_request_background_followup", "continued") && len(brain.states) > 0
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("missing continued background follow-up action; events = %+v", snapshot.Events)
	})
	failedStatuses := 0
	for _, event := range snapshot.Events {
		if event.Type != core.EventTaskStatus || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Status core.TaskStatus `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.Status == core.TaskFailed {
			failedStatuses++
		}
	}
	if failedStatuses != 1 {
		t.Fatalf("retry recorded %d failed task statuses, want only the seeded failure; task actions:\n%s", failedStatuses, taskActionPayloads(snapshot.Events, taskID))
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskFailed {
		t.Fatalf("task status = %q, want non-terminal after background follow-up failure", task.Status)
	}
	if len(brain.states) == 0 {
		t.Fatalf("objective replan was not resumed after background follow-up failure")
	}
}

func TestServiceRestoresDurableLoopPromptWhenRemoteRetryWorkspaceUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-remote-loop-degraded-resume"
	previousWorkerID := "worker-remote-loop-old"
	task := core.Task{
		ID:     taskID,
		Title:  "Loop",
		Prompt: "Keep looking for reliability improvements.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "codex",
			"loopIntervalSeconds": 0,
		}),
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":      previousWorkerID,
			"workerKind":    "codex",
			"nodeId":        "node-remote-loop-old",
			"targetId":      "vm-loop",
			"targetKind":    "ssh",
			"remoteSession": "aged-old",
			"remoteRunDir":  "/runs/old",
			"remoteWorkDir": "/repo-old",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/old",
			CWD:           "/repo-old",
			SourceRoot:    "/repo-old",
			WorkspaceName: "aged-old",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      previousWorkerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	targets := NewTargetRegistry([]TargetConfig{{
		ID:           "vm-loop",
		Kind:         TargetKindSSH,
		Host:         "loop-vm",
		WorkDir:      "/repo-default",
		WorkRoot:     "/runs",
		CheckoutRoot: "/checkouts",
		Capacity:     TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
	}})
	runner := &recordingBuildRunner{kind: "codex"}
	executor := &fakeRemoteExecutor{directoryErr: exitCodeError{code: 1}}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	_, err := service.runPlannedWorker(ctx, task, Plan{
		WorkerKind: "codex",
		Prompt:     "compact provider-resume prompt",
		Metadata: map[string]any{
			"executionMode":         "loop",
			"loopIteration":         2,
			"loopRole":              "worker_loop",
			"loopWorkerKind":        "codex",
			"retryContextKind":      "durable_loop",
			"retryFromWorkerID":     previousWorkerID,
			"retryResumeSessionID":  "thread-remote-loop",
			"retryRemoteSession":    "aged-old",
			"retryRemoteRunDir":     "/runs/old",
			"retryRemoteWorkDir":    "/repo-old",
			"retryTargetID":         "vm-loop",
			"retryTargetKind":       "ssh",
			"targetSelectionSource": "retry",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if runner.spec.WorkDir == "/repo-old" {
		t.Fatalf("remote retry reused unavailable work dir %q", runner.spec.WorkDir)
	}
	if runner.spec.ResumeSessionID != "" {
		t.Fatalf("remote retry session = %q, want stripped degraded resume", runner.spec.ResumeSessionID)
	}
	if !strings.Contains(runner.spec.Prompt, "# Durable Agent Loop") || !strings.Contains(runner.spec.Prompt, "Keep looking for reliability improvements.") {
		t.Fatalf("remote retry prompt did not restore durable loop prompt:\n%s", runner.spec.Prompt)
	}
	if strings.Contains(runner.spec.Prompt, "compact provider-resume prompt") {
		t.Fatalf("remote retry prompt kept compact resume prompt after degradation:\n%s", runner.spec.Prompt)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, taskID, `"retryWorkspaceReused":false`) {
		t.Fatalf("missing remote retry workspace reuse failure metadata")
	}
	if eventPayloadContains(snapshot.Events, core.EventWorkerCreated, taskID, `"retryResumeSessionID":"thread-remote-loop"`) {
		t.Fatalf("retry resume session metadata survived degraded remote resume")
	}
}

func TestServiceIgnoresSchedulerTargetLabels(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewLocalTargetRegistry()
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run local work",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "frontend"},
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Local", Prompt: "Run locally."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "local" {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if !eventContains(snapshot.Events, core.EventWorkerCreated, "ignoredTargetLabels") {
		t.Fatalf("missing ignored target label metadata")
	}
}

func TestServiceIgnoresSchedulerRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run normal work",
		Metadata: map[string]any{
			"requiredTargetID": "pinned",
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Normal", Prompt: "Run normally."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "local" {
		t.Fatalf("nodes = %+v, want scheduler requiredTargetID ignored and local selected", snapshot.ExecutionNodes)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "ignoredRequiredTargetID", "pinned") {
		t.Fatalf("missing ignored required target metadata")
	}
}

func TestServiceTaskRequiredTargetIDWinsOverSchedulerRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "scheduler-pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "task-pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run pinned work",
		Metadata: map[string]any{
			"requiredTargetID": "scheduler-pinned",
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Pinned",
		Prompt:   "Run on task-pinned.",
		Metadata: core.MustJSON(map[string]any{"requiredTargetID": "task-pinned"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "task-pinned" {
		t.Fatalf("nodes = %+v, want task requiredTargetID to win", snapshot.ExecutionNodes)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "ignoredRequiredTargetID", "scheduler-pinned") {
		t.Fatalf("missing ignored scheduler required target metadata")
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "requiredTargetID", "task-pinned") {
		t.Fatalf("missing task required target metadata")
	}
}

func TestServiceUsesTaskTargetLabels(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Labels: map[string]string{"location": "local"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "frontend", Kind: TargetKindLocal, Labels: map[string]string{"role": "frontend"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run frontend work",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Frontend",
		Prompt:   "Run on frontend target.",
		Metadata: core.MustJSON(map[string]any{"targetLabels": map[string]any{"role": "frontend"}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "frontend" {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
}

func TestServiceUsesProjectTargetRequirements(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "small", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "large", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("small", core.TargetHealth{}, core.TargetResources{MemoryAvailableMB: 4096, DiskAvailableMB: 50_000})
	targets.UpdateHealth("large", core.TargetHealth{}, core.TargetResources{MemoryAvailableMB: 32_768, DiskAvailableMB: 200_000})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run resource-heavy work",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	project, err := service.CreateProject(ctx, core.Project{
		ID:        "heavy",
		LocalPath: t.TempDir(),
		Requirements: core.ProjectRequirements{
			MemoryMB:  16_384,
			StorageMB: 100_000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: project.ID, Title: "Heavy", Prompt: "Run heavy work."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "large" {
		t.Fatalf("nodes = %+v, want large target", snapshot.ExecutionNodes)
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, task.ID, `"requiredMemoryMB":16384`) {
		t.Fatalf("missing required memory metadata")
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, task.ID, `"requiredStorageMB":100000`) {
		t.Fatalf("missing required storage metadata")
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "targetRequirementsSource", "project") {
		t.Fatalf("missing project requirements source metadata")
	}
}

func TestServiceFollowUpCanMoveAwayFromBaseWorkerTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	baseWorkerID := "base-worker"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-target-inheritance",
		WorkerID: baseWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   baseWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-base",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-fast", Kind: TargetKindSSH, Host: "vm-fast", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "follow up",
		Metadata: map[string]any{
			"baseWorkerID": baseWorkerID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-fast" {
		t.Fatalf("target = %q, want vm-fast", target.ID)
	}
}

func TestServiceSelectExecutionTargetFallsBackLocalAfterRequirementsFail(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-small", Kind: TargetKindSSH, Host: "vm-small", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("local", core.TargetHealth{}, core.TargetResources{MemoryAvailableMB: 1024, DiskAvailableMB: 1024})
	targets.UpdateHealth("vm-small", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{MemoryAvailableMB: 1024, DiskAvailableMB: 1024})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "needs resources",
		Metadata: map[string]any{
			"requiredMemoryMB":  int64(16_384),
			"requiredStorageMB": int64(100_000),
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" || target.Kind != TargetKindLocal {
		t.Fatalf("target = %+v, want local fallback", target)
	}
	if plan.Metadata["retryTargetFallbackToID"] != "local" {
		t.Fatalf("fallback to = %v, want local", plan.Metadata["retryTargetFallbackToID"])
	}
}

func TestServiceRetryInheritsPreviousWorkerTargetWithoutRetryTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	previousWorkerID := "previous-worker"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-retry-target-inheritance",
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-previous",
			"targetId":   "vm-previous",
			"targetKind": "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-previous", Kind: TargetKindSSH, Host: "vm-previous", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryFromWorkerID": previousWorkerID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-previous" {
		t.Fatalf("target = %q, want vm-previous", target.ID)
	}
}

func TestServiceRetryFallsBackWhenExplicitRetryTargetIsUnhealthy(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-bad", Kind: TargetKindSSH, Host: "vm-bad", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-good", Kind: TargetKindSSH, Host: "vm-good", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-bad", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})
	targets.UpdateHealth("vm-good", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.1, MemoryAvailableMB: 8192})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-bad",
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-good" {
		t.Fatalf("target = %q, want vm-good", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-bad" {
		t.Fatalf("fallback from = %v, want vm-bad", plan.Metadata["retryTargetFallbackFromID"])
	}
	if plan.Metadata["retryTargetFallbackToID"] != "vm-good" {
		t.Fatalf("fallback to = %v, want vm-good", plan.Metadata["retryTargetFallbackToID"])
	}
	if reason, _ := plan.Metadata["retryTargetFallbackReason"].(string); reason == "" {
		t.Fatalf("missing retryTargetFallbackReason")
	}
}

func TestServiceRetryTargetIDFallsBackWhenTargetLacksRequestedWorkerTool(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-pinned", Kind: TargetKindSSH, Host: "vm-pinned", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-pinned", core.TargetHealth{
		Status:    "ok",
		Reachable: true,
		Tmux:      true,
		Tools:     map[string]bool{"codex": false},
	}, core.TargetResources{})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "codex",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-pinned",
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %q, want local", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-pinned" {
		t.Fatalf("fallback from = %v, want vm-pinned", plan.Metadata["retryTargetFallbackFromID"])
	}
	if reason, _ := plan.Metadata["retryTargetFallbackReason"].(string); !strings.Contains(reason, `execution target "vm-pinned" does not support worker kind "codex"`) {
		t.Fatalf("fallback reason = %q, want unsupported worker kind", reason)
	}
}

func TestServiceSelectsAnotherTargetAfterWorkerKindMarkedUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-new", Kind: TargetKindSSH, Host: "vm-new", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 10}},
		{ID: "vm-old", Kind: TargetKindSSH, Host: "vm-old", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	if !targets.MarkWorkerKindUnavailable("vm-new", "codex", "missing auth") {
		t.Fatalf("failed to mark target worker kind unavailable")
	}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "codex",
		Prompt:     "retry elsewhere",
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-old" {
		t.Fatalf("target = %q, want vm-old", target.ID)
	}
}

func TestServiceSelectsLocalAfterAllRemoteWorkerKindsMarkedUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-a", Kind: TargetKindSSH, Host: "vm-a", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 10}},
		{ID: "vm-b", Kind: TargetKindSSH, Host: "vm-b", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.MarkWorkerKindUnavailable("vm-a", "codex", "missing auth")
	targets.MarkWorkerKindUnavailable("vm-b", "codex", "missing auth")
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "codex",
		Prompt:     "retry locally",
		Metadata:   map[string]any{},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" || target.Kind != TargetKindLocal {
		t.Fatalf("target = %+v, want local", target)
	}
	if plan.Metadata["retryTargetFallbackToID"] != "local" {
		t.Fatalf("fallback to = %v, want local", plan.Metadata["retryTargetFallbackToID"])
	}
}

func TestServiceRetryFallsBackWhenPreviousWorkerTargetIsUnhealthy(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	previousWorkerID := "previous-worker-fallback"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-retry-target-fallback",
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "mock",
			"nodeId":     "node-previous",
			"targetId":   "vm-bad",
			"targetKind": "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-bad", Kind: TargetKindSSH, Host: "vm-bad", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-good", Kind: TargetKindSSH, Host: "vm-good", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-bad", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})
	targets.UpdateHealth("vm-good", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.1, MemoryAvailableMB: 8192})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryFromWorkerID": previousWorkerID,
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-good" {
		t.Fatalf("target = %q, want vm-good", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-bad" {
		t.Fatalf("fallback from = %v, want vm-bad", plan.Metadata["retryTargetFallbackFromID"])
	}
	if plan.Metadata["retryTargetFallbackToID"] != "vm-good" {
		t.Fatalf("fallback to = %v, want vm-good", plan.Metadata["retryTargetFallbackToID"])
	}
}

func TestServiceRetryTargetReuseFallsBackWhenTargetLacksRequestedWorkerTool(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	previousWorkerID := "previous-worker"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-retry-target-tool",
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-previous",
			"targetId":   "vm-previous",
			"targetKind": "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-previous", Kind: TargetKindSSH, Host: "vm-previous", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-previous", core.TargetHealth{
		Status:    "ok",
		Reachable: true,
		Tmux:      true,
		Tools:     map[string]bool{"codex": false},
	}, core.TargetResources{})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "codex",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryFromWorkerID": previousWorkerID,
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %q, want local", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-previous" {
		t.Fatalf("fallback from = %v, want vm-previous", plan.Metadata["retryTargetFallbackFromID"])
	}
	if reason, _ := plan.Metadata["retryTargetFallbackReason"].(string); !strings.Contains(reason, `execution target "vm-previous" does not support worker kind "codex"`) {
		t.Fatalf("fallback reason = %q, want unsupported worker kind", reason)
	}
}

func TestServiceSelectExecutionTargetEnforcesRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "must run pinned",
		Metadata:   map[string]any{"requiredTargetID": "pinned"},
	})
	if err != nil {
		t.Fatalf("selectExecutionTarget err = %v, want nil", err)
	}
	if target.ID != "pinned" {
		t.Fatalf("target = %q, want pinned", target.ID)
	}
}

func TestServiceSelectExecutionTargetFailsWhenRequiredTargetUnknown(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	_, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "must run pinned",
		Metadata:   map[string]any{"requiredTargetID": "missing"},
	})
	if err == nil {
		t.Fatal("selectExecutionTarget with unknown requiredTargetID succeeded, want hard error (no local fallback)")
	}
	if !strings.Contains(err.Error(), `required execution target "missing"`) {
		t.Fatalf("err = %v, want error mentioning required target", err)
	}
}

func TestServiceRetryReturnsErrorWhenNoFallbackTargetEligible(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-only", Kind: TargetKindSSH, Host: "vm-only", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-only", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-only",
		},
	}
	_, err := service.selectExecutionTarget(ctx, plan)
	if err == nil {
		t.Fatal("expected error when no fallback target is eligible")
	}
	if !strings.Contains(err.Error(), "vm-only") {
		t.Fatalf("error should mention the original target; err = %v", err)
	}
	if _, fellBack := plan.Metadata["retryTargetFallbackToID"]; fellBack {
		t.Fatalf("plan should not record fallback metadata when no eligible target exists: %+v", plan.Metadata)
	}
}

func TestServiceRetryFallbackRespectsTargetLabels(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-bad", Kind: TargetKindSSH, Host: "vm-bad", WorkDir: "/repo", Labels: map[string]string{"role": "gpu"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-other", Kind: TargetKindSSH, Host: "vm-other", WorkDir: "/repo", Labels: map[string]string{"role": "cpu"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-bad", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})
	targets.UpdateHealth("vm-other", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.1, MemoryAvailableMB: 8192})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-bad",
			"targetLabels":  map[string]string{"role": "gpu"},
		},
	}
	_, err := service.selectExecutionTarget(ctx, plan)
	if err == nil {
		t.Fatal("expected error: no healthy target matches the required labels")
	}
}

func TestServiceRegisterTargetProbesImmediately(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"checkoutRootOK=true",
		"tmux=false",
		"repoPresent=false",
		"cpuCount=4",
		"load1=0.3",
	}, "\n")}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: testWorkItemPlan("mock", "noop")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, NewLocalTargetRegistry(), SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	_, err := service.RegisterTarget(ctx, core.TargetConfig{
		ID:       "vm-1",
		Kind:     "ssh",
		Host:     "vm.local",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: core.TargetCapacity{MaxWorkers: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range snapshot.Targets {
		if target.ID == "vm-1" {
			if target.Health.Status != "unhealthy" || !strings.Contains(target.Health.Error, "tmux") || target.Resources.CPUCount != 4 {
				t.Fatalf("target health = %+v resources = %+v", target.Health, target.Resources)
			}
			return
		}
	}
	t.Fatalf("missing registered target: %+v", snapshot.Targets)
}

func appendInterruptedPullRequestFollowUpPlanning(t *testing.T, ctx context.Context, store eventstore.Store, taskID string) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair dirty PR",
			"prompt": "Fix the dirty pull request branch.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
		{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":           "pr-1",
				"repo":         "owner/repo",
				"number":       7,
				"url":          "https://github.com/owner/repo/pull/7",
				"branch":       "codex/aged-test",
				"base":         "main",
				"title":        "Repair dirty PR",
				"state":        "OPEN",
				"checksStatus": "passing",
				"mergeStatus":  "DIRTY",
				"reviewStatus": "",
				"metadata":     map[string]any{"workerId": "worker-original"},
			}),
		},
		{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":      "pr-1",
				"attempt": 1,
				"reason":  "pull_request_needs_work",
			}),
		},
		{
			Type:   core.EventTaskSteered,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"message": pullRequestFollowUpPrompt(core.PullRequest{
					ID:           "pr-1",
					TaskID:       taskID,
					Repo:         "owner/repo",
					Number:       7,
					URL:          "https://github.com/owner/repo/pull/7",
					Branch:       "codex/aged-test",
					Base:         "main",
					State:        "OPEN",
					ChecksStatus: "passing",
					MergeStatus:  "DIRTY",
				}),
			}),
		},
		{
			Type:   core.EventApprovalDecided,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"approved": true,
				"answer":   "resume dirty PR follow-up",
				"reason":   "user_feedback",
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskPlanning,
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
}

func ptrInt(value int) *int {
	return &value
}

func ptrString(value string) *string {
	return &value
}

func seedSteerableWorkerGraph(t *testing.T, ctx context.Context, store eventstore.Store, taskID string, workerID string) {
	t.Helper()
	events := []core.Event{
		{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  "Task",
				"prompt": "Prompt",
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
		{
			Type:   core.EventTaskPlanned,
			TaskID: taskID,
			Payload: core.MustJSON(Plan{
				WorkerKind: "mock",
				Prompt:     "initial worker",
			}),
		},
		{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-1",
				"workerId":   workerID,
				"workerKind": "mock",
				"role":       "benchmark",
				"spawnId":    "bench",
			}),
		},
		{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"kind":    "mock",
				"command": []string{"mock"},
			}),
		},
		{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerSucceeded,
				"summary": "finished benchmark",
			}),
		},
	}
	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
}

func workItemByKindTarget(snapshot core.Snapshot, kind string, targetKind string, targetID string) (core.WorkItem, bool) {
	for _, item := range snapshot.WorkItems {
		if item.Kind == kind && item.TargetKind == targetKind && item.TargetID == targetID {
			return item, true
		}
	}
	return core.WorkItem{}, false
}

func workItemByID(snapshot core.Snapshot, id string) (core.WorkItem, bool) {
	for _, item := range snapshot.WorkItems {
		if item.ID == id {
			return item, true
		}
	}
	return core.WorkItem{}, false
}

func questionByID(snapshot core.Snapshot, id string) core.Question {
	for _, question := range snapshot.Questions {
		if question.ID == id {
			return question
		}
	}
	return core.Question{}
}

func seedRunningPullRequestFollowUp(t *testing.T, ctx context.Context, service *Service, store eventstore.Store, taskID string) {
	t.Helper()
	appendTestEvents(t, store,
		core.Event{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-1",
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
				"branch": "codex/aged-test",
				"base":   "main",
				"title":  "Covered PR feedback",
				"state":  "OPEN",
			}),
		},
		core.Event{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":                "pr-1",
				"attempt":           1,
				"reason":            "pull_request_needs_work",
				"feedbackSignature": "review-1",
			}),
		},
	)
	if err := service.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         "pr_followup_test",
		"kind":       "pr.followup",
		"targetKind": "pull_request",
		"targetId":   "pr-1",
		"reason":     "Handle queued PR feedback.",
		"prompt":     "Fix the PR review feedback.",
		"metadata": map[string]any{
			"backgroundPullRequestFollowUp": true,
			"pullRequestID":                 "pr-1",
			"feedbackSignature":             "review-1",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.recordWorkItemStarted(ctx, taskID, "pr_followup_test", "followup-worker"); err != nil {
		t.Fatal(err)
	}
}

func workItemByKind(snapshot core.Snapshot, kind string) (core.WorkItem, bool) {
	for _, item := range snapshot.WorkItems {
		if item.Kind == kind {
			return item, true
		}
	}
	return core.WorkItem{}, false
}

type fixedBrain struct {
	plan Plan
	err  error
}

func (b fixedBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	if b.err != nil {
		return b.plan, b.err
	}
	return testPlanWithImplicitWorkItem(b.plan), nil
}

type fixedAssistantBrain struct {
	fixedBrain
	answer string
}

func (b fixedAssistantBrain) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	return core.AssistantResponse{
		ConversationID: req.ConversationID,
		Message:        b.answer,
		Metadata:       core.MustJSON(map[string]any{"brain": "test"}),
	}, nil
}

type recordingAssistant struct {
	requests []core.AssistantRequest
}

func (a *recordingAssistant) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	a.requests = append(a.requests, req)
	sessionID := nonEmpty(req.ProviderSessionID, "session-1")
	return core.AssistantResponse{
		ConversationID:    req.ConversationID,
		Message:           "answer",
		Provider:          "codex",
		ProviderSessionID: sessionID,
		Metadata:          core.MustJSON(map[string]any{"assistant": "codex", "providerSessionId": sessionID}),
	}, nil
}

func appendTestEvents(t *testing.T, store eventstore.Store, events ...core.Event) {
	t.Helper()
	ctx := context.Background()
	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
}

type fakePullRequestPublisher struct {
	published        PullRequestPublishSpec
	publishedSpecs   []PullRequestPublishSpec
	publishedWorkers []string
	publishCalls     int
	updated          PullRequestPublishSpec
	updatedPR        core.PullRequest
	updateCalls      int
	errOnce          error
	errCount         int
	status           core.PullRequest
	inspectCalls     int
	commentCalls     int
	commentPR        core.PullRequest
	commentSpec      PullRequestCommentSpec
	list             []core.PullRequest
	listSpec         PullRequestListSpec
	listCalls        int
}

type prPublishingServiceOptions struct {
	brain      BrainProvider
	runners    map[string]worker.Runner
	publisher  *fakePullRequestPublisher
	workDir    string
	cwd        string
	sourceRoot string
	changes    WorkspaceChanges
	applyCalls *int
}

func newPRPublishingService(t *testing.T, store eventstore.Store, opts prPublishingServiceOptions) (*Service, *fakePullRequestPublisher) {
	t.Helper()
	publisher := opts.publisher
	if publisher == nil {
		publisher = &fakePullRequestPublisher{}
	}
	if opts.runners == nil {
		opts.runners = map[string]worker.Runner{
			"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		}
	}
	if opts.workDir == "" {
		opts.workDir = t.TempDir()
	}
	if opts.cwd == "" {
		opts.cwd = t.TempDir()
	}
	if opts.sourceRoot == "" {
		opts.sourceRoot = t.TempDir()
	}
	service := NewServiceWithWorkspaceManager(store, opts.brain, opts.runners, opts.workDir, fakeWorkspaceManager{
		cwd:        opts.cwd,
		sourceRoot: opts.sourceRoot,
		changes:    opts.changes,
		applyCalls: opts.applyCalls,
	})
	service.SetPullRequestPublisher(publisher)
	return service, publisher
}

type fakeTitleGenerator struct {
	title string
	err   error
}

func (g fakeTitleGenerator) GenerateTitle(context.Context, string) (string, error) {
	return g.title, g.err
}

func (p *fakePullRequestPublisher) Publish(_ context.Context, spec PullRequestPublishSpec) (core.PullRequest, error) {
	p.published = spec
	p.publishCalls++
	p.publishedSpecs = append(p.publishedSpecs, spec)
	p.publishedWorkers = append(p.publishedWorkers, spec.WorkerID)
	if p.errCount > 0 && p.publishCalls <= p.errCount {
		return core.PullRequest{}, errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply")
	}
	if p.errOnce != nil && p.publishCalls == 1 {
		return core.PullRequest{}, p.errOnce
	}
	branch := strings.TrimSpace(spec.Branch)
	if branch == "" {
		branch = defaultPRBranch(spec)
	}
	return core.PullRequest{
		ID:           fmt.Sprintf("pr-%d", p.publishCalls),
		TaskID:       spec.TaskID,
		Repo:         spec.Repo,
		Number:       11 + p.publishCalls,
		URL:          fmt.Sprintf("https://github.com/%s/pull/%d", spec.Repo, 11+p.publishCalls),
		Branch:       branch,
		Base:         nonEmpty(spec.Base, "main"),
		Title:        spec.Title,
		State:        "OPEN",
		Draft:        spec.Draft,
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
		Metadata:     core.MustJSON(spec.Metadata),
	}, nil
}

func (p *fakePullRequestPublisher) Update(_ context.Context, pr core.PullRequest, spec PullRequestPublishSpec) (core.PullRequest, error) {
	p.updated = spec
	p.updatedPR = pr
	p.updateCalls++
	if p.errCount > 0 && p.updateCalls <= p.errCount {
		return core.PullRequest{}, errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply")
	}
	updated := pr
	if spec.Repo != "" {
		updated.Repo = spec.Repo
	}
	if spec.Branch != "" {
		updated.Branch = spec.Branch
	}
	if spec.Base != "" {
		updated.Base = spec.Base
	}
	if spec.Title != "" {
		updated.Title = spec.Title
	}
	if len(spec.Metadata) > 0 {
		updated.Metadata = core.MustJSON(spec.Metadata)
	}
	if updated.State == "" {
		updated.State = "OPEN"
	}
	if updated.ChecksStatus == "" {
		updated.ChecksStatus = "pending"
	}
	return updated, nil
}

func (p *fakePullRequestPublisher) Inspect(_ context.Context, pr core.PullRequest) (core.PullRequest, error) {
	p.inspectCalls++
	if p.status.ID == "" {
		p.status.ID = pr.ID
	}
	if p.status.TaskID == "" {
		p.status.TaskID = pr.TaskID
	}
	if p.status.Repo == "" {
		p.status.Repo = pr.Repo
	}
	if p.status.Number == 0 {
		p.status.Number = pr.Number
	}
	if p.status.URL == "" {
		p.status.URL = pr.URL
	}
	if p.status.Branch == "" {
		p.status.Branch = pr.Branch
	}
	if p.status.Base == "" {
		p.status.Base = pr.Base
	}
	if p.status.Title == "" {
		p.status.Title = pr.Title
	}
	return p.status, nil
}

func (p *fakePullRequestPublisher) Comment(_ context.Context, pr core.PullRequest, spec PullRequestCommentSpec) error {
	p.commentCalls++
	p.commentPR = pr
	p.commentSpec = spec
	return nil
}

func (p *fakePullRequestPublisher) List(_ context.Context, spec PullRequestListSpec) ([]core.PullRequest, error) {
	p.listCalls++
	p.listSpec = spec
	if len(p.list) > 0 {
		out := make([]core.PullRequest, len(p.list))
		copy(out, p.list)
		for index := range out {
			if out[index].TaskID == "" {
				out[index].TaskID = spec.TaskID
			}
			if out[index].Repo == "" {
				out[index].Repo = spec.Repo
			}
			if len(out[index].Metadata) == 0 {
				out[index].Metadata = core.MustJSON(spec.Metadata)
			}
		}
		return out, nil
	}
	return []core.PullRequest{{
		ID:           "pr-watch-1",
		TaskID:       spec.TaskID,
		Repo:         spec.Repo,
		Number:       12,
		URL:          "https://github.com/" + spec.Repo + "/pull/12",
		Branch:       "feature",
		Base:         "main",
		Title:        "Watch me",
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
		Metadata:     core.MustJSON(spec.Metadata),
	}}, nil
}

type replanningBrain struct {
	plan      Plan
	decisions []ReplanDecision
	states    []OrchestrationState
}

func (b *replanningBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testPlanWithImplicitWorkItem(b.plan), nil
}

func (b *replanningBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if len(b.decisions) == 0 {
		return ReplanDecision{Action: "complete"}, nil
	}
	decision := b.decisions[0]
	b.decisions = b.decisions[1:]
	return testDecisionWithImplicitWorkItem(decision), nil
}

type baseHandoffReplanningBrain struct {
	plan     Plan
	followUp Plan
	states   []OrchestrationState
}

func (b *baseHandoffReplanningBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testPlanWithImplicitWorkItem(b.plan), nil
}

func (b *baseHandoffReplanningBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if len(b.states) > 1 {
		return ReplanDecision{Action: "complete", Rationale: "validation completed"}, nil
	}
	plan := testPlanWithImplicitWorkItem(b.followUp)
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["baseWorkerID"] = latestCandidateWorkerID(state.Results)
	return ReplanDecision{
		Action:    "continue",
		Rationale: "validate on top of the candidate",
		Plan:      &plan,
	}, nil
}

type completionReviewBrain struct {
	BrainProvider
	ReplanProvider
	reviews     []CompletionReview
	reviewCalls int
}

func (b *completionReviewBrain) ReviewCompletion(context.Context, core.Task, WorkerTurnResult, string) (CompletionReview, error) {
	b.reviewCalls++
	if len(b.reviews) == 0 {
		return CompletionReview{Ready: true}, nil
	}
	review := b.reviews[0]
	b.reviews = b.reviews[1:]
	return review, nil
}

type completionValidationBrain struct {
	states      []OrchestrationState
	reviewCalls int
}

func (b *completionValidationBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testWorkItemPlan("change", "make change"), nil
}

func (b *completionValidationBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	switch len(b.states) {
	case 1:
		return ReplanDecision{Action: "complete", Rationale: "initial implementation is ready"}, nil
	case 2:
		return testDecisionWithImplicitWorkItem(ReplanDecision{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "validate",
				Prompt:     "validate the existing candidate without making changes",
			},
			Rationale: "validate the blocked candidate",
		}), nil
	default:
		return ReplanDecision{
			Action:          "complete",
			Rationale:       "validation worker confirmed the base candidate",
			PullRequestBody: "## Summary\n- Implement cancellation fix.\n\n## Validation\n- go test ./internal/orchestrator",
		}, nil
	}
}

func (b *completionValidationBrain) ReviewCompletion(context.Context, core.Task, WorkerTurnResult, string) (CompletionReview, error) {
	b.reviewCalls++
	if b.reviewCalls == 1 {
		return CompletionReview{Ready: false, Reason: "candidate needs independent validation"}, nil
	}
	return CompletionReview{Ready: true}, nil
}

type publicationReviewBrain struct {
	BrainProvider
	ReplanProvider
	reviews     []PublicationReview
	reviewCalls int
}

func (b *publicationReviewBrain) ReviewPublication(context.Context, core.Task, WorkerTurnResult, PlanAction) (PublicationReview, error) {
	b.reviewCalls++
	if len(b.reviews) == 0 {
		return PublicationReview{Ready: true}, nil
	}
	review := b.reviews[0]
	b.reviews = b.reviews[1:]
	return review, nil
}

type publicationReadinessValidationBrain struct {
	states      []OrchestrationState
	reviewCalls int
}

func (b *publicationReadinessValidationBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testWorkItemPlan("change", "produce benchmark harness candidate"), nil
}

func (b *publicationReadinessValidationBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	switch len(b.states) {
	case 1:
		return ReplanDecision{
			Action:          "complete",
			Rationale:       "benchmark harness candidate is ready",
			PullRequestBody: "## Summary\n- Add benchmark harness.\n\n## Validation\n- benchmark command",
		}, nil
	case 2:
		return testDecisionWithImplicitWorkItem(ReplanDecision{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "validate",
				Prompt:     "validate whether the blocked benchmark-only candidate should publish",
			},
			Rationale: "validate the blocked benchmark-only candidate",
		}), nil
	case 3:
		return ReplanDecision{
			Action:    "complete",
			Rationale: "no-change validation says no product optimization remains",
		}, nil
	default:
		return ReplanDecision{
			Action:  "wait",
			Message: "No publishable optimization remains under the current criteria.",
		}, nil
	}
}

func (b *publicationReadinessValidationBrain) ReviewPublication(context.Context, core.Task, WorkerTurnResult, PlanAction) (PublicationReview, error) {
	b.reviewCalls++
	if b.reviewCalls == 1 {
		return PublicationReview{Ready: false, Reason: "candidate is benchmark-harness-only, not a product optimization"}, nil
	}
	return PublicationReview{Ready: true}, nil
}

type continueThenSelectLatestBrain struct {
	plan   Plan
	states []OrchestrationState
}

func (b *continueThenSelectLatestBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testPlanWithImplicitWorkItem(b.plan), nil
}

func (b *continueThenSelectLatestBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if state.Turn == 1 {
		return testDecisionWithImplicitWorkItem(ReplanDecision{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "follow",
				Prompt:     "patch the candidate",
			},
		}), nil
	}
	return ReplanDecision{
		Action:    "complete",
		Rationale: "dynamic candidate is complete",
	}, nil
}

type continueForTurnsBrain struct {
	plan            Plan
	continueTurns   int
	finishObjective bool
	states          []OrchestrationState
}

func (b *continueForTurnsBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testPlanWithImplicitWorkItem(b.plan), nil
}

func (b *continueForTurnsBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if state.Turn > b.continueTurns {
		if b.finishObjective {
			return ReplanDecision{
				Action:    "finish_objective",
				Rationale: "broad objective is satisfied after continued progress",
				Message:   "Broad objective finished after continued progress.",
			}, nil
		}
		return ReplanDecision{
			Action:    "complete",
			Rationale: "dynamic candidate is complete after continued progress",
		}, nil
	}
	return testDecisionWithImplicitWorkItem(ReplanDecision{
		Action: "continue",
		Plan: &Plan{
			WorkerKind: "follow",
			Prompt:     "continue improving the candidate",
		},
		Rationale: "more work remains",
	}), nil
}

type errorReplanningBrain struct {
	plan Plan
	err  error
}

func (b *errorReplanningBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testPlanWithImplicitWorkItem(b.plan), nil
}

func (b *errorReplanningBrain) Replan(context.Context, core.Task, OrchestrationState) (ReplanDecision, error) {
	return ReplanDecision{}, b.err
}

type finalSelectingBrain struct {
	plan   Plan
	role   string
	states []OrchestrationState
}

func (b *finalSelectingBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return testPlanWithImplicitWorkItem(b.plan), nil
}

func (b *finalSelectingBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	for i := len(state.Results) - 1; i >= 0; i-- {
		result := state.Results[i]
		if result.Role == b.role && resultHasCandidateChanges(result) {
			return ReplanDecision{
				Action:    "complete",
				Rationale: "completed after reviewing " + b.role + " candidate",
			}, nil
		}
	}
	return ReplanDecision{Action: "complete", Rationale: "no matching candidate"}, nil
}

type sequenceBrain struct {
	mu       sync.Mutex
	plans    []Plan
	steering []string
}

func (b *sequenceBrain) Plan(_ context.Context, _ core.Task, steering []string) (Plan, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.steering = append(b.steering[:0], steering...)
	if len(b.plans) == 0 {
		return Plan{}, errors.New("no plans left")
	}
	plan := b.plans[0]
	b.plans = b.plans[1:]
	return testPlanWithImplicitWorkItem(plan), nil
}

type recordingRunner struct {
	kind            string
	prompt          string
	workDir         string
	resumeSessionID string
	reasoningEffort string
}

type eventRunner struct {
	kind   string
	events []worker.Event
}

type eventThenFailRunner struct {
	kind   string
	events []worker.Event
	err    error
}

type failingRunner struct {
	kind string
	err  error
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
	spec    worker.Spec
	calls   int
}

type sequenceEventRunner struct {
	mu      sync.Mutex
	kind    string
	events  [][]worker.Event
	prompt  string
	workDir string
	calls   int
}

type flakyRunner struct {
	mu    sync.Mutex
	kind  string
	calls int
}

type blockingEventRunner struct {
	mu      sync.Mutex
	kind    string
	started chan<- string
	release <-chan struct{}
	summary string
	prompt  string
}

type restartOnSteeringRunner struct {
	mu                 sync.Mutex
	started            chan<- struct{}
	firstCancelSeen    chan<- struct{}
	firstCancelRelease <-chan struct{}
	retryStarted       chan<- struct{}
	retryRelease       <-chan struct{}
	calls              int
	prompt             string
	resumeSessionID    string
}

type steeringRunner struct {
	started chan<- struct{}
	got     chan<- string
}

type buildOnlyRunner struct {
	kind    string
	command []string
}

type recordingBuildRunner struct {
	kind string
	spec worker.Spec
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

func (r eventThenFailRunner) Kind() string {
	return r.kind
}

func (r eventThenFailRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r eventThenFailRunner) Run(ctx context.Context, _ worker.Spec, sink worker.Sink) error {
	for _, event := range r.events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return r.err
}

func (r failingRunner) Kind() string {
	return r.kind
}

func (r failingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r failingRunner) Run(context.Context, worker.Spec, worker.Sink) error {
	return r.err
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
	r.spec = spec
	r.calls++
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

func (r *recordingEventRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingEventRunner) specValue() worker.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spec
}

func (r *sequenceEventRunner) Kind() string {
	return r.kind
}

func (r *sequenceEventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *sequenceEventRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	r.calls++
	call := r.calls
	r.mu.Unlock()
	events := []worker.Event{{Kind: worker.EventResult, Text: "ok"}}
	if call > 0 && call <= len(r.events) {
		events = r.events[call-1]
	} else if len(r.events) > 0 {
		events = r.events[len(r.events)-1]
	}
	for _, event := range events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r *sequenceEventRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *sequenceEventRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *flakyRunner) Kind() string {
	return r.kind
}

func (r *flakyRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *flakyRunner) Run(ctx context.Context, _ worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		return errors.New("transient worker failure")
	}
	return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: "retry succeeded"})
}

func (r *flakyRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
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

func (r *restartOnSteeringRunner) Kind() string {
	return "codex"
}

func (r *restartOnSteeringRunner) Capabilities() worker.Capabilities {
	return worker.Capabilities{ResumeSession: true}
}

func (r *restartOnSteeringRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *restartOnSteeringRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.calls++
	call := r.calls
	if call > 1 {
		r.prompt = spec.Prompt
		r.resumeSessionID = spec.ResumeSessionID
	}
	r.mu.Unlock()
	if call == 1 {
		if err := sink.Event(ctx, worker.Event{
			Kind: worker.EventLog,
			Raw:  json.RawMessage(`{"type":"thread.started","thread_id":"thread-1"}`),
		}); err != nil {
			return err
		}
		close(r.started)
		<-ctx.Done()
		if r.firstCancelSeen != nil {
			r.firstCancelSeen <- struct{}{}
		}
		if r.firstCancelRelease != nil {
			<-r.firstCancelRelease
		}
		return ctx.Err()
	}
	if r.retryStarted != nil {
		r.retryStarted <- struct{}{}
	}
	if r.retryRelease != nil {
		select {
		case <-r.retryRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: "resumed with steering"})
}

func (r *restartOnSteeringRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *restartOnSteeringRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *restartOnSteeringRunner) resumeSessionIDValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resumeSessionID
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

func (r *recordingBuildRunner) Kind() string {
	return r.kind
}

func (r *recordingBuildRunner) Capabilities() worker.Capabilities {
	return worker.Capabilities{ResumeSession: true}
}

func (r *recordingBuildRunner) BuildCommand(spec worker.Spec) []string {
	r.spec = spec
	return []string{"worker", spec.WorkDir, spec.ResumeSessionID}
}

func (r *recordingBuildRunner) Run(context.Context, worker.Spec, worker.Sink) error {
	return errors.New("recording build runner should not run locally")
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

type localCallbackRunner struct {
	kind           string
	prompt         string
	parentWorkerID string
}

func (r *localCallbackRunner) Kind() string {
	return r.kind
}

func (r *localCallbackRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *localCallbackRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	if r.parentWorkerID != "" {
		return nil
	}
	r.parentWorkerID = spec.ID
	callbackDir := filepath.Join(os.TempDir(), "aged-worker-callbacks", spec.ID, "callbacks")
	if err := os.MkdirAll(callbackDir, 0o755); err != nil {
		return err
	}
	body := `{"type":"create_task","promptBase64":"` + base64.StdEncoding.EncodeToString([]byte("follow up from local")) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte("Local follow-up")) + `","parentTaskIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.TaskID)) + `","parentWorkerIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.ID)) + `"}`
	return os.WriteFile(filepath.Join(callbackDir, "create-task.local.json"), []byte(body), 0o644)
}

type localPublishPRCallbackRunner struct {
	kind           string
	title          string
	body           string
	prompt         string
	parentWorkerID string
}

func (r *localPublishPRCallbackRunner) Kind() string {
	return r.kind
}

func (r *localPublishPRCallbackRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *localPublishPRCallbackRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	if r.parentWorkerID != "" {
		return nil
	}
	r.parentWorkerID = spec.ID
	callbackDir := filepath.Join(os.TempDir(), "aged-worker-callbacks", spec.ID, "callbacks")
	if err := os.MkdirAll(callbackDir, 0o755); err != nil {
		return err
	}
	title := nonEmpty(r.title, "Local callback PR")
	body := nonEmpty(r.body, "Callback PR body")
	payload := `{"type":"publish_pull_request","bodyBase64":"` + base64.StdEncoding.EncodeToString([]byte(body)) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte(title)) + `","repoBase64":"` + base64.StdEncoding.EncodeToString([]byte("owner/repo")) + `","parentTaskIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.TaskID)) + `","parentWorkerIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.ID)) + `","continueAfterPublish":true}`
	return os.WriteFile(filepath.Join(callbackDir, "publish-pr.local.json"), []byte(payload), 0o644)
}

func (r *recordingRunner) Kind() string {
	return r.kind
}

func (r *recordingRunner) Capabilities() worker.Capabilities {
	return worker.Capabilities{ResumeSession: true}
}

func (r *recordingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *recordingRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	r.resumeSessionID = spec.ResumeSessionID
	r.reasoningEffort = spec.ReasoningEffort
	return nil
}

type fakeWorkspaceManager struct {
	cwd          string
	sourceRoot   string
	sharedRoot   string
	baseWorkDir  string
	baseRevision string
	changes      WorkspaceChanges
	diff         string
	applyCalls   *int
	diffCalls    *int
	prepareCalls *int
	prepareErr   error
	applyErr     error

	failPrepareAfter int
	failPrepareUntil int
	failApplyUntil   int
}

type transientAppendErrorStore struct {
	eventstore.Store
	mu           sync.Mutex
	eventType    core.EventType
	failuresLeft int
	failures     int
	err          error
}

type snapshotCountingStore struct {
	eventstore.Store
	mu        sync.Mutex
	snapshots int
}

func (s *snapshotCountingStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	s.mu.Lock()
	s.snapshots++
	s.mu.Unlock()
	return s.Store.Snapshot(ctx)
}

func (s *snapshotCountingStore) snapshotCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshots
}

func (s *transientAppendErrorStore) Append(ctx context.Context, event core.Event) (core.Event, error) {
	s.mu.Lock()
	if event.Type == s.eventType && s.failuresLeft > 0 {
		s.failuresLeft--
		s.failures++
		err := s.err
		s.mu.Unlock()
		return core.Event{}, err
	}
	s.mu.Unlock()
	return s.Store.Append(ctx, event)
}

func (s *transientAppendErrorStore) failureCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

type resettableSnapshotCountingStore struct {
	eventstore.Store
	mu    sync.Mutex
	calls int
}

func (s *resettableSnapshotCountingStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Store.Snapshot(ctx)
}

func (s *resettableSnapshotCountingStore) snapshotCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *resettableSnapshotCountingStore) resetSnapshotCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = 0
}

type startFailRemoteExecutor struct {
	fakeRemoteExecutor
	err error
}

func (e *startFailRemoteExecutor) Run(ctx context.Context, argv []string) (string, error) {
	joined := strings.Join(argv, " ")
	if strings.Contains(joined, "tmux new-session") {
		return "", e.err
	}
	return e.fakeRemoteExecutor.Run(ctx, argv)
}

type sequencingWorkspaceManager struct {
	fakeWorkspaceManager
	mu      sync.Mutex
	changes []WorkspaceChanges
	calls   int
}

type recordingWorkspaceManager struct {
	workDir      string
	baseWorkDir  string
	baseRevision string
	changes      WorkspaceChanges
}

func (m *recordingWorkspaceManager) Prepare(_ context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	m.workDir = spec.WorkDir
	m.baseWorkDir = spec.BaseWorkDir
	m.baseRevision = spec.BaseRevision
	return PreparedWorkspace{
		Root:          spec.WorkDir,
		CWD:           spec.WorkDir,
		SourceRoot:    spec.WorkDir,
		WorkspaceName: "shared",
		Change:        "@ fake",
		Status:        "The working copy has no changes.",
		Mode:          string(WorkspaceModeShared),
		VCSType:       "jj",
		WorkerID:      spec.WorkerID,
		TaskID:        spec.TaskID,
	}, nil
}

func (m *recordingWorkspaceManager) Cleanup(_ context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	return WorkspaceCleanup{
		Root:    workspace.Root,
		CWD:     workspace.CWD,
		Mode:    workspace.Mode,
		VCSType: workspace.VCSType,
		Policy:  workspace.CleanupPolicy,
		Result:  result,
		Reason:  "fake cleanup retained workspace",
	}, nil
}

func (m *recordingWorkspaceManager) DescribeChanges(_ context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	if m.changes.Root != "" || m.changes.CWD != "" || m.changes.Dirty || len(m.changes.ChangedFiles) > 0 {
		changes := m.changes
		if changes.Root == "" {
			changes.Root = workspace.Root
		}
		if changes.CWD == "" {
			changes.CWD = workspace.CWD
		}
		if changes.Mode == "" {
			changes.Mode = workspace.Mode
		}
		if changes.VCSType == "" {
			changes.VCSType = workspace.VCSType
		}
		return changes, nil
	}
	return WorkspaceChanges{
		Root:    workspace.Root,
		CWD:     workspace.CWD,
		Mode:    workspace.Mode,
		VCSType: workspace.VCSType,
		Status:  workspace.Status,
	}, nil
}

func (m *recordingWorkspaceManager) ApplyChanges(_ context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	return WorkerApplyResult{
		SourceRoot:    workspace.SourceRoot,
		WorkspaceRoot: workspace.Root,
		Method:        "fake_merge",
		AppliedFiles:  changes.ChangedFiles,
	}, nil
}

func (m fakeWorkspaceManager) Prepare(_ context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	if m.prepareCalls != nil {
		*m.prepareCalls = *m.prepareCalls + 1
		if m.prepareErr != nil {
			if m.failPrepareUntil > 0 && *m.prepareCalls <= m.failPrepareUntil {
				return PreparedWorkspace{}, m.prepareErr
			}
			if m.failPrepareAfter > 0 && *m.prepareCalls > m.failPrepareAfter {
				return PreparedWorkspace{}, m.prepareErr
			}
		}
	}
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
		BaseChange: nonEmpty(m.baseRevision, spec.BaseRevision),
		Status:     "The working copy has no changes.",
		Mode:       mode,
		VCSType:    "jj",
		Dirty:      false,
		WorkerID:   spec.WorkerID,
		TaskID:     spec.TaskID,
	}, nil
}

func (m fakeWorkspaceManager) PrepareShared(_ context.Context, spec WorkspaceSpec) (SharedWorkspace, error) {
	root := m.sharedRoot
	if root == "" {
		root = filepath.Join(nonEmpty(m.cwd, os.TempDir()), ".aged-shared", shortID(spec.TaskID))
	}
	shared := SharedWorkspace{
		Root:         root,
		ArtifactsDir: filepath.Join(root, "artifacts"),
		WorkersDir:   filepath.Join(root, "workers"),
		WorkerDir:    filepath.Join(root, "workers", shortID(spec.WorkerID)),
	}
	for _, dir := range []string{shared.Root, shared.ArtifactsDir, shared.WorkersDir, shared.WorkerDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return SharedWorkspace{}, err
		}
	}
	return shared, nil
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

func (m *sequencingWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	m.mu.Lock()
	index := m.calls
	m.calls++
	m.mu.Unlock()
	if index < len(m.changes) {
		base := m.fakeWorkspaceManager
		base.changes = m.changes[index]
		return base.DescribeChanges(ctx, workspace)
	}
	return m.fakeWorkspaceManager.DescribeChanges(ctx, workspace)
}

func (m fakeWorkspaceManager) DescribeDiff(context.Context, PreparedWorkspace) (string, error) {
	if m.diffCalls != nil {
		*m.diffCalls = *m.diffCalls + 1
	}
	return m.diff, nil
}

func (m fakeWorkspaceManager) ApplyChanges(_ context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	if m.applyCalls != nil {
		*m.applyCalls = *m.applyCalls + 1
		if m.applyErr != nil && m.failApplyUntil > 0 && *m.applyCalls <= m.failApplyUntil {
			return WorkerApplyResult{}, m.applyErr
		}
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
	return waitForTaskStatusWithin(t, store, taskID, status, 2*time.Second)
}

func waitForTaskStatusWithin(t *testing.T, store eventstore.Store, taskID string, status core.TaskStatus, timeout time.Duration) core.Snapshot {
	t.Helper()
	return waitForSnapshotWithin(t, store, func(snapshot core.Snapshot) bool {
		for _, task := range snapshot.Tasks {
			if task.ID == taskID && task.Status == status {
				return true
			}
		}
		return false
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not reach %s; snapshot = %+v", taskID, status, snapshot.Tasks)
	}, timeout)
}

func waitForPullRequests(t *testing.T, store eventstore.Store, taskID string, count int) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		found := 0
		for _, pr := range snapshot.PullRequests {
			if pr.TaskID == taskID {
				found++
			}
		}
		return found >= count
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not publish %d pull requests; pull requests = %+v", taskID, count, snapshot.PullRequests)
	})
}

func waitForTaskStatusEventCount(t *testing.T, store eventstore.Store, taskID string, status core.TaskStatus, count int) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		found := 0
		for _, event := range snapshot.Events {
			if event.Type != core.EventTaskStatus || event.TaskID != taskID {
				continue
			}
			var payload struct {
				Status core.TaskStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Status == status {
				found++
			}
		}
		return found >= count
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record %d %s status events; events = %+v", taskID, count, status, snapshot.Events)
	})
}

func waitForEvent(t *testing.T, store eventstore.Store, eventType core.EventType, taskID string) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasEvent(snapshot.Events, eventType, taskID, "")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record event %s; events = %+v", taskID, eventType, snapshot.Events)
	})
}

func waitForEventCount(t *testing.T, store eventstore.Store, eventType core.EventType, taskID string, count int) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return countEvents(snapshot.Events, eventType, taskID) >= count
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record %d events of type %s; events = %+v", taskID, count, eventType, snapshot.Events)
	})
}

func waitForSnapshot(t *testing.T, store eventstore.Store, ready func(core.Snapshot) bool, failure func(core.Snapshot) string) core.Snapshot {
	t.Helper()
	return waitForSnapshotWithin(t, store, ready, failure, 2*time.Second)
}

func waitForSnapshotWithin(t *testing.T, store eventstore.Store, ready func(core.Snapshot) bool, failure func(core.Snapshot) string, timeout time.Duration) core.Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if ready(snapshot) {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("%s", failure(snapshot))
	return core.Snapshot{}
}

func taskWorkspaceCWD(snapshot core.Snapshot, taskID string) string {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerWorkspace || event.TaskID != taskID {
			continue
		}
		var workspace PreparedWorkspace
		if err := json.Unmarshal(event.Payload, &workspace); err == nil {
			return workspace.CWD
		}
	}
	return ""
}

func hasEvent(events []core.Event, eventType core.EventType, taskID string, workerID string) bool {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && (workerID == "" || event.WorkerID == workerID) {
			return true
		}
	}
	return false
}

func firstEventID(events []core.Event, eventType core.EventType, taskID string, workerID string) int64 {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && (workerID == "" || event.WorkerID == workerID) {
			return event.ID
		}
	}
	return 0
}

func firstEventIDWithPayloadValue(events []core.Event, eventType core.EventType, taskID string, key string, want string) int64 {
	for _, event := range events {
		if event.Type != eventType || event.TaskID != taskID {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payloadValueMatchesRef(stringMetadataValue(payload[key]), want) {
			return event.ID
		}
		if metadata, ok := payload["metadata"].(map[string]any); ok && payloadValueMatchesRef(stringMetadataValue(metadata[key]), want) {
			return event.ID
		}
	}
	return 0
}

func payloadValueMatchesRef(value string, want string) bool {
	return value == want || strings.HasSuffix(value, ":"+want)
}

func hasTaskAction(events []core.Event, taskID string, kind string, status string) bool {
	return countTaskActions(events, taskID, kind, status) > 0
}

func taskActionPayloads(events []core.Event, taskID string) string {
	var payloads []string
	for _, event := range events {
		if event.Type == core.EventTaskAction && event.TaskID == taskID {
			payloads = append(payloads, string(event.Payload))
		}
	}
	return strings.Join(payloads, "\n")
}

func countTaskActions(events []core.Event, taskID string, kind string, status string) int {
	count := 0
	for _, event := range events {
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind   string `json:"kind"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == kind && payload.Status == status {
			count++
		}
	}
	return count
}

func resultErrorContains(results []WorkerTurnResult, needle string) bool {
	for _, result := range results {
		if strings.Contains(result.Error, needle) {
			return true
		}
	}
	return false
}

func replanStatesContainResultError(states []OrchestrationState, needle string) bool {
	for _, state := range states {
		if resultErrorContains(state.Results, needle) {
			return true
		}
	}
	return false
}

func latestEventOfType(events []core.Event, eventType core.EventType, taskID string) core.Event {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Type == eventType && event.TaskID == taskID {
			return event
		}
	}
	return core.Event{}
}

func taskEventSummary(events []core.Event, taskID string) string {
	var parts []string
	for _, event := range events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskAction, core.EventTaskReplanned, core.EventApprovalNeeded, core.EventTaskStatus:
			parts = append(parts, fmt.Sprintf("%s:%s", event.Type, truncateStringForPrompt(string(event.Payload), 400)))
		}
	}
	return strings.Join(parts, " | ")
}

func hasMilestone(milestones []core.TaskMilestone, name string) bool {
	for _, milestone := range milestones {
		if milestone.Name == name {
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

func countTaskStatusErrors(events []core.Event, taskID string, status core.TaskStatus, errorNeedle string) int {
	count := 0
	for _, event := range events {
		if event.Type != core.EventTaskStatus || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Status core.TaskStatus `json:"status"`
			Error  string          `json:"error"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil {
			continue
		}
		if payload.Status == status && strings.Contains(payload.Error, errorNeedle) {
			count++
		}
	}
	return count
}

func countTaskActionEventsExcludingKind(events []core.Event, taskID string, excludedKind string) int {
	count := 0
	for _, event := range events {
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(event.Payload, &payload) == nil && payload.Kind == excludedKind {
			continue
		}
		count++
	}
	return count
}

func latestExecutionNodeForTask(snapshot core.Snapshot, taskID string, excludeWorkerID string) core.ExecutionNode {
	for i := len(snapshot.ExecutionNodes) - 1; i >= 0; i-- {
		node := snapshot.ExecutionNodes[i]
		if node.TaskID == taskID && node.WorkerID != excludeWorkerID {
			return node
		}
	}
	return core.ExecutionNode{}
}

func eventPayloadContains(events []core.Event, eventType core.EventType, taskID string, needle string) bool {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && strings.Contains(string(event.Payload), needle) {
			return true
		}
	}
	return false
}

func flattenCommands(commands [][]string) []string {
	flattened := make([]string, 0, len(commands))
	for _, command := range commands {
		flattened = append(flattened, strings.Join(command, " "))
	}
	return flattened
}

func hasEventPayloadValue(events []core.Event, eventType core.EventType, taskID string, key string, want string) bool {
	for _, event := range events {
		if event.Type != eventType || event.TaskID != taskID {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if stringMetadataValue(payload[key]) == want {
			return true
		}
		if metadata, ok := payload["metadata"].(map[string]any); ok && stringMetadataValue(metadata[key]) == want {
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
	Error            string                 `json:"error"`
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
			Error            string                 `json:"error"`
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
		Error            string                 `json:"error"`
		NeedsInput       bool                   `json:"needsInput"`
		LogCount         int                    `json:"logCount"`
		ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
		WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
	}{}
}
