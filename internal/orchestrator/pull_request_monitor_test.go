package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestServiceDefaultPullRequestMonitorRefreshesTrackedPullRequests(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("success", "CLEAN", "APPROVED")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	if publisher.inspectCalls != 1 {
		t.Fatalf("inspect calls = %d, want 1", publisher.inspectCalls)
	}
	if snapshot.PullRequests[0].ChecksStatus != "success" || snapshot.PullRequests[0].MergeStatus != "CLEAN" || snapshot.PullRequests[0].ReviewStatus != "APPROVED" {
		t.Fatalf("pull request = %+v", snapshot.PullRequests[0])
	}
}

func TestServiceDefaultPullRequestMonitorSkipsTerminalTaskPullRequests(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("success", "CLEAN", "APPROVED")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskCanceled)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.inspectCalls != 0 {
		t.Fatalf("inspect calls = %d, want 0", publisher.inspectCalls)
	}
	if hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") || hasEvent(snapshot.Events, core.EventPRFollowUp, "task-1", "") {
		t.Fatalf("monitor events = %+v", snapshot.Events)
	}
}

func TestServiceDefaultPullRequestMonitorContinuesTasksForPRsNeedingAttention(t *testing.T) {
	cases := []struct {
		name   string
		status core.PullRequest
	}{
		{name: "failing checks", status: monitoredPullRequestStatus("failing", "CLEAN", "APPROVED")},
		{name: "dirty branch", status: monitoredPullRequestStatus("success", "DIRTY", "APPROVED")},
		{name: "conflicting mergeability", status: monitoredPullRequestStatus("success", "CONFLICTING", "APPROVED")},
		{name: "new comment", status: monitoredPullRequestStatus("success", "CLEAN", "COMMENTED")},
		{name: "untriggered feedback metadata", status: monitoredPullRequestStatusWithMetadata("success", "CLEAN", "", core.MustJSON(map[string]any{
			"latestPullRequestFeedbackSignature":          "2026-05-11T22:01:05Z:conversation:IC_1",
			"latestPullRequestFeedbackTriggeredSignature": "2026-05-11T21:59:00Z:conversation:IC_0",
		}))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openTestStore(t)
			defer store.Close()

			publisher := &fakePullRequestPublisher{status: tc.status}
			service := newTestPullRequestMonitorService(t, store, publisher)
			appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

			if err := service.MonitorPullRequestsOnce(ctx); err != nil {
				t.Fatal(err)
			}

			snapshot := waitForEvent(t, store, core.EventPRFollowUp, "task-1")
			if !hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") {
				t.Fatalf("missing status check event")
			}
			if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
				t.Fatalf("automatic pull request feedback steered task")
			}
			followUp := latestPullRequestFollowUpPayload(t, snapshot, "task-1")
			if followUp.Status != "queued" || followUp.ID != "pr-1" || followUp.Attempt != 1 {
				t.Fatalf("follow-up payload = %+v", followUp)
			}
		})
	}
}

func TestServiceDefaultPullRequestMonitorQueuesFailingCheckContext(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	metadata := core.MustJSON(map[string]any{
		"latestFailingCheckName":       "Go / unit",
		"latestFailingCheckStatus":     "COMPLETED",
		"latestFailingCheckConclusion": "FAILURE",
		"latestFailingCheckURL":        "https://github.com/owner/repo/actions/runs/1/job/2",
		"latestFailingCheckSummary":    "TestFoo failed at internal/foo_test.go:42",
	})
	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatusWithMetadata("failing", "CLEAN", "APPROVED", metadata)}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRFollowUp, "task-1")
	followUp := latestPullRequestFollowUpPayload(t, snapshot, "task-1")
	for _, want := range []string{
		"Failing check context:",
		"Go / unit (FAILURE)",
		"https://github.com/owner/repo/actions/runs/1/job/2",
		"internal/foo_test.go:42",
	} {
		if !strings.Contains(followUp.Prompt, want) {
			t.Fatalf("follow-up prompt missing %q:\n%s", want, followUp.Prompt)
		}
	}
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("automatic pull request feedback steered task")
	}
}

func TestServiceDefaultPullRequestMonitorSkipsCleanPRs(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("success", "CLEAN", "APPROVED")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	if hasEvent(snapshot.Events, core.EventPRFollowUp, "task-1", "") {
		t.Fatalf("clean pull request started follow-up")
	}
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("clean pull request steered task")
	}
}

func TestServicePullRequestMonitorDoesNotRewriteUnchangedPRState(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("success", "CLEAN", "APPROVED")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	first := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	firstEventCount := len(first.Events)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.inspectCalls != 2 {
		t.Fatalf("inspect calls = %d, want 2", publisher.inspectCalls)
	}
	if len(second.Events) != firstEventCount {
		t.Fatalf("event count after unchanged poll = %d, want %d", len(second.Events), firstEventCount)
	}
	if countEvents(second.Events, core.EventPRStatusChecked, "task-1") != 1 {
		t.Fatalf("status check events = %d, want 1", countEvents(second.Events, core.EventPRStatusChecked, "task-1"))
	}
	if countEvents(second.Events, core.EventTaskArtifact, "task-1") != 1 {
		t.Fatalf("artifact events = %d, want 1", countEvents(second.Events, core.EventTaskArtifact, "task-1"))
	}
	if countEvents(second.Events, core.EventTaskObjective, "task-1") != 1 {
		t.Fatalf("objective events = %d, want 1", countEvents(second.Events, core.EventTaskObjective, "task-1"))
	}
}

func TestServiceDefaultPullRequestMonitorSkipsBlockedPendingPRs(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("pending", "BLOCKED", "")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	if hasEvent(snapshot.Events, core.EventPRFollowUp, "task-1", "") {
		t.Fatalf("pending blocked pull request started follow-up")
	}
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveWaitingExternal || task.ObjectivePhase != "pr_open" {
		t.Fatalf("task objective = %s/%s, want waiting_external/pr_open", task.ObjectiveStatus, task.ObjectivePhase)
	}
}

func TestServicePullRequestMonitorDoesNotCancelRunningTaskForClosedPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	status := monitoredPullRequestStatus("success", "CLEAN", "APPROVED")
	status.State = "CLOSED"
	publisher := &fakePullRequestPublisher{status: status}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskRunning)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskCanceled {
		t.Fatalf("task status = %q, want running task not canceled by closed PR", task.Status)
	}
	if task.ObjectivePhase != "intermediate_pr_closed" {
		t.Fatalf("objective phase = %q, want intermediate_pr_closed", task.ObjectivePhase)
	}
}

func TestServicePullRequestMonitorDoesNotTerminalizeIntermediatePullRequest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     string
		wantPhase string
	}{
		{name: "closed", state: "CLOSED", wantPhase: "intermediate_pr_closed"},
		{name: "merged", state: "MERGED", wantPhase: "intermediate_pr_merged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openTestStore(t)
			defer store.Close()

			status := monitoredPullRequestStatusWithMetadata("success", "CLEAN", "APPROVED", core.MustJSON(map[string]any{
				"continueAfterPublish": true,
				"publicationPhase":     "intermediate",
			}))
			status.State = tc.state
			publisher := &fakePullRequestPublisher{status: status}
			service := newTestPullRequestMonitorService(t, store, publisher)
			appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

			if err := service.MonitorPullRequestsOnce(ctx); err != nil {
				t.Fatal(err)
			}

			snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
			task, ok := findTask(snapshot, "task-1")
			if !ok {
				t.Fatal("missing task")
			}
			if task.Status == core.TaskCanceled || task.Status == core.TaskSucceeded {
				t.Fatalf("task status = %q, want intermediate PR not terminalize task", task.Status)
			}
			if task.ObjectivePhase != tc.wantPhase {
				t.Fatalf("objective phase = %q, want %q", task.ObjectivePhase, tc.wantPhase)
			}
		})
	}
}

func TestServicePullRequestMonitorReplansAfterIntermediatePullRequestMerged(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	status := monitoredPullRequestStatusWithMetadata("success", "CLEAN", "APPROVED", core.MustJSON(map[string]any{
		"continueAfterPublish": true,
		"publicationPhase":     "intermediate",
	}))
	status.State = "MERGED"
	publisher := &fakePullRequestPublisher{status: status}
	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:  "wait",
		Message: "continue after the merged intermediate PR",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: "task-1",
		Payload: core.MustJSON(Plan{
			WorkerKind: "mock",
			Prompt:     "first slice",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"kind":     "mock",
			"metadata": map[string]any{"workerKind": "mock"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "published first slice",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, "task-1", "intermediate_pull_request_terminal_replan", "started") &&
			countEvents(snapshot.Events, core.EventTaskReplanned, "task-1") > 0
	}, func(snapshot core.Snapshot) string {
		return "missing intermediate PR terminal replan"
	})
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status == core.TaskFailed || task.Status == core.TaskCanceled {
		t.Fatalf("task status = %q, want intermediate PR merge to keep objective resumable", task.Status)
	}
	if len(brain.states) == 0 {
		t.Fatalf("intermediate PR merge did not enter dynamic replanning")
	}
}

func TestServicePullRequestMonitorAutoMergesReadyPRsWhenAllowed(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:        "project-1",
		Name:      "Project",
		LocalPath: projectRoot,
		Repo:      "owner/repo",
		PullRequestPolicy: core.PullRequestPolicy{
			AllowMerge:  true,
			AutoMerge:   true,
			MergeMethod: "rebase",
		},
	}}, "project-1")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &mergeTrackingPullRequestPublisher{
		fakePullRequestPublisher: fakePullRequestPublisher{status: monitoredPullRequestStatus("passing", "UNKNOWN", "APPROVED")},
	}
	service := newTestPullRequestMonitorService(t, store, publisher)
	service.SetProjects(projects)
	appendTrackedPullRequest(t, ctx, store, "task-1", "project-1", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, "task-1", core.TaskSucceeded)
	if publisher.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", publisher.mergeCalls)
	}
	if publisher.mergeSpec.Repo != "owner/repo" || publisher.mergeSpec.Number != 7 || publisher.mergeSpec.Method != "rebase" {
		t.Fatalf("merge spec = %+v", publisher.mergeSpec)
	}
	if snapshot.PullRequests[0].State != "MERGED" {
		t.Fatalf("pull request state = %q, want MERGED", snapshot.PullRequests[0].State)
	}
	if snapshot.Tasks[0].ObjectivePhase != "merged" {
		t.Fatalf("objective phase = %q, want merged", snapshot.Tasks[0].ObjectivePhase)
	}
}

func TestServicePullRequestMonitorUsesPullRequestRepoMergePolicy(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	agedRoot := t.TempDir()
	denoRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{
			ID:        "aged",
			Name:      "aged",
			LocalPath: agedRoot,
			Repo:      "nathanwhit/aged",
			PullRequestPolicy: core.PullRequestPolicy{
				AllowMerge: true,
				AutoMerge:  true,
			},
		},
		{
			ID:           "deno",
			Name:         "deno",
			LocalPath:    denoRoot,
			Repo:         "nathanwhit/deno",
			UpstreamRepo: "denoland/deno",
			PullRequestPolicy: core.PullRequestPolicy{
				AllowMerge: false,
				AutoMerge:  false,
			},
		},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	status := monitoredPullRequestStatus("passing", "UNKNOWN", "APPROVED")
	status.Repo = "denoland/deno"
	status.URL = "https://github.com/denoland/deno/pull/33992"
	publisher := &mergeTrackingPullRequestPublisher{
		fakePullRequestPublisher: fakePullRequestPublisher{status: status},
	}
	service := newTestPullRequestMonitorService(t, store, publisher)
	service.SetProjects(projects)
	appendTrackedPullRequestWithRepo(t, ctx, store, "task-1", "aged", core.TaskWaiting, "denoland/deno", 33992)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	if publisher.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", publisher.mergeCalls)
	}
	if snapshot.PullRequests[0].State == "MERGED" {
		t.Fatalf("pull request state = %q, want unmerged", snapshot.PullRequests[0].State)
	}
}

func TestServicePullRequestMonitorStartsFollowUpWhenAutoMergeFails(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:        "project-1",
		Name:      "Project",
		LocalPath: projectRoot,
		Repo:      "owner/repo",
		PullRequestPolicy: core.PullRequestPolicy{
			AllowMerge: true,
			AutoMerge:  true,
		},
	}}, "project-1")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &mergeTrackingPullRequestPublisher{
		fakePullRequestPublisher: fakePullRequestPublisher{status: monitoredPullRequestStatus("passing", "UNKNOWN", "APPROVED")},
		mergeErr:                 errors.New("merge conflict after base branch changed"),
	}
	service := newTestPullRequestMonitorService(t, store, publisher)
	service.SetProjects(projects)
	appendTrackedPullRequest(t, ctx, store, "task-1", "project-1", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRFollowUp, "task-1")
	if publisher.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", publisher.mergeCalls)
	}
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("automatic pull request feedback steered task")
	}
	if snapshot.Tasks[0].ObjectivePhase != "pr_needs_work" {
		t.Fatalf("objective phase = %q, want pr_needs_work", snapshot.Tasks[0].ObjectivePhase)
	}
	if got := pullRequestAutoMergeError(snapshot.PullRequests[0]); !strings.Contains(got, "merge conflict") {
		t.Fatalf("auto merge error = %q", got)
	}
}

func TestServiceDefaultPullRequestMonitorQueuesFeedbackWhileTaskRunning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	metadata := core.MustJSON(map[string]any{
		"latestPullRequestFeedbackSignature":          "2026-05-11T22:01:05Z:conversation:IC_1",
		"latestPullRequestFeedbackTriggeredSignature": "2026-05-11T21:59:00Z:conversation:IC_0",
	})
	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatusWithMetadata("success", "CLEAN", "", metadata)}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskRunning)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForEvent(t, store, core.EventPRFollowUp, "task-1")
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("automatic pull request feedback steered task")
	}
	if !pullRequestHasUntriggeredFeedback(snapshot.PullRequests[0]) {
		t.Fatalf("queued feedback was marked handled before pull request update")
	}
	followUp := latestPullRequestFollowUpPayload(t, snapshot, "task-1")
	if followUp.Status != "queued" || followUp.ID != "pr-1" {
		t.Fatalf("follow-up payload = %+v", followUp)
	}
	if item, ok := pullRequestFollowUpWorkItem(snapshot, "task-1", "pr-1", "2026-05-11T22:01:05Z:conversation:IC_1"); !ok || item.Status != core.WorkItemQueued || item.Kind != "pr.followup" {
		t.Fatalf("work item = %+v, ok=%v; want queued pr.followup", item, ok)
	}
	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvents(snapshot.Events, core.EventPRFollowUp, "task-1"); got != 1 {
		t.Fatalf("pull request follow-up events = %d, want 1", got)
	}
	if got := countEvents(snapshot.Events, core.EventWorkItemQueued, "task-1"); got != 1 {
		t.Fatalf("work item queued events = %d, want 1", got)
	}
}

func TestPullRequestFollowUpPlanIgnoresFeedbackAlreadyBeingHandled(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskRunning)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id": "pr-1",
			"metadata": map[string]any{
				"latestPullRequestFeedbackSignature":          "2026-05-11T22:01:05Z:conversation:IC_1",
				"latestPullRequestFeedbackTriggeredSignature": "2026-05-11T21:59:00Z:conversation:IC_0",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":                "pr-1",
			"attempt":           1,
			"status":            "queued",
			"reason":            "pull_request_needs_work",
			"repo":              "owner/repo",
			"number":            7,
			"url":               "https://github.com/owner/repo/pull/7",
			"branch":            "codex/aged-test",
			"feedbackSignature": "2026-05-11T22:01:05Z:conversation:IC_1",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	service := newTestPullRequestMonitorService(t, store, &fakePullRequestPublisher{})
	if err := service.recordPullRequestFollowUpWorkItem(ctx, core.PullRequest{
		ID:     "pr-1",
		TaskID: "task-1",
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/aged-test",
		Base:   "main",
	}, "address review", "2026-05-11T22:01:05Z:conversation:IC_1"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending := pendingPullRequestFeedback(snapshot, "task-1"); len(pending) != 1 {
		t.Fatalf("pending feedback = %+v, want original pending feedback preserved", pending)
	}
	if _, _, ok := pullRequestFollowUpForPlan(snapshot, "task-1", Plan{
		WorkItems: []WorkItemRequest{{
			ID:              "continue-objective",
			Kind:            "objective.implement",
			Reason:          "continue independent objective work",
			Prompt:          "continue",
			TargetKind:      "objective",
			TargetID:        "task-1",
			WorkerKind:      "mock",
			ReasoningEffort: "low",
		}},
	}); ok {
		t.Fatal("planner treated already-covered PR feedback as requiring another follow-up")
	}
}

func TestQueuePlanWorkItemsSkipsDuplicatePullRequestFollowUp(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := newTestPullRequestMonitorService(t, store, &fakePullRequestPublisher{})
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskRunning)
	if err := service.recordPullRequestFollowUpWorkItem(ctx, core.PullRequest{
		ID:     "pr-1",
		TaskID: "task-1",
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/aged-test",
		Base:   "main",
	}, "address review", "sig-1"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	queued, err := service.queuePlanWorkItems(ctx, task, Plan{
		Metadata: map[string]any{"planID": "plan-1"},
		WorkItems: []WorkItemRequest{{
			ID:              "handle-pr-feedback",
			Kind:            "pr.followup",
			Reason:          "handle review",
			Prompt:          "address review",
			TargetKind:      "pull_request",
			TargetID:        "pr-1",
			WorkerKind:      "mock",
			ReasoningEffort: "low",
			Metadata:        map[string]any{"feedbackSignature": "sig-1"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 0 {
		t.Fatalf("queued duplicate work items = %+v, want none", queued)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, item := range snapshot.WorkItems {
		if item.TaskID == "task-1" && item.Kind == "pr.followup" && item.TargetID == "pr-1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("pr.followup work item count = %d, want 1; items = %+v", count, snapshot.WorkItems)
	}
	if !hasTaskAction(snapshot.Events, "task-1", "duplicate_pull_request_followup_skipped", "skipped") {
		t.Fatal("missing duplicate skip task action")
	}
}

func TestServicePullRequestMonitorStartsBackgroundFollowUpWhileObjectiveWorkerRuns(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	metadata := core.MustJSON(map[string]any{
		"latestPullRequestFeedbackSignature":          "2026-05-11T22:01:05Z:conversation:IC_1",
		"latestPullRequestFeedbackTriggeredSignature": "2026-05-11T21:59:00Z:conversation:IC_0",
	})
	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatusWithMetadata("success", "CLEAN", "COMMENTED", metadata)}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskRunning)
	appendActiveWorker(t, ctx, store, "task-1", "objective-worker")

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		item, ok := pullRequestFollowUpWorkItemByTarget(snapshot, "task-1", "pr-1")
		return hasTaskAction(snapshot.Events, "task-1", "pull_request_background_followup", "completed") &&
			ok && item.Status == core.WorkItemSucceeded
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task did not complete background follow-up; events = %+v workItems = %+v", snapshot.Events, snapshot.WorkItems)
	})
	pending := pendingPullRequestFeedback(snapshot, "task-1")
	if len(pending) != 1 || pending[0].PullRequestID != "pr-1" || pending[0].FeedbackSignature != "2026-05-11T22:01:05Z:conversation:IC_1" {
		t.Fatalf("pending feedback = %+v, want signed feedback to survive watch-only background follow-up", pending)
	}
	if !workerActive(snapshot, "objective-worker") {
		t.Fatalf("objective worker was not left active; workers = %+v", snapshot.Workers)
	}
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskRunning {
		t.Fatalf("task status = %q, want broad objective to keep running", task.Status)
	}
	backgroundWorkers := 0
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID != "task-1" || node.WorkerID == "objective-worker" {
			continue
		}
		metadata := map[string]any{}
		if len(node.Metadata) > 0 {
			_ = json.Unmarshal(node.Metadata, &metadata)
		}
		if boolMetadata(metadata, "backgroundPullRequestFollowUp") && stringMetadata(metadata, "pullRequestID") == "pr-1" {
			backgroundWorkers++
		}
	}
	if backgroundWorkers != 1 {
		t.Fatalf("background follow-up workers = %d, want 1; nodes = %+v", backgroundWorkers, snapshot.ExecutionNodes)
	}
	if item, ok := pullRequestFollowUpWorkItemByTarget(snapshot, "task-1", "pr-1"); !ok || item.Status != core.WorkItemSucceeded {
		t.Fatalf("work item = %+v, ok=%v; want succeeded", item, ok)
	} else {
		var metadata struct {
			PlanActions []PlanAction `json:"planActions"`
		}
		if err := json.Unmarshal(item.Metadata, &metadata); err != nil {
			t.Fatal(err)
		}
		if len(metadata.PlanActions) == 0 || metadata.PlanActions[0].Kind != "update_pull_request" {
			t.Fatalf("work item plan actions = %+v, want update_pull_request first", metadata.PlanActions)
		}
		if metadata.PlanActions[0].WorkerID != item.ID {
			t.Fatalf("update_pull_request worker id = %q, want work item id %q", metadata.PlanActions[0].WorkerID, item.ID)
		}
	}
}

func TestServicePullRequestMonitorBackgroundFollowUpUpdatesExistingPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	metadata := core.MustJSON(map[string]any{
		"latestPullRequestFeedbackSignature":          "2026-05-11T22:01:05Z:conversation:IC_1",
		"latestPullRequestFeedbackTriggeredSignature": "2026-05-11T21:59:00Z:conversation:IC_0",
		"latestPullRequestFeedbackBody":               "Please address the review.",
	})
	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatusWithMetadata("success", "CLEAN", "COMMENTED", metadata)}
	service := newTestPullRequestMonitorServiceWithWorkspace(t, store, publisher, fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty: true,
			Diff:  "diff --git a/web/src/main.tsx b/web/src/main.tsx\n",
			ChangedFiles: []WorkspaceChangedFile{{
				Path:   "web/src/main.tsx",
				Status: "modified",
			}},
		},
	})
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskRunning)
	appendActiveWorker(t, ctx, store, "task-1", "objective-worker")

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return publisher.updateCalls == 1 &&
			eventPayloadContains(snapshot.Events, core.EventTaskAction, "task-1", `"kind":"update_pull_request"`)
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("background follow-up did not update PR; updateCalls=%d events=%+v workItems=%+v", publisher.updateCalls, snapshot.Events, snapshot.WorkItems)
	})
	item, ok := pullRequestFollowUpWorkItemByTarget(snapshot, "task-1", "pr-1")
	if !ok {
		t.Fatal("missing pull request follow-up work item")
	}
	if item.Status != core.WorkItemSucceeded {
		t.Fatalf("work item status = %q, want succeeded; item = %+v", item.Status, item)
	}
	if publisher.updatedPR.ID != "pr-1" {
		t.Fatalf("updated PR id = %q, want pr-1", publisher.updatedPR.ID)
	}
	if publisher.updated.WorkerID == "" {
		t.Fatalf("missing update worker id: %+v", publisher.updated)
	}
	if publisher.updated.WorkerID != item.WorkerID {
		t.Fatalf("update worker id = %q, want work item worker %q", publisher.updated.WorkerID, item.WorkerID)
	}
	if publisher.updated.Branch != "codex/aged-test" {
		t.Fatalf("update branch = %q, want codex/aged-test", publisher.updated.Branch)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskAction, "task-1", `"workerId":"`+publisher.updated.WorkerID+`"`) {
		t.Fatalf("missing update action worker id %q in events", publisher.updated.WorkerID)
	}
	if !workerActive(snapshot, "objective-worker") {
		t.Fatalf("objective worker was not left active; workers = %+v", snapshot.Workers)
	}
}

func TestServiceDefaultPullRequestMonitorQueuesNewFeedbackWhenOldFollowUpPending(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatusWithMetadata("success", "CLEAN", "COMMENTED", core.MustJSON(map[string]any{
		"latestPullRequestFeedbackSignature":          "2026-05-20T18:57:36Z:review_thread:PRRC_2",
		"latestPullRequestFeedbackTriggeredSignature": "2026-05-20T18:25:57Z:conversation:IC_1",
		"latestPullRequestFeedbackBody":               "Please add focused tests.",
	}))}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: "task-1",
		Payload: core.MustJSON(Plan{
			WorkerKind: "mock",
			Prompt:     "repair pending checks",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"status":  "queued",
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEventCount(t, store, core.EventPRFollowUp, "task-1", 2)
	followUp := latestPullRequestFollowUpPayload(t, snapshot, "task-1")
	if followUp.FeedbackSignature != "2026-05-20T18:57:36Z:review_thread:PRRC_2" {
		t.Fatalf("follow-up feedback signature = %q", followUp.FeedbackSignature)
	}
	if followUp.Attempt != 2 {
		t.Fatalf("follow-up attempt = %d, want 2", followUp.Attempt)
	}
	pending := pendingPullRequestFeedback(snapshot, "task-1")
	if len(pending) != 1 || pending[0].FeedbackSignature != followUp.FeedbackSignature {
		t.Fatalf("pending feedback = %+v, want only latest signature", pending)
	}
}

func TestPendingPullRequestFeedbackSurvivesFailedFollowUpPlan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id": "pr-1",
			"metadata": map[string]any{
				"latestPullRequestFeedbackSignature":          "sig-1",
				"latestPullRequestFeedbackTriggeredSignature": "sig-0",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":                "pr-1",
			"attempt":           1,
			"status":            "queued",
			"reason":            "pull_request_needs_work",
			"repo":              "owner/repo",
			"number":            7,
			"url":               "https://github.com/owner/repo/pull/7",
			"branch":            "codex/aged-test",
			"feedbackSignature": "sig-1",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: "task-1",
		Payload: core.MustJSON(Plan{
			WorkerKind: "codex",
			Prompt:     "address PR feedback",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskAction,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"kind":   "worker_failure_recovery",
			"status": "continued",
			"error":  "prepare remote checkout: pathspec did not match",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := pendingPullRequestFeedback(snapshot, "task-1")
	if len(pending) != 1 || pending[0].PullRequestID != "pr-1" || pending[0].FeedbackSignature != "sig-1" {
		t.Fatalf("pending feedback = %+v, want unhandled PR feedback to remain queued", pending)
	}
}

func TestPendingPullRequestFeedbackSurvivesWatchActionForSignedFeedback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id": "pr-1",
			"metadata": map[string]any{
				"latestPullRequestFeedbackSignature":          "sig-1",
				"latestPullRequestFeedbackTriggeredSignature": "sig-0",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":                "pr-1",
			"attempt":           1,
			"status":            "queued",
			"reason":            "pull_request_needs_work",
			"repo":              "owner/repo",
			"number":            7,
			"url":               "https://github.com/owner/repo/pull/7",
			"branch":            "codex/aged-test",
			"feedbackSignature": "sig-1",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: "task-1",
		Payload: core.MustJSON(Plan{
			WorkerKind: "codex",
			Prompt:     "address PR feedback",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskAction,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "watch_pull_requests",
			"inputs": map[string]any{
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := pendingPullRequestFeedback(snapshot, "task-1")
	if len(pending) != 1 || pending[0].PullRequestID != "pr-1" || pending[0].FeedbackSignature != "sig-1" {
		t.Fatalf("pending feedback = %+v, want signed feedback to survive watch-only action", pending)
	}
}

func TestPendingPullRequestDescriptionFeedbackRequiresMetadataUpdate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id": "pr-1",
			"metadata": map[string]any{
				"latestPullRequestFeedbackSignature":          "sig-description",
				"latestPullRequestFeedbackTriggeredSignature": "sig-old",
				"latestPullRequestFeedbackBody":               "improve the description",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":                "pr-1",
			"attempt":           1,
			"status":            "queued",
			"reason":            "pull_request_needs_work",
			"repo":              "owner/repo",
			"number":            7,
			"url":               "https://github.com/owner/repo/pull/7",
			"branch":            "codex/aged-test",
			"feedbackSignature": "sig-description",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskAction,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"kind":          "update_pull_request",
			"pullRequestId": "pr-1",
			"inputs": map[string]any{
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := pendingPullRequestFeedback(snapshot, "task-1")
	if len(pending) != 1 || pending[0].FeedbackSignature != "sig-description" {
		t.Fatalf("pending feedback = %+v, want description feedback to survive generic update", pending)
	}

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskAction,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"kind":          "update_pull_request",
			"pullRequestId": "pr-1",
			"inputs": map[string]any{
				"repo":         "owner/repo",
				"number":       7,
				"url":          "https://github.com/owner/repo/pull/7",
				"body":         "## Summary\n- Explain the PR clearly.\n\n## Validation\n- Not run.",
				"metadataOnly": true,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending := pendingPullRequestFeedback(snapshot, "task-1"); len(pending) != 0 {
		t.Fatalf("pending feedback = %+v, want metadata update to clear description feedback", pending)
	}
}

func TestPendingPullRequestFeedbackClearsAlreadyTriggeredSignedFeedbackAfterWatchAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	signature := "2026-05-11T22:01:05Z:conversation:IC_1"
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id": "pr-1",
			"metadata": map[string]any{
				"latestPullRequestFeedbackSignature":          signature,
				"latestPullRequestFeedbackTriggeredSignature": signature,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":                "pr-1",
			"attempt":           1,
			"status":            "queued",
			"reason":            "pull_request_needs_work",
			"repo":              "owner/repo",
			"number":            7,
			"url":               "https://github.com/owner/repo/pull/7",
			"branch":            "codex/aged-test",
			"feedbackSignature": signature,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskAction,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "watch_pull_requests",
			"inputs": map[string]any{
				"repo":   "owner/repo",
				"number": 7,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending := pendingPullRequestFeedback(snapshot, "task-1"); len(pending) != 0 {
		t.Fatalf("pending feedback = %+v, want already-triggered signed feedback cleared by watch", pending)
	}
}

func TestPendingPullRequestFeedbackClearsStatusFollowUpAfterWatchAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"status":  "queued",
			"reason":  "pull_request_needs_work",
			"repo":    "owner/repo",
			"number":  7,
			"url":     "https://github.com/owner/repo/pull/7",
			"branch":  "codex/aged-test",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskAction,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "watch_pull_requests",
			"inputs": map[string]any{
				"repo":   "owner/repo",
				"number": 7,
				"url":    "https://github.com/owner/repo/pull/7",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending := pendingPullRequestFeedback(snapshot, "task-1"); len(pending) != 0 {
		t.Fatalf("pending feedback = %+v, want status follow-up cleared by watch", pending)
	}
}

func TestServicePullRequestFeedbackQueueResumesWaitingTaskWithPendingState(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:  "wait",
		Message: "pause after observing feedback",
	}}}
	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("failing", "CLEAN", "APPROVED")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	service.brain = brain
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: "task-1",
		Payload: core.MustJSON(Plan{
			WorkerKind: "mock",
			Prompt:     "original plan",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "published a pull request",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventTaskReplanned, "task-1")
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("automatic pull request feedback steered task")
	}
	if len(brain.states) == 0 {
		t.Fatalf("brain did not receive replan state")
	}
	pending := brain.states[0].PendingPullRequestFeedback
	if len(pending) != 1 || pending[0].PullRequestID != "pr-1" || pending[0].ChecksStatus != "failing" {
		t.Fatalf("pending pull request feedback = %+v", pending)
	}
}

func TestServiceWatchPullRequestsReusesExistingPublishedPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{list: []core.PullRequest{{
		ID:           "listed-pr",
		Repo:         "owner/repo",
		Number:       7,
		URL:          "https://github.com/owner/repo/pull/7",
		Branch:       "codex/aged-test",
		Base:         "main",
		Title:        "Task",
		State:        "OPEN",
		ChecksStatus: "passing",
		MergeStatus:  "CLEAN",
	}}}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	prs, err := service.WatchPullRequests(ctx, "task-1", core.WatchPullRequestsRequest{
		Repo:   "owner/repo",
		Number: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 1 || prs[0].ID != "pr-1" {
		t.Fatalf("watched prs = %+v, want existing pr-1", prs)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].ID != "pr-1" {
		t.Fatalf("snapshot pull requests = %+v", snapshot.PullRequests)
	}
}

func TestServiceWatchPullRequestsDoesNotAttachRepoWideResultsToExistingTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{list: []core.PullRequest{{
		ID:     "listed-existing",
		Repo:   "owner/repo",
		Number: 7,
		URL:    "https://github.com/owner/repo/pull/7",
		Branch: "codex/aged-existing",
		State:  "OPEN",
	}, {
		ID:     "listed-random",
		Repo:   "owner/repo",
		Number: 99,
		URL:    "https://github.com/owner/repo/pull/99",
		Branch: "unrelated",
		State:  "OPEN",
	}}}
	service := newTestPullRequestMonitorService(t, store, publisher)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	prs, err := service.WatchPullRequests(ctx, "task-1", core.WatchPullRequestsRequest{
		Repo:  "owner/repo",
		State: "open",
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.listCalls != 0 {
		t.Fatalf("list calls = %d, want 0 for broad watch on task with existing PRs", publisher.listCalls)
	}
	if publisher.inspectCalls != 1 {
		t.Fatalf("inspect calls = %d, want 1", publisher.inspectCalls)
	}
	if len(prs) != 1 || prs[0].ID != "pr-1" || prs[0].Number != 7 {
		t.Fatalf("watched prs = %+v, want only existing task PR", prs)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].ID != "pr-1" {
		t.Fatalf("snapshot pull requests = %+v", snapshot.PullRequests)
	}
}

func TestServiceWatchPullRequestsReturnsPersistedNormalizedPullRequests(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{list: []core.PullRequest{{
		ID:           "listed-pr",
		Repo:         "owner/repo",
		Number:       12,
		URL:          "https://github.com/owner/repo/pull/12",
		Branch:       "feature",
		Base:         "main",
		Title:        "Watch me",
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
	}}}
	service := newTestPullRequestMonitorService(t, store, publisher)
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

	prs, err := service.WatchPullRequests(ctx, "task-1", core.WatchPullRequestsRequest{
		Repo:   "owner/repo",
		Number: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 1 {
		t.Fatalf("watched prs = %+v", prs)
	}
	wantID := "github:owner/repo#12"
	if prs[0].ID != wantID || prs[0].TaskID != "task-1" {
		t.Fatalf("returned pr = %+v, want id %q and task task-1", prs[0], wantID)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("snapshot pull requests = %+v", snapshot.PullRequests)
	}
	persisted := snapshot.PullRequests[0]
	if persisted.ID != prs[0].ID || persisted.TaskID != prs[0].TaskID {
		t.Fatalf("persisted pr = %+v, returned pr = %+v", persisted, prs[0])
	}
}

func TestServicePullRequestMonitorRespectsProjectOptOut(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	monitorPullRequests := false
	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:        "project-1",
		Name:      "Project",
		LocalPath: projectRoot,
		Repo:      "owner/repo",
		PullRequestPolicy: core.PullRequestPolicy{
			MonitorPullRequests: &monitorPullRequests,
		},
	}}, "project-1")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePullRequestPublisher{status: monitoredPullRequestStatus("failing", "DIRTY", "COMMENTED")}
	service := newTestPullRequestMonitorService(t, store, publisher)
	service.SetProjects(projects)
	appendTrackedPullRequest(t, ctx, store, "task-1", "project-1", core.TaskWaiting)

	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.inspectCalls != 0 {
		t.Fatalf("inspect calls = %d, want 0", publisher.inspectCalls)
	}
	if hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") || hasEvent(snapshot.Events, core.EventPRFollowUp, "task-1", "") {
		t.Fatalf("monitor events = %+v", snapshot.Events)
	}
}

type mergeTrackingPullRequestPublisher struct {
	fakePullRequestPublisher
	merged     core.PullRequest
	mergeSpec  PullRequestMergeSpec
	mergeCalls int
	mergeErr   error
}

func (p *mergeTrackingPullRequestPublisher) Merge(_ context.Context, pr core.PullRequest, spec PullRequestMergeSpec) (core.PullRequest, error) {
	p.mergeCalls++
	p.merged = pr
	p.mergeSpec = spec
	if p.mergeErr != nil {
		return core.PullRequest{}, p.mergeErr
	}
	merged := pr
	merged.State = "MERGED"
	merged.ChecksStatus = "passing"
	merged.ChecksConclusion = "SUCCESS"
	merged.MergeStatus = "MERGED"
	merged.Mergeable = "MERGEABLE"
	return merged, nil
}

func newTestPullRequestMonitorService(t *testing.T, store *eventstore.SQLiteStore, publisher PullRequestPublisher) *Service {
	t.Helper()
	return newTestPullRequestMonitorServiceWithWorkspace(t, store, publisher, fakeWorkspaceManager{cwd: t.TempDir()})
}

func newTestPullRequestMonitorServiceWithWorkspace(t *testing.T, store *eventstore.SQLiteStore, publisher PullRequestPublisher, workspace fakeWorkspaceManager) *Service {
	t.Helper()
	if workspace.cwd == "" {
		workspace.cwd = t.TempDir()
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "continue")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), workspace)
	service.SetPullRequestPublisher(publisher)
	return service
}

func appendTrackedPullRequest(t *testing.T, ctx context.Context, store *eventstore.SQLiteStore, taskID string, projectID string, status core.TaskStatus) {
	appendTrackedPullRequestWithRepo(t, ctx, store, taskID, projectID, status, "owner/repo", 7)
}

func appendTrackedPullRequestWithRepo(t *testing.T, ctx context.Context, store *eventstore.SQLiteStore, taskID string, projectID string, status core.TaskStatus, repo string, number int) {
	t.Helper()
	taskPayload := map[string]any{
		"title":  "Task",
		"prompt": "Prompt",
	}
	if projectID != "" {
		taskPayload["projectId"] = projectID
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskCreated,
		TaskID:  taskID,
		Payload: core.MustJSON(taskPayload),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": status,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   repo,
			"number": number,
			"url":    "https://github.com/" + repo + "/pull/" + fmt.Sprint(number),
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
}

func appendActiveWorker(t *testing.T, ctx context.Context, store *eventstore.SQLiteStore, taskID string, workerID string) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-" + workerID,
			"workerId":   workerID,
			"workerKind": "mock",
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
}

func workerActive(snapshot core.Snapshot, workerID string) bool {
	for _, worker := range snapshot.Workers {
		if worker.ID == workerID && !isTerminalWorkerStatus(worker.Status) {
			return true
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID == workerID && !isTerminalWorkerStatus(node.Status) {
			return true
		}
	}
	return false
}

func pullRequestFollowUpWorkItemByTarget(snapshot core.Snapshot, taskID string, prID string) (core.WorkItem, bool) {
	for _, item := range snapshot.WorkItems {
		if item.TaskID == taskID && item.Kind == "pr.followup" && item.TargetKind == "pull_request" && item.TargetID == prID {
			return item, true
		}
	}
	return core.WorkItem{}, false
}

type testPullRequestFollowUpPayload struct {
	ID                string `json:"id"`
	Attempt           int    `json:"attempt"`
	Status            string `json:"status"`
	Prompt            string `json:"prompt"`
	ChecksStatus      string `json:"checksStatus"`
	MergeStatus       string `json:"mergeStatus"`
	ReviewStatus      string `json:"reviewStatus"`
	FeedbackSignature string `json:"feedbackSignature"`
}

func latestPullRequestFollowUpPayload(t *testing.T, snapshot core.Snapshot, taskID string) testPullRequestFollowUpPayload {
	t.Helper()
	var out testPullRequestFollowUpPayload
	found := false
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventPRFollowUp {
			continue
		}
		if err := json.Unmarshal(event.Payload, &out); err != nil {
			t.Fatal(err)
		}
		found = true
	}
	if !found {
		t.Fatalf("missing pull request follow-up event for task %s", taskID)
	}
	return out
}

func monitoredPullRequestStatus(checks string, merge string, review string) core.PullRequest {
	return monitoredPullRequestStatusWithMetadata(checks, merge, review, nil)
}

func monitoredPullRequestStatusWithMetadata(checks string, merge string, review string, metadata []byte) core.PullRequest {
	return core.PullRequest{
		ID:           "pr-1",
		Repo:         "owner/repo",
		Number:       7,
		URL:          "https://github.com/owner/repo/pull/7",
		Branch:       "codex/aged-test",
		Base:         "main",
		Title:        "Task",
		State:        "OPEN",
		ChecksStatus: checks,
		MergeStatus:  merge,
		ReviewStatus: review,
		Metadata:     metadata,
	}
}
