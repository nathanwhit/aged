package orchestrator

import (
	"context"
	"errors"
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
			if !hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
				t.Fatalf("missing task steering event")
			}
		})
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
			AllowMerge: true,
			AutoMerge:  true,
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
	if publisher.mergeSpec.Repo != "owner/repo" || publisher.mergeSpec.Number != 7 || publisher.mergeSpec.Method != "squash" {
		t.Fatalf("merge spec = %+v", publisher.mergeSpec)
	}
	if snapshot.PullRequests[0].State != "MERGED" {
		t.Fatalf("pull request state = %q, want MERGED", snapshot.PullRequests[0].State)
	}
	if snapshot.Tasks[0].ObjectivePhase != "merged" {
		t.Fatalf("objective phase = %q, want merged", snapshot.Tasks[0].ObjectivePhase)
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
	if !hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("missing task steering event")
	}
	if snapshot.Tasks[0].ObjectivePhase != "pr_needs_work" {
		t.Fatalf("objective phase = %q, want pr_needs_work", snapshot.Tasks[0].ObjectivePhase)
	}
	if got := pullRequestAutoMergeError(snapshot.PullRequests[0]); !strings.Contains(got, "merge conflict") {
		t.Fatalf("auto merge error = %q", got)
	}
}

func TestServiceDefaultPullRequestMonitorKeepsFeedbackPendingWhileTaskRunning(t *testing.T) {
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
	snapshot := waitForEvent(t, store, core.EventPRStatusChecked, "task-1")
	if hasEvent(snapshot.Events, core.EventPRFollowUp, "task-1", "") {
		t.Fatalf("running task started follow-up")
	}
	if !pullRequestHasUntriggeredFeedback(snapshot.PullRequests[0]) {
		t.Fatalf("running task feedback was marked handled")
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
	if err := service.MonitorPullRequestsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot = waitForEvent(t, store, core.EventPRFollowUp, "task-1")
	if !hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("missing task steering event")
	}
	if pullRequestHasUntriggeredFeedback(snapshot.PullRequests[0]) {
		t.Fatalf("follow-up feedback was not marked handled")
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
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "continue"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	return service
}

func appendTrackedPullRequest(t *testing.T, ctx context.Context, store *eventstore.SQLiteStore, taskID string, projectID string, status core.TaskStatus) {
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
