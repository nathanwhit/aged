package orchestrator

import (
	"context"
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
		{name: "new comment", status: monitoredPullRequestStatus("success", "CLEAN", "COMMENTED")},
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

func newTestPullRequestMonitorService(t *testing.T, store *eventstore.SQLiteStore, publisher *fakePullRequestPublisher) *Service {
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
	}
}
