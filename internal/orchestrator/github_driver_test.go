package orchestrator

import (
	"context"
	"testing"

	"aged/internal/core"
	"aged/internal/worker"
)

func TestGitHubDriverCreatesIssueTasksIdempotently(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled: true,
		Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo", Labels: []string{"aged"}}},
		PullRequests: GitHubPullRequestDriverConfig{
			AutoPublish: boolPtr(false),
		},
	}, fakeGitHubClient{issues: []GitHubIssue{{
		Repo:   "owner/repo",
		Number: 12,
		Title:  "Add feature",
		Body:   "Please add the feature.",
		URL:    "https://github.com/owner/repo/issues/12",
		Labels: []string{"aged"},
	}}})

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "github-issue", "owner/repo#12")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing github issue task")
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskCreated, task.ID) != 1 {
		t.Fatalf("task.created count = %d, want 1", countEvents(snapshot.Events, core.EventTaskCreated, task.ID))
	}
}

func TestGitHubDriverPublishesSucceededIssueTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:      "GitHub issue owner/repo#12",
		Prompt:     "Fix it.",
		Source:     "github-issue",
		ExternalID: "owner/repo#12",
		Metadata:   core.MustJSON(map[string]any{"repo": "owner/repo", "number": 12}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)

	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled: true,
		PullRequests: GitHubPullRequestDriverConfig{
			Repos:       []string{"owner/repo"},
			AutoBabysit: boolPtr(false),
		},
	}, fakeGitHubClient{})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q, want owner/repo", publisher.published.Repo)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].TaskID != task.ID {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
}

func TestGitHubDriverRefreshesAndBabysitsPRsNeedingAttention(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		ID:           "pr-1",
		Repo:         "owner/repo",
		Number:       7,
		URL:          "https://github.com/owner/repo/pull/7",
		Branch:       "codex/aged-test",
		Base:         "main",
		Title:        "Task",
		State:        "OPEN",
		ChecksStatus: "failing",
		MergeStatus:  "BLOCKED",
		ReviewStatus: "CHANGES_REQUESTED",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "babysit"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
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

	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled:      true,
		PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
	}, fakeGitHubClient{})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 2 {
		t.Fatalf("tasks = %+v", snapshot.Tasks)
	}
	if !hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") {
		t.Fatalf("missing pr status check event")
	}
	if !hasEvent(snapshot.Events, core.EventPRBabysitter, "task-1", "") {
		t.Fatalf("missing pr babysitter event")
	}
}

type fakeGitHubClient struct {
	issues []GitHubIssue
}

func (c fakeGitHubClient) ListIssues(context.Context, string, []string, int) ([]GitHubIssue, error) {
	return c.issues, nil
}

func boolPtr(value bool) *bool {
	return &value
}
