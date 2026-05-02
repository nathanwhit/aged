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

func TestGitHubDriverIssueTaskUsesGitHubCompletionWhenAutoPublishEnabled(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)
	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled: true,
		Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo", Labels: []string{"aged"}}},
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
	waitForPullRequests(t, store, task.ID, 1)
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	task, ok = findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveWaitingExternal {
		t.Fatalf("task status = %q objective = %q", task.Status, task.ObjectiveStatus)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q", publisher.published.Repo)
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

	taskID := "task-gh-12"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "GitHub issue owner/repo#12",
			"prompt": "Fix it.",
			"metadata": map[string]any{
				"source":         "github-issue",
				"externalId":     "owner/repo#12",
				"repo":           "owner/repo",
				"number":         12,
				"completionMode": "github",
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
	if len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].TaskID != taskID {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
}

func TestGitHubDriverPublishesSucceededIssueTaskThroughForkProject(t *testing.T) {
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
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, projectRoot, fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	taskID := "task-gh-fork-12"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":     "GitHub issue owner/repo#12",
			"prompt":    "Fix it.",
			"projectId": "fork",
			"metadata": map[string]any{
				"source":         "github-issue",
				"externalId":     "owner/repo#12",
				"repo":           "owner/repo",
				"number":         12,
				"completionMode": "github",
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

	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled:      true,
		PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
	}, fakeGitHubClient{})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventPRFollowUp, "task-1")
	if len(snapshot.Tasks) != 1 {
		t.Fatalf("tasks = %+v", snapshot.Tasks)
	}
	if !hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") {
		t.Fatalf("missing pr status check event")
	}
	if !hasEvent(snapshot.Events, core.EventPRFollowUp, "task-1", "") {
		t.Fatalf("missing pr follow-up event")
	}
	if !hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("missing task steering event")
	}
}

func TestPullRequestNeedsBabysitterForNewConversationComment(t *testing.T) {
	if !pullRequestNeedsBabysitter(core.PullRequest{State: "OPEN", ReviewStatus: "COMMENTED"}) {
		t.Fatal("COMMENTED PR should need babysitter follow-up")
	}
}

func TestGitHubDriverMonitorsUpstreamPullRequestsFromIssueSources(t *testing.T) {
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
		ChecksStatus: "passing",
		MergeStatus:  "CLEAN",
		ReviewStatus: "APPROVED",
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
		Enabled: true,
		Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo"}},
		PullRequests: GitHubPullRequestDriverConfig{
			AutoBabysit: boolPtr(false),
		},
	}, fakeGitHubClient{})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") {
		t.Fatalf("missing pr status check event")
	}
}

func TestGitHubDriverRefreshesMergedPRToSatisfyTask(t *testing.T) {
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
		State:        "MERGED",
		ChecksStatus: "success",
		MergeStatus:  "CLEAN",
		ReviewStatus: "APPROVED",
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

	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled:      true,
		PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
	}, fakeGitHubClient{})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, "task-1", core.TaskSucceeded)
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveSatisfied || task.ObjectivePhase != "merged" {
		t.Fatalf("objective = %q phase %q", task.ObjectiveStatus, task.ObjectivePhase)
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
