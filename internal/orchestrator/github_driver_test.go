package orchestrator

import (
	"context"
	"encoding/json"
	"testing"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

type githubDriverTestFixture struct {
	ctx       context.Context
	store     *eventstore.SQLiteStore
	service   *Service
	driver    *GitHubDriver
	publisher *fakePullRequestPublisher
}

type githubDriverTestOptions struct {
	config      GitHubDriverConfig
	client      fakeGitHubClient
	planPrompt  string
	runnerText  string
	projectRoot string
	workspace   fakeWorkspaceManager
	publisher   *fakePullRequestPublisher
}

func newGitHubDriverTestFixture(t *testing.T, opts githubDriverTestOptions) githubDriverTestFixture {
	t.Helper()

	ctx := context.Background()
	store := openTestStore(t)
	t.Cleanup(func() { store.Close() })

	planPrompt := opts.planPrompt
	if planPrompt == "" {
		planPrompt = "do it"
	}
	runnerText := opts.runnerText
	if runnerText == "" {
		runnerText = "done"
	}
	projectRoot := opts.projectRoot
	if projectRoot == "" {
		projectRoot = t.TempDir()
	}
	workspace := opts.workspace
	if workspace.cwd == "" {
		workspace.cwd = t.TempDir()
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: planPrompt}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: runnerText}}},
	}, projectRoot, workspace)
	if opts.publisher != nil {
		service.SetPullRequestPublisher(opts.publisher)
	}

	return githubDriverTestFixture{
		ctx:       ctx,
		store:     store,
		service:   service,
		driver:    NewGitHubDriver(service, opts.config, opts.client),
		publisher: opts.publisher,
	}
}

func TestGitHubDriverCreatesIssueTasksIdempotently(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		config: GitHubDriverConfig{
			Enabled: true,
			Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo", Labels: []string{"aged"}}},
			PullRequests: GitHubPullRequestDriverConfig{
				AutoPublish: boolPtr(false),
			},
		},
		client: fakeGitHubClient{issues: []GitHubIssue{{
			Repo:   "owner/repo",
			Number: 12,
			Title:  "Add feature",
			Body:   "Please add the feature.",
			URL:    "https://github.com/owner/repo/issues/12",
			Labels: []string{"aged"},
		}}},
	})

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := fixture.service.FindTaskByExternalID(fixture.ctx, "github-issue", "owner/repo#12")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing github issue task")
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["completionMode"] != "local" {
		t.Fatalf("metadata = %+v", metadata)
	}
	_ = waitForTaskStatus(t, fixture.store, task.ID, core.TaskSucceeded)
	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskCreated, task.ID) != 1 {
		t.Fatalf("task.created count = %d, want 1", countEvents(snapshot.Events, core.EventTaskCreated, task.ID))
	}
}

func TestGitHubDriverIssueTaskUsesGitHubCompletionWhenAutoPublishEnabled(t *testing.T) {
	publisher := &fakePullRequestPublisher{}
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		workspace: fakeWorkspaceManager{
			sourceRoot: t.TempDir(),
			changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		},
		publisher: publisher,
		config: GitHubDriverConfig{
			Enabled: true,
			Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo", Labels: []string{"aged"}}},
		},
		client: fakeGitHubClient{issues: []GitHubIssue{{
			Repo:   "owner/repo",
			Number: 12,
			Title:  "Add feature",
			Body:   "Please add the feature.",
			URL:    "https://github.com/owner/repo/issues/12",
			Labels: []string{"aged"},
		}}},
	})

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := fixture.service.FindTaskByExternalID(fixture.ctx, "github-issue", "owner/repo#12")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing github issue task")
	}
	waitForPullRequests(t, fixture.store, task.ID, 1)
	snapshot := waitForTaskStatus(t, fixture.store, task.ID, core.TaskWaiting)
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
	publisher := &fakePullRequestPublisher{}
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		publisher: publisher,
		config: GitHubDriverConfig{
			Enabled: true,
			PullRequests: GitHubPullRequestDriverConfig{
				Repos:       []string{"owner/repo"},
				AutoBabysit: boolPtr(false),
			},
		},
		client: fakeGitHubClient{},
	})

	taskID := "task-gh-12"
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
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
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q, want owner/repo", publisher.published.Repo)
	}
	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].TaskID != taskID {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
}

func TestGitHubDriverPublishesSucceededIssueTaskThroughForkProject(t *testing.T) {
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
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		projectRoot: projectRoot,
		publisher:   publisher,
		config: GitHubDriverConfig{
			Enabled: true,
			PullRequests: GitHubPullRequestDriverConfig{
				Repos:       []string{"owner/repo"},
				AutoBabysit: boolPtr(false),
			},
		},
		client: fakeGitHubClient{},
	})
	fixture.service.SetProjects(projects)

	taskID := "task-gh-fork-12"
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
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
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
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
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		planPrompt: "babysit",
		runnerText: "ready",
		publisher:  publisher,
		config: GitHubDriverConfig{
			Enabled:      true,
			PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
		},
		client: fakeGitHubClient{},
	})
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
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

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, fixture.store, core.EventPRFollowUp, "task-1")
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
	if !pullRequestNeedsBabysitter(core.PullRequest{
		State: "OPEN",
		Metadata: core.MustJSON(map[string]any{
			"latestPullRequestFeedbackSignature":          "2026-05-11T22:01:05Z:conversation:IC_1",
			"latestPullRequestFeedbackTriggeredSignature": "2026-05-11T21:59:00Z:conversation:IC_0",
		}),
	}) {
		t.Fatal("PR with untriggered feedback should need babysitter follow-up")
	}
}

func TestGitHubDriverMonitorsUpstreamPullRequestsFromIssueSources(t *testing.T) {
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
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		planPrompt: "babysit",
		runnerText: "ready",
		publisher:  publisher,
		config: GitHubDriverConfig{
			Enabled: true,
			Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo"}},
			PullRequests: GitHubPullRequestDriverConfig{
				AutoBabysit: boolPtr(false),
			},
		},
		client: fakeGitHubClient{},
	})
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
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

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventPRStatusChecked, "task-1", "") {
		t.Fatalf("missing pr status check event")
	}
}

func TestGitHubDriverRefreshesMergedPRToSatisfyTask(t *testing.T) {
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
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		planPrompt: "babysit",
		runnerText: "ready",
		publisher:  publisher,
		config: GitHubDriverConfig{
			Enabled:      true,
			PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
		},
		client: fakeGitHubClient{},
	})
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
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

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, fixture.store, "task-1", core.TaskSucceeded)
	task, ok := findTask(snapshot, "task-1")
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveSatisfied || task.ObjectivePhase != "merged" {
		t.Fatalf("objective = %q phase %q", task.ObjectiveStatus, task.ObjectivePhase)
	}
}

func TestGitHubDriverDoesNotRecreateIssueTaskAfterPullRequestMergedAndCleared(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		config: GitHubDriverConfig{
			Enabled: true,
			Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo"}},
			PullRequests: GitHubPullRequestDriverConfig{
				Enabled:     boolPtr(false),
				AutoPublish: boolPtr(false),
			},
		},
		client: fakeGitHubClient{issues: []GitHubIssue{{
			Repo:   "owner/repo",
			Number: 12,
			Title:  "Add feature",
			URL:    "https://github.com/owner/repo/issues/12",
		}, {
			Repo:   "owner/repo",
			Number: 13,
			Title:  "Different bug",
			URL:    "https://github.com/owner/repo/issues/13",
		}}},
	})

	taskID := "task-issue-12"
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "GitHub issue owner/repo#12: Add feature",
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
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskMilestone,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"name":    "pr_merged",
			"phase":   "merged",
			"summary": "Pull request merged.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:    core.EventTaskStatus,
		TaskID:  taskID,
		Payload: core.MustJSON(map[string]any{"status": core.TaskSucceeded}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.ClearTask(fixture.ctx, taskID); err != nil {
		t.Fatal(err)
	}

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	createdForResolved := 0
	createdForFollowUp := 0
	for _, event := range snapshot.Events {
		if event.Type != core.EventTaskCreated {
			continue
		}
		var payload struct {
			Metadata map[string]any `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		switch stringMetadataValue(payload.Metadata["externalId"]) {
		case "owner/repo#12":
			createdForResolved++
		case "owner/repo#13":
			createdForFollowUp++
		}
	}
	if createdForResolved != 1 {
		t.Fatalf("task.created events for owner/repo#12 = %d, want 1 (no duplicate after PR merge + clear)", createdForResolved)
	}
	if createdForFollowUp != 1 {
		t.Fatalf("task.created events for owner/repo#13 = %d, want 1 (still-open unrelated issue must spawn a task)", createdForFollowUp)
	}
}

func TestGitHubDriverCreatesMentionTasksWithLocalCompletion(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		planPrompt: "review it",
		runnerText: "commented",
		config: GitHubDriverConfig{
			Enabled: true,
			Mentions: GitHubMentionDriverConfig{
				Enabled: boolPtr(true),
				Repos:   []string{"owner/repo"},
			},
			PullRequests: GitHubPullRequestDriverConfig{
				AutoPublish: boolPtr(false),
			},
		},
		client: fakeGitHubClient{mentions: []GitHubMention{{
			ID:          "thread-1",
			Repo:        "owner/repo",
			SubjectType: "PullRequest",
			Number:      12,
			Title:       "Add feature",
			URL:         "https://github.com/owner/repo/pull/12",
			Reason:      "review_requested",
			Body:        "@aged can you review this?",
			Author:      "octocat",
			CommentURL:  "https://github.com/owner/repo/pull/12#issuecomment-1",
		}}},
	})

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := fixture.service.FindTaskByExternalID(fixture.ctx, "github-mention", "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing github mention task")
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["completionMode"] != "local" {
		t.Fatalf("metadata completionMode = %v, want local", metadata["completionMode"])
	}
	if metadata["reason"] != "review_requested" || metadata["subjectType"] != "PullRequest" {
		t.Fatalf("metadata = %+v", metadata)
	}
	_ = waitForTaskStatus(t, fixture.store, task.ID, core.TaskSucceeded)
	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskCreated, task.ID) != 1 {
		t.Fatalf("task.created count = %d, want 1", countEvents(snapshot.Events, core.EventTaskCreated, task.ID))
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v, want none for review-comment-only mention task", snapshot.PullRequests)
	}
}

func TestGitHubDriverSkipsMentionReasonsAndReposOutsideConfig(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, githubDriverTestOptions{
		planPrompt: "review it",
		runnerText: "commented",
		config: GitHubDriverConfig{
			Enabled: true,
			Mentions: GitHubMentionDriverConfig{
				Enabled: boolPtr(true),
				Repos:   []string{"owner/repo"},
				Reasons: []string{"mention"},
			},
			PullRequests: GitHubPullRequestDriverConfig{
				Enabled: boolPtr(false),
			},
		},
		client: fakeGitHubClient{mentions: []GitHubMention{{
			ID:     "thread-1",
			Repo:   "owner/repo",
			Number: 12,
			Reason: "review_requested",
		}, {
			ID:     "thread-2",
			Repo:   "other/repo",
			Number: 13,
			Reason: "mention",
		}}},
	})

	if err := fixture.driver.RunOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 0 {
		t.Fatalf("tasks = %+v, want none", snapshot.Tasks)
	}
}

type fakeGitHubClient struct {
	issues   []GitHubIssue
	mentions []GitHubMention
}

func (c fakeGitHubClient) ListIssues(context.Context, string, []string, int) ([]GitHubIssue, error) {
	return c.issues, nil
}

func (c fakeGitHubClient) ListMentions(context.Context, int) ([]GitHubMention, error) {
	return c.mentions, nil
}

func boolPtr(value bool) *bool {
	return &value
}
