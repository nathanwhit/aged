package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

type githubDriverTestFixture struct {
	ctx     context.Context
	store   *eventstore.SQLiteStore
	service *Service
	driver  *GitHubDriver
}

type githubDriverTestOptions struct {
	planPrompt  string
	runnerText  string
	projectRoot string
	workspace   fakeWorkspaceManager
	publisher   *fakePullRequestPublisher
}

func newGitHubDriverTestFixture(t *testing.T, config GitHubDriverConfig, client *fakeGitHubClient, options ...githubDriverTestOptions) githubDriverTestFixture {
	t.Helper()
	var opts githubDriverTestOptions
	if len(options) > 0 {
		opts = options[0]
	}

	store := openTestStore(t)
	t.Cleanup(func() { store.Close() })

	if opts.planPrompt == "" {
		opts.planPrompt = "do it"
	}
	if opts.runnerText == "" {
		opts.runnerText = "done"
	}
	if opts.projectRoot == "" {
		opts.projectRoot = t.TempDir()
	}
	if opts.workspace.cwd == "" {
		opts.workspace.cwd = t.TempDir()
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: opts.planPrompt}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: opts.runnerText}}},
	}, opts.projectRoot, opts.workspace)
	if opts.publisher != nil {
		service.SetPullRequestPublisher(opts.publisher)
	}

	return githubDriverTestFixture{
		ctx:     context.Background(),
		store:   store,
		service: service,
		driver:  NewGitHubDriver(service, config, client),
	}
}

func TestGitHubDriverCreatesIssueTasksIdempotently(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo", Labels: []string{"aged"}}},
		PullRequests: GitHubPullRequestDriverConfig{
			AutoPublish: boolPtr(false),
		},
	}, &fakeGitHubClient{issues: []GitHubIssue{{
		Repo:   "owner/repo",
		Number: 12,
		Title:  "Add feature",
		Body:   "Please add the feature.",
		URL:    "https://github.com/owner/repo/issues/12",
		Labels: []string{"aged"},
	}}})

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
	if _, ok := metadata["completionMode"]; ok {
		t.Fatalf("metadata = %+v, want no completionMode", metadata)
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

func TestGitHubDriverDoesNotAutoPublishSucceededIssueTask(t *testing.T) {
	publisher := &fakePullRequestPublisher{}
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		PullRequests: GitHubPullRequestDriverConfig{
			Repos:       []string{"owner/repo"},
			AutoBabysit: boolPtr(false),
		},
	}, &fakeGitHubClient{}, githubDriverTestOptions{
		publisher: publisher,
	})

	taskID := "task-gh-12"
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "GitHub issue owner/repo#12",
			"prompt": "Fix it.",
			"metadata": map[string]any{
				"source":     "github-issue",
				"externalId": "owner/repo#12",
				"repo":       "owner/repo",
				"number":     12,
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
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
	}
	snapshot, err := fixture.store.Snapshot(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
}

func TestGitHubDriverDoesNotAutoPublishSucceededIssueTaskThroughForkProject(t *testing.T) {
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
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		PullRequests: GitHubPullRequestDriverConfig{
			Repos:       []string{"owner/repo"},
			AutoBabysit: boolPtr(false),
		},
	}, &fakeGitHubClient{}, githubDriverTestOptions{
		projectRoot: projectRoot,
		publisher:   publisher,
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
				"source":     "github-issue",
				"externalId": "owner/repo#12",
				"repo":       "owner/repo",
				"number":     12,
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
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
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
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled:      true,
		PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
	}, &fakeGitHubClient{}, githubDriverTestOptions{
		planPrompt: "babysit",
		runnerText: "ready",
		publisher:  publisher,
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
	if hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("automatic pull request feedback steered task")
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

func TestPullRequestNeedsBabysitterIgnoresBlockedPendingRequirements(t *testing.T) {
	if pullRequestNeedsBabysitter(core.PullRequest{
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "BLOCKED",
	}) {
		t.Fatal("pending externally blocked PR should not need babysitter follow-up")
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
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo"}},
		PullRequests: GitHubPullRequestDriverConfig{
			AutoBabysit: boolPtr(false),
		},
	}, &fakeGitHubClient{}, githubDriverTestOptions{
		planPrompt: "babysit",
		runnerText: "ready",
		publisher:  publisher,
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
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled:      true,
		PullRequests: GitHubPullRequestDriverConfig{Repos: []string{"owner/repo"}},
	}, &fakeGitHubClient{}, githubDriverTestOptions{
		planPrompt: "babysit",
		runnerText: "ready",
		publisher:  publisher,
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
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		Issues:  []GitHubIssueSourceConfig{{Repo: "owner/repo"}},
		PullRequests: GitHubPullRequestDriverConfig{
			Enabled:     boolPtr(false),
			AutoPublish: boolPtr(false),
		},
	}, &fakeGitHubClient{issues: []GitHubIssue{{
		Repo:   "owner/repo",
		Number: 12,
		Title:  "Add feature",
		URL:    "https://github.com/owner/repo/issues/12",
	}, {
		Repo:   "owner/repo",
		Number: 13,
		Title:  "Different bug",
		URL:    "https://github.com/owner/repo/issues/13",
	}}})

	taskID := "task-issue-12"
	if _, err := fixture.store.Append(fixture.ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "GitHub issue owner/repo#12: Add feature",
			"prompt": "Fix it.",
			"metadata": map[string]any{
				"source":     "github-issue",
				"externalId": "owner/repo#12",
				"repo":       "owner/repo",
				"number":     12,
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

func TestGitHubDriverRoutesMentionTasksToMentionRepoProject(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		Mentions: GitHubMentionDriverConfig{
			Enabled: boolPtr(true),
			Repos:   []string{"denoland/deno"},
		},
		PullRequests: GitHubPullRequestDriverConfig{
			AutoPublish: boolPtr(false),
		},
	}, &fakeGitHubClient{mentions: []GitHubMention{{
		ID:          "thread-1",
		Repo:        "denoland/deno",
		SubjectType: "PullRequest",
		Number:      33992,
		Title:       "Dedupe JS sources",
		URL:         "https://github.com/denoland/deno/pull/33992",
		Reason:      "mention",
	}}})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: t.TempDir(), Repo: "nathanwhit/aged", UpstreamRepo: "nathanwhit/aged"},
		{ID: "deno", Name: "deno", LocalPath: t.TempDir(), Repo: "nathanwhit/deno", UpstreamRepo: "denoland/deno"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	fixture.service.SetProjects(projects)

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
	if task.ProjectID != "deno" {
		t.Fatalf("mention task project = %q, want deno", task.ProjectID)
	}
}

func TestGitHubDriverRoutesMentionsToExistingTrackedPullRequestTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	client := &fakeGitHubClient{mentions: []GitHubMention{{
		ID:          "thread-1",
		Repo:        "owner/repo",
		SubjectType: "PullRequest",
		Number:      7,
		Title:       "Add feature",
		URL:         "https://github.com/owner/repo/pull/7",
		Reason:      "mention",
		Body:        "@aged please take another look",
	}}}
	service := newTestPullRequestMonitorService(t, store, &fakePullRequestPublisher{})
	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled: true,
		Mentions: GitHubMentionDriverConfig{
			Enabled: boolPtr(true),
			Repos:   []string{"owner/repo"},
		},
	}, client)
	appendTrackedPullRequest(t, ctx, store, "task-1", "", core.TaskWaiting)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForEvent(t, store, core.EventTaskSteered, "task-1")
	if _, ok, err := service.FindTaskByExternalID(ctx, "github-mention", "thread-1"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("created a separate github mention task")
	}
	if got := countEvents(snapshot.Events, core.EventTaskSteered, "task-1"); got != 1 {
		t.Fatalf("task steered events = %d, want 1", got)
	}
	if got := countGitHubMentionRoutedActions(snapshot.Events, "task-1", "thread-1"); got != 1 {
		t.Fatalf("routed mention actions = %d, want 1", got)
	}
}

func TestGitHubDriverMentionsIncludeReadAndAdvanceCursor(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "review it")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "commented"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	client := &fakeGitHubClient{mentions: []GitHubMention{{
		ID:        "thread-1",
		Repo:      "owner/repo",
		Number:    12,
		Reason:    "mention",
		UpdatedAt: "2026-05-13T13:16:07Z",
	}}}
	driver := NewGitHubDriver(service, GitHubDriverConfig{
		Enabled: true,
		Mentions: GitHubMentionDriverConfig{
			Enabled: boolPtr(true),
			Repos:   []string{"owner/repo"},
			Limit:   7,
		},
		PullRequests: GitHubPullRequestDriverConfig{
			Enabled: boolPtr(false),
		},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.mentionOptions) != 1 {
		t.Fatalf("mention options count = %d, want 1", len(client.mentionOptions))
	}
	if !client.mentionOptions[0].IncludeRead {
		t.Fatalf("IncludeRead = false, want true")
	}
	if client.mentionOptions[0].Limit != 7 {
		t.Fatalf("Limit = %d, want 7", client.mentionOptions[0].Limit)
	}
	if _, err := time.Parse(time.RFC3339, client.mentionOptions[0].Since); err != nil {
		t.Fatalf("Since = %q, want RFC3339: %v", client.mentionOptions[0].Since, err)
	}
	cursor, err := store.Setting(ctx, githubMentionPollCursorSetting)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(cursor) == "" {
		t.Fatal("missing mention poll cursor")
	}
	client.mentions = nil
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.mentionOptions) != 2 {
		t.Fatalf("mention options count = %d, want 2", len(client.mentionOptions))
	}
	firstCursor, err := time.Parse(time.RFC3339Nano, cursor)
	if err != nil {
		t.Fatal(err)
	}
	secondSince, err := time.Parse(time.RFC3339, client.mentionOptions[1].Since)
	if err != nil {
		t.Fatal(err)
	}
	if !secondSince.Equal(firstCursor.Add(-githubMentionCursorOverlap).Truncate(time.Second)) {
		t.Fatalf("second since = %s, want cursor minus overlap from %s", secondSince, firstCursor)
	}
}

func TestGitHubDriverSkipsMentionReasonsAndReposOutsideConfig(t *testing.T) {
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{
		Enabled: true,
		Mentions: GitHubMentionDriverConfig{
			Enabled: boolPtr(true),
			Repos:   []string{"owner/repo"},
			Reasons: []string{"mention"},
		},
		PullRequests: GitHubPullRequestDriverConfig{
			Enabled: boolPtr(false),
		},
	}, &fakeGitHubClient{mentions: []GitHubMention{{
		ID:     "thread-1",
		Repo:   "owner/repo",
		Number: 12,
		Reason: "review_requested",
	}, {
		ID:     "thread-2",
		Repo:   "other/repo",
		Number: 13,
		Reason: "mention",
	}}}, githubDriverTestOptions{
		planPrompt: "review it",
		runnerText: "commented",
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
	issues         []GitHubIssue
	mentions       []GitHubMention
	mentionOptions []GitHubMentionListOptions
}

func (c fakeGitHubClient) ListIssues(context.Context, string, []string, int) ([]GitHubIssue, error) {
	return c.issues, nil
}

func (c *fakeGitHubClient) ListMentions(_ context.Context, options GitHubMentionListOptions) ([]GitHubMention, error) {
	c.mentionOptions = append(c.mentionOptions, options)
	return c.mentions, nil
}

func countGitHubMentionRoutedActions(events []core.Event, taskID string, externalID string) int {
	var count int
	for _, event := range events {
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind       string `json:"kind"`
			Source     string `json:"source"`
			ExternalID string `json:"externalId"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == "github_mention_routed" && payload.Source == "github-mention" && payload.ExternalID == externalID {
			count++
		}
	}
	return count
}

func boolPtr(value bool) *bool {
	return &value
}
