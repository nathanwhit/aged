package orchestrator

import (
	"context"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

func TestServiceGitHubDriverConfigHotToggles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture := newGitHubDriverTestFixture(t, GitHubDriverConfig{}, &fakeGitHubClient{})
	store := fixture.store
	service := fixture.service
	projects, err := NewProjectRegistry([]core.Project{{
		ID:           "fork",
		Name:         "Fork",
		LocalPath:    t.TempDir(),
		Repo:         "fork-owner/repo",
		UpstreamRepo: "owner/repo",
		GitHubIssues: core.GitHubIssuePolicy{
			Enabled:    true,
			Labels:     []string{"aged"},
			IssueLimit: 7,
		},
		GitHubMentions: core.GitHubMentionPolicy{
			Enabled: true,
			Reasons: []string{"review_requested"},
			Limit:   3,
		},
	}}, "fork")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	service.Drivers().SetGitHubClient(&fakeGitHubClient{
		issues: []GitHubIssue{{
			Repo:   "owner/repo",
			Number: 12,
			Title:  "Add feature",
			Body:   "Please add the feature.",
			URL:    "https://github.com/owner/repo/issues/12",
			Labels: []string{"aged"},
		}},
		mentions: []GitHubMention{{
			ID:          "thread-1",
			Repo:        "owner/repo",
			SubjectType: "PullRequest",
			Number:      15,
			Title:       "Review me",
			URL:         "https://github.com/owner/repo/pull/15",
			Reason:      "review_requested",
		}},
	})

	state, err := service.Drivers().StartGitHubDriver(ctx, GitHubDriverConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if state.Running || state.Config.Enabled {
		t.Fatalf("initial state = %+v", state)
	}

	state, err = service.Drivers().ConfigureGitHubDriver(GitHubDriverConfig{
		Enabled:         true,
		IntervalSeconds: 3600,
		PullRequests: GitHubPullRequestDriverConfig{
			AutoPublish: boolPtr(false),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !state.Running || !state.Config.Enabled || state.Config.IntervalSeconds != 3600 {
		t.Fatalf("enabled state = %+v", state)
	}
	if len(state.Config.Issues) != 1 || state.Config.Issues[0].ProjectID != "fork" || state.Config.Issues[0].IssueLimit != 7 {
		t.Fatalf("effective issues = %+v", state.Config.Issues)
	}
	if state.Config.Mentions.Enabled == nil || !*state.Config.Mentions.Enabled || len(state.Config.Mentions.Repos) != 1 || state.Config.Mentions.Repos[0] != "owner/repo" || state.Config.Mentions.Limit != 3 {
		t.Fatalf("effective mentions = %+v", state.Config.Mentions)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin, ok := pluginByID(snapshot.Plugins, "driver:github")
	if !ok || !plugin.Enabled || plugin.Status != "running" || !plugin.Driver.Managed {
		t.Fatalf("github plugin = %+v found=%v", plugin, ok)
	}

	task := waitForGitHubIssueTask(t, service, "github-issue", "owner/repo#12")
	if task.ProjectID != "fork" {
		t.Fatalf("task project = %q, want fork", task.ProjectID)
	}
	mentionTask := waitForGitHubIssueTask(t, service, "github-mention", "thread-1")
	if mentionTask.ProjectID != "fork" {
		t.Fatalf("mention task project = %q, want fork", mentionTask.ProjectID)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)

	state, err = service.Drivers().ConfigureGitHubDriver(GitHubDriverConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if state.Running || state.Config.Enabled {
		t.Fatalf("disabled state = %+v", state)
	}
}

func TestServiceDiscordDriverConfigHotToggles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: testWorkItemPlan("mock", "do it")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.Drivers().SetDiscordClient(&fakeDiscordClient{
		me:       DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{"chan": {}},
	})

	state, err := service.Drivers().StartDiscordDriver(ctx, DiscordDriverConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if state.Running || state.Config.Enabled {
		t.Fatalf("initial state = %+v", state)
	}

	state, err = service.Drivers().ConfigureDiscordDriver(DiscordDriverConfig{
		Enabled:         true,
		Token:           "secret-token",
		IntervalSeconds: 3600,
		MessageLimit:    10,
		Channels:        []DiscordChannelConfig{{ID: "chan", TaskPrefix: "task:"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !state.Running || !state.Config.Enabled || state.Config.IntervalSeconds != 3600 || state.Config.Token != "" {
		t.Fatalf("enabled state = %+v", state)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin, ok := pluginByID(snapshot.Plugins, "driver:discord")
	if !ok || !plugin.Enabled || plugin.Status != "running" || !plugin.Driver.Managed {
		t.Fatalf("discord plugin = %+v found=%v", plugin, ok)
	}

	state, err = service.Drivers().ConfigureDiscordDriver(DiscordDriverConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if state.Running || state.Config.Enabled || state.Config.Token != "" {
		t.Fatalf("disabled state = %+v", state)
	}
}

func waitForGitHubIssueTask(t *testing.T, service *Service, source string, externalID string) core.Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, ok, err := service.FindTaskByExternalID(context.Background(), source, externalID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for task %s/%s", source, externalID)
	return core.Task{}
}
