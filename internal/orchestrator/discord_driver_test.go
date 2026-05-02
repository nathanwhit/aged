package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

func TestDiscordDriverSkipsHistoryThenCreatesTaskFromPrefix(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "task: old task", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled: true,
		Channels: []DiscordChannelConfig{{
			ID: "chan",
		}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 0 {
		t.Fatalf("history task was processed: %+v", snapshot.Tasks)
	}

	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "task: add the Discord driver", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "discord", "discord:2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing discord task")
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(client.sent[len(client.sent)-1], "Created aged task") {
		t.Fatalf("sent messages = %+v", client.sent)
	}
}

func TestDiscordDriverAnswersThenDoItCreatesSuggestedTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer:     "We need a Discord driver.\n\nAGED_TASK_PROMPT:\nImplement a Discord driver for aged.",
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: t.TempDir(), Repo: "nathanwhit/aged"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "how far are we from Discord?", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Reply `do it`") {
		t.Fatalf("sent messages = %+v", client.sent)
	}
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "do it", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "discord", "discord:2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !strings.Contains(task.Prompt, "Implement a Discord driver") {
		t.Fatalf("task = %+v ok=%v", task, ok)
	}
}

func TestDiscordDriverStructuredProposalRoutesProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer: `{
			"reply": "I can do that in the Node.js project.",
			"proposedTask": {
				"projectId": "node",
				"title": "Improve Node Benchmarks",
				"prompt": "Improve performance on the Node.js benchmarks."
			}
		}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: t.TempDir(), Repo: "nathanwhit/aged"},
		{ID: "node", Name: "Node.js", LocalPath: t.TempDir(), Repo: "nodejs/node"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "improve node perf", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Reply `do it`") {
		t.Fatalf("sent messages = %+v", client.sent)
	}
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "do it", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "discord", "discord:2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || task.ProjectID != "node" || task.Title != "Improve Node Benchmarks" {
		t.Fatalf("task = %+v ok=%v", task, ok)
	}
}

func TestDiscordDriverDoItRecoversProposalFromAssistantEvents(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer:     `{"reply":"I can do that in aged.","proposedTask":{"projectId":"aged","prompt":"Implement the project-aware Discord task."}}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: t.TempDir(), Repo: "nathanwhit/aged"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "make discord project-aware", Author: DiscordUser{ID: "user"}}},
		},
	}
	firstDriver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)
	if err := firstDriver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "do it", Author: DiscordUser{ID: "user"}})
	restartedDriver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)
	restartedDriver.lastSeen["chan"] = "1"
	restartedDriver.initialized["chan"] = true
	if err := restartedDriver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "discord", "discord:2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || task.ProjectID != "aged" || !strings.Contains(task.Prompt, "project-aware Discord") {
		t.Fatalf("task = %+v ok=%v", task, ok)
	}
}

func TestDiscordDriverAssistantCanCreateTaskWithoutDoIt(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer: `{
			"action": "create_task",
			"reply": "Starting that in aged.",
			"proposedTask": {
				"projectId": "aged",
				"title": "Add Project Routing",
				"prompt": "Add project-aware routing to Discord conversations."
			}
		}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: t.TempDir(), Repo: "nathanwhit/aged"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "ok let's make the discord project routing work", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "discord", "discord:1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || task.ProjectID != "aged" || task.Title != "Add Project Routing" {
		t.Fatalf("task = %+v ok=%v", task, ok)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Created aged task") {
		t.Fatalf("sent messages = %+v", client.sent)
	}
}

func TestDiscordDriverAsksAssistantInSelectedProjectWorkDir(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	assistant := &recordingAssistant{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	agedDir := t.TempDir()
	nodeDir := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: agedDir, Repo: "nathanwhit/aged"},
		{ID: "node", Name: "Node.js", LocalPath: nodeDir, Repo: "nodejs/node"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	service.SetAssistant(assistant)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "what does node do for streams?", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(assistant.requests) != 1 {
		t.Fatalf("assistant requests = %+v", assistant.requests)
	}
	if assistant.requests[0].WorkDir != nodeDir {
		t.Fatalf("assistant workdir = %q, want %q", assistant.requests[0].WorkDir, nodeDir)
	}
	if !strings.Contains(assistant.requests[0].ConversationID, ":node") {
		t.Fatalf("conversation id = %q", assistant.requests[0].ConversationID)
	}
}

func TestDiscordDriverListsProjects(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer:     `{"action":"list_projects","reply":"Listing projects.","project":null,"proposedTask":null}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	agedDir := t.TempDir()
	nodeDir := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: agedDir, Repo: "nathanwhit/aged"},
		{ID: "node", Name: "Node.js", LocalPath: nodeDir, Repo: "nodejs/node"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "list projects", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	reply := client.sent[len(client.sent)-1]
	if !strings.Contains(reply, "`aged`") || !strings.Contains(reply, "`node`") {
		t.Fatalf("reply = %q", reply)
	}
}

func TestDiscordDriverShowsTaskDetail(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "detail done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Discord detail",
		Prompt: "Show this task in Discord.",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"show_task","reply":"Showing task.","taskId":"` + task.ID + `","project":null,"proposedTask":null}`,
	})

	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "what happened with " + task.ID + "?", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	reply := client.sent[len(client.sent)-1]
	if !strings.Contains(reply, "Task `"+task.ID+"`") || !strings.Contains(reply, "Workers:") || !strings.Contains(reply, "Available actions:") {
		t.Fatalf("reply = %q", reply)
	}
}

func TestDiscordDriverShowsWorkerDetail(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "worker detail done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Discord worker detail",
		Prompt: "Show this worker in Discord.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) == 0 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	workerID := snapshot.Workers[0].ID
	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"show_worker","reply":"Showing worker.","workerId":"` + workerID + `","project":null,"proposedTask":null}`,
	})

	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "show worker " + workerID, Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	reply := client.sent[len(client.sent)-1]
	if !strings.Contains(reply, "Worker `"+workerID+"`") || !strings.Contains(reply, "Recent events:") || !strings.Contains(reply, "Available actions:") {
		t.Fatalf("reply = %q", reply)
	}
}

func TestDiscordDriverSteersTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "run"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
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
	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"steer_task","taskId":"task-1","message":"focus on the failing test","project":null,"proposedTask":null}`,
	})
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "tell task-1 to focus on the failing test", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventTaskSteered, "task-1", "") {
		t.Fatalf("missing steering event: %+v", snapshot.Events)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Sent steering") {
		t.Fatalf("sent = %+v", client.sent)
	}
}

func TestDiscordDriverPublishesPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
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
	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Publish me", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"publish_pr","taskId":"` + task.ID + `","confirmed":false,"publishPr":{"repo":"owner/repo","base":"main","draft":true},"project":null,"proposedTask":null}`,
	})
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "publish a draft PR for " + task.ID, Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("unconfirmed publish calls = %d, want 0", publisher.publishCalls)
	}
	reply := client.sent[len(client.sent)-1]
	if !strings.Contains(reply, "Publishing a PR") || !strings.Contains(reply, "explicit confirmation") {
		t.Fatalf("unconfirmed reply = %q", reply)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"publish_pr","taskId":"` + task.ID + `","confirmed":true,"publishPr":{"repo":"owner/repo","base":"main","draft":true},"project":null,"proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "yes, publish a draft PR for " + task.ID, Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if publisher.publishCalls != 1 || publisher.published.Repo != "owner/repo" || !publisher.published.Draft {
		t.Fatalf("publisher = %+v calls=%d", publisher.published, publisher.publishCalls)
	}
	reply = client.sent[len(client.sent)-1]
	if !strings.Contains(reply, "Published pull request") || !strings.Contains(reply, "https://github.com/owner/repo/pull/12") {
		t.Fatalf("reply = %q", reply)
	}
}

func TestDiscordDecisionResolvesNaturalReferences(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	snapshot := core.Snapshot{
		Tasks: []core.Task{
			{ID: "11111111-aaaa-task", Title: "Old", Status: core.TaskSucceeded, CreatedAt: now},
			{ID: "22222222-bbbb-task", Title: "Latest", Status: core.TaskQueued, CreatedAt: now.Add(time.Minute)},
			{ID: "33333333-cccc-task", Title: "Running", Status: core.TaskRunning, CreatedAt: now.Add(2 * time.Minute)},
			{ID: "44444444-dddd-task", Title: "Failed", Status: core.TaskFailed, CreatedAt: now.Add(3 * time.Minute)},
		},
		Workers: []core.Worker{
			{ID: "aaaaaaaa-worker", TaskID: "33333333-cccc-task", Status: core.WorkerSucceeded},
			{ID: "bbbbbbbb-worker", TaskID: "44444444-dddd-task", Status: core.WorkerFailed},
		},
		PullRequests: []core.PullRequest{
			{ID: "pr-abcdef12", TaskID: "33333333-cccc-task", Repo: "owner/repo", Number: 12, URL: "https://github.com/owner/repo/pull/12"},
			{ID: "pr-fedcba98", TaskID: "44444444-dddd-task", Repo: "owner/repo", Number: 13, URL: "https://github.com/owner/repo/pull/13"},
		},
	}

	tests := []struct {
		name          string
		decision      DiscordAssistantDecision
		content       string
		wantTask      string
		wantWorker    string
		wantPubWorker string
		wantPR        string
		wantWatchNum  int
	}{
		{
			name:     "short task id",
			decision: DiscordAssistantDecision{Action: "show_task", TaskID: "22222222"},
			wantTask: "22222222-bbbb-task",
		},
		{
			name:     "latest task",
			decision: DiscordAssistantDecision{Action: "show_task", TaskID: "latest task"},
			wantTask: "44444444-dddd-task",
		},
		{
			name:     "running task from content",
			decision: DiscordAssistantDecision{Action: "cancel_task"},
			content:  "cancel the running task",
			wantTask: "33333333-cccc-task",
		},
		{
			name:     "failed task from content",
			decision: DiscordAssistantDecision{Action: "retry_task"},
			content:  "retry the failed task",
			wantTask: "44444444-dddd-task",
		},
		{
			name:       "short worker id",
			decision:   DiscordAssistantDecision{Action: "show_worker", WorkerID: "aaaaaaaa"},
			wantWorker: "aaaaaaaa-worker",
		},
		{
			name:          "publish worker id from content",
			decision:      DiscordAssistantDecision{Action: "publish_pr", TaskID: "33333333"},
			content:       "publish worker aaaaaaaa as a PR",
			wantTask:      "33333333-cccc-task",
			wantPubWorker: "aaaaaaaa-worker",
		},
		{
			name:     "pull request url",
			decision: DiscordAssistantDecision{Action: "refresh_pr"},
			content:  "refresh https://github.com/owner/repo/pull/12",
			wantPR:   "pr-abcdef12",
		},
		{
			name:     "pull request number",
			decision: DiscordAssistantDecision{Action: "babysit_pr", PullRequestID: "13"},
			wantPR:   "pr-fedcba98",
		},
		{
			name:         "watch pull request number from content",
			decision:     DiscordAssistantDecision{Action: "watch_prs", TaskID: "33333333"},
			content:      "watch PR 12",
			wantTask:     "33333333-cccc-task",
			wantWatchNum: 12,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decision, prompt := resolveDiscordDecision(snapshot, tc.decision, tc.content)
			if prompt != "" {
				t.Fatalf("prompt = %q", prompt)
			}
			if tc.wantTask != "" && decision.TaskID != tc.wantTask {
				t.Fatalf("task id = %q, want %q", decision.TaskID, tc.wantTask)
			}
			if tc.wantWorker != "" && decision.WorkerID != tc.wantWorker {
				t.Fatalf("worker id = %q, want %q", decision.WorkerID, tc.wantWorker)
			}
			if tc.wantPubWorker != "" && decision.PublishPR.WorkerID != tc.wantPubWorker {
				t.Fatalf("publish worker id = %q, want %q", decision.PublishPR.WorkerID, tc.wantPubWorker)
			}
			if tc.wantPR != "" && decision.PullRequestID != tc.wantPR {
				t.Fatalf("pull request id = %q, want %q", decision.PullRequestID, tc.wantPR)
			}
			if tc.wantWatchNum != 0 && decision.WatchPRs.Number != tc.wantWatchNum {
				t.Fatalf("watch number = %d, want %d", decision.WatchPRs.Number, tc.wantWatchNum)
			}
		})
	}
}

func TestDiscordDecisionPromptsForAmbiguousNaturalTaskReference(t *testing.T) {
	snapshot := core.Snapshot{
		Tasks: []core.Task{
			{ID: "11111111-running", Status: core.TaskRunning},
			{ID: "22222222-running", Status: core.TaskRunning},
		},
	}
	_, prompt := resolveDiscordDecision(snapshot, DiscordAssistantDecision{Action: "cancel_task"}, "cancel the running task")
	if !strings.Contains(prompt, "Multiple running tasks match") {
		t.Fatalf("prompt = %q", prompt)
	}
}

func TestDiscordDriverCreatesProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectDir := t.TempDir()
	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer: `{
			"action": "create_project",
			"reply": "Creating the project.",
			"project": {
				"id": "node",
				"name": "Node.js",
				"localPath": "` + projectDir + `",
				"repo": "nodejs/node",
				"vcs": "auto",
				"defaultBase": "main"
			},
			"proposedTask": null
		}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "aged", Name: "aged", LocalPath: t.TempDir(), Repo: "nathanwhit/aged"},
	}, "aged")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "add node project", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := projectByID(snapshot.Projects, "node"); !ok {
		t.Fatalf("projects = %+v", snapshot.Projects)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Created project `node`") {
		t.Fatalf("sent = %+v", client.sent)
	}
}

func TestDiscordDriverUpdatesProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	agedDir := t.TempDir()
	nodeDir := t.TempDir()
	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer: `{
			"action": "update_project",
			"projectId": "node",
			"reply": "Updating node.",
			"project": {
				"name": "Node Runtime",
				"defaultBase": "trunk"
			},
			"proposedTask": null
		}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := service.CreateProject(ctx, core.Project{ID: "aged", Name: "aged", LocalPath: agedDir, DefaultBase: "main"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateProject(ctx, core.Project{
		ID:           "node",
		Name:         "Node.js",
		LocalPath:    nodeDir,
		Repo:         "nodejs/node",
		DefaultBase:  "main",
		TargetLabels: map[string]string{"role": "ci"},
		PullRequestPolicy: core.PullRequestPolicy{
			BranchPrefix: "aged/",
			Draft:        true,
			AllowMerge:   true,
			AutoMerge:    true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "update node default base to trunk", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	project, ok := projectByID(snapshot.Projects, "node")
	if !ok || project.Name != "Node Runtime" || project.DefaultBase != "trunk" || project.LocalPath != nodeDir || project.Repo != "nodejs/node" || project.TargetLabels["role"] != "ci" || project.PullRequestPolicy.BranchPrefix != "aged/" || !project.PullRequestPolicy.Draft || !project.PullRequestPolicy.AllowMerge || !project.PullRequestPolicy.AutoMerge {
		t.Fatalf("project = %+v ok=%v", project, ok)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Updated project `node`") {
		t.Fatalf("sent = %+v", client.sent)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{
			"action": "update_project",
			"projectId": "node",
			"project": {
				"repo": "",
				"defaultBase": "",
				"targetLabels": {},
				"pullRequestPolicy": {
					"branchPrefix": "",
					"draft": false,
					"allowMerge": false,
					"autoMerge": false
				}
			},
			"proposedTask": null
		}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "clear node repo and disable PR defaults", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	project, ok = projectByID(snapshot.Projects, "node")
	if !ok || project.Repo != "" || project.DefaultBase != "main" || len(project.TargetLabels) != 0 || project.PullRequestPolicy.BranchPrefix != "codex/aged-" || project.PullRequestPolicy.Draft || project.PullRequestPolicy.AllowMerge || project.PullRequestPolicy.AutoMerge {
		t.Fatalf("cleared project = %+v ok=%v", project, ok)
	}
}

func TestDiscordDriverShowsProjectHealth(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	nodeDir := t.TempDir()
	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer:     `{"action":"project_health","projectId":"node","reply":"Checking node.","project":null,"proposedTask":null}`,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := service.CreateProject(ctx, core.Project{ID: "node", Name: "Node.js", LocalPath: nodeDir, DefaultBase: "main"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "health for node", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "node"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	reply := client.sent[len(client.sent)-1]
	if !strings.Contains(reply, "Project `node` health") || !strings.Contains(reply, "path: `ok`") {
		t.Fatalf("reply = %q", reply)
	}
}

func TestDiscordDriverDeleteProjectRequiresConfirmation(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer:     `{"action":"delete_project","projectId":"node","confirmed":false,"reply":"Delete node?","project":null,"proposedTask":null}`,
	}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := service.CreateProject(ctx, core.Project{ID: "aged", Name: "aged", LocalPath: t.TempDir(), DefaultBase: "main"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateProject(ctx, core.Project{ID: "node", Name: "Node.js", LocalPath: t.TempDir(), DefaultBase: "main"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "delete node", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan", DefaultProjectID: "aged"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := projectByID(snapshot.Projects, "node"); !ok {
		t.Fatalf("node was deleted before confirmation: %+v", snapshot.Projects)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "explicit confirmation") {
		t.Fatalf("sent = %+v", client.sent)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"delete_project","projectId":"node","confirmed":true,"reply":"Deleting node.","project":null,"proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "yes, delete node", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := projectByID(snapshot.Projects, "node"); ok {
		t.Fatalf("node was not deleted: %+v", snapshot.Projects)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Deleted project `node`") {
		t.Fatalf("sent = %+v", client.sent)
	}
}

func TestDiscordDriverContextIncludesTargetsAndPlugins(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	assistant := &recordingAssistant{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetAssistant(assistant)
	if _, err := service.RegisterTarget(ctx, core.TargetConfig{
		ID:       "local-small",
		Kind:     "local",
		Labels:   map[string]string{"role": "small"},
		Capacity: core.TargetCapacity{MaxWorkers: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RegisterPlugin(ctx, core.Plugin{
		ID:      "integration:test",
		Name:    "Test Integration",
		Kind:    "integration",
		Enabled: false,
	}); err != nil {
		t.Fatal(err)
	}
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "what targets and plugins are configured?", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(assistant.requests) != 1 {
		t.Fatalf("assistant requests = %+v", assistant.requests)
	}
	var contextPayload struct {
		Targets []core.TargetState `json:"targets"`
		Plugins []core.Plugin      `json:"plugins"`
	}
	if err := json.Unmarshal(assistant.requests[0].Context, &contextPayload); err != nil {
		t.Fatal(err)
	}
	var foundTarget, foundPlugin bool
	for _, target := range contextPayload.Targets {
		if target.ID == "local-small" {
			foundTarget = true
		}
	}
	for _, plugin := range contextPayload.Plugins {
		if plugin.ID == "integration:test" {
			foundPlugin = true
		}
	}
	if !foundTarget || !foundPlugin {
		t.Fatalf("context targets=%+v plugins=%+v", contextPayload.Targets, contextPayload.Plugins)
	}
}

func TestDiscordDriverManagesTargets(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer: `{
			"action": "create_target",
			"target": {
				"id": "local-small",
				"kind": "local",
				"port": 2222,
				"insecureIgnoreHostKey": true,
				"labels": {"role": "small"},
				"capacity": {"maxWorkers": 2, "cpuWeight": 2, "memoryGB": 16}
			},
			"proposedTask": null
		}`,
	}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "add a small local target", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target, ok := targetByID(snapshot.Targets, "local-small")
	if !ok || target.Capacity.MaxWorkers != 2 {
		t.Fatalf("target = %+v ok=%v", target, ok)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Created target `local-small`") {
		t.Fatalf("sent = %+v", client.sent)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{
			"action": "update_target",
			"targetId": "local-small",
			"target": {
				"labels": {"role": "large"},
				"capacity": {"maxWorkers": 4}
			},
			"proposedTask": null
		}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "make local-small larger", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target, ok = targetByID(snapshot.Targets, "local-small")
	if !ok || target.Labels["role"] != "large" || target.Capacity.MaxWorkers != 4 {
		t.Fatalf("updated target = %+v ok=%v", target, ok)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{
			"action": "update_target",
			"targetId": "local-small",
			"target": {
				"port": 0,
				"insecureIgnoreHostKey": false,
				"labels": {},
				"capacity": {"memoryGB": 0}
			},
			"proposedTask": null
		}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "3", ChannelID: "chan", Content: "clear local-small optional target settings", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target, ok = targetByID(snapshot.Targets, "local-small")
	if !ok || target.Port != 0 || target.InsecureIgnoreHostKey || len(target.Labels) != 0 || target.Capacity.MemoryGB != 0 || target.Capacity.MaxWorkers != 4 || target.Capacity.CPUWeight != 2 {
		t.Fatalf("cleared target = %+v ok=%v", target, ok)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{
			"action": "update_target",
			"targetId": "local-small",
			"target": {
				"capacity": {"maxWorkers": 0, "cpuWeight": 0}
			},
			"proposedTask": null
		}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "4", ChannelID: "chan", Content: "zero out local-small worker capacity", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target, ok = targetByID(snapshot.Targets, "local-small")
	if !ok || target.Capacity.MaxWorkers != 1 || target.Capacity.CPUWeight != 1 || target.Capacity.MemoryGB != 0 {
		t.Fatalf("zero capacity target = %+v ok=%v", target, ok)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"target_health","targetId":"local-small","proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "5", ChannelID: "chan", Content: "health for local-small", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Target `local-small` health") {
		t.Fatalf("sent = %+v", client.sent)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"delete_target","targetId":"local-small","confirmed":false,"proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "6", ChannelID: "chan", Content: "delete local-small", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := targetByID(snapshot.Targets, "local-small"); !ok {
		t.Fatalf("target was deleted before confirmation: %+v", snapshot.Targets)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "explicit confirmation") {
		t.Fatalf("sent = %+v", client.sent)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"delete_target","targetId":"local-small","confirmed":true,"proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "7", ChannelID: "chan", Content: "yes, delete local-small", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := targetByID(snapshot.Targets, "local-small"); ok {
		t.Fatalf("target was not deleted: %+v", snapshot.Targets)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Deleted target `local-small`") {
		t.Fatalf("sent = %+v", client.sent)
	}
}

func TestDiscordDriverManagesPluginsWithDeleteConfirmation(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}},
		answer: `{
			"action": "create_plugin",
			"plugin": {
				"id": "integration:test",
				"name": "Test Integration",
				"kind": "integration",
				"enabled": false,
				"endpoint": "https://example.invalid/plugin",
				"capabilities": ["inspect"],
				"config": {"env": "test"}
			},
			"proposedTask": null
		}`,
	}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "register test plugin", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin, ok := pluginByID(snapshot.Plugins, "integration:test")
	if !ok || plugin.Config["env"] != "test" {
		t.Fatalf("plugin = %+v ok=%v", plugin, ok)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{
			"action": "update_plugin",
			"pluginId": "integration:test",
			"plugin": {
				"name": "Updated Integration",
				"enabled": true,
				"config": {"env": "updated"}
			},
			"proposedTask": null
		}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "enable and rename test plugin", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin, ok = pluginByID(snapshot.Plugins, "integration:test")
	if !ok || plugin.Name != "Updated Integration" || !plugin.Enabled || plugin.Config["env"] != "updated" {
		t.Fatalf("updated plugin = %+v ok=%v", plugin, ok)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{
			"action": "update_plugin",
			"pluginId": "integration:test",
			"plugin": {
				"enabled": false,
				"endpoint": "",
				"capabilities": [],
				"config": {}
			},
			"proposedTask": null
		}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "3", ChannelID: "chan", Content: "disable and clear test plugin settings", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin, ok = pluginByID(snapshot.Plugins, "integration:test")
	if !ok || plugin.Enabled || plugin.Status != "disabled" || plugin.Endpoint != "" || len(plugin.Capabilities) != 0 || len(plugin.Config) != 0 || plugin.Name != "Updated Integration" {
		t.Fatalf("cleared plugin = %+v ok=%v", plugin, ok)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"delete_plugin","pluginId":"integration:test","confirmed":false,"proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "4", ChannelID: "chan", Content: "delete test plugin", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pluginByID(snapshot.Plugins, "integration:test"); !ok {
		t.Fatalf("plugin was deleted before confirmation: %+v", snapshot.Plugins)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "explicit confirmation") {
		t.Fatalf("sent = %+v", client.sent)
	}

	service.SetAssistant(fixedAssistantBrain{
		answer: `{"action":"delete_plugin","pluginId":"integration:test","confirmed":true,"proposedTask":null}`,
	})
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "5", ChannelID: "chan", Content: "yes, delete test plugin", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pluginByID(snapshot.Plugins, "integration:test"); ok {
		t.Fatalf("plugin was not deleted: %+v", snapshot.Plugins)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Deleted plugin `integration:test`") {
		t.Fatalf("sent = %+v", client.sent)
	}
}

func TestDiscordDriverAssistantFallbackStillAllowsDoIt(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	client := &fakeDiscordClient{
		me: DiscordUser{ID: "bot", Bot: true},
		messages: map[string][]DiscordMessage{
			"chan": {{ID: "1", ChannelID: "chan", Content: "add Discord support", Author: DiscordUser{ID: "user"}}},
		},
	}
	driver := NewDiscordDriver(service, DiscordDriverConfig{
		Enabled:        true,
		ProcessHistory: true,
		Channels:       []DiscordChannelConfig{{ID: "chan"}},
	}, client)

	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(client.sent[len(client.sent)-1], "Reply `do it`") {
		t.Fatalf("sent messages = %+v", client.sent)
	}
	client.messages["chan"] = append(client.messages["chan"], DiscordMessage{ID: "2", ChannelID: "chan", Content: "do it", Author: DiscordUser{ID: "user"}})
	if err := driver.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	task, ok, err := service.FindTaskByExternalID(ctx, "discord", "discord:2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || task.Prompt != "add Discord support" {
		t.Fatalf("task = %+v ok=%v", task, ok)
	}
}

type fakeDiscordClient struct {
	me       DiscordUser
	messages map[string][]DiscordMessage
	sent     []string
}

func targetByID(targets []core.TargetState, id string) (core.TargetState, bool) {
	for _, target := range targets {
		if target.ID == id {
			return target, true
		}
	}
	return core.TargetState{}, false
}

func pluginByID(plugins []core.Plugin, id string) (core.Plugin, bool) {
	for _, plugin := range plugins {
		if plugin.ID == id {
			return plugin, true
		}
	}
	return core.Plugin{}, false
}

func (c *fakeDiscordClient) Me(context.Context) (DiscordUser, error) {
	return c.me, nil
}

func (c *fakeDiscordClient) ListMessages(_ context.Context, channelID string, afterID string, limit int) ([]DiscordMessage, error) {
	var out []DiscordMessage
	for _, message := range c.messages[channelID] {
		if afterID != "" && message.ID <= afterID {
			continue
		}
		out = append(out, message)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}

func (c *fakeDiscordClient) SendMessage(_ context.Context, _ string, content string) error {
	c.sent = append(c.sent, content)
	return nil
}
