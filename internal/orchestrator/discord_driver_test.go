package orchestrator

import (
	"context"
	"strings"
	"testing"

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
