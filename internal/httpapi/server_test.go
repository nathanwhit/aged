package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/orchestrator"
	"aged/internal/worker"
)

func postMCP(t *testing.T, serverURL string, body string) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, serverURL+"/mcp", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Mcp-Method", "test")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("mcp status = %d", res.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestCreateTaskRejectsUserWorkerSelection(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"title": "Do work",
		"prompt": "User request",
		"kind": "mock"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func waitForHTTPTaskStatus(t *testing.T, store eventstore.Store, taskID string, status core.TaskStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range snapshot.Tasks {
			if task.ID == taskID && task.Status == status {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach %s", taskID, status)
}

func TestCreateTaskAcceptsOnlyUserWorkRequest(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"title": "Do work",
		"prompt": "User request"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func TestCreateTaskAllowsGeneratedTitle(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"prompt": "Implement parser retry path"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func TestMCPEndpointInitializesAndListsTools(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	init := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "initialize",
		"params": {
			"protocolVersion": "2025-11-25",
			"capabilities": {},
			"clientInfo": {"name": "test", "version": "1"}
		}
	}`)
	result := init["result"].(map[string]any)
	if result["protocolVersion"] != mcpProtocolVersion {
		t.Fatalf("initialize result = %+v", result)
	}

	tools := postMCP(t, server.URL, `{"jsonrpc":"2.0","id":"tools","method":"tools/list"}`)
	list := tools["result"].(map[string]any)["tools"].([]any)
	var found bool
	for _, item := range list {
		tool := item.(map[string]any)
		if tool["name"] == "aged_create_task" {
			found = true
		}
	}
	if !found {
		t.Fatalf("aged_create_task missing from tools: %+v", list)
	}
}

func TestMCPCreateTaskAndReadResources(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	created := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "tools/call",
		"params": {
			"name": "aged_create_task",
			"arguments": {
				"title": "MCP task",
				"prompt": "Run through MCP"
			}
		}
	}`)
	content := created["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var task core.Task
	if err := json.Unmarshal([]byte(content["text"].(string)), &task); err != nil {
		t.Fatal(err)
	}

	resources := postMCP(t, server.URL, `{"jsonrpc":"2.0","id":2,"method":"resources/list"}`)
	list := resources["result"].(map[string]any)["resources"].([]any)
	var foundTaskResource bool
	for _, item := range list {
		resource := item.(map[string]any)
		if resource["uri"] == "aged://tasks/"+task.ID {
			foundTaskResource = true
		}
	}
	if !foundTaskResource {
		t.Fatalf("task resource missing: %+v", list)
	}

	read := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 3,
		"method": "resources/read",
		"params": {"uri": "aged://tasks/`+task.ID+`"}
	}`)
	contents := read["result"].(map[string]any)["contents"].([]any)
	text := contents[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, "MCP task") {
		t.Fatalf("resource text = %s", text)
	}
}

func TestMCPProjectTools(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	created := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "tools/call",
		"params": {
			"name": "aged_create_project",
			"arguments": {
				"id": "node",
				"name": "Node.js",
				"localPath": "/tmp/node",
				"repo": "nodejs/node",
				"vcs": "auto",
				"defaultBase": "main"
			}
		}
	}`)
	content := created["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var project core.Project
	if err := json.Unmarshal([]byte(content["text"].(string)), &project); err != nil {
		t.Fatal(err)
	}
	if project.ID != "node" {
		t.Fatalf("create project result = %+v", project)
	}

	listed := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 2,
		"method": "tools/call",
		"params": {
			"name": "aged_list_projects",
			"arguments": {}
		}
	}`)
	listContent := listed["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var projects []core.Project
	if err := json.Unmarshal([]byte(listContent["text"].(string)), &projects); err != nil {
		t.Fatal(err)
	}
	var foundNode bool
	for _, project := range projects {
		if project.ID == "node" {
			foundNode = true
		}
	}
	if !foundNode {
		t.Fatalf("list projects result = %+v", projects)
	}
}

func TestRetryTaskEndpointRetriesFailedTask(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "missing"}, map[string]worker.Runner{}, t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"title": "Do work",
		"prompt": "User request"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("create status = %d", res.StatusCode)
	}
	var task core.Task
	if err := json.NewDecoder(res.Body).Decode(&task); err != nil {
		t.Fatal(err)
	}
	waitForHTTPTaskStatus(t, store, task.ID, core.TaskFailed)

	retry, err := http.Post(server.URL+"/api/tasks/"+task.ID+"/retry", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Body.Close()
	if retry.StatusCode != http.StatusAccepted {
		t.Fatalf("retry status = %d", retry.StatusCode)
	}
	var retried core.Task
	if err := json.NewDecoder(retry.Body).Decode(&retried); err != nil {
		t.Fatal(err)
	}
	if retried.ID != task.ID {
		t.Fatalf("retried task = %q, want %q", retried.ID, task.ID)
	}
}

func TestProjectsEndpointReturnsConfiguredProjects(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	projects, err := orchestrator.NewProjectRegistry([]core.Project{{
		ID:        "repo",
		Name:      "Repo",
		LocalPath: t.TempDir(),
		Repo:      "owner/repo",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	service.SetProjects(projects)
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Get(server.URL + "/api/projects")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func TestCreateProjectEndpointPersistsProject(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/projects", "application/json", strings.NewReader(`{
		"id": "other",
		"name": "Other",
		"localPath": "/tmp/other",
		"repo": "owner/other",
		"upstreamRepo": "upstream/other",
		"headRepoOwner": "owner",
		"pushRemote": "fork",
		"vcs": "git",
		"defaultBase": "main"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", res.StatusCode)
	}

	var created core.Project
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID != "other" || created.LocalPath != "/tmp/other" || created.UpstreamRepo != "upstream/other" || created.HeadRepoOwner != "owner" || created.PushRemote != "fork" {
		t.Fatalf("created = %+v", created)
	}

	projects, _, err := store.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 1 || projects[0].ID != "other" || projects[0].UpstreamRepo != "upstream/other" || projects[0].HeadRepoOwner != "owner" || projects[0].PushRemote != "fork" {
		t.Fatalf("projects = %+v", projects)
	}
}

func TestTaskLookupFindsExternalSourceTask(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks", "application/json", strings.NewReader(`{
		"title": "GitHub issue",
		"prompt": "Fix it",
		"source": "github",
		"externalId": "owner/repo#123",
		"metadata": { "repo": "owner/repo", "issue": 123 }
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("create status = %d", res.StatusCode)
	}

	lookup, err := http.Get(server.URL + "/api/tasks/lookup?source=github&externalId=owner%2Frepo%23123")
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Body.Close()
	if lookup.StatusCode != http.StatusOK {
		t.Fatalf("lookup status = %d", lookup.StatusCode)
	}
}

func TestAssistantEndpointReturnsAnswer(t *testing.T) {
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, assistantBrain{}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/assistant", "application/json", strings.NewReader(`{
		"message": "What can you do?"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func TestClearTerminalTasksEndpointHidesFinishedTask(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Finished task",
			"prompt": "Clear me",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks/clear-terminal", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", res.StatusCode)
	}

	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 0 {
		t.Fatalf("tasks = %d, want 0", len(snapshot.Tasks))
	}
	if countEventType(snapshot.Events, core.EventTaskCleared) != 1 {
		t.Fatalf("task.cleared events = %d, want 1", countEventType(snapshot.Events, core.EventTaskCleared))
	}
}

type assistantBrain struct{}

func (assistantBrain) Plan(context.Context, core.Task, []string) (orchestrator.Plan, error) {
	return orchestrator.Plan{WorkerKind: "mock", Prompt: "unused"}, nil
}

func (assistantBrain) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	return core.AssistantResponse{ConversationID: req.ConversationID, Message: "answer"}, nil
}

func countEventType(events []core.Event, eventType core.EventType) int {
	count := 0
	for _, event := range events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}
