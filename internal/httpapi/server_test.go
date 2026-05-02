package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
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

func hasAvailableAction(actions []orchestrator.AvailableAction, name string) bool {
	for _, action := range actions {
		if action.Name == name {
			return true
		}
	}
	return false
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

func TestSnapshotCanOmitEventsAndExposeLastEventID(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: "task-a", Payload: core.MustJSON(map[string]any{"title": "Task A", "prompt": "Load quickly"})},
		{Type: core.EventWorkerOutput, TaskID: "task-a", WorkerID: "worker-a", Payload: core.MustJSON(map[string]any{"text": strings.Repeat("x", 256)})},
		{Type: core.EventTaskStatus, TaskID: "task-a", Payload: core.MustJSON(map[string]any{"status": core.TaskSucceeded})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Get(server.URL + "/api/snapshot?events=none")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var snapshot core.Snapshot
	if err := json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 0 {
		t.Fatalf("events = %d, want 0", len(snapshot.Events))
	}
	if snapshot.LastEventID != 3 {
		t.Fatalf("last event id = %d, want 3", snapshot.LastEventID)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].Status != core.TaskSucceeded {
		t.Fatalf("tasks = %+v", snapshot.Tasks)
	}
}

func TestTaskEventsEndpointLimitsWorkerOutput(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	taskID := "task-events"
	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Events", "prompt": "Lazy detail"})},
		{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: "worker-events", Payload: core.MustJSON(map[string]any{"kind": "mock"})},
		{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: "worker-events", Payload: core.MustJSON(map[string]any{"text": "first"})},
		{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: "worker-events", Payload: core.MustJSON(map[string]any{"text": "second"})},
		{Type: core.EventWorkerCompleted, TaskID: taskID, WorkerID: "worker-events", Payload: core.MustJSON(map[string]any{"status": core.WorkerSucceeded})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Get(server.URL + "/api/tasks/" + taskID + "/events?limit=1")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var events []core.Event
	if err := json.NewDecoder(res.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("events = %d, want 4", len(events))
	}
	var outputCount int
	for _, event := range events {
		if event.Type == core.EventWorkerOutput {
			outputCount++
		}
	}
	if outputCount != 1 {
		t.Fatalf("worker.output events = %d, want 1; events = %+v", outputCount, events)
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

func TestMCPTaskDetailIncludesWorkersEventsAndActions(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	taskID := "task-detail"
	workerID := "worker-detail"
	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Detail task", "prompt": "Expose task detail through MCP"})},
		{Type: core.EventExecutionPlanned, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"nodeId": "node-detail", "workerId": workerID, "workerKind": "mock", "role": "implementation"})},
		{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock", "prompt": "do it"})},
		{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})},
		{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "result", "text": "detail done"})},
		{Type: core.EventWorkerCompleted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"status": core.WorkerSucceeded, "summary": "detail done"})},
		{Type: core.EventTaskStatus, TaskID: taskID, Payload: core.MustJSON(map[string]any{"status": core.TaskSucceeded})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	detailResult := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "tools/call",
		"params": {
			"name": "aged_task_detail",
			"arguments": {"taskId": "`+taskID+`"}
		}
	}`)
	content := detailResult["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var detail orchestrator.TaskDetail
	if err := json.Unmarshal([]byte(content["text"].(string)), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Task.ID != taskID {
		t.Fatalf("detail task = %+v", detail.Task)
	}
	if len(detail.Workers) == 0 {
		t.Fatalf("detail workers missing: %+v", detail)
	}
	if len(detail.RecentEvents) == 0 {
		t.Fatalf("detail events missing: %+v", detail)
	}
	if !hasAvailableAction(detail.AvailableActions, "aged_clear_task") || !hasAvailableAction(detail.AvailableActions, "aged_publish_pr") {
		t.Fatalf("detail actions = %+v", detail.AvailableActions)
	}
}

func TestRegisterPluginEndpointPersistsAndExposesPlugin(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/plugins", "application/json", strings.NewReader(`{
		"id": "runner:lint",
		"name": "Lint Runner",
		"kind": "runner",
		"protocol": "aged-runner-v1",
		"enabled": true,
		"command": ["aged-lint"],
		"capabilities": ["lint"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", res.StatusCode)
	}

	plugins, err := store.ListPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plugins) != 1 || plugins[0].ID != "runner:lint" {
		t.Fatalf("plugins = %+v", plugins)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, plugin := range snapshot.Plugins {
		if plugin.ID == "runner:lint" {
			found = true
		}
	}
	if !found {
		t.Fatalf("snapshot plugins = %+v", snapshot.Plugins)
	}
}

func TestRegisterTargetEndpointPersistsAndExposesTarget(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/targets", "application/json", strings.NewReader(`{
		"id": "local-ci",
		"kind": "local",
		"labels": {"location": "remote"},
		"capacity": {"maxWorkers": 2, "cpuWeight": 8, "memoryGB": 32}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", res.StatusCode)
	}

	targets, err := store.ListTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].ID != "local-ci" || targets[0].Labels["location"] != "remote" {
		t.Fatalf("targets = %+v", targets)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, target := range snapshot.Targets {
		if target.ID == "local-ci" && target.Health.Status == "ok" {
			found = true
		}
	}
	if !found {
		t.Fatalf("snapshot targets = %+v", snapshot.Targets)
	}

	healthRes, err := http.Post(server.URL+"/api/targets/local-ci/health", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer healthRes.Body.Close()
	if healthRes.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", healthRes.StatusCode)
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

	projectDir := t.TempDir()
	created := postMCP(t, server.URL, fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "tools/call",
		"params": {
			"name": "aged_create_project",
			"arguments": {
				"id": "node",
				"name": "Node.js",
				"localPath": %q,
				"repo": "nodejs/node",
				"vcs": "auto",
				"defaultBase": "main",
				"targetLabels": {"role": "ci"},
				"pullRequestPolicy": {
					"branchPrefix": "aged/",
					"draft": true,
					"allowMerge": true,
					"autoMerge": true
				}
			}
		}
	}`, projectDir))
	content := created["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var project core.Project
	if err := json.Unmarshal([]byte(content["text"].(string)), &project); err != nil {
		t.Fatal(err)
	}
	if project.ID != "node" {
		t.Fatalf("create project result = %+v", project)
	}
	_ = postMCP(t, server.URL, fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 2,
		"method": "tools/call",
		"params": {
			"name": "aged_create_project",
			"arguments": {
				"id": "keep",
				"name": "Keep",
				"localPath": %q,
				"defaultBase": "main"
			}
		}
	}`, t.TempDir()))

	listed := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 3,
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

	updated := postMCP(t, server.URL, fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 4,
		"method": "tools/call",
		"params": {
			"name": "aged_update_project",
			"arguments": {
				"id": "node",
				"name": "Node Runtime",
				"defaultBase": "trunk"
			}
		}
	}`))
	updateContent := updated["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var updatedProject core.Project
	if err := json.Unmarshal([]byte(updateContent["text"].(string)), &updatedProject); err != nil {
		t.Fatal(err)
	}
	if updatedProject.Name != "Node Runtime" || updatedProject.DefaultBase != "trunk" || updatedProject.LocalPath != projectDir || updatedProject.Repo != "nodejs/node" || updatedProject.TargetLabels["role"] != "ci" || updatedProject.PullRequestPolicy.BranchPrefix != "aged/" || !updatedProject.PullRequestPolicy.Draft || !updatedProject.PullRequestPolicy.AllowMerge || !updatedProject.PullRequestPolicy.AutoMerge {
		t.Fatalf("update project result = %+v", updatedProject)
	}

	cleared := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 41,
		"method": "tools/call",
		"params": {
			"name": "aged_update_project",
			"arguments": {
				"id": "node",
				"repo": "",
				"defaultBase": "",
				"targetLabels": {},
				"pullRequestPolicy": {
					"branchPrefix": "",
					"draft": false,
					"allowMerge": false,
					"autoMerge": false
				}
			}
		}
	}`)
	clearContent := cleared["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var clearedProject core.Project
	if err := json.Unmarshal([]byte(clearContent["text"].(string)), &clearedProject); err != nil {
		t.Fatal(err)
	}
	if clearedProject.Repo != "" || clearedProject.DefaultBase != "main" || len(clearedProject.TargetLabels) != 0 || clearedProject.PullRequestPolicy.BranchPrefix != "codex/aged-" || clearedProject.PullRequestPolicy.Draft || clearedProject.PullRequestPolicy.AllowMerge || clearedProject.PullRequestPolicy.AutoMerge {
		t.Fatalf("cleared project result = %+v", clearedProject)
	}

	checked := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 5,
		"method": "tools/call",
		"params": {
			"name": "aged_project_health",
			"arguments": {"projectId": "node"}
		}
	}`)
	healthContent := checked["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var health core.ProjectHealth
	if err := json.Unmarshal([]byte(healthContent["text"].(string)), &health); err != nil {
		t.Fatal(err)
	}
	if health.ProjectID != "node" || health.PathStatus != "ok" {
		t.Fatalf("health result = %+v", health)
	}

	deleted := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 6,
		"method": "tools/call",
		"params": {
			"name": "aged_delete_project",
			"arguments": {"projectId": "node"}
		}
	}`)
	deleteContent := deleted["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var deleteResult map[string]bool
	if err := json.Unmarshal([]byte(deleteContent["text"].(string)), &deleteResult); err != nil {
		t.Fatal(err)
	}
	if !deleteResult["ok"] {
		t.Fatalf("delete project result = %+v", deleteResult)
	}
}

func TestMCPTargetAndPluginTools(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	createdTarget := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "tools/call",
		"params": {
			"name": "aged_create_target",
			"arguments": {
				"id": "local-small",
				"kind": "local",
				"port": 2222,
				"insecureIgnoreHostKey": true,
				"labels": {"role": "small"},
				"capacity": {"maxWorkers": 2, "cpuWeight": 3, "memoryGB": 16}
			}
		}
	}`)
	targetContent := createdTarget["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var target core.TargetConfig
	if err := json.Unmarshal([]byte(targetContent["text"].(string)), &target); err != nil {
		t.Fatal(err)
	}
	if target.ID != "local-small" || target.Capacity.MaxWorkers != 2 {
		t.Fatalf("create target result = %+v", target)
	}

	listedTargets := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 2,
		"method": "tools/call",
		"params": {"name": "aged_list_targets", "arguments": {}}
	}`)
	targetListContent := listedTargets["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var targets []core.TargetState
	if err := json.Unmarshal([]byte(targetListContent["text"].(string)), &targets); err != nil {
		t.Fatal(err)
	}
	var foundTarget bool
	for _, target := range targets {
		if target.ID == "local-small" {
			foundTarget = true
		}
	}
	if !foundTarget {
		t.Fatalf("list targets result = %+v", targets)
	}

	updatedTarget := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 3,
		"method": "tools/call",
		"params": {
			"name": "aged_update_target",
			"arguments": {
				"id": "local-small",
				"labels": {"role": "large"},
				"capacity": {"maxWorkers": 4}
			}
		}
	}`)
	updateTargetContent := updatedTarget["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var updatedTargetConfig core.TargetConfig
	if err := json.Unmarshal([]byte(updateTargetContent["text"].(string)), &updatedTargetConfig); err != nil {
		t.Fatal(err)
	}
	if updatedTargetConfig.Labels["role"] != "large" || updatedTargetConfig.Capacity.MaxWorkers != 4 || updatedTargetConfig.Kind != "local" || updatedTargetConfig.Port != 2222 || !updatedTargetConfig.InsecureIgnoreHostKey || updatedTargetConfig.Capacity.CPUWeight != 3 || updatedTargetConfig.Capacity.MemoryGB != 16 {
		t.Fatalf("update target result = %+v", updatedTargetConfig)
	}

	clearedTarget := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 31,
		"method": "tools/call",
		"params": {
			"name": "aged_update_target",
			"arguments": {
				"id": "local-small",
				"port": 0,
				"insecureIgnoreHostKey": false,
				"labels": {},
				"capacity": {"memoryGB": 0}
			}
		}
	}`)
	clearTargetContent := clearedTarget["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var clearedTargetConfig core.TargetConfig
	if err := json.Unmarshal([]byte(clearTargetContent["text"].(string)), &clearedTargetConfig); err != nil {
		t.Fatal(err)
	}
	if clearedTargetConfig.Port != 0 || clearedTargetConfig.InsecureIgnoreHostKey || len(clearedTargetConfig.Labels) != 0 || clearedTargetConfig.Capacity.MemoryGB != 0 || clearedTargetConfig.Capacity.MaxWorkers != 4 || clearedTargetConfig.Capacity.CPUWeight != 3 {
		t.Fatalf("cleared target result = %+v", clearedTargetConfig)
	}

	zeroCapacityTarget := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 32,
		"method": "tools/call",
		"params": {
			"name": "aged_update_target",
			"arguments": {
				"id": "local-small",
				"capacity": {"maxWorkers": 0, "cpuWeight": 0}
			}
		}
	}`)
	zeroCapacityTargetContent := zeroCapacityTarget["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var zeroCapacityTargetConfig core.TargetConfig
	if err := json.Unmarshal([]byte(zeroCapacityTargetContent["text"].(string)), &zeroCapacityTargetConfig); err != nil {
		t.Fatal(err)
	}
	if zeroCapacityTargetConfig.Capacity.MaxWorkers != 1 || zeroCapacityTargetConfig.Capacity.CPUWeight != 1 || zeroCapacityTargetConfig.Capacity.MemoryGB != 0 {
		t.Fatalf("zero capacity target result = %+v", zeroCapacityTargetConfig)
	}

	healthResult := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 4,
		"method": "tools/call",
		"params": {"name": "aged_target_health", "arguments": {"targetId": "local-small"}}
	}`)
	healthContent := healthResult["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var targetHealth core.TargetState
	if err := json.Unmarshal([]byte(healthContent["text"].(string)), &targetHealth); err != nil {
		t.Fatal(err)
	}
	if targetHealth.ID != "local-small" || targetHealth.Health.Status != "ok" {
		t.Fatalf("target health result = %+v", targetHealth)
	}

	createdPlugin := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 5,
		"method": "tools/call",
		"params": {
			"name": "aged_create_plugin",
			"arguments": {
				"id": "integration:test",
				"name": "Test Integration",
				"kind": "integration",
				"enabled": false,
				"endpoint": "https://example.invalid/plugin",
				"capabilities": ["inspect"],
				"config": {"env": "test"}
			}
		}
	}`)
	pluginContent := createdPlugin["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var plugin core.Plugin
	if err := json.Unmarshal([]byte(pluginContent["text"].(string)), &plugin); err != nil {
		t.Fatal(err)
	}
	if plugin.ID != "integration:test" || plugin.Config["env"] != "test" {
		t.Fatalf("create plugin result = %+v", plugin)
	}

	listedPlugins := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 6,
		"method": "tools/call",
		"params": {"name": "aged_list_plugins", "arguments": {}}
	}`)
	pluginListContent := listedPlugins["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var plugins []core.Plugin
	if err := json.Unmarshal([]byte(pluginListContent["text"].(string)), &plugins); err != nil {
		t.Fatal(err)
	}
	var foundPlugin bool
	for _, plugin := range plugins {
		if plugin.ID == "integration:test" {
			foundPlugin = true
		}
	}
	if !foundPlugin {
		t.Fatalf("list plugins result = %+v", plugins)
	}

	updatedPlugin := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 7,
		"method": "tools/call",
		"params": {
			"name": "aged_update_plugin",
			"arguments": {
				"id": "integration:test",
				"enabled": true,
				"config": {"env": "updated"}
			}
		}
	}`)
	updatePluginContent := updatedPlugin["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var updatedPluginConfig core.Plugin
	if err := json.Unmarshal([]byte(updatePluginContent["text"].(string)), &updatedPluginConfig); err != nil {
		t.Fatal(err)
	}
	if updatedPluginConfig.Name != "Test Integration" || updatedPluginConfig.Kind != "integration" || !updatedPluginConfig.Enabled || updatedPluginConfig.Endpoint == "" || len(updatedPluginConfig.Capabilities) != 1 || updatedPluginConfig.Capabilities[0] != "inspect" || updatedPluginConfig.Config["env"] != "updated" {
		t.Fatalf("update plugin result = %+v", updatedPluginConfig)
	}

	clearedPlugin := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 71,
		"method": "tools/call",
		"params": {
			"name": "aged_update_plugin",
			"arguments": {
				"id": "integration:test",
				"enabled": false,
				"endpoint": "",
				"capabilities": [],
				"config": {}
			}
		}
	}`)
	clearPluginContent := clearedPlugin["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var clearedPluginConfig core.Plugin
	if err := json.Unmarshal([]byte(clearPluginContent["text"].(string)), &clearedPluginConfig); err != nil {
		t.Fatal(err)
	}
	if clearedPluginConfig.Enabled || clearedPluginConfig.Status != "disabled" || clearedPluginConfig.Endpoint != "" || len(clearedPluginConfig.Capabilities) != 0 || len(clearedPluginConfig.Config) != 0 || clearedPluginConfig.Name != "Test Integration" {
		t.Fatalf("cleared plugin result = %+v", clearedPluginConfig)
	}

	deletedPlugin := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 8,
		"method": "tools/call",
		"params": {"name": "aged_delete_plugin", "arguments": {"pluginId": "integration:test"}}
	}`)
	deletePluginContent := deletedPlugin["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var deletePluginResult map[string]bool
	if err := json.Unmarshal([]byte(deletePluginContent["text"].(string)), &deletePluginResult); err != nil {
		t.Fatal(err)
	}
	if !deletePluginResult["ok"] {
		t.Fatalf("delete plugin result = %+v", deletePluginResult)
	}

	deletedTarget := postMCP(t, server.URL, `{
		"jsonrpc": "2.0",
		"id": 9,
		"method": "tools/call",
		"params": {"name": "aged_delete_target", "arguments": {"targetId": "local-small"}}
	}`)
	deleteTargetContent := deletedTarget["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var deleteTargetResult map[string]bool
	if err := json.Unmarshal([]byte(deleteTargetContent["text"].(string)), &deleteTargetResult); err != nil {
		t.Fatal(err)
	}
	if !deleteTargetResult["ok"] {
		t.Fatalf("delete target result = %+v", deleteTargetResult)
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

	projectDir := t.TempDir()
	body := fmt.Sprintf(`{
		"id": "other",
		"name": "Other",
		"localPath": %q,
		"repo": "owner/other",
		"upstreamRepo": "upstream/other",
		"headRepoOwner": "owner",
		"pushRemote": "fork",
		"vcs": "git",
		"defaultBase": "main"
	}`, projectDir)
	res, err := http.Post(server.URL+"/api/projects", "application/json", strings.NewReader(body))
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
	if created.ID != "other" || created.LocalPath != projectDir || created.UpstreamRepo != "upstream/other" || created.HeadRepoOwner != "owner" || created.PushRemote != "fork" {
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

func TestProjectUpdateDeleteAndHealthEndpoints(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	root := t.TempDir()
	other := t.TempDir()
	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), root)
	if _, err := service.CreateProject(ctx, core.Project{ID: "keep", LocalPath: root}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateProject(ctx, core.Project{ID: "edit", LocalPath: other}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	updateBody := fmt.Sprintf(`{"id":"edit","name":"Edited","localPath":%q,"repo":"owner/edit","defaultBase":"trunk"}`, other)
	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/projects/edit", strings.NewReader(updateBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("update status = %d", res.StatusCode)
	}
	var updated core.Project
	if err := json.NewDecoder(res.Body).Decode(&updated); err != nil {
		t.Fatal(err)
	}
	if updated.Name != "Edited" || updated.DefaultBase != "trunk" {
		t.Fatalf("updated = %+v", updated)
	}

	healthRes, err := http.Get(server.URL + "/api/projects/edit/health")
	if err != nil {
		t.Fatal(err)
	}
	defer healthRes.Body.Close()
	if healthRes.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", healthRes.StatusCode)
	}
	var health core.ProjectHealth
	if err := json.NewDecoder(healthRes.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health.ProjectID != "edit" || health.PathStatus != "ok" {
		t.Fatalf("health = %+v", health)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, server.URL+"/api/projects/edit", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteRes, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteRes.Body.Close()
	if deleteRes.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", deleteRes.StatusCode)
	}
	projects, _, err := store.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 1 || projects[0].ID != "keep" {
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
