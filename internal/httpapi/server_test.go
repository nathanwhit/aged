package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

type fakeMCPRemoteExecutor struct{}

func (fakeMCPRemoteExecutor) Run(_ context.Context, argv []string) (string, error) {
	if strings.Contains(strings.Join(argv, " "), "repoPresent=") {
		return "checkoutRootOK=true\ntmux=true\nrepoPresent=true\ncpuCount=4\nload1=0.1\n", nil
	}
	return "", nil
}

type httpAPITestHarness struct {
	store   eventstore.Store
	service *orchestrator.Service
	server  *httptest.Server
}

func newHTTPAPITestHarness(t *testing.T) httpAPITestHarness {
	t.Helper()
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	t.Cleanup(server.Close)
	return httpAPITestHarness{store: store, service: service, server: server}
}

func TestCreateTaskRejectsUserWorkerSelection(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

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

func hasHTTPEvent(events []core.Event, eventType core.EventType, taskID string, workerID string) bool {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && event.WorkerID == workerID {
			return true
		}
	}
	return false
}

func TestCreateTaskAcceptsOnlyUserWorkRequest(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

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
	h := newHTTPAPITestHarness(t)
	server := h.server

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

func TestSnapshotTaskCardsKeepTerminalRowsWithoutTerminalDetails(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: "done", Payload: core.MustJSON(map[string]any{
			"title":  "Done",
			"prompt": "large prompt",
			"metadata": map[string]any{
				"objectiveMode": "broad",
				"loopPrompt":    strings.Repeat("x", 2048),
			},
		})},
		{Type: core.EventWorkerCreated, TaskID: "done", WorkerID: "done-worker", Payload: core.MustJSON(map[string]any{"kind": "mock"})},
		{Type: core.EventWorkItemQueued, TaskID: "done", Payload: core.MustJSON(map[string]any{"id": "done-work", "kind": "objective.implement"})},
		{Type: core.EventTaskArtifact, TaskID: "done", Payload: core.MustJSON(map[string]any{"id": "done-artifact", "kind": "benchmark", "name": "Done benchmark", "ref": "shared/done", "metadata": map[string]any{"content": strings.Repeat("a", 2048), "workerId": "done-worker"}})},
		{Type: core.EventApprovalNeeded, TaskID: "done", WorkerID: "done-worker", Payload: core.MustJSON(map[string]any{"reason": "done_question", "question": "Done question?"})},
		{Type: core.EventTaskSteered, TaskID: "done", Payload: core.MustJSON(map[string]any{"message": "Done steering."})},
		{Type: core.EventPRPublished, TaskID: "done", Payload: core.MustJSON(map[string]any{"id": "repo#1", "repo": "owner/repo", "number": 1, "url": "https://github.com/owner/repo/pull/1", "branch": "done-pr", "state": "OPEN", "metadata": map[string]any{"latestPullRequestFeedbackSignature": "done-sig", "latestPullRequestFeedbackBody": "Done feedback."}})},
		{Type: core.EventPRFollowUp, TaskID: "done", Payload: core.MustJSON(map[string]any{"id": "repo#1", "repo": "owner/repo", "number": 1, "feedbackSignature": "done-sig", "prompt": "Handle done feedback."})},
		{Type: core.EventTaskStatus, TaskID: "done", Payload: core.MustJSON(map[string]any{"status": core.TaskSucceeded})},
		{Type: core.EventTaskCreated, TaskID: "active", Payload: core.MustJSON(map[string]any{
			"title":  "Active",
			"prompt": "active prompt",
			"metadata": map[string]any{
				"executionMode": "loop",
				"loopPrompt":    strings.Repeat("y", 2048),
			},
		})},
		{Type: core.EventWorkerCreated, TaskID: "active", WorkerID: "active-worker", Payload: core.MustJSON(map[string]any{"kind": "mock", "prompt": strings.Repeat("z", 2048), "metadata": map[string]any{"large": strings.Repeat("m", 2048)}})},
		{Type: core.EventWorkItemQueued, TaskID: "active", Payload: core.MustJSON(map[string]any{"id": "active-work", "kind": "objective.implement"})},
		{Type: core.EventTaskArtifact, TaskID: "active", Payload: core.MustJSON(map[string]any{"id": "active-artifact", "kind": "benchmark", "name": "Active benchmark", "ref": "shared/active", "metadata": map[string]any{"content": strings.Repeat("b", 2048), "workerId": "active-worker"}})},
		{Type: core.EventApprovalNeeded, TaskID: "active", WorkerID: "active-worker", Payload: core.MustJSON(map[string]any{"reason": "active_question", "question": "Active question?"})},
		{Type: core.EventTaskSteered, TaskID: "active", Payload: core.MustJSON(map[string]any{"message": "Active steering."})},
		{Type: core.EventPRPublished, TaskID: "active", Payload: core.MustJSON(map[string]any{"id": "repo#2", "repo": "owner/repo", "number": 2, "url": "https://github.com/owner/repo/pull/2", "branch": "active-pr", "state": "OPEN", "metadata": map[string]any{"latestPullRequestFeedbackSignature": "active-sig", "latestPullRequestFeedbackBody": "Active feedback."}})},
		{Type: core.EventPRFollowUp, TaskID: "active", Payload: core.MustJSON(map[string]any{"id": "repo#2", "repo": "owner/repo", "number": 2, "feedbackSignature": "active-sig", "prompt": "Handle active feedback."})},
		{Type: core.EventTaskStatus, TaskID: "active", Payload: core.MustJSON(map[string]any{"status": core.TaskRunning})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Get(server.URL + "/api/snapshot?events=none&tasks=cards")
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
	if len(snapshot.Tasks) != 2 {
		t.Fatalf("tasks = %+v, want active and terminal task cards", snapshot.Tasks)
	}
	if taskByID(snapshot.Tasks, "done").Prompt != "" {
		t.Fatalf("terminal task prompt = %q, want stripped card payload", taskByID(snapshot.Tasks, "done").Prompt)
	}
	if taskByID(snapshot.Tasks, "active").Prompt != "" {
		t.Fatalf("active task prompt = %q, want stripped card payload", taskByID(snapshot.Tasks, "active").Prompt)
	}
	if strings.Contains(string(taskByID(snapshot.Tasks, "done").Metadata), "loopPrompt") {
		t.Fatalf("task card metadata kept large loopPrompt: %s", taskByID(snapshot.Tasks, "done").Metadata)
	}
	if !strings.Contains(string(taskByID(snapshot.Tasks, "done").Metadata), "objectiveMode") {
		t.Fatalf("task card metadata dropped card metadata: %s", taskByID(snapshot.Tasks, "done").Metadata)
	}
	if len(snapshot.Workers) != 1 || snapshot.Workers[0].TaskID != "active" {
		t.Fatalf("workers = %+v, want only active task workers", snapshot.Workers)
	}
	if snapshot.Workers[0].Prompt != "" || len(snapshot.Workers[0].Metadata) != 0 {
		t.Fatalf("worker card kept detail payload: %+v", snapshot.Workers[0])
	}
	if len(snapshot.PullRequestFeedback) != 1 || snapshot.PullRequestFeedback[0].TaskID != "active" || snapshot.PullRequestFeedback[0].FeedbackBody != "" {
		t.Fatalf("card feedback = %+v, want compact active feedback only", snapshot.PullRequestFeedback)
	}
	if len(snapshot.Artifacts) != 1 || snapshot.Artifacts[0].TaskID != "active" || snapshot.Artifacts[0].ID != "active-artifact" {
		t.Fatalf("card artifacts = %+v, want compact active artifacts only", snapshot.Artifacts)
	}
	if strings.Contains(string(snapshot.Artifacts[0].Metadata), "content") || !strings.Contains(string(snapshot.Artifacts[0].Metadata), "workerId") {
		t.Fatalf("card artifact metadata = %s, want compact metadata", snapshot.Artifacts[0].Metadata)
	}
	if len(snapshot.Steering) != 1 || snapshot.Steering[0].TaskID != "active" || snapshot.Steering[0].Message != "Active steering." {
		t.Fatalf("card steering = %+v, want active steering only", snapshot.Steering)
	}
	activeSummary := managerSummaryByTask(snapshot.ManagerSummary, "active")
	if activeSummary.TaskID == "" || activeSummary.PendingApprovals != 1 || activeSummary.PendingFeedback != 1 || activeSummary.AttentionCount < 2 {
		t.Fatalf("active manager summary = %+v, want pending approval and feedback attention", activeSummary)
	}
	doneSummary := managerSummaryByTask(snapshot.ManagerSummary, "done")
	if doneSummary.TaskID == "" || doneSummary.AttentionCount != 0 || doneSummary.PendingApprovals != 0 || doneSummary.PendingFeedback != 0 || doneSummary.Artifacts != 0 {
		t.Fatalf("terminal manager summary = %+v, want non-actionable compact summary without terminal detail payloads", doneSummary)
	}

	taskRes, err := http.Get(server.URL + "/api/tasks/done")
	if err != nil {
		t.Fatal(err)
	}
	defer taskRes.Body.Close()
	if taskRes.StatusCode != http.StatusOK {
		t.Fatalf("task status = %d", taskRes.StatusCode)
	}
	var taskSnapshot core.Snapshot
	if err := json.NewDecoder(taskRes.Body).Decode(&taskSnapshot); err != nil {
		t.Fatal(err)
	}
	if len(taskSnapshot.Tasks) != 1 || taskSnapshot.Tasks[0].Prompt != "large prompt" {
		t.Fatalf("task snapshot tasks = %+v", taskSnapshot.Tasks)
	}
	if len(taskSnapshot.Workers) != 1 || taskSnapshot.Workers[0].ID != "done-worker" {
		t.Fatalf("task snapshot workers = %+v", taskSnapshot.Workers)
	}
	if len(taskSnapshot.WorkItems) != 1 || taskSnapshot.WorkItems[0].ID != "done-work" {
		t.Fatalf("task snapshot work items = %+v", taskSnapshot.WorkItems)
	}
	if len(taskSnapshot.Artifacts) != 1 || taskSnapshot.Artifacts[0].ID != "done-artifact" || taskSnapshot.Artifacts[0].TaskID != "done" {
		t.Fatalf("task snapshot artifacts = %+v", taskSnapshot.Artifacts)
	}
	if len(taskSnapshot.Questions) != 1 || taskSnapshot.Questions[0].Question != "Done question?" {
		t.Fatalf("task snapshot questions = %+v", taskSnapshot.Questions)
	}
	if len(taskSnapshot.Sessions) != 1 || taskSnapshot.Sessions[0].WorkerID != "done-worker" {
		t.Fatalf("task snapshot sessions = %+v", taskSnapshot.Sessions)
	}
	if len(taskSnapshot.PullRequestFeedback) != 1 || taskSnapshot.PullRequestFeedback[0].TaskID != "done" || taskSnapshot.PullRequestFeedback[0].FeedbackBody != "Done feedback." {
		t.Fatalf("task snapshot feedback = %+v", taskSnapshot.PullRequestFeedback)
	}
	if len(taskSnapshot.Steering) != 1 || taskSnapshot.Steering[0].TaskID != "done" || taskSnapshot.Steering[0].Message != "Done steering." {
		t.Fatalf("task snapshot steering = %+v", taskSnapshot.Steering)
	}
	if len(taskSnapshot.ManagerSummary) != 1 || taskSnapshot.ManagerSummary[0].TaskID != "done" || taskSnapshot.ManagerSummary[0].PendingFeedback != 1 || taskSnapshot.ManagerSummary[0].PendingApprovals != 1 {
		t.Fatalf("task scoped manager summary = %+v, want only done summary", taskSnapshot.ManagerSummary)
	}
}

func TestTaskAssignmentsEndpoint(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"title": "Assignments", "prompt": "Track work"})},
		{Type: core.EventWorkItemQueued, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{
			"id":         "queued-work",
			"kind":       "objective.validate",
			"targetKind": "objective",
			"targetId":   "task-assignments",
			"reason":     "Validate the result.",
			"metadata": map[string]any{
				"workerKind": "codex",
				"dependsOn":  []string{"implementation"},
			},
		})},
		{Type: core.EventExecutionPlanned, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   "worker-1",
			"workerKind": "codex",
			"role":       "implementation",
			"targetKind": "ssh",
			"targetId":   "vm-1",
			"dependsOn":  []string{"plan"},
		})},
		{Type: core.EventWorkerCreated, TaskID: "task-assignments", WorkerID: "worker-1", Payload: core.MustJSON(map[string]any{"kind": "codex"})},
		{Type: core.EventWorkItemQueued, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"id": "running-work", "kind": "objective.implement"})},
		{Type: core.EventWorkItemStarted, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"id": "running-work", "workerId": "worker-1"})},
		{Type: core.EventWorkerStarted, TaskID: "task-assignments", WorkerID: "worker-1", Payload: core.MustJSON(map[string]any{})},
		{Type: core.EventWorkerOutput, TaskID: "task-assignments", WorkerID: "worker-1", Payload: core.MustJSON(map[string]any{"kind": "tool", "text": "go test ./..."})},
		{Type: core.EventTaskArtifact, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"id": "artifact-1", "kind": "benchmark", "name": "Benchmark", "ref": "shared/bench.txt", "metadata": map[string]any{"workerId": "worker-1", "pullRequestID": "pr-1"}})},
		{Type: core.EventApprovalNeeded, TaskID: "task-assignments", WorkerID: "worker-1", Payload: core.MustJSON(map[string]any{"reason": "approval", "question": "Continue?"})},
		{Type: core.EventPRPublished, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"id": "pr-1", "repo": "owner/repo", "number": 7, "url": "https://github.com/owner/repo/pull/7", "title": "Assignments", "state": "OPEN", "metadata": map[string]any{"workerId": "worker-1", "latestPullRequestFeedbackSignature": "sig-1", "latestPullRequestFeedbackBody": "Please add tests."}})},
		{Type: core.EventPRFollowUp, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"id": "pr-1", "feedbackSignature": "sig-1", "reason": "review", "prompt": "Handle feedback."})},
		{Type: core.EventTaskSteered, TaskID: "task-assignments", Payload: core.MustJSON(map[string]any{"message": "Use the existing parser."})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Get(server.URL + "/api/tasks/task-assignments/assignments")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var result core.TaskAssignmentsResponse
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.TaskID != "task-assignments" {
		t.Fatalf("task id = %q", result.TaskID)
	}
	for _, sourceKind := range []string{"session", "work_item", "pull_request", "pull_request_feedback", "question", "artifact", "steering", "execution_node"} {
		if !hasHTTPAssignmentSourceKind(result.Assignments, sourceKind) {
			t.Fatalf("missing %s assignment in %+v", sourceKind, result.Assignments)
		}
	}
	running := httpAssignmentBySource(t, result.Assignments, "work_item", "running-work")
	if running.Status != string(core.WorkItemRunning) || running.WorkerID != "worker-1" || running.NodeID != "node-1" || running.SessionID != "worker-1" || running.CurrentAction == "" {
		t.Fatalf("running work assignment = %+v", running)
	}
	artifact := httpAssignmentBySource(t, result.Assignments, "artifact", "artifact-1")
	if artifact.TargetKind != "pull_request" || artifact.TargetID != "pr-1" || artifact.WorkerID != "worker-1" {
		t.Fatalf("artifact assignment = %+v", artifact)
	}

	missing, err := http.Get(server.URL + "/api/tasks/missing/assignments")
	if err != nil {
		t.Fatal(err)
	}
	defer missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d", missing.StatusCode)
	}
}

func TestEventStreamUsesLastEventIDAndWritesSSEID(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: "task-a", Payload: core.MustJSON(map[string]any{"title": "Task A", "prompt": "First"})},
		{Type: core.EventTaskStatus, TaskID: "task-a", Payload: core.MustJSON(map[string]any{"status": core.TaskRunning})},
		{Type: core.EventTaskStatus, TaskID: "task-a", Payload: core.MustJSON(map[string]any{"status": core.TaskSucceeded})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, server.URL+"/api/events/stream?after=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Last-Event-ID", "2")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}

	frame, err := readSSEFrame(bufio.NewReader(res.Body))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(frame, "id: 3\n") {
		t.Fatalf("frame missing id 3:\n%s", frame)
	}
	if strings.Contains(frame, "id: 2\n") {
		t.Fatalf("frame replayed stale after cursor:\n%s", frame)
	}
	if !strings.Contains(frame, "event: event\n") {
		t.Fatalf("frame missing event name:\n%s", frame)
	}
}

func readSSEFrame(reader *bufio.Reader) (string, error) {
	var frame strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return frame.String(), err
		}
		frame.WriteString(line)
		if line == "\n" {
			return frame.String(), nil
		}
	}
}

func TestTaskEventsEndpointLimitsTotalHistory(t *testing.T) {
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
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Type != core.EventWorkerCompleted {
		t.Fatalf("event type = %q, want worker.completed; events = %+v", events[0].Type, events)
	}
}

func TestSessionTailEndpointReturnsWorkerScopedEvents(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	taskID := "task-session-tail"
	workerID := "worker-session-tail"
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Tail", "prompt": "Tail session"})}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock"})}); err != nil {
		t.Fatal(err)
	}
	started, err := store.Append(ctx, core.Event{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: "other-worker", Payload: core.MustJSON(map[string]any{"text": "ignore"})}); err != nil {
		t.Fatal(err)
	}
	output, err := store.Append(ctx, core.Event{Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"text": "session output"})})
	if err != nil {
		t.Fatal(err)
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Get(fmt.Sprintf("%s/api/sessions/%s/tail?after=%d", server.URL, workerID, started.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var tail core.SessionTail
	if err := json.NewDecoder(res.Body).Decode(&tail); err != nil {
		t.Fatal(err)
	}
	if tail.SessionID != workerID || tail.WorkerID != workerID || tail.TaskID != taskID {
		t.Fatalf("tail identity = %+v", tail)
	}
	if tail.LastEventID != output.ID {
		t.Fatalf("lastEventId = %d, want %d", tail.LastEventID, output.ID)
	}
	if len(tail.Events) != 1 || tail.Events[0].ID != output.ID {
		t.Fatalf("events = %+v, want output %d", tail.Events, output.ID)
	}
	if tail.CurrentAction == nil || !strings.Contains(tail.CurrentAction.Text, "session output") {
		t.Fatalf("current action = %+v", tail.CurrentAction)
	}
}

func TestSessionControlEndpointsDelegateToWorker(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	taskID := "task-session-control"
	workerID := "worker-session-control"
	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: taskID, Payload: core.MustJSON(map[string]any{"title": "Control", "prompt": "Control session"})},
		{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(orchestrator.Plan{WorkerKind: "mock", Prompt: "work"})},
		{Type: core.EventExecutionPlanned, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"nodeId": "node-session-control", "workerId": workerID, "workerKind": "mock"})},
		{Type: core.EventWorkerCreated, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{"kind": "mock"})},
		{Type: core.EventWorkerStarted, TaskID: taskID, WorkerID: workerID, Payload: core.MustJSON(map[string]any{})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	steerRes, err := http.Post(server.URL+"/api/sessions/"+workerID+"/steer", "application/json", strings.NewReader(`{"message":"focus this session"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer steerRes.Body.Close()
	if steerRes.StatusCode != http.StatusNoContent {
		t.Fatalf("steer status = %d", steerRes.StatusCode)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasHTTPEvent(snapshot.Events, core.EventWorkerSteered, taskID, workerID) {
		t.Fatalf("missing worker steering event: %+v", snapshot.Events)
	}

	cancelRes, err := http.Post(server.URL+"/api/sessions/"+workerID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelRes.Body.Close()
	if cancelRes.StatusCode != http.StatusNoContent {
		t.Fatalf("cancel status = %d", cancelRes.StatusCode)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasHTTPEvent(snapshot.Events, core.EventWorkerCompleted, taskID, workerID) {
		t.Fatalf("missing worker completed event: %+v", snapshot.Events)
	}
}

func TestMCPEndpointInitializesAndListsTools(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

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
	h := newHTTPAPITestHarness(t)
	server := h.server

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
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if _, ok := metadata["completionMode"]; ok {
		t.Fatalf("metadata = %+v", metadata)
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

func TestMCPPullRequestResourceURIEscapesID(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	prID := "github:owner/repo#7"
	for _, event := range []core.Event{
		{Type: core.EventTaskCreated, TaskID: "task-pr", Payload: core.MustJSON(map[string]any{"title": "PR task", "prompt": "Track PR"})},
		{Type: core.EventPRPublished, TaskID: "task-pr", Payload: core.MustJSON(map[string]any{
			"id":     prID,
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "aged/task-pr",
			"base":   "main",
			"title":  "Fix URI escaping",
			"state":  "open",
		})},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	resources := postMCP(t, server.URL, `{"jsonrpc":"2.0","id":1,"method":"resources/list"}`)
	list := resources["result"].(map[string]any)["resources"].([]any)
	var prURI string
	for _, item := range list {
		resource := item.(map[string]any)
		if resource["name"] == "pull-request-"+prID {
			prURI = resource["uri"].(string)
		}
	}
	if prURI == "" {
		t.Fatalf("pull request resource missing: %+v", list)
	}

	parsed, err := url.Parse(prURI)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Fragment != "" {
		t.Fatalf("uri fragment = %q, want empty for %q", parsed.Fragment, prURI)
	}
	if parsed.String() != prURI {
		t.Fatalf("uri round trip = %q, want %q", parsed.String(), prURI)
	}

	read := postMCP(t, server.URL, fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 2,
		"method": "resources/read",
		"params": {"uri": %q}
	}`, prURI))
	contents := read["result"].(map[string]any)["contents"].([]any)
	text := contents[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, prID) {
		t.Fatalf("resource text = %s", text)
	}
	if value, ok := findMCPResource(core.Snapshot{PullRequests: []core.PullRequest{{ID: "bad%zz"}}}, "aged://pull-requests/bad%zz"); ok {
		t.Fatalf("malformed escaped pull request URI matched %+v", value)
	}
}

func TestMCPTaskDetailIncludesWorkersEventsAndActions(t *testing.T) {
	ctx := context.Background()
	h := newHTTPAPITestHarness(t)
	store := h.store
	server := h.server

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
	h := newHTTPAPITestHarness(t)
	store := h.store
	service := h.service
	server := h.server

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

func TestUpdatePluginRejectsIDMismatchAsBadRequest(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/plugins/runner:lint", strings.NewReader(`{
		"id": "runner:fmt",
		"name": "Lint Runner",
		"kind": "runner",
		"enabled": true
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["error"] != "plugin id mismatch" {
		t.Fatalf("error = %q", payload["error"])
	}
}

func TestGitHubDriverEndpointHotTogglesConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	service.Drivers().SetGitHubClient(fakeHTTPGitHubClient{})
	if _, err := service.Drivers().StartGitHubDriver(ctx, orchestrator.GitHubDriverConfig{}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/drivers/github", strings.NewReader(`{
		"enabled": true,
		"intervalSeconds": 3600,
		"issueLimit": 5,
		"issues": [{"repo": "owner/repo", "labels": ["aged"], "projectId": "default"}],
		"pullRequests": {"enabled": true, "autoPublish": false, "autoBabysit": false, "draft": true}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var state orchestrator.GitHubDriverRuntimeState
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if !state.Running || !state.Config.Enabled || state.Config.IntervalSeconds != 3600 || state.Config.IssueLimit != 5 {
		t.Fatalf("state = %+v", state)
	}

	res, err = http.Get(server.URL + "/api/drivers/github")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if !state.Running || len(state.Config.Issues) != 1 || state.Config.Issues[0].Repo != "owner/repo" {
		t.Fatalf("get state = %+v", state)
	}
}

func TestDiscordDriverEndpointHotTogglesConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	service.Drivers().SetDiscordClient(fakeHTTPDiscordClient{})
	if _, err := service.Drivers().StartDiscordDriver(ctx, orchestrator.DiscordDriverConfig{}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/drivers/discord", strings.NewReader(`{
		"enabled": true,
		"token": "secret-token",
		"intervalSeconds": 3600,
		"messageLimit": 5,
		"channels": [{"id": "chan", "taskPrefix": "task:"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var state orchestrator.DiscordDriverRuntimeState
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if !state.Running || !state.Config.Enabled || state.Config.IntervalSeconds != 3600 || state.Config.MessageLimit != 5 || state.Config.Token != "" {
		t.Fatalf("state = %+v", state)
	}

	res, err = http.Get(server.URL + "/api/drivers/discord")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if !state.Running || len(state.Config.Channels) != 1 || state.Config.Channels[0].ID != "chan" || state.Config.Token != "" {
		t.Fatalf("get state = %+v", state)
	}
}

type fakeHTTPGitHubClient struct{}

func (fakeHTTPGitHubClient) ListIssues(context.Context, string, []string, int) ([]orchestrator.GitHubIssue, error) {
	return nil, nil
}

func (fakeHTTPGitHubClient) ListMentions(context.Context, orchestrator.GitHubMentionListOptions) ([]orchestrator.GitHubMention, error) {
	return nil, nil
}

type fakeHTTPDiscordClient struct{}

func (fakeHTTPDiscordClient) Me(context.Context) (orchestrator.DiscordUser, error) {
	return orchestrator.DiscordUser{ID: "bot", Bot: true}, nil
}

func (fakeHTTPDiscordClient) ListMessages(context.Context, string, string, int) ([]orchestrator.DiscordMessage, error) {
	return nil, nil
}

func (fakeHTTPDiscordClient) SendMessage(context.Context, string, string) error {
	return nil
}

func TestRegisterTargetEndpointPersistsAndExposesTarget(t *testing.T) {
	ctx := context.Background()
	h := newHTTPAPITestHarness(t)
	store := h.store
	service := h.service
	server := h.server

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

func TestUpdateTargetRejectsIDMismatchAsBadRequest(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/targets/local-ci", strings.NewReader(`{
		"id": "remote-ci",
		"kind": "local"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["error"] != "target id mismatch" {
		t.Fatalf("error = %q", payload["error"])
	}
}

func TestDeleteMissingTargetAndPluginReturnsNotFound(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

	for _, path := range []string{"/api/targets/missing", "/api/plugins/missing"} {
		req, err := http.NewRequest(http.MethodDelete, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("DELETE %s status = %d, want %d", path, res.StatusCode, http.StatusNotFound)
		}
	}
}

func TestMCPDeleteMissingTargetAndPluginReturnsNotFound(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

	tests := []struct {
		name    string
		body    string
		message string
	}{
		{
			name: "target",
			body: `{
				"jsonrpc": "2.0",
				"id": 1,
				"method": "tools/call",
				"params": {"name": "aged_delete_target", "arguments": {"targetId": "definitely-not-a-target"}}
			}`,
			message: "target not found",
		},
		{
			name: "plugin",
			body: `{
				"jsonrpc": "2.0",
				"id": 2,
				"method": "tools/call",
				"params": {"name": "aged_delete_plugin", "arguments": {"pluginId": "definitely-not-a-plugin"}}
			}`,
			message: "plugin not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := postMCP(t, server.URL, tt.body)
			rpcErr, ok := payload["error"].(map[string]any)
			if !ok {
				t.Fatalf("payload error = %+v", payload)
			}
			if rpcErr["code"] != float64(-32004) || rpcErr["message"] != tt.message {
				t.Fatalf("mcp error = %+v", rpcErr)
			}
		})
	}
}

func TestMCPProjectTools(t *testing.T) {
	h := newHTTPAPITestHarness(t)
	server := h.server

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
				"requirements": {"memoryMb": 8192, "storageMb": 50000},
				"pullRequestPolicy": {
					"branchPrefix": "aged/",
					"draft": true,
					"allowMerge": true,
					"autoMerge": true,
					"mergeMethod": "rebase"
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
	if updatedProject.Name != "Node Runtime" || updatedProject.DefaultBase != "trunk" || updatedProject.LocalPath != projectDir || updatedProject.Repo != "nodejs/node" || updatedProject.TargetLabels["role"] != "ci" || updatedProject.Requirements.MemoryMB != 8192 || updatedProject.Requirements.StorageMB != 50_000 || updatedProject.PullRequestPolicy.BranchPrefix != "aged/" || !updatedProject.PullRequestPolicy.Draft || !updatedProject.PullRequestPolicy.AllowMerge || !updatedProject.PullRequestPolicy.AutoMerge || updatedProject.PullRequestPolicy.MergeMethod != "rebase" {
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
				"requirements": {},
				"pullRequestPolicy": {
					"branchPrefix": "",
					"draft": false,
					"allowMerge": false,
					"autoMerge": false,
					"mergeMethod": ""
				}
			}
		}
	}`)
	clearContent := cleared["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
	var clearedProject core.Project
	if err := json.Unmarshal([]byte(clearContent["text"].(string)), &clearedProject); err != nil {
		t.Fatal(err)
	}
	if clearedProject.Repo != "" || clearedProject.DefaultBase != "main" || len(clearedProject.TargetLabels) != 0 || clearedProject.Requirements.MemoryMB != 0 || clearedProject.Requirements.StorageMB != 0 || clearedProject.PullRequestPolicy.BranchPrefix != "codex/aged-" || clearedProject.PullRequestPolicy.Draft || clearedProject.PullRequestPolicy.AllowMerge || clearedProject.PullRequestPolicy.AutoMerge || clearedProject.PullRequestPolicy.MergeMethod != "squash" {
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
	h := newHTTPAPITestHarness(t)
	server := h.server

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

func TestMCPUpdateTargetSyncsSSHCheckoutAliases(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewServiceWithWorkspaceManagerAndTargets(
		store,
		orchestrator.StaticBrain{WorkerKind: "mock"},
		worker.DefaultRunners(),
		t.TempDir(),
		nil,
		orchestrator.NewLocalTargetRegistry(),
		orchestrator.SSHRunner{Executor: fakeMCPRemoteExecutor{}},
	)
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	registerConflictingTarget := func(checkoutRoot, workDir string) {
		t.Helper()
		_, err := service.RegisterTarget(ctx, core.TargetConfig{
			ID:           "ssh-aliases",
			Kind:         "ssh",
			Host:         "example.invalid",
			CheckoutRoot: checkoutRoot,
			WorkDir:      workDir,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	updateTarget := func(arguments string) core.TargetConfig {
		t.Helper()
		result := postMCP(t, server.URL, fmt.Sprintf(`{
			"jsonrpc": "2.0",
			"id": "update-target",
			"method": "tools/call",
			"params": {"name": "aged_update_target", "arguments": %s}
		}`, arguments))
		content := result["result"].(map[string]any)["content"].([]any)[0].(map[string]any)
		var target core.TargetConfig
		if err := json.Unmarshal([]byte(content["text"].(string)), &target); err != nil {
			t.Fatal(err)
		}
		return target
	}

	registerConflictingTarget("/old-checkout", "/old-work")
	target := updateTarget(`{"id": "ssh-aliases", "workDir": " /new-work "}`)
	if target.CheckoutRoot != "/new-work" || target.WorkDir != "/new-work" {
		t.Fatalf("workDir-only update target = %+v", target)
	}

	registerConflictingTarget("/old-checkout", "/old-work")
	target = updateTarget(`{"id": "ssh-aliases", "checkoutRoot": " /new-checkout "}`)
	if target.CheckoutRoot != "/new-checkout" || target.WorkDir != "/new-checkout" {
		t.Fatalf("checkoutRoot-only update target = %+v", target)
	}

	registerConflictingTarget("/old-checkout", "/old-work")
	target = updateTarget(`{"id": "ssh-aliases", "checkoutRoot": " /explicit-checkout ", "workDir": " /explicit-work "}`)
	if target.CheckoutRoot != "/explicit-checkout" || target.WorkDir != "/explicit-work" {
		t.Fatalf("explicit alias update target = %+v", target)
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

func TestRecommendApplyPolicyEndpointReturnsNotFoundForMissingTask(t *testing.T) {
	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	service := orchestrator.NewService(store, orchestrator.StaticBrain{WorkerKind: "missing"}, map[string]worker.Runner{}, t.TempDir())
	server := httptest.NewServer(New(service, nil).Routes())
	defer server.Close()

	res, err := http.Post(server.URL+"/api/tasks/missing-task/apply-policy", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", res.StatusCode, http.StatusNotFound)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range snapshot.Events {
		if event.Type == core.EventApplyPolicy && event.TaskID == "missing-task" {
			t.Fatalf("recorded apply-policy event for missing task")
		}
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
	h := newHTTPAPITestHarness(t)
	store := h.store
	server := h.server

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
		"defaultBase": "main",
		"requirements": {"memoryMb": 16384, "storageMb": 100000},
		"githubIssues": {"enabled": true, "labels": ["aged"], "issueLimit": 5},
		"githubMentions": {"enabled": true, "reasons": ["mention", "review_requested"], "limit": 8},
		"reviewPolicy": {"enabled": true, "beforeCompletionPr": true, "beforeIntermediatePr": false, "blockingSeverities": ["p1"], "reviewerKinds": ["claude"], "promptSetId": "aged-review", "maxAttempts": 3, "instructions": "Check scheduler lifecycle."},
		"pullRequestPolicy": {"mergeMethod": "merge"}
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
	if created.ID != "other" || created.LocalPath != projectDir || created.UpstreamRepo != "upstream/other" || created.HeadRepoOwner != "owner" || created.PushRemote != "fork" || created.Requirements.MemoryMB != 16_384 || created.Requirements.StorageMB != 100_000 || !created.GitHubIssues.Enabled || created.GitHubIssues.IssueLimit != 5 || !created.GitHubMentions.Enabled || created.GitHubMentions.Limit != 8 || !created.ReviewPolicy.Enabled || created.ReviewPolicy.BeforeIntermediatePR || created.ReviewPolicy.PromptSetID != "aged-review" || created.ReviewPolicy.MaxAttempts != 3 || created.PullRequestPolicy.MergeMethod != "merge" {
		t.Fatalf("created = %+v", created)
	}

	projects, _, err := store.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 1 || projects[0].ID != "other" || projects[0].UpstreamRepo != "upstream/other" || projects[0].HeadRepoOwner != "owner" || projects[0].PushRemote != "fork" || projects[0].Requirements.MemoryMB != 16_384 || projects[0].Requirements.StorageMB != 100_000 || !projects[0].GitHubIssues.Enabled || len(projects[0].GitHubIssues.Labels) != 1 || !projects[0].GitHubMentions.Enabled || len(projects[0].GitHubMentions.Reasons) != 2 || !projects[0].ReviewPolicy.Enabled || projects[0].ReviewPolicy.BeforeIntermediatePR || projects[0].ReviewPolicy.PromptSetID != "aged-review" || projects[0].ReviewPolicy.Instructions != "Check scheduler lifecycle." || projects[0].PullRequestPolicy.MergeMethod != "merge" {
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
	h := newHTTPAPITestHarness(t)
	server := h.server

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
	var created core.Task
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(created.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if _, ok := metadata["completionMode"]; ok {
		t.Fatalf("metadata = %+v", metadata)
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

func taskByID(tasks []core.Task, id string) core.Task {
	for _, task := range tasks {
		if task.ID == id {
			return task
		}
	}
	return core.Task{}
}

func managerSummaryByTask(summaries []core.ManagerSummary, taskID string) core.ManagerSummary {
	for _, summary := range summaries {
		if summary.TaskID == taskID {
			return summary
		}
	}
	return core.ManagerSummary{}
}

func hasHTTPAssignmentSourceKind(rows []core.TaskAssignment, sourceKind string) bool {
	for _, row := range rows {
		if row.SourceKind == sourceKind {
			return true
		}
	}
	return false
}

func httpAssignmentBySource(t *testing.T, rows []core.TaskAssignment, sourceKind string, sourceID string) core.TaskAssignment {
	t.Helper()
	for _, row := range rows {
		if row.SourceKind == sourceKind && row.SourceID == sourceID {
			return row
		}
	}
	t.Fatalf("missing assignment %s/%s in %+v", sourceKind, sourceID, rows)
	return core.TaskAssignment{}
}
