package eventstore

import (
	"context"
	"path/filepath"
	"testing"

	"aged/internal/core"
)

func TestSnapshotReplaysMoreThanDefaultEventPage(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	taskID := "task-1"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Long task",
			"prompt":   "Generate enough events to cross the default page size.",
			"metadata": map[string]any{},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 205; i++ {
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: "worker-1",
			Payload: core.MustJSON(map[string]any{
				"stream": "stdout",
				"text":   "progress",
			}),
		}); err != nil {
			t.Fatal(err)
		}
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

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 207 {
		t.Fatalf("events = %d, want 207", len(snapshot.Events))
	}
	if len(snapshot.Tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(snapshot.Tasks))
	}
	if snapshot.Tasks[0].Status != core.TaskSucceeded {
		t.Fatalf("task status = %q, want %q", snapshot.Tasks[0].Status, core.TaskSucceeded)
	}
}

func TestSnapshotProjectsTaskObjectiveMilestonesAndArtifacts(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	taskID := "task-objective"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Resolve issue",
			"prompt": "Open a PR and babysit it.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskArtifact,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":   "pr-1",
			"kind": "github_pull_request",
			"name": "owner/repo#12",
			"url":  "https://github.com/owner/repo/pull/12",
			"ref":  "codex/aged-test",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskMilestone,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"name":    "pr_opened",
			"phase":   "pr_opened",
			"summary": "Pull request opened.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskObjective,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.ObjectiveWaitingExternal,
			"phase":  "pr_opened",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := snapshot.Tasks[0]
	if task.Status != core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveWaitingExternal || task.ObjectivePhase != "pr_opened" {
		t.Fatalf("task state = status %q objective %q phase %q", task.Status, task.ObjectiveStatus, task.ObjectivePhase)
	}
	if len(task.Milestones) != 1 || task.Milestones[0].Name != "pr_opened" {
		t.Fatalf("milestones = %+v", task.Milestones)
	}
	if len(task.Artifacts) != 1 || task.Artifacts[0].ID != "pr-1" {
		t.Fatalf("artifacts = %+v", task.Artifacts)
	}
}

func TestSnapshotProjectsExecutionNodes(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-0",
			"workerId":      "worker-1",
			"workerKind":    "codex",
			"planId":        "plan-1",
			"spawnId":       "implementation",
			"role":          "implementer",
			"reason":        "Implement the change.",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/worker-1",
			"remoteWorkDir": "/repo",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-1",
		WorkerID: "worker-2",
		Payload: core.MustJSON(map[string]any{
			"nodeId":       "node-1",
			"workerId":     "worker-2",
			"workerKind":   "claude",
			"planId":       "plan-1",
			"spawnId":      "review",
			"role":         "reviewer",
			"reason":       "Review the implementation.",
			"targetId":     "vm-1",
			"targetKind":   "ssh",
			"dependsOn":    []string{"implementation"},
			"parentNodeId": "node-0",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload:  core.MustJSON(map[string]any{}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.ExecutionNodes) != 2 {
		t.Fatalf("execution nodes = %d, want 2", len(snapshot.ExecutionNodes))
	}
	node := snapshot.ExecutionNodes[0]
	if node.ID != "node-0" || node.Status != core.WorkerRunning || node.Role != "implementer" || node.TargetID != "vm-1" || node.RemoteSession != "aged-worker" {
		t.Fatalf("node = %+v", node)
	}
	if len(snapshot.OrchestrationGraphs) != 1 {
		t.Fatalf("graphs = %d, want 1", len(snapshot.OrchestrationGraphs))
	}
	graph := snapshot.OrchestrationGraphs[0]
	if graph.TaskID != "task-1" || graph.Summary.Total != 2 || graph.Summary.Running != 1 {
		t.Fatalf("graph = %+v", graph)
	}
	if len(graph.Edges) != 2 {
		t.Fatalf("graph edges = %+v, want parent and dependency edges", graph.Edges)
	}
}

func TestProjectsPersistInSQLite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "aged.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	project := core.Project{
		ID:            "aged",
		Name:          "aged",
		LocalPath:     "/tmp/aged",
		Repo:          "owner/aged",
		UpstreamRepo:  "upstream/aged",
		HeadRepoOwner: "owner",
		PushRemote:    "fork",
		VCS:           "jj",
		DefaultBase:   "main",
		WorkspaceRoot: ".aged/workspaces",
		TargetLabels:  map[string]string{"pool": "local"},
	}
	if _, err := store.SaveProject(ctx, project, true); err != nil {
		t.Fatal(err)
	}
	store.Close()

	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	projects, defaultID, err := reopened.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if defaultID != "aged" {
		t.Fatalf("default project = %q, want aged", defaultID)
	}
	if len(projects) != 1 {
		t.Fatalf("projects = %d, want 1", len(projects))
	}
	if projects[0].Repo != "owner/aged" || projects[0].UpstreamRepo != "upstream/aged" || projects[0].HeadRepoOwner != "owner" || projects[0].PushRemote != "fork" || projects[0].TargetLabels["pool"] != "local" {
		t.Fatalf("project = %+v", projects[0])
	}
}

func TestSnapshotHidesClearedTasksAndKeepsEvents(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "aged.db"))
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
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   "worker-1",
			"workerKind": "mock",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   "task-1",
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "mock",
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
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskCleared,
		TaskID:  "task-1",
		Payload: core.MustJSON(map[string]any{"reason": "test"}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 0 {
		t.Fatalf("tasks = %d, want 0", len(snapshot.Tasks))
	}
	if len(snapshot.Workers) != 0 {
		t.Fatalf("workers = %d, want 0", len(snapshot.Workers))
	}
	if len(snapshot.ExecutionNodes) != 0 {
		t.Fatalf("execution nodes = %d, want 0", len(snapshot.ExecutionNodes))
	}
	if len(snapshot.Events) != 5 {
		t.Fatalf("events = %d, want 5", len(snapshot.Events))
	}
}
