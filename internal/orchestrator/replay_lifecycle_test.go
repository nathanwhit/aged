package orchestrator

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
)

func TestReplayLongRunningTaskLifecycleReconstructsTaskDetail(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "aged.db")
	store, err := eventstore.OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	taskID := "task-long-running"
	events := []core.Event{
		{
			At:     base,
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"projectId": "project-1",
				"title":     "Stabilize durable loop",
				"prompt":    "Run a long-lived implementation, publish a PR, and wait for checks.",
				"metadata": map[string]any{
					"completionMode": "github",
				},
			}),
		},
		{
			At:     base.Add(time.Second),
			Type:   core.EventTaskPlanned,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"objective": "implement and publish",
				"phase":     "planning",
			}),
		},
		{
			At:     base.Add(2 * time.Second),
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskRunning,
			}),
		},
		{
			At:       base.Add(3 * time.Second),
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: "worker-impl",
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-impl",
				"workerId":   "worker-impl",
				"workerKind": "codex",
				"planId":     "plan-1",
				"spawnId":    "implementation",
				"role":       "implementer",
				"reason":     "Implement the requested change.",
				"targetId":   "vm-1",
				"targetKind": "exe",
			}),
		},
		{
			At:       base.Add(4 * time.Second),
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: "worker-impl",
			Payload: core.MustJSON(map[string]any{
				"kind":    "codex",
				"command": []string{"codex", "exec", "-"},
				"prompt":  "Implement the durable loop fix.",
			}),
		},
		{
			At:       base.Add(5 * time.Second),
			Type:     core.EventWorkerStarted,
			TaskID:   taskID,
			WorkerID: "worker-impl",
			Payload:  core.MustJSON(map[string]any{}),
		},
		{
			At:       base.Add(6 * time.Second),
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: "worker-impl",
			Payload: core.MustJSON(map[string]any{
				"status":       core.WorkerSucceeded,
				"changedFiles": []map[string]any{{"path": "internal/orchestrator/service.go", "status": "modified"}},
			}),
		},
		{
			At:     base.Add(7 * time.Second),
			Type:   core.EventTaskSteered,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"message": "Address check feedback before publishing.",
			}),
		},
		{
			At:     base.Add(8 * time.Second),
			Type:   core.EventTaskObjective,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.ObjectiveActive,
				"phase":  "replan_after_feedback",
			}),
		},
		{
			At:       base.Add(9 * time.Second),
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: "worker-repair",
			Payload: core.MustJSON(map[string]any{
				"nodeId":       "node-repair",
				"workerId":     "worker-repair",
				"workerKind":   "codex",
				"planId":       "plan-2",
				"parentNodeId": "node-impl",
				"spawnId":      "repair",
				"role":         "repairer",
				"reason":       "Incorporate follow-up steering.",
				"dependsOn":    []string{"implementation"},
				"targetId":     "vm-1",
				"targetKind":   "exe",
			}),
		},
		{
			At:       base.Add(10 * time.Second),
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: "worker-repair",
			Payload: core.MustJSON(map[string]any{
				"kind":    "codex",
				"command": []string{"codex", "exec", "-"},
				"prompt":  "Apply the follow-up steering.",
			}),
		},
		{
			At:       base.Add(11 * time.Second),
			Type:     core.EventWorkerStarted,
			TaskID:   taskID,
			WorkerID: "worker-repair",
			Payload:  core.MustJSON(map[string]any{}),
		},
		{
			At:       base.Add(12 * time.Second),
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: "worker-repair",
			Payload: core.MustJSON(map[string]any{
				"status":       core.WorkerSucceeded,
				"changedFiles": []map[string]any{{"path": "internal/orchestrator/task_detail.go", "status": "modified"}},
			}),
		},
		{
			At:     base.Add(13 * time.Second),
			Type:   core.EventTaskCandidate,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"workerId": "worker-repair",
			}),
		},
		{
			At:     base.Add(14 * time.Second),
			Type:   core.EventTaskMilestone,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"name":    "candidate_selected",
				"phase":   "review_complete",
				"summary": "Selected the repaired worker output.",
			}),
		},
		{
			At:     base.Add(15 * time.Second),
			Type:   core.EventTaskArtifact,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":   "artifact-pr-42",
				"kind": "github_pull_request",
				"name": "owner/repo#42",
				"url":  "https://github.com/owner/repo/pull/42",
				"ref":  "codex/durable-loop",
			}),
		},
		{
			At:     base.Add(16 * time.Second),
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":     "pr-42",
				"repo":   "owner/repo",
				"number": 42,
				"url":    "https://github.com/owner/repo/pull/42",
				"branch": "codex/durable-loop",
				"base":   "main",
				"title":  "Stabilize durable loop",
				"state":  "OPEN",
				"draft":  true,
				"metadata": map[string]any{
					"workerId": "worker-repair",
				},
			}),
		},
		{
			At:     base.Add(17 * time.Second),
			Type:   core.EventPRStatusChecked,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":               "pr-42",
				"state":            "OPEN",
				"checksStatus":     "passing",
				"checksConclusion": "success",
				"mergeStatus":      "CLEAN",
				"mergeable":        "MERGEABLE",
				"reviewStatus":     "APPROVED",
			}),
		},
		{
			At:     base.Add(18 * time.Second),
			Type:   core.EventTaskObjective,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.ObjectiveWaitingExternal,
				"phase":  "pr_opened",
			}),
		},
		{
			At:     base.Add(19 * time.Second),
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
	}
	for _, event := range events {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	deleteSnapshotProjection(t, ctx, dbPath)

	replayedStore, err := eventstore.OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer replayedStore.Close()
	snapshot, err := replayedStore.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := BuildTaskDetail(snapshot, taskID, 50)
	if err != nil {
		t.Fatal(err)
	}

	if detail.Task.Status != core.TaskWaiting || detail.Task.ObjectiveStatus != core.ObjectiveWaitingExternal || detail.Task.ObjectivePhase != "pr_opened" {
		t.Fatalf("task state = status %q objective %q phase %q", detail.Task.Status, detail.Task.ObjectiveStatus, detail.Task.ObjectivePhase)
	}
	if detail.Task.FinalCandidateWorkerID != "worker-repair" {
		t.Fatalf("final candidate = %q, want worker-repair", detail.Task.FinalCandidateWorkerID)
	}
	if len(detail.Task.Milestones) != 1 || detail.Task.Milestones[0].Name != "candidate_selected" {
		t.Fatalf("milestones = %+v", detail.Task.Milestones)
	}
	if len(detail.Task.Artifacts) != 1 || detail.Task.Artifacts[0].ID != "artifact-pr-42" || detail.Task.Artifacts[0].Ref != "codex/durable-loop" {
		t.Fatalf("artifacts = %+v", detail.Task.Artifacts)
	}
	if len(detail.Workers) != 2 {
		t.Fatalf("workers = %d, want 2", len(detail.Workers))
	}
	repair := detailWorkerByID(t, detail.Workers, "worker-repair")
	if repair.Worker.Status != core.WorkerSucceeded {
		t.Fatalf("repair worker status = %q", repair.Worker.Status)
	}
	if repair.ExecutionNode == nil || repair.ExecutionNode.ID != "node-repair" || repair.ExecutionNode.ParentNodeID != "node-impl" || repair.ExecutionNode.Status != core.WorkerSucceeded {
		t.Fatalf("repair execution node = %+v", repair.ExecutionNode)
	}
	if len(repair.ChangedFiles) != 1 || repair.ChangedFiles[0].Path != "internal/orchestrator/task_detail.go" {
		t.Fatalf("repair changed files = %+v", repair.ChangedFiles)
	}
	if len(detail.ExecutionNodes) != 2 {
		t.Fatalf("execution nodes = %+v", detail.ExecutionNodes)
	}
	if detail.OrchestrationGraph == nil || detail.OrchestrationGraph.Summary.Total != 2 || detail.OrchestrationGraph.Summary.Done != 2 {
		t.Fatalf("orchestration graph = %+v", detail.OrchestrationGraph)
	}
	if len(detail.OrchestrationGraph.Edges) != 2 {
		t.Fatalf("graph edges = %+v, want parent and dependency edges", detail.OrchestrationGraph.Edges)
	}
	if len(detail.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", detail.PullRequests)
	}
	pr := detail.PullRequests[0]
	if pr.ID != "pr-42" || pr.ChecksStatus != "passing" || pr.ChecksConclusion != "success" || pr.MergeStatus != "CLEAN" || pr.Mergeable != "MERGEABLE" || pr.ReviewStatus != "APPROVED" {
		t.Fatalf("pull request = %+v", pr)
	}
	if got := detail.RecentEvents[len(detail.RecentEvents)-1].Type; got != core.EventTaskStatus {
		t.Fatalf("last recent event = %q, want %q", got, core.EventTaskStatus)
	}
}

func deleteSnapshotProjection(t *testing.T, ctx context.Context, dbPath string) {
	t.Helper()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `DELETE FROM snapshot_projection`); err != nil {
		t.Fatal(err)
	}
}

func detailWorkerByID(t *testing.T, workers []TaskDetailWorker, workerID string) TaskDetailWorker {
	t.Helper()

	for _, worker := range workers {
		if worker.Worker.ID == workerID {
			return worker
		}
	}
	t.Fatalf("missing worker %q in %+v", workerID, workers)
	return TaskDetailWorker{}
}
