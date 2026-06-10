package orchestrator

import (
	"errors"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
)

func TestBuildTaskAssignmentsProjectsTaskScopedRows(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(9 * time.Minute)
	snapshot := core.Snapshot{
		Tasks: []core.Task{{
			ID:        "task-1",
			Title:     "Task",
			Status:    core.TaskRunning,
			CreatedAt: now,
			UpdatedAt: now,
		}},
		Workers: []core.Worker{
			{ID: "worker-1", TaskID: "task-1", Kind: "codex", Status: core.WorkerRunning, CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(6 * time.Minute)},
			{ID: "worker-orphan", TaskID: "task-1", Kind: "debug", Status: core.WorkerQueued, CreatedAt: now.Add(11 * time.Minute), UpdatedAt: now.Add(11 * time.Minute)},
			{ID: "worker-other", TaskID: "task-2", Kind: "codex", Status: core.WorkerQueued, CreatedAt: now, UpdatedAt: now},
		},
		ExecutionNodes: []core.ExecutionNode{{
			ID:           "node-1",
			TaskID:       "task-1",
			WorkerID:     "worker-1",
			WorkerKind:   "codex",
			Status:       core.WorkerRunning,
			Role:         "implementation",
			TargetKind:   "ssh",
			TargetID:     "vm-1",
			ParentNodeID: "node-parent",
			SpawnID:      "impl",
			DependsOn:    []string{"plan"},
			Reason:       "Implement the task.",
			CreatedAt:    now.Add(2 * time.Minute),
			UpdatedAt:    now.Add(6 * time.Minute),
		}},
		Sessions: []core.Session{{
			ID:                 "worker-1",
			TaskID:             "task-1",
			WorkerID:           "worker-1",
			NodeID:             "node-1",
			WorkerKind:         "codex",
			Role:               "implementation",
			Status:             core.WorkerRunning,
			TargetKind:         "ssh",
			TargetID:           "vm-1",
			CurrentAction:      "go test ./...",
			CurrentActionLabel: "tool",
			CreatedAt:          now.Add(2 * time.Minute),
			StartedAt:          timePtr(now.Add(3 * time.Minute)),
			UpdatedAt:          now.Add(7 * time.Minute),
		}},
		WorkItems: []core.WorkItem{
			{
				ID:         "queued",
				TaskID:     "task-1",
				Kind:       "objective.validate",
				Status:     core.WorkItemQueued,
				TargetKind: "objective",
				TargetID:   "task-1",
				Reason:     "Validate result.",
				CreatedAt:  now.Add(30 * time.Second),
				UpdatedAt:  now.Add(30 * time.Second),
				Metadata:   core.MustJSON(map[string]any{"workerKind": "codex", "dependsOn": []string{"implementation"}}),
			},
			{
				ID:        "running",
				TaskID:    "task-1",
				Kind:      "objective.implement",
				Status:    core.WorkItemRunning,
				WorkerID:  "worker-1",
				CreatedAt: now.Add(time.Minute),
				UpdatedAt: now.Add(5 * time.Minute),
			},
		},
		PullRequests: []core.PullRequest{{
			ID:        "pr-1",
			TaskID:    "task-1",
			Repo:      "owner/repo",
			Number:    7,
			URL:       "https://github.com/owner/repo/pull/7",
			Title:     "Implement task",
			State:     "OPEN",
			CreatedAt: now.Add(4 * time.Minute),
			UpdatedAt: now.Add(4 * time.Minute),
			Metadata:  core.MustJSON(map[string]any{"workerId": "worker-1"}),
		}},
		PullRequestFeedback: []core.PullRequestFeedback{{
			ID:                "feedback-1",
			TaskID:            "task-1",
			PullRequestID:     "pr-1",
			Status:            "handled",
			Reason:            "review",
			FeedbackSignature: "sig-1",
			CreatedAt:         now.Add(5 * time.Minute),
			UpdatedAt:         completedAt,
			HandledAt:         &completedAt,
		}},
		Questions: []core.Question{{
			ID:        "question-1",
			TaskID:    "task-1",
			WorkerID:  "worker-1",
			Reason:    "approval",
			Question:  "Continue?",
			CreatedAt: now.Add(6 * time.Minute),
			UpdatedAt: now.Add(6 * time.Minute),
		}},
		Artifacts: []core.Artifact{{
			ID:        "artifact-1",
			TaskID:    "task-1",
			Kind:      "benchmark",
			Name:      "Benchmark",
			Ref:       "shared/bench.txt",
			CreatedAt: now.Add(7 * time.Minute),
			UpdatedAt: now.Add(7 * time.Minute),
			Metadata:  core.MustJSON(map[string]any{"workerId": "worker-1", "workerKind": "codex", "pullRequestID": "pr-1"}),
		}},
		Steering: []core.SteeringItem{{
			ID:         "steer-1",
			TaskID:     "task-1",
			WorkerID:   "worker-1",
			TargetKind: "worker",
			TargetID:   "worker-1",
			Status:     "pending",
			Reason:     "user_feedback",
			Message:    "Use the existing parser.",
			CreatedAt:  now.Add(8 * time.Minute),
			UpdatedAt:  now.Add(8 * time.Minute),
		}},
	}

	result, err := BuildTaskAssignments(snapshot, "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if result.TaskID != "task-1" {
		t.Fatalf("task id = %q", result.TaskID)
	}
	if len(result.Assignments) != 10 {
		t.Fatalf("assignments = %d, want 10: %+v", len(result.Assignments), result.Assignments)
	}

	running := assignmentBySource(t, result.Assignments, "work_item", "running")
	if running.NodeID != "node-1" || running.SessionID != "worker-1" || running.WorkerKind != "codex" || running.CurrentAction != "go test ./..." || running.CurrentActionLabel != "tool" {
		t.Fatalf("running work assignment was not hydrated: %+v", running)
	}
	if running.StartedAt == nil || running.CompletedAt != nil {
		t.Fatalf("running work timing = started %v completed %v", running.StartedAt, running.CompletedAt)
	}

	queued := assignmentBySource(t, result.Assignments, "work_item", "queued")
	if queued.Role != "validate" || queued.WorkerKind != "codex" || len(queued.DependsOn) != 1 || queued.DependsOn[0] != "implementation" {
		t.Fatalf("queued work assignment = %+v", queued)
	}

	artifact := assignmentBySource(t, result.Assignments, "artifact", "artifact-1")
	if artifact.TargetKind != "pull_request" || artifact.TargetID != "pr-1" || artifact.WorkerID != "worker-1" || artifact.NodeID != "node-1" {
		t.Fatalf("artifact assignment = %+v", artifact)
	}

	feedback := assignmentBySource(t, result.Assignments, "pull_request_feedback", "feedback-1")
	if feedback.Status != "handled" || feedback.CompletedAt == nil || feedback.TargetID != "pr-1" {
		t.Fatalf("feedback assignment = %+v", feedback)
	}

	orphan := assignmentBySource(t, result.Assignments, "worker", "worker-orphan")
	if orphan.Kind != "debug" || orphan.WorkerID != "worker-orphan" {
		t.Fatalf("orphan worker assignment = %+v", orphan)
	}
}

func TestBuildTaskAssignmentsMissingTask(t *testing.T) {
	_, err := BuildTaskAssignments(core.Snapshot{}, "missing")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func assignmentBySource(t *testing.T, rows []core.TaskAssignment, sourceKind string, sourceID string) core.TaskAssignment {
	t.Helper()
	for _, row := range rows {
		if row.SourceKind == sourceKind && row.SourceID == sourceID {
			return row
		}
	}
	t.Fatalf("missing assignment %s/%s in %+v", sourceKind, sourceID, rows)
	return core.TaskAssignment{}
}
