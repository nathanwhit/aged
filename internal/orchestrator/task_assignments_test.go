package orchestrator

import (
	"context"
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
		ExecutionNodes: []core.ExecutionNode{
			{
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
			},
			{
				ID:         "node-debug",
				TaskID:     "task-1",
				WorkerKind: "codex",
				Status:     core.WorkerQueued,
				Role:       "review",
				Reason:     "Waiting for capacity.",
				TargetKind: "ssh",
				TargetID:   "vm-2",
				CreatedAt:  now.Add(10 * time.Minute),
				UpdatedAt:  now.Add(10 * time.Minute),
			},
		},
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
	if len(result.Assignments) != 11 {
		t.Fatalf("assignments = %d, want 11: %+v", len(result.Assignments), result.Assignments)
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

	if len(result.DisplayRows) == 0 {
		t.Fatal("display rows were empty")
	}
	questionRow := displayRowByID(t, result.DisplayRows, "question:question-1")
	if questionRow.Kind != "question" || questionRow.Tone != "warning" || questionRow.Selection == nil || questionRow.Selection.QuestionID != "question-1" {
		t.Fatalf("question display row = %+v", questionRow)
	}
	sessionRow := displayRowByID(t, result.DisplayRows, "session:worker-1")
	if sessionRow.Kind != "session" || sessionRow.Title != "Implementation" || sessionRow.Owner != "Worker worker-1" || len(sessionRow.Actions) != 2 {
		t.Fatalf("session display row = %+v", sessionRow)
	}
	prRow := displayRowByID(t, result.DisplayRows, "pr:pr-1")
	if prRow.Title != "Implement task" || prRow.Subtitle == prRow.Title || len(prRow.Actions) != 3 || prRow.Selection == nil || prRow.Selection.PullRequestID != "pr-1" {
		t.Fatalf("pull request display row = %+v", prRow)
	}
	nodeRow := displayRowByID(t, result.DisplayRows, "execution_node:node-debug")
	if nodeRow.Kind != "debug" || nodeRow.Title != "Review" || nodeRow.ProjectContext != "Ssh vm-2" {
		t.Fatalf("execution node display row = %+v", nodeRow)
	}
	orphanRow := displayRowByID(t, result.DisplayRows, "debug_worker:worker-orphan")
	if orphanRow.Kind != "debug" || orphanRow.Title != "debug" || len(orphanRow.Actions) != 1 || orphanRow.Actions[0].Kind != "cancel-worker" {
		t.Fatalf("orphan display row = %+v", orphanRow)
	}
}

func TestBuildTaskAssignmentsProjectsTaskLifecycleDisplayRows(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		task          core.Task
		wantID        string
		wantTitle     string
		wantTone      string
		wantActionLen int
	}{
		{
			name:   "failed",
			task:   core.Task{ID: "task-1", Status: core.TaskFailed, Error: "build failed", ObjectivePhase: "objective.", CreatedAt: now, UpdatedAt: now.Add(time.Minute)},
			wantID: "task_failure:task-1", wantTitle: "Task failure", wantTone: "danger", wantActionLen: 2,
		},
		{
			name:   "succeeded",
			task:   core.Task{ID: "task-1", Status: core.TaskSucceeded, CreatedAt: now, UpdatedAt: now.Add(time.Minute)},
			wantID: "task_complete:task-1", wantTitle: "Task finished", wantTone: "good", wantActionLen: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := BuildTaskAssignments(core.Snapshot{Tasks: []core.Task{tc.task}}, "task-1")
			if err != nil {
				t.Fatal(err)
			}
			row := displayRowByID(t, result.DisplayRows, tc.wantID)
			if row.Title != tc.wantTitle || row.Tone != tc.wantTone || len(row.Actions) != tc.wantActionLen {
				t.Fatalf("lifecycle row = %+v", row)
			}
			if row.Subtitle == "" {
				t.Fatalf("lifecycle row subtitle was empty: %+v", row)
			}
		})
	}
}

func TestBuildTaskAssignmentsMissingTask(t *testing.T) {
	_, err := BuildTaskAssignments(core.Snapshot{}, "missing")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestTaskAssignmentsUsesStoreScopedSnapshot(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	store := &taskAssignmentsScopedSnapshotStore{
		snapshot: core.Snapshot{
			Tasks: []core.Task{{
				ID:        "task-1",
				Status:    core.TaskRunning,
				CreatedAt: now,
				UpdatedAt: now,
			}},
			WorkItems: []core.WorkItem{{
				ID:        "work-1",
				TaskID:    "task-1",
				Kind:      "objective.implement",
				Status:    core.WorkItemQueued,
				CreatedAt: now,
				UpdatedAt: now,
			}},
		},
	}
	service := NewService(store, StaticBrain{}, nil, t.TempDir())

	result, err := service.TaskAssignments(context.Background(), "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if store.taskAssignmentsCalls != 1 || store.taskAssignmentsTaskID != "task-1" {
		t.Fatalf("task assignment snapshot calls = %d taskID = %q", store.taskAssignmentsCalls, store.taskAssignmentsTaskID)
	}
	if store.snapshotCalls != 0 || store.snapshotSummaryCalls != 0 {
		t.Fatalf("global snapshot calls = Snapshot:%d SnapshotSummary:%d", store.snapshotCalls, store.snapshotSummaryCalls)
	}
	if result.TaskID != "task-1" || len(result.Assignments) != 1 || result.Assignments[0].SourceID != "work-1" {
		t.Fatalf("assignments = %+v", result)
	}
}

type taskAssignmentsScopedSnapshotStore struct {
	eventstore.Store
	snapshot              core.Snapshot
	taskAssignmentsCalls  int
	taskAssignmentsTaskID string
	snapshotCalls         int
	snapshotSummaryCalls  int
}

func (s *taskAssignmentsScopedSnapshotStore) TaskAssignmentsSnapshot(ctx context.Context, taskID string) (core.Snapshot, error) {
	s.taskAssignmentsCalls++
	s.taskAssignmentsTaskID = taskID
	return s.snapshot, nil
}

func (s *taskAssignmentsScopedSnapshotStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	s.snapshotCalls++
	return core.Snapshot{}, errors.New("TaskAssignments must not call global Snapshot")
}

func (s *taskAssignmentsScopedSnapshotStore) SnapshotSummary(ctx context.Context) (core.Snapshot, error) {
	s.snapshotSummaryCalls++
	return core.Snapshot{}, errors.New("TaskAssignments must not call global SnapshotSummary")
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

func displayRowByID(t *testing.T, rows []core.TaskAssignmentDisplayRow, id string) core.TaskAssignmentDisplayRow {
	t.Helper()
	for _, row := range rows {
		if row.ID == id {
			return row
		}
	}
	t.Fatalf("missing display row %s in %+v", id, rows)
	return core.TaskAssignmentDisplayRow{}
}
