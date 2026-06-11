package eventstore

import (
	"testing"
	"time"

	"aged/internal/core"
)

func TestBuildManagerSummariesSelectsLatestActionByTimestamp(t *testing.T) {
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	older := base.Add(time.Minute)
	newer := base.Add(2 * time.Minute)
	tasks := map[string]core.Task{
		"task": {
			ID:        "task",
			Status:    core.TaskRunning,
			CreatedAt: base,
			UpdatedAt: base,
		},
	}
	sessions := map[string]core.Session{
		"newer": {
			ID:                 "newer",
			TaskID:             "task",
			WorkerID:           "newer",
			Status:             core.WorkerRunning,
			CurrentAction:      "newer session action",
			CurrentActionLabel: "tool",
			CurrentActionAt:    &newer,
			UpdatedAt:          newer,
		},
		"older": {
			ID:                 "older",
			TaskID:             "task",
			WorkerID:           "older",
			Status:             core.WorkerRunning,
			CurrentAction:      "older session action",
			CurrentActionLabel: "log",
			CurrentActionAt:    &older,
			UpdatedAt:          older,
		},
	}

	summaries := buildManagerSummaries(tasks, nil, nil, nil, nil, sessions, nil, nil, nil)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %+v, want one summary", summaries)
	}
	if summaries[0].LatestAction != "newer session action" || !summaries[0].LatestActionAt.Equal(newer) || summaries[0].LatestActionLabel != "tool" {
		t.Fatalf("latest action = %+v, want newer session action", summaries[0])
	}
}

func TestBuildManagerSummariesSelectsLatestWorkItemFallbackByTimestamp(t *testing.T) {
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	sessionAt := base.Add(time.Minute)
	olderWorkItemAt := base.Add(-time.Minute)
	newerWorkItemAt := base.Add(2 * time.Minute)
	tasks := map[string]core.Task{
		"task": {
			ID:        "task",
			Status:    core.TaskRunning,
			CreatedAt: base,
			UpdatedAt: base,
		},
	}
	sessions := map[string]core.Session{
		"session": {
			ID:                 "session",
			TaskID:             "task",
			WorkerID:           "session",
			Status:             core.WorkerRunning,
			CurrentAction:      "session action",
			CurrentActionLabel: "tool",
			CurrentActionAt:    &sessionAt,
			UpdatedAt:          sessionAt,
		},
	}
	workItems := map[string]core.WorkItem{
		"older": {
			ID:        "older",
			TaskID:    "task",
			Status:    core.WorkItemFailed,
			Error:     "older work item error",
			UpdatedAt: olderWorkItemAt,
		},
		"newer": {
			ID:        "newer",
			TaskID:    "task",
			Status:    core.WorkItemFailed,
			Error:     "newer work item error",
			UpdatedAt: newerWorkItemAt,
		},
	}

	summaries := buildManagerSummaries(tasks, nil, workItems, nil, nil, sessions, nil, nil, nil)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %+v, want one summary", summaries)
	}
	if summaries[0].LatestAction != "newer work item error" || !summaries[0].LatestActionAt.Equal(newerWorkItemAt) || summaries[0].LatestActionLabel != "Work item" {
		t.Fatalf("latest action = %+v, want newer work item error", summaries[0])
	}
}

func TestBuildManagerSummariesCountsOnlyCurrentSignals(t *testing.T) {
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	tasks := map[string]core.Task{
		"task": {
			ID:        "task",
			Status:    core.TaskRunning,
			CreatedAt: base,
			UpdatedAt: base,
		},
	}
	sessions := map[string]core.Session{
		"running-session": {
			ID:        "running-session",
			TaskID:    "task",
			WorkerID:  "running-session-worker",
			Status:    core.WorkerRunning,
			UpdatedAt: base.Add(time.Minute),
		},
		"failed-session": {
			ID:        "failed-session",
			TaskID:    "task",
			WorkerID:  "failed-session-worker",
			Status:    core.WorkerFailed,
			UpdatedAt: base.Add(2 * time.Minute),
		},
	}
	workers := map[string]core.Worker{
		"queued-worker": {
			ID:        "queued-worker",
			TaskID:    "task",
			Status:    core.WorkerQueued,
			UpdatedAt: base.Add(3 * time.Minute),
		},
		"failed-worker": {
			ID:        "failed-worker",
			TaskID:    "task",
			Status:    core.WorkerFailed,
			UpdatedAt: base.Add(4 * time.Minute),
		},
	}
	workItems := map[string]core.WorkItem{
		"queued-work": {
			ID:        "queued-work",
			TaskID:    "task",
			Status:    core.WorkItemQueued,
			UpdatedAt: base.Add(5 * time.Minute),
		},
		"running-work": {
			ID:        "running-work",
			TaskID:    "task",
			Status:    core.WorkItemRunning,
			UpdatedAt: base.Add(6 * time.Minute),
		},
		"failed-work": {
			ID:        "failed-work",
			TaskID:    "task",
			Status:    core.WorkItemFailed,
			Error:     "old failed child attempt",
			UpdatedAt: base.Add(7 * time.Minute),
		},
	}
	pullRequests := map[string]core.PullRequest{
		"open-pr": {
			ID:        "open-pr",
			TaskID:    "task",
			State:     "OPEN",
			UpdatedAt: base.Add(8 * time.Minute),
		},
		"merged-pr": {
			ID:        "merged-pr",
			TaskID:    "task",
			State:     "MERGED",
			UpdatedAt: base.Add(9 * time.Minute),
		},
	}
	feedback := map[string]core.PullRequestFeedback{
		"open-feedback": {
			ID:            "open-feedback",
			TaskID:        "task",
			PullRequestID: "open-pr",
			Status:        "pending",
			UpdatedAt:     base.Add(10 * time.Minute),
		},
		"merged-feedback": {
			ID:            "merged-feedback",
			TaskID:        "task",
			PullRequestID: "merged-pr",
			Status:        "pending",
			UpdatedAt:     base.Add(11 * time.Minute),
		},
	}
	questions := map[string]core.Question{
		"question": {
			ID:        "question",
			TaskID:    "task",
			Decided:   false,
			UpdatedAt: base.Add(12 * time.Minute),
		},
	}
	artifacts := map[string]core.Artifact{
		"artifact": {
			ID:        "artifact",
			TaskID:    "task",
			Kind:      "benchmark",
			UpdatedAt: base.Add(13 * time.Minute),
		},
		"worker-log": {
			ID:        "worker-log",
			TaskID:    "task",
			Kind:      "worker_log",
			Name:      "Remote stdout",
			UpdatedAt: base.Add(14 * time.Minute),
		},
		"pr-artifact": {
			ID:        "open-pr",
			TaskID:    "task",
			Kind:      "github_pull_request",
			Name:      "Open PR",
			UpdatedAt: base.Add(15 * time.Minute),
		},
	}
	steering := map[string]core.SteeringItem{
		"steering": {
			ID:        "steering",
			TaskID:    "task",
			Status:    "pending",
			UpdatedAt: base.Add(16 * time.Minute),
		},
	}

	summaries := buildManagerSummaries(tasks, workers, workItems, artifacts, questions, sessions, pullRequests, feedback, steering)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %+v, want one summary", summaries)
	}
	summary := summaries[0]
	if summary.ActiveSignals != 8 {
		t.Fatalf("active signals = %d, want 8: %+v", summary.ActiveSignals, summary)
	}
	if summary.AttentionCount != 3 {
		t.Fatalf("attention count = %d, want 3: %+v", summary.AttentionCount, summary)
	}
	if summary.ActiveSessions != 1 || summary.ActiveWorkers != 1 || summary.ActiveWorkItems != 2 || summary.PullRequests != 1 || summary.Artifacts != 1 {
		t.Fatalf("summary counts = %+v, want active session/worker/work/pr/artifact counts 1/1/2/1/1", summary)
	}
	if summary.LatestAction != "old failed child attempt" {
		t.Fatalf("latest action = %q, want failed child attempt retained for diagnostics", summary.LatestAction)
	}
}
