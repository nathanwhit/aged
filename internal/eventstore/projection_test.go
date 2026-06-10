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
