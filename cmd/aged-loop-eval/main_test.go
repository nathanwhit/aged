package main

import (
	"testing"
	"time"

	"aged/internal/core"
)

func TestCollectMetricsFlagsStaleRunningWorkers(t *testing.T) {
	ended := time.Date(2026, 5, 7, 5, 0, 0, 0, time.UTC)
	snapshot := core.Snapshot{
		Workers: []core.Worker{
			{
				ID:        "fresh",
				TaskID:    "task-1",
				Status:    core.WorkerRunning,
				UpdatedAt: ended.Add(-2 * time.Minute),
			},
			{
				ID:        "stale",
				TaskID:    "task-1",
				Status:    core.WorkerRunning,
				UpdatedAt: ended.Add(-20 * time.Minute),
			},
			{
				ID:        "other-task",
				TaskID:    "task-2",
				Status:    core.WorkerRunning,
				UpdatedAt: ended.Add(-30 * time.Minute),
			},
		},
	}

	metrics := collectMetrics("task-1", snapshot, nil, ended, nil, 15*time.Minute)
	if metrics.RunningWorkers != 2 {
		t.Fatalf("running workers = %d, want 2", metrics.RunningWorkers)
	}
	if metrics.StaleRunningWorkers != 1 {
		t.Fatalf("stale running workers = %d, want 1", metrics.StaleRunningWorkers)
	}
	if metrics.SecondsSinceOldestRunningWorker == nil || *metrics.SecondsSinceOldestRunningWorker != 1200 {
		t.Fatalf("oldest running worker age = %v, want 1200", metrics.SecondsSinceOldestRunningWorker)
	}
	check := checkByName(scoreChecks(evalResult{Metrics: metrics}), "no_stale_running_workers")
	if check.Status != "fail" {
		t.Fatalf("stale worker check = %+v, want fail", check)
	}
}

func TestCollectMetricsCanDisableStaleRunningWorkerCheck(t *testing.T) {
	ended := time.Date(2026, 5, 7, 5, 0, 0, 0, time.UTC)
	snapshot := core.Snapshot{
		Workers: []core.Worker{{
			ID:        "stale",
			TaskID:    "task-1",
			Status:    core.WorkerRunning,
			UpdatedAt: ended.Add(-20 * time.Minute),
		}},
	}

	metrics := collectMetrics("task-1", snapshot, nil, ended, nil, 0)
	if metrics.RunningWorkers != 1 {
		t.Fatalf("running workers = %d, want 1", metrics.RunningWorkers)
	}
	if metrics.StaleRunningWorkers != 0 {
		t.Fatalf("stale running workers = %d, want 0", metrics.StaleRunningWorkers)
	}
	check := checkByName(scoreChecks(evalResult{Metrics: metrics}), "no_stale_running_workers")
	if check.Status != "pass" {
		t.Fatalf("stale worker check = %+v, want pass", check)
	}
}

func checkByName(checks []evalCheck, name string) evalCheck {
	for _, check := range checks {
		if check.Name == name {
			return check
		}
	}
	return evalCheck{}
}
