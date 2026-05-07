package main

import (
	"testing"
	"time"

	"aged/internal/core"
)

func TestCollectMetricsFlagsStaleRunningWorker(t *testing.T) {
	ended := time.Date(2026, 5, 7, 5, 0, 0, 0, time.UTC)
	metrics := collectMetrics("task-1", core.Snapshot{
		Workers: []core.Worker{
			{
				ID:        "worker-stale",
				TaskID:    "task-1",
				Status:    core.WorkerRunning,
				CreatedAt: ended.Add(-30 * time.Minute),
				UpdatedAt: ended.Add(-20 * time.Minute),
			},
			{
				ID:        "worker-recent",
				TaskID:    "task-1",
				Status:    core.WorkerRunning,
				CreatedAt: ended.Add(-30 * time.Minute),
				UpdatedAt: ended.Add(-2 * time.Minute),
			},
			{
				ID:        "worker-other-task",
				TaskID:    "task-2",
				Status:    core.WorkerRunning,
				UpdatedAt: ended.Add(-30 * time.Minute),
			},
			{
				ID:        "worker-done",
				TaskID:    "task-1",
				Status:    core.WorkerSucceeded,
				UpdatedAt: ended.Add(-30 * time.Minute),
			},
		},
	}, nil, ended, nil, 10*time.Minute)

	if metrics.StaleRunningWorkers != 1 {
		t.Fatalf("stale workers = %d, want 1", metrics.StaleRunningWorkers)
	}
	if metrics.MaxRunningWorkerSilenceSeconds == nil || *metrics.MaxRunningWorkerSilenceSeconds != (20*time.Minute).Seconds() {
		t.Fatalf("max silence = %v, want 1200s", metrics.MaxRunningWorkerSilenceSeconds)
	}
}

func TestScoreChecksFailsOnStaleRunningWorker(t *testing.T) {
	checks := scoreChecks(evalResult{
		StaleWorkerAfterSec: (10 * time.Minute).Seconds(),
		Metrics: evalMetrics{
			StaleRunningWorkers: 1,
		},
	})

	for _, check := range checks {
		if check.Name == "no_stale_running_workers" {
			if check.Status != "fail" {
				t.Fatalf("stale worker check status = %q, want fail", check.Status)
			}
			return
		}
	}
	t.Fatal("missing no_stale_running_workers check")
}
