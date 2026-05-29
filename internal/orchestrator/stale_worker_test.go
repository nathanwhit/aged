package orchestrator

import (
	"testing"
	"time"

	"aged/internal/core"
)

func TestEvaluateWorkerStaleness(t *testing.T) {
	base := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	threshold := 5 * time.Minute

	cases := []struct {
		name        string
		worker      core.Worker
		latestEvent *core.Event
		now         time.Time
		threshold   time.Duration
		wantNil     bool
		wantStale   bool
		wantSilence float64
	}{
		{
			name: "running worker silent past threshold is stale",
			worker: core.Worker{
				ID:        "w1",
				Status:    core.WorkerRunning,
				CreatedAt: base.Add(-30 * time.Minute),
				UpdatedAt: base.Add(-10 * time.Minute),
			},
			now:         base,
			threshold:   threshold,
			wantStale:   true,
			wantSilence: (10 * time.Minute).Seconds(),
		},
		{
			name: "running worker with recent event is fresh",
			worker: core.Worker{
				ID:        "w2",
				Status:    core.WorkerRunning,
				CreatedAt: base.Add(-30 * time.Minute),
				UpdatedAt: base.Add(-10 * time.Minute),
			},
			latestEvent: &core.Event{At: base.Add(-1 * time.Minute), Type: core.EventWorkerOutput},
			now:         base,
			threshold:   threshold,
			wantStale:   false,
			wantSilence: (1 * time.Minute).Seconds(),
		},
		{
			name: "terminal workers are not projected",
			worker: core.Worker{
				ID:        "w3",
				Status:    core.WorkerSucceeded,
				CreatedAt: base.Add(-30 * time.Minute),
				UpdatedAt: base.Add(-30 * time.Minute),
			},
			now:       base,
			threshold: threshold,
			wantNil:   true,
		},
		{
			name:      "non-positive threshold disables projection",
			worker:    core.Worker{ID: "w4", Status: core.WorkerRunning, UpdatedAt: base.Add(-time.Hour)},
			now:       base,
			threshold: 0,
			wantNil:   true,
		},
		{
			name: "missing timestamps yield no projection",
			worker: core.Worker{
				ID:     "w5",
				Status: core.WorkerQueued,
			},
			now:       base,
			threshold: threshold,
			wantNil:   true,
		},
		{
			name: "queued worker just created is not stale",
			worker: core.Worker{
				ID:        "w6",
				Status:    core.WorkerQueued,
				CreatedAt: base.Add(-30 * time.Second),
			},
			now:         base,
			threshold:   threshold,
			wantStale:   false,
			wantSilence: (30 * time.Second).Seconds(),
		},
		{
			name: "negative silence clamped to zero",
			worker: core.Worker{
				ID:        "w7",
				Status:    core.WorkerRunning,
				CreatedAt: base,
				UpdatedAt: base.Add(time.Minute),
			},
			now:         base,
			threshold:   threshold,
			wantStale:   false,
			wantSilence: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateWorkerStaleness(tc.worker, tc.latestEvent, tc.now, tc.threshold)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("expected nil staleness, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected staleness projection, got nil")
			}
			if got.Stale != tc.wantStale {
				t.Errorf("Stale = %v, want %v", got.Stale, tc.wantStale)
			}
			if got.SilenceSeconds != tc.wantSilence {
				t.Errorf("SilenceSeconds = %v, want %v", got.SilenceSeconds, tc.wantSilence)
			}
			if got.ThresholdSeconds != tc.threshold.Seconds() {
				t.Errorf("ThresholdSeconds = %v, want %v", got.ThresholdSeconds, tc.threshold.Seconds())
			}
		})
	}
}

func TestBuildTaskDetailAtPopulatesStaleness(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	threshold := 5 * time.Minute
	taskID := "task-1"
	staleWorker := core.Worker{
		ID:        "w-stale",
		TaskID:    taskID,
		Kind:      "mock",
		Status:    core.WorkerRunning,
		CreatedAt: now.Add(-30 * time.Minute),
		UpdatedAt: now.Add(-20 * time.Minute),
	}
	freshWorker := core.Worker{
		ID:        "w-fresh",
		TaskID:    taskID,
		Kind:      "mock",
		Status:    core.WorkerRunning,
		CreatedAt: now.Add(-30 * time.Minute),
		UpdatedAt: now.Add(-30 * time.Minute),
	}
	doneWorker := core.Worker{
		ID:        "w-done",
		TaskID:    taskID,
		Kind:      "mock",
		Status:    core.WorkerSucceeded,
		CreatedAt: now.Add(-1 * time.Hour),
		UpdatedAt: now.Add(-45 * time.Minute),
	}
	snapshot := core.Snapshot{
		Tasks: []core.Task{{
			ID:        taskID,
			Title:     "test",
			Status:    core.TaskRunning,
			CreatedAt: now.Add(-1 * time.Hour),
			UpdatedAt: now.Add(-30 * time.Minute),
		}},
		Workers: []core.Worker{staleWorker, freshWorker, doneWorker},
		Events: []core.Event{
			{ID: 1, At: now.Add(-1 * time.Minute), Type: core.EventWorkerOutput, TaskID: taskID, WorkerID: freshWorker.ID},
		},
	}

	detail, err := BuildTaskDetailAt(snapshot, taskID, 10, now, threshold)
	if err != nil {
		t.Fatalf("BuildTaskDetailAt: %v", err)
	}
	if len(detail.Workers) != 3 {
		t.Fatalf("expected 3 projected workers, got %d", len(detail.Workers))
	}

	byID := map[string]TaskDetailWorker{}
	for _, w := range detail.Workers {
		byID[w.Worker.ID] = w
	}

	if s := byID[staleWorker.ID].Staleness; s == nil || !s.Stale {
		t.Errorf("expected stale worker projection to be marked stale, got %+v", s)
	}
	if s := byID[freshWorker.ID].Staleness; s == nil || s.Stale {
		t.Errorf("expected fresh worker projection to be present and not stale, got %+v", s)
	}
	if s := byID[doneWorker.ID].Staleness; s != nil {
		t.Errorf("expected terminal worker to have no staleness projection, got %+v", s)
	}
}

func TestBuildTaskDetailAtThresholdZeroDisablesProjection(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	taskID := "task-1"
	snapshot := core.Snapshot{
		Tasks: []core.Task{{ID: taskID, Title: "t", Status: core.TaskRunning, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}},
		Workers: []core.Worker{{
			ID: "w", TaskID: taskID, Kind: "mock", Status: core.WorkerRunning,
			CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
		}},
	}
	detail, err := BuildTaskDetailAt(snapshot, taskID, 10, now, 0)
	if err != nil {
		t.Fatalf("BuildTaskDetailAt: %v", err)
	}
	if detail.Workers[0].Staleness != nil {
		t.Errorf("expected nil staleness when threshold disabled, got %+v", detail.Workers[0].Staleness)
	}
}
