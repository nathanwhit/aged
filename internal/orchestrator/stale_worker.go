package orchestrator

import (
	"time"

	"aged/internal/core"
	"aged/internal/envutil"
)

// DefaultStaleWorkerThreshold is how long a nonterminal worker may go without
// any recorded activity before the daemon flags it as stale for visibility.
// This is a *projection* threshold: it does not cancel the worker, only
// surfaces the silence to operators through snapshots, the dashboard, and the
// task-detail API. The eval runner has its own independent
// `-stale-worker-after` flag that turns silence into a scorecard failure.
const DefaultStaleWorkerThreshold = 15 * time.Minute

// StaleWorkerThresholdEnv lets operators override the default daemon-side
// staleness window via environment variable. Setting it to zero (or a
// non-positive duration) disables stale-worker visibility entirely.
const StaleWorkerThresholdEnv = "AGED_STALE_WORKER_AFTER"

// WorkerStaleness is the daemon-side projection of how recently a worker has
// shown signs of life. It is attached to nonterminal workers in task-detail
// projections so operators can spot workers that have gone quiet.
type WorkerStaleness struct {
	LastActivityAt   time.Time `json:"lastActivityAt"`
	SilenceSeconds   float64   `json:"silenceSeconds"`
	ThresholdSeconds float64   `json:"thresholdSeconds"`
	Stale            bool      `json:"stale"`
}

// LoadStaleWorkerThreshold returns the configured staleness window, falling
// back to the default. A non-positive value disables the projection.
func LoadStaleWorkerThreshold() time.Duration {
	return envutil.Duration(StaleWorkerThresholdEnv, DefaultStaleWorkerThreshold)
}

// workerLastActivity returns the most recent timestamp that indicates the
// worker is making progress: explicit worker.UpdatedAt, the latest worker
// event we have on hand, or the worker's creation time as a floor.
func workerLastActivity(worker core.Worker, latestEvent *core.Event) time.Time {
	candidates := []time.Time{worker.UpdatedAt, worker.CreatedAt}
	if latestEvent != nil {
		candidates = append(candidates, latestEvent.At)
	}
	var latest time.Time
	for _, candidate := range candidates {
		if candidate.IsZero() {
			continue
		}
		if candidate.After(latest) {
			latest = candidate
		}
	}
	return latest
}

// EvaluateWorkerStaleness computes a WorkerStaleness projection for a single
// worker. Terminal workers and a non-positive threshold yield a nil result
// because there is nothing meaningful to surface.
func EvaluateWorkerStaleness(worker core.Worker, latestEvent *core.Event, now time.Time, threshold time.Duration) *WorkerStaleness {
	if threshold <= 0 {
		return nil
	}
	if isTerminalWorkerStatus(worker.Status) {
		return nil
	}
	activity := workerLastActivity(worker, latestEvent)
	if activity.IsZero() {
		return nil
	}
	silence := now.Sub(activity)
	if silence < 0 {
		silence = 0
	}
	return &WorkerStaleness{
		LastActivityAt:   activity,
		SilenceSeconds:   silence.Seconds(),
		ThresholdSeconds: threshold.Seconds(),
		Stale:            silence >= threshold,
	}
}
