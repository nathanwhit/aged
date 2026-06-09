package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"aged/internal/core"
)

var errPullRequestWorkerNotPublishable = errors.New("pull request publishing requires a successful worker with candidate changes")

func resolvePullRequestWorkerID(snapshot core.Snapshot, task core.Task, requestedWorkerID string) (string, error) {
	workerID := strings.TrimSpace(requestedWorkerID)
	if !canPublishPullRequestForTask(task) && workerID == "" {
		return "", fmt.Errorf("provide workerId when publishing before task completion")
	}
	if workerID == "" {
		candidates := applyCandidates(snapshot, task.ID)
		unapplied := unappliedCandidates(candidates)
		switch len(unapplied) {
		case 0:
			if latest := latestAppliedWorker(snapshot, task.ID); latest != "" {
				workerID = latest
			}
		case 1:
			workerID = unapplied[0].WorkerID
		default:
			return "", fmt.Errorf("multiple unapplied worker changes exist; provide workerId")
		}
	}
	if workerID == "" {
		return "", nil
	}
	if !workerBelongsToTask(snapshot, workerID, task.ID) {
		return "", fmt.Errorf("worker does not belong to task")
	}
	if !workerIsPublishableCandidate(snapshot, workerID) {
		return "", errPullRequestWorkerNotPublishable
	}
	return workerID, nil
}

func applyCandidates(snapshot core.Snapshot, taskID string) []ApplyCandidate {
	workers := map[string]core.Worker{}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID {
			workers[worker.ID] = worker
		}
	}
	nodesByWorker := map[string]string{}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.WorkerID != "" {
			nodesByWorker[node.WorkerID] = node.ID
		}
	}
	applied := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerApplied {
			applied[event.WorkerID] = true
		}
	}
	var candidates []ApplyCandidate
	for _, event := range snapshot.Events {
		if event.Type != core.EventWorkerCompleted || event.TaskID != taskID {
			continue
		}
		worker := workers[event.WorkerID]
		if worker.ID == "" {
			continue
		}
		var payload struct {
			Status           core.WorkerStatus      `json:"status"`
			Summary          string                 `json:"summary,omitempty"`
			ChangedFiles     []WorkspaceChangedFile `json:"changedFiles,omitempty"`
			WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		changedFiles := payload.ChangedFiles
		if len(changedFiles) == 0 {
			changedFiles = payload.WorkspaceChanges.ChangedFiles
		}
		changes := payload.WorkspaceChanges
		if len(changes.ChangedFiles) == 0 {
			changes.ChangedFiles = changedFiles
		}
		if payload.Status != core.WorkerSucceeded || !resultHasCandidateChanges(WorkerTurnResult{Changes: changes}) {
			continue
		}
		candidates = append(candidates, ApplyCandidate{
			WorkerID:     event.WorkerID,
			NodeID:       nodesByWorker[event.WorkerID],
			WorkerKind:   worker.Kind,
			Summary:      payload.Summary,
			ChangedFiles: changedFiles,
			Applied:      applied[event.WorkerID],
		})
	}
	return candidates
}

func unappliedCandidates(candidates []ApplyCandidate) []ApplyCandidate {
	var out []ApplyCandidate
	for _, candidate := range candidates {
		if !candidate.Applied {
			out = append(out, candidate)
		}
	}
	return out
}

func latestAppliedWorker(snapshot core.Snapshot, taskID string) string {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type == core.EventWorkerApplied && event.TaskID == taskID {
			return event.WorkerID
		}
	}
	return ""
}

func workerBelongsToTask(snapshot core.Snapshot, workerID string, taskID string) bool {
	for _, worker := range snapshot.Workers {
		if worker.ID == workerID && worker.TaskID == taskID {
			return true
		}
	}
	for _, event := range snapshot.Events {
		if event.WorkerID == workerID && event.TaskID == taskID {
			return true
		}
	}
	return false
}

func workerIsPublishableCandidate(snapshot core.Snapshot, workerID string) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.WorkerID != workerID || event.Type != core.EventWorkerCompleted {
			continue
		}
		var payload struct {
			Status           core.WorkerStatus `json:"status"`
			WorkspaceChanges WorkspaceChanges  `json:"workspaceChanges"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false
		}
		return payload.Status == core.WorkerSucceeded && resultHasCandidateChanges(WorkerTurnResult{Changes: payload.WorkspaceChanges})
	}
	return false
}

func candidateResults(results []WorkerTurnResult) []WorkerTurnResult {
	candidates := []WorkerTurnResult{}
	for _, result := range results {
		if result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
			candidates = append(candidates, result)
		}
	}
	return candidates
}

func latestCandidateWorkerID(results []WorkerTurnResult) string {
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		if result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
			return result.WorkerID
		}
	}
	return ""
}

func resultHasCandidateChanges(result WorkerTurnResult) bool {
	return result.Changes.Dirty ||
		len(result.Changes.ChangedFiles) > 0 ||
		strings.TrimSpace(result.Changes.Diff) != "" ||
		strings.TrimSpace(result.Changes.PublishDiff) != ""
}
