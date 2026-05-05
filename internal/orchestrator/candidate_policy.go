package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"

	"aged/internal/core"
)

func resolvePullRequestWorkerID(snapshot core.Snapshot, task core.Task, requestedWorkerID string) (string, error) {
	workerID := strings.TrimSpace(requestedWorkerID)
	if !canPublishPullRequestForTask(task) && workerID == "" {
		return "", fmt.Errorf("provide workerId when publishing before task completion")
	}
	if workerID == "" {
		if task.FinalCandidateWorkerID != "" {
			workerID = task.FinalCandidateWorkerID
		} else {
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
	}
	if workerID == "" {
		return "", nil
	}
	if !workerBelongsToTask(snapshot, workerID, task.ID) {
		return "", fmt.Errorf("worker does not belong to task")
	}
	if !workerIsPublishableCandidate(snapshot, workerID) {
		return "", fmt.Errorf("pull request publishing requires a successful worker with candidate changes")
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
		if payload.Status != core.WorkerSucceeded || len(changedFiles) == 0 {
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

func resolveFinalCandidate(results []WorkerTurnResult, selectedWorkerID string) (string, string, error) {
	candidates := candidateResults(results)
	selectedWorkerID = strings.TrimSpace(selectedWorkerID)
	if selectedWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == selectedWorkerID {
				return selectedWorkerID, "orchestrator selected final candidate explicitly", nil
			}
		}
		if ancestorID := selectedCandidateAncestor(results, selectedWorkerID); ancestorID != "" {
			return ancestorID, fmt.Sprintf("orchestrator selected worker %s; using nearest changed candidate ancestor", selectedWorkerID), nil
		}
		if fallbackID, fallbackReason, err := resolveFinalCandidate(results, ""); err == nil && fallbackID != "" {
			return fallbackID, fmt.Sprintf("selected final candidate %q was not applyable; fallback selected %s", selectedWorkerID, fallbackReason), nil
		}
		return "", "", fmt.Errorf("selected final candidate %q is not a successful worker with candidate changes", selectedWorkerID)
	}
	switch len(candidates) {
	case 0:
		return "", "", nil
	case 1:
		return candidates[0].WorkerID, "only successful worker with candidate changes", nil
	}
	leaves := candidateLeaves(candidates)
	if len(leaves) == 1 {
		return leaves[0].WorkerID, "only remaining candidate leaf after dependency lineage", nil
	}
	ids := make([]string, 0, len(leaves))
	for _, leaf := range leaves {
		ids = append(ids, leaf.WorkerID)
	}
	return "", "", fmt.Errorf("multiple competing final candidates remain (%s); the orchestrator must select finalCandidateWorkerId or schedule a consolidation/validation worker", strings.Join(ids, ", "))
}

func selectedCandidateAncestor(results []WorkerTurnResult, selectedWorkerID string) string {
	byID := map[string]WorkerTurnResult{}
	for _, result := range results {
		byID[result.WorkerID] = result
	}
	current, ok := byID[selectedWorkerID]
	if !ok || current.Status != core.WorkerSucceeded {
		return ""
	}
	seen := map[string]bool{selectedWorkerID: true}
	for strings.TrimSpace(current.BaseWorkerID) != "" {
		parentID := current.BaseWorkerID
		if seen[parentID] {
			return ""
		}
		seen[parentID] = true
		parent, ok := byID[parentID]
		if !ok {
			return ""
		}
		if parent.Status == core.WorkerSucceeded && resultHasCandidateChanges(parent) {
			return parent.WorkerID
		}
		current = parent
	}
	return ""
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

func candidateLeaves(candidates []WorkerTurnResult) []WorkerTurnResult {
	candidateIDs := map[string]bool{}
	for _, candidate := range candidates {
		candidateIDs[candidate.WorkerID] = true
	}
	hasCandidateChild := map[string]bool{}
	for _, candidate := range candidates {
		if candidateIDs[candidate.BaseWorkerID] {
			hasCandidateChild[candidate.BaseWorkerID] = true
		}
	}
	leaves := []WorkerTurnResult{}
	for _, candidate := range candidates {
		if !hasCandidateChild[candidate.WorkerID] {
			leaves = append(leaves, candidate)
		}
	}
	return leaves
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

func latestCandidateLeaf(results []WorkerTurnResult) (string, string) {
	return latestCandidateLeafExcluding(results, nil)
}

func latestCandidateLeafExcluding(results []WorkerTurnResult, blocked map[string]string) (string, string) {
	leaves := candidateLeaves(candidateResults(results))
	if len(leaves) == 0 {
		return "", ""
	}
	for i := len(results) - 1; i >= 0; i-- {
		for _, leaf := range leaves {
			if _, isBlocked := blocked[leaf.WorkerID]; isBlocked {
				continue
			}
			if results[i].WorkerID == leaf.WorkerID {
				return leaf.WorkerID, "selected latest successful candidate leaf after ambiguous deterministic fallback"
			}
		}
	}
	return "", ""
}

func resultHasCandidateChanges(result WorkerTurnResult) bool {
	return result.Changes.Dirty || len(result.Changes.ChangedFiles) > 0 || strings.TrimSpace(result.Changes.Diff) != ""
}
