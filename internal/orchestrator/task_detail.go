package orchestrator

import (
	"context"
	"encoding/json"
	"strings"

	"aged/internal/core"
	"aged/internal/eventstore"
)

const defaultTaskDetailEventLimit = 30

type TaskDetail struct {
	Task               core.Task                 `json:"task"`
	Project            *core.Project             `json:"project,omitempty"`
	Workers            []TaskDetailWorker        `json:"workers"`
	ExecutionNodes     []core.ExecutionNode      `json:"executionNodes"`
	OrchestrationGraph *core.OrchestrationGraph  `json:"orchestrationGraph,omitempty"`
	PullRequests       []core.PullRequest        `json:"pullRequests"`
	RecentEvents       []core.Event              `json:"recentEvents"`
	ApplyPolicy        ApplyPolicyRecommendation `json:"applyPolicy"`
	AvailableActions   []AvailableAction         `json:"availableActions"`
}

type TaskDetailWorker struct {
	Worker           core.Worker            `json:"worker"`
	ExecutionNode    *core.ExecutionNode    `json:"executionNode,omitempty"`
	LatestEvent      *core.Event            `json:"latestEvent,omitempty"`
	Completion       json.RawMessage        `json:"completion,omitempty"`
	ChangedFiles     []WorkspaceChangedFile `json:"changedFiles,omitempty"`
	Applied          bool                   `json:"applied"`
	AvailableActions []AvailableAction      `json:"availableActions"`
}

type WorkerDetail struct {
	Task         core.Task        `json:"task"`
	Worker       TaskDetailWorker `json:"worker"`
	RecentEvents []core.Event     `json:"recentEvents"`
}

type AvailableAction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	TargetID    string `json:"targetId,omitempty"`
}

func (s *Service) TaskDetail(ctx context.Context, taskID string) (TaskDetail, error) {
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return TaskDetail{}, err
	}
	return BuildTaskDetail(snapshot, taskID, defaultTaskDetailEventLimit)
}

func (s *Service) WorkerDetail(ctx context.Context, workerID string) (WorkerDetail, error) {
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return WorkerDetail{}, err
	}
	return BuildWorkerDetail(snapshot, workerID, defaultTaskDetailEventLimit)
}

func BuildTaskDetail(snapshot core.Snapshot, taskID string, eventLimit int) (TaskDetail, error) {
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return TaskDetail{}, eventstore.ErrNotFound
	}
	detail := TaskDetail{
		Task:               task,
		Workers:            taskDetailWorkers(snapshot, taskID),
		ExecutionNodes:     taskExecutionNodes(snapshot, taskID),
		PullRequests:       taskPullRequests(snapshot, taskID),
		RecentEvents:       recentTaskEvents(snapshot.Events, taskID, eventLimit),
		ApplyPolicy:        taskApplyPolicy(snapshot, taskID),
		AvailableActions:   taskAvailableActions(snapshot, task),
		OrchestrationGraph: taskOrchestrationGraph(snapshot, taskID),
	}
	if project, ok := projectByID(snapshot.Projects, task.ProjectID); ok {
		detail.Project = &project
	}
	return detail, nil
}

func BuildWorkerDetail(snapshot core.Snapshot, workerID string, eventLimit int) (WorkerDetail, error) {
	var worker core.Worker
	for _, candidate := range snapshot.Workers {
		if candidate.ID == workerID {
			worker = candidate
			break
		}
	}
	if worker.ID == "" {
		return WorkerDetail{}, eventstore.ErrNotFound
	}
	task, ok := findTask(snapshot, worker.TaskID)
	if !ok {
		return WorkerDetail{}, eventstore.ErrNotFound
	}
	for _, item := range taskDetailWorkers(snapshot, worker.TaskID) {
		if item.Worker.ID == workerID {
			return WorkerDetail{
				Task:         task,
				Worker:       item,
				RecentEvents: recentWorkerEvents(snapshot.Events, workerID, eventLimit),
			}, nil
		}
	}
	return WorkerDetail{}, eventstore.ErrNotFound
}

func taskDetailWorkers(snapshot core.Snapshot, taskID string) []TaskDetailWorker {
	nodesByWorker := map[string]core.ExecutionNode{}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.WorkerID != "" {
			nodesByWorker[node.WorkerID] = node
		}
	}
	applied := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerApplied && event.TaskID == taskID {
			applied[event.WorkerID] = true
		}
	}
	var workers []TaskDetailWorker
	for _, worker := range snapshot.Workers {
		if worker.TaskID != taskID {
			continue
		}
		item := TaskDetailWorker{
			Worker:  worker,
			Applied: applied[worker.ID],
		}
		if node, ok := nodesByWorker[worker.ID]; ok {
			item.ExecutionNode = &node
		}
		if event, ok := latestWorkerEvent(snapshot.Events, worker.ID); ok {
			item.LatestEvent = &event
		}
		if completion, changedFiles, ok := workerCompletion(snapshot.Events, taskID, worker.ID); ok {
			item.Completion = completion
			item.ChangedFiles = changedFiles
		}
		item.AvailableActions = workerAvailableActions(worker, applied[worker.ID], len(item.ChangedFiles) > 0)
		workers = append(workers, item)
	}
	return workers
}

func taskExecutionNodes(snapshot core.Snapshot, taskID string) []core.ExecutionNode {
	var nodes []core.ExecutionNode
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func taskPullRequests(snapshot core.Snapshot, taskID string) []core.PullRequest {
	var pullRequests []core.PullRequest
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID {
			pullRequests = append(pullRequests, pr)
		}
	}
	return pullRequests
}

func taskOrchestrationGraph(snapshot core.Snapshot, taskID string) *core.OrchestrationGraph {
	for _, graph := range snapshot.OrchestrationGraphs {
		if graph.TaskID == taskID {
			return &graph
		}
	}
	return nil
}

func recentTaskEvents(events []core.Event, taskID string, limit int) []core.Event {
	if limit <= 0 {
		limit = defaultTaskDetailEventLimit
	}
	var out []core.Event
	for index := len(events) - 1; index >= 0 && len(out) < limit; index-- {
		if events[index].TaskID == taskID {
			out = append(out, events[index])
		}
	}
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out
}

func recentWorkerEvents(events []core.Event, workerID string, limit int) []core.Event {
	if limit <= 0 {
		limit = defaultTaskDetailEventLimit
	}
	var out []core.Event
	for index := len(events) - 1; index >= 0 && len(out) < limit; index-- {
		if events[index].WorkerID == workerID {
			out = append(out, events[index])
		}
	}
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out
}

func latestWorkerEvent(events []core.Event, workerID string) (core.Event, bool) {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.WorkerID == workerID && strings.HasPrefix(string(event.Type), "worker.") {
			return event, true
		}
	}
	return core.Event{}, false
}

func workerCompletion(events []core.Event, taskID string, workerID string) (json.RawMessage, []WorkspaceChangedFile, bool) {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.Type != core.EventWorkerCompleted || event.TaskID != taskID || event.WorkerID != workerID {
			continue
		}
		var payload struct {
			ChangedFiles     []WorkspaceChangedFile `json:"changedFiles,omitempty"`
			WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return event.Payload, nil, true
		}
		changedFiles := payload.ChangedFiles
		if len(changedFiles) == 0 {
			changedFiles = payload.WorkspaceChanges.ChangedFiles
		}
		return event.Payload, changedFiles, true
	}
	return nil, nil, false
}

func taskApplyPolicy(snapshot core.Snapshot, taskID string) ApplyPolicyRecommendation {
	candidates := applyCandidates(snapshot, taskID)
	recommendation := ApplyPolicyRecommendation{
		TaskID:     taskID,
		Strategy:   "none",
		Reason:     "no unapplied successful workers with source changes",
		Candidates: candidates,
	}
	task, ok := findTask(snapshot, taskID)
	if ok && task.FinalCandidateWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == task.FinalCandidateWorkerID {
				if candidate.Applied {
					recommendation.Strategy = "already_applied"
					recommendation.Reason = "final task candidate has already been applied"
				} else {
					recommendation.Strategy = "apply_final"
					recommendation.Reason = "orchestrator selected a final task candidate"
				}
				return recommendation
			}
		}
	}
	unapplied := 0
	for _, candidate := range candidates {
		if !candidate.Applied {
			unapplied++
		}
	}
	switch {
	case unapplied == 1:
		recommendation.Strategy = "apply_single"
		recommendation.Reason = "exactly one unapplied successful worker has source changes"
	case unapplied > 1:
		recommendation.Strategy = "manual_select"
		recommendation.Reason = "multiple unapplied successful workers have source changes; select one or schedule a review/benchmark comparison before applying"
	}
	return recommendation
}

func taskAvailableActions(snapshot core.Snapshot, task core.Task) []AvailableAction {
	actions := []AvailableAction{
		{Name: "aged_steer_task", Description: "Send steering, feedback, or an answer to a waiting task.", TargetID: task.ID},
		{Name: "aged_watch_prs", Description: "Attach existing GitHub pull requests to this task.", TargetID: task.ID},
	}
	if !isTerminalTaskStatus(task.Status) {
		actions = append(actions, AvailableAction{Name: "aged_cancel_task", Description: "Cancel active work for this task.", TargetID: task.ID})
	}
	if task.Status == core.TaskFailed || task.Status == core.TaskCanceled {
		actions = append(actions, AvailableAction{Name: "aged_retry_task", Description: "Retry this task from its persisted plan.", TargetID: task.ID})
	}
	if isTerminalTaskStatus(task.Status) {
		actions = append(actions, AvailableAction{Name: "aged_clear_task", Description: "Hide this finished task from active snapshots.", TargetID: task.ID})
	}
	if taskCanApplyFinalResult(snapshot, task) {
		actions = append(actions, AvailableAction{Name: "aged_apply_task_result", Description: "Apply the selected final worker result locally.", TargetID: task.ID})
	}
	if canPublishPullRequestForTask(task) && len(taskPullRequests(snapshot, task.ID)) == 0 {
		actions = append(actions, AvailableAction{Name: "aged_publish_pr", Description: "Open a GitHub pull request for this task result.", TargetID: task.ID})
	}
	for _, pr := range taskPullRequests(snapshot, task.ID) {
		actions = append(actions,
			AvailableAction{Name: "aged_refresh_pr", Description: "Refresh PR checks, reviews, and mergeability.", TargetID: pr.ID},
			AvailableAction{Name: "aged_babysit_pr", Description: "Start or return a babysitter task for this PR.", TargetID: pr.ID},
		)
	}
	return actions
}

func workerAvailableActions(worker core.Worker, applied bool, hasChanges bool) []AvailableAction {
	actions := []AvailableAction{
		{Name: "aged_worker_detail", Description: "Inspect worker command, prompt, workspace, completion, and event history.", TargetID: worker.ID},
	}
	if !isTerminalWorkerStatus(worker.Status) {
		actions = append(actions, AvailableAction{Name: "aged_cancel_worker", Description: "Cancel this worker.", TargetID: worker.ID})
	}
	if worker.Status == core.WorkerSucceeded && hasChanges && !applied {
		actions = append(actions,
			AvailableAction{Name: "aged_review_worker_changes", Description: "Review this worker's diff and changed files.", TargetID: worker.ID},
			AvailableAction{Name: "aged_apply_worker_changes", Description: "Manually apply this worker's changes.", TargetID: worker.ID},
		)
	}
	return actions
}

func taskCanApplyFinalResult(snapshot core.Snapshot, task core.Task) bool {
	if !isTerminalTaskStatus(task.Status) || strings.TrimSpace(task.FinalCandidateWorkerID) == "" {
		return false
	}
	if taskCompletionMode(task) == "github" {
		return false
	}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerApplied && event.WorkerID == task.FinalCandidateWorkerID {
			return false
		}
	}
	return true
}

func taskCompletionMode(task core.Task) string {
	var metadata map[string]any
	if len(task.Metadata) > 0 && json.Unmarshal(task.Metadata, &metadata) == nil {
		if value, ok := metadata["completionMode"].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "local"
}
