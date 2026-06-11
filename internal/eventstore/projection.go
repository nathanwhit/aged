package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"aged/internal/core"
)

type readModelState struct {
	Tasks                 map[string]core.Task                `json:"tasks"`
	Workers               map[string]core.Worker              `json:"workers"`
	Nodes                 map[string]core.ExecutionNode       `json:"nodes"`
	WorkItems             map[string]core.WorkItem            `json:"workItems"`
	Artifacts             map[string]core.Artifact            `json:"artifacts"`
	MemoryEntries         map[string]core.MemoryEntry         `json:"memoryEntries"`
	Questions             map[string]core.Question            `json:"questions"`
	Sessions              map[string]core.Session             `json:"sessions"`
	PullRequests          map[string]core.PullRequest         `json:"pullRequests"`
	PullRequestFeedback   map[string]core.PullRequestFeedback `json:"pullRequestFeedback"`
	Steering              map[string]core.SteeringItem        `json:"steering"`
	PullRequestAliases    map[string]string                   `json:"pullRequestAliases"`
	PullRequestIdentities map[string]string                   `json:"pullRequestIdentities"`
	ClearedTasks          map[string]bool                     `json:"clearedTasks"`
	WorkerNodes           map[string]string                   `json:"workerNodes"`
	WorkspaceMetadata     map[string]json.RawMessage          `json:"workspaceMetadata"`
}

func newReadModelState() readModelState {
	state := readModelState{}
	state.ensure()
	return state
}

func (p *readModelState) ensure() {
	if p.Tasks == nil {
		p.Tasks = map[string]core.Task{}
	}
	if p.Workers == nil {
		p.Workers = map[string]core.Worker{}
	}
	if p.Nodes == nil {
		p.Nodes = map[string]core.ExecutionNode{}
	}
	if p.WorkItems == nil {
		p.WorkItems = map[string]core.WorkItem{}
	}
	if p.Artifacts == nil {
		p.Artifacts = map[string]core.Artifact{}
	}
	if p.MemoryEntries == nil {
		p.MemoryEntries = map[string]core.MemoryEntry{}
	}
	if p.Questions == nil {
		p.Questions = map[string]core.Question{}
	}
	if p.Sessions == nil {
		p.Sessions = map[string]core.Session{}
	}
	if p.PullRequests == nil {
		p.PullRequests = map[string]core.PullRequest{}
	}
	if p.PullRequestFeedback == nil {
		p.PullRequestFeedback = map[string]core.PullRequestFeedback{}
	}
	if p.Steering == nil {
		p.Steering = map[string]core.SteeringItem{}
	}
	if p.PullRequestAliases == nil {
		p.PullRequestAliases = map[string]string{}
	}
	if p.PullRequestIdentities == nil {
		p.PullRequestIdentities = map[string]string{}
	}
	if p.ClearedTasks == nil {
		p.ClearedTasks = map[string]bool{}
	}
	if p.WorkerNodes == nil {
		p.WorkerNodes = map[string]string{}
	}
	if p.WorkspaceMetadata == nil {
		p.WorkspaceMetadata = map[string]json.RawMessage{}
	}
}

func (p *readModelState) snapshot(lastEventID int64, events []core.Event, includeEvents bool) core.Snapshot {
	p.ensure()
	filteredTasks := filterClearedTasks(p.Tasks, p.ClearedTasks)
	filteredNodes := filterClearedExecutionNodes(p.Nodes, p.ClearedTasks)
	filteredWorkers := filterClearedWorkers(p.Workers, p.ClearedTasks)
	filteredWorkItems := filterClearedWorkItems(p.WorkItems, p.ClearedTasks)
	filteredArtifacts := filterClearedArtifacts(p.Artifacts, p.ClearedTasks)
	filteredQuestions := filterClearedQuestions(p.Questions, p.ClearedTasks)
	filteredSessions := filterClearedSessions(p.Sessions, p.ClearedTasks)
	filteredPullRequests := filterClearedPullRequests(p.PullRequests, p.ClearedTasks)
	filteredPullRequestFeedback := filterClearedPullRequestFeedback(p.PullRequestFeedback, p.ClearedTasks)
	filteredSteering := filterClearedSteering(p.Steering, p.ClearedTasks)
	return core.Snapshot{
		Tasks:               orderedTasks(filteredTasks),
		Workers:             orderedWorkers(filteredWorkers),
		ExecutionNodes:      orderedExecutionNodes(filteredNodes),
		WorkItems:           orderedWorkItems(filteredWorkItems),
		Artifacts:           orderedArtifacts(filteredArtifacts),
		MemoryEntries:       orderedMemoryEntries(filterClearedMemoryEntries(p.MemoryEntries, p.ClearedTasks)),
		Questions:           orderedQuestions(filteredQuestions),
		Sessions:            orderedSessions(filteredSessions),
		PullRequests:        orderedPullRequests(filteredPullRequests),
		PullRequestFeedback: orderedPullRequestFeedback(filteredPullRequestFeedback),
		Steering:            orderedSteering(filteredSteering),
		ManagerSummary:      buildManagerSummaries(filteredTasks, filteredWorkers, filteredWorkItems, filteredArtifacts, filteredQuestions, filteredSessions, filteredPullRequests, filteredPullRequestFeedback, filteredSteering),
		LastEventID:         lastEventID,
		Events:              snapshotResponseEvents(events, includeEvents),
	}
}

func (p *readModelState) taskCardsSnapshot(lastEventID int64) core.Snapshot {
	p.ensure()
	filteredTasks := filterClearedTasks(p.Tasks, p.ClearedTasks)
	activeTasks := map[string]bool{}
	taskCards := make(map[string]core.Task, len(filteredTasks))
	for id, task := range filteredTasks {
		if !isTerminalTaskStatus(task.Status) {
			activeTasks[id] = true
		}
		taskCards[id] = compactTaskCard(task)
	}
	workers := filterTasks(p.Workers, p.ClearedTasks, activeTasks, func(worker core.Worker) string { return worker.TaskID })
	nodes := filterTasks(p.Nodes, p.ClearedTasks, activeTasks, func(node core.ExecutionNode) string { return node.TaskID })
	pullRequests := filterTasks(p.PullRequests, p.ClearedTasks, activeTasks, func(pr core.PullRequest) string { return pr.TaskID })
	artifacts := filterTasks(p.Artifacts, p.ClearedTasks, activeTasks, func(artifact core.Artifact) string { return artifact.TaskID })
	pullRequestFeedback := filterTasks(p.PullRequestFeedback, p.ClearedTasks, activeTasks, func(feedback core.PullRequestFeedback) string { return feedback.TaskID })
	steering := filterTasks(p.Steering, p.ClearedTasks, activeTasks, func(item core.SteeringItem) string { return item.TaskID })
	sessions := filterTasks(p.Sessions, p.ClearedTasks, activeTasks, func(session core.Session) string { return session.TaskID })
	questions := filterTasks(p.Questions, p.ClearedTasks, activeTasks, func(question core.Question) string { return question.TaskID })
	workItems := filterTasks(p.WorkItems, p.ClearedTasks, activeTasks, func(item core.WorkItem) string { return item.TaskID })
	workers = compactCardWorkers(workers)
	nodes = compactCardExecutionNodes(nodes)
	pullRequests = compactCardPullRequests(pullRequests)
	sessions = compactCardSessions(sessions)
	return core.Snapshot{
		Tasks:               orderedTasks(taskCards),
		Workers:             orderedWorkers(workers),
		ExecutionNodes:      orderedExecutionNodes(nodes),
		WorkItems:           orderedWorkItems(workItems),
		Artifacts:           orderedArtifacts(compactCardArtifacts(artifacts)),
		Questions:           orderedQuestions(questions),
		Sessions:            orderedSessions(sessions),
		PullRequests:        orderedPullRequests(pullRequests),
		PullRequestFeedback: orderedPullRequestFeedback(compactCardPullRequestFeedback(pullRequestFeedback)),
		Steering:            orderedSteering(compactCardSteering(steering)),
		ManagerSummary:      buildManagerSummaries(filteredTasks, workers, workItems, artifacts, questions, sessions, pullRequests, pullRequestFeedback, steering),
		LastEventID:         lastEventID,
	}
}

func compactTaskCard(task core.Task) core.Task {
	task.Prompt = ""
	task.Error = truncateCardText(task.Error, 1200)
	task.Metadata = compactTaskCardMetadata(task.Metadata)
	task.Milestones = nil
	task.WorkPlan = nil
	task.Artifacts = nil
	return task
}

func compactCardArtifacts(artifacts map[string]core.Artifact) map[string]core.Artifact {
	out := make(map[string]core.Artifact, len(artifacts))
	for id, artifact := range artifacts {
		artifact.Metadata = compactArtifactCardMetadata(artifact.Metadata)
		out[id] = artifact
	}
	return out
}

func compactArtifactCardMetadata(metadata json.RawMessage) json.RawMessage {
	if len(metadata) == 0 || string(metadata) == "null" {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(metadata, &decoded); err != nil {
		return nil
	}
	kept := map[string]any{}
	for _, key := range []string{
		"workerId",
		"workerKind",
		"repo",
		"number",
		"branch",
		"base",
		"path",
	} {
		if value, ok := decoded[key]; ok && value != nil {
			kept[key] = value
		}
	}
	if len(kept) == 0 {
		return nil
	}
	data, err := json.Marshal(kept)
	if err != nil {
		return nil
	}
	return data
}

func compactTaskCardMetadata(metadata json.RawMessage) json.RawMessage {
	if len(metadata) == 0 || string(metadata) == "null" {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(metadata, &decoded); err != nil {
		return nil
	}
	kept := map[string]any{}
	for _, key := range []string{
		"executionMode",
		"objectiveMode",
		"loopIntervalSeconds",
		"requiredTargetID",
	} {
		if value, ok := decoded[key]; ok && value != nil {
			kept[key] = value
		}
	}
	if len(kept) == 0 {
		return nil
	}
	data, err := json.Marshal(kept)
	if err != nil {
		return nil
	}
	return data
}

func compactCardWorkers(workers map[string]core.Worker) map[string]core.Worker {
	out := make(map[string]core.Worker, len(workers))
	for id, worker := range workers {
		out[id] = compactCardWorker(worker)
	}
	return out
}

func compactCardWorker(worker core.Worker) core.Worker {
	worker.Prompt = ""
	worker.PromptError = truncateCardText(worker.PromptError, 1200)
	worker.Metadata = nil
	return worker
}

func compactCardExecutionNodes(nodes map[string]core.ExecutionNode) map[string]core.ExecutionNode {
	out := make(map[string]core.ExecutionNode, len(nodes))
	for id, node := range nodes {
		out[id] = compactCardExecutionNode(node)
	}
	return out
}

func compactCardExecutionNode(node core.ExecutionNode) core.ExecutionNode {
	node.Metadata = nil
	return node
}

func compactCardPullRequests(pullRequests map[string]core.PullRequest) map[string]core.PullRequest {
	out := make(map[string]core.PullRequest, len(pullRequests))
	for id, pullRequest := range pullRequests {
		out[id] = compactCardPullRequest(pullRequest)
	}
	return out
}

func compactCardPullRequest(pullRequest core.PullRequest) core.PullRequest {
	pullRequest.Metadata = nil
	return pullRequest
}

func compactCardPullRequestFeedback(feedbackRows map[string]core.PullRequestFeedback) map[string]core.PullRequestFeedback {
	out := map[string]core.PullRequestFeedback{}
	for id, feedback := range feedbackRows {
		if feedback.Status != "pending" {
			continue
		}
		feedback.FeedbackBody = ""
		feedback.Prompt = ""
		feedback.Metadata = nil
		out[id] = feedback
	}
	return out
}

func compactCardSteering(items map[string]core.SteeringItem) map[string]core.SteeringItem {
	out := map[string]core.SteeringItem{}
	for id, item := range items {
		if item.Status != "pending" {
			continue
		}
		item.Message = truncateCardText(item.Message, 400)
		item.Metadata = nil
		out[id] = item
	}
	return out
}

func compactCardSessions(sessions map[string]core.Session) map[string]core.Session {
	out := make(map[string]core.Session, len(sessions))
	for id, session := range sessions {
		session.Metadata = nil
		out[id] = session
	}
	return out
}

func truncateCardText(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func activeProjectionTasks(tasks map[string]core.Task, cleared map[string]bool) map[string]bool {
	active := map[string]bool{}
	for id, task := range tasks {
		if cleared[id] || isTerminalTaskStatus(task.Status) {
			continue
		}
		active[id] = true
	}
	return active
}

func filterTasks[T any](values map[string]T, cleared map[string]bool, keptTasks map[string]bool, taskID func(T) string) map[string]T {
	out := map[string]T{}
	for id, value := range values {
		task := taskID(value)
		if cleared[task] || !keptTasks[task] {
			continue
		}
		out[id] = value
	}
	return out
}

func buildManagerSummaries(
	tasks map[string]core.Task,
	workers map[string]core.Worker,
	workItems map[string]core.WorkItem,
	artifacts map[string]core.Artifact,
	questions map[string]core.Question,
	sessions map[string]core.Session,
	pullRequests map[string]core.PullRequest,
	pullRequestFeedback map[string]core.PullRequestFeedback,
	steering map[string]core.SteeringItem,
) []core.ManagerSummary {
	summaries := make(map[string]core.ManagerSummary, len(tasks))
	for id, task := range tasks {
		tone := "info"
		if task.Status == core.TaskSucceeded {
			tone = "good"
		}
		if task.Status == core.TaskFailed || task.Status == core.TaskCanceled || task.Error != "" {
			tone = "danger"
		}
		summaries[id] = core.ManagerSummary{
			TaskID:    id,
			Tone:      tone,
			UpdatedAt: task.UpdatedAt,
		}
	}
	sessionWorkers := map[string]bool{}
	for _, session := range sessions {
		sessionWorkers[session.WorkerID] = true
		summary, ok := summaries[session.TaskID]
		if !ok {
			continue
		}
		if !isTerminalWorkerStatus(session.Status) {
			summary.ActiveSignals++
			summary.ActiveSessions++
			if session.Status == core.WorkerWaiting || session.Status == core.WorkerQueued {
				summary.AttentionCount++
				summary.Tone = managerSummaryTone(summary.Tone, "warning")
			}
		}
		actionAt := valueTime(session.CurrentActionAt, session.UpdatedAt)
		if managerSummaryLatestActionAfter(summary, session.CurrentAction, actionAt) {
			summary.LatestAction = session.CurrentAction
			summary.LatestActionAt = actionAt
			summary.LatestActionLabel = session.CurrentActionLabel
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, session.UpdatedAt)
		summaries[session.TaskID] = summary
	}
	for _, worker := range workers {
		summary, ok := summaries[worker.TaskID]
		if !ok {
			continue
		}
		if !isTerminalWorkerStatus(worker.Status) {
			summary.ActiveWorkers++
		}
		if !sessionWorkers[worker.ID] && !isTerminalWorkerStatus(worker.Status) {
			summary.ActiveSignals++
			if worker.Status == core.WorkerWaiting || worker.Status == core.WorkerQueued {
				summary.AttentionCount++
				summary.Tone = managerSummaryTone(summary.Tone, "warning")
			}
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, worker.UpdatedAt)
		summaries[worker.TaskID] = summary
	}
	for _, item := range workItems {
		summary, ok := summaries[item.TaskID]
		if !ok {
			continue
		}
		if item.Status == core.WorkItemQueued || item.Status == core.WorkItemRunning {
			summary.ActiveSignals++
			summary.ActiveWorkItems++
			summary.Tone = managerSummaryTone(summary.Tone, "warning")
		}
		if item.Error != "" && managerSummaryLatestActionAfter(summary, item.Error, item.UpdatedAt) {
			summary.LatestAction = item.Error
			summary.LatestActionAt = item.UpdatedAt
			summary.LatestActionLabel = "Work item"
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, item.UpdatedAt)
		summaries[item.TaskID] = summary
	}
	for _, question := range questions {
		summary, ok := summaries[question.TaskID]
		if !ok {
			continue
		}
		if !question.Decided {
			summary.ActiveSignals++
			summary.AttentionCount++
			summary.PendingApprovals++
			summary.Tone = managerSummaryTone(summary.Tone, "warning")
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, question.UpdatedAt)
		summaries[question.TaskID] = summary
	}
	for _, feedback := range pullRequestFeedback {
		summary, ok := summaries[feedback.TaskID]
		if !ok {
			continue
		}
		if feedback.Status == "pending" && !pendingFeedbackTargetsTerminalPullRequest(feedback, pullRequests) {
			summary.ActiveSignals++
			summary.AttentionCount++
			summary.PendingFeedback++
			summary.Tone = managerSummaryTone(summary.Tone, "warning")
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, feedback.UpdatedAt)
		summaries[feedback.TaskID] = summary
	}
	for _, pr := range pullRequests {
		summary, ok := summaries[pr.TaskID]
		if !ok {
			continue
		}
		if !isTerminalPullRequestState(pr.State) {
			summary.ActiveSignals++
			summary.PullRequests++
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, pr.UpdatedAt)
		summaries[pr.TaskID] = summary
	}
	for _, artifact := range artifacts {
		summary, ok := summaries[artifact.TaskID]
		if !ok {
			continue
		}
		summary.Artifacts++
		summary.UpdatedAt = latestTime(summary.UpdatedAt, artifact.UpdatedAt)
		summaries[artifact.TaskID] = summary
	}
	for _, item := range steering {
		summary, ok := summaries[item.TaskID]
		if !ok {
			continue
		}
		if item.Status == "pending" || item.Status == "queued" || item.Status == "running" {
			summary.ActiveSignals++
		}
		summary.UpdatedAt = latestTime(summary.UpdatedAt, item.UpdatedAt)
		summaries[item.TaskID] = summary
	}
	return orderedManagerSummaries(summaries, tasks)
}

func pendingFeedbackTargetsTerminalPullRequest(feedback core.PullRequestFeedback, pullRequests map[string]core.PullRequest) bool {
	if feedback.PullRequestID == "" {
		return false
	}
	pr, ok := pullRequests[feedback.PullRequestID]
	return ok && isTerminalPullRequestState(pr.State)
}

func isTerminalPullRequestState(state string) bool {
	return strings.EqualFold(state, "MERGED") || strings.EqualFold(state, "CLOSED")
}

func managerSummaryTone(current string, next string) string {
	rank := map[string]int{"good": 0, "info": 1, "warning": 2, "danger": 3}
	if rank[next] > rank[current] {
		return next
	}
	return current
}

func managerSummaryLatestActionAfter(summary core.ManagerSummary, action string, at time.Time) bool {
	if action == "" {
		return false
	}
	if summary.LatestAction == "" {
		return true
	}
	if at.After(summary.LatestActionAt) {
		return true
	}
	if at.Equal(summary.LatestActionAt) {
		return action > summary.LatestAction
	}
	return false
}

func latestTime(left time.Time, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func valueTime(value *time.Time, fallback time.Time) time.Time {
	if value != nil {
		return *value
	}
	return fallback
}

func orderedManagerSummaries(values map[string]core.ManagerSummary, tasks map[string]core.Task) []core.ManagerSummary {
	return orderedSnapshotValues(values, func(summary core.ManagerSummary) string { return summary.TaskID }, func(summary core.ManagerSummary) time.Time {
		if task, ok := tasks[summary.TaskID]; ok {
			return task.CreatedAt
		}
		return summary.UpdatedAt
	})
}

type sessionExecutionPayload struct {
	NodeID        string          `json:"nodeId"`
	WorkerID      string          `json:"workerId,omitempty"`
	WorkerKind    string          `json:"workerKind"`
	SpawnID       string          `json:"spawnId,omitempty"`
	Role          string          `json:"role,omitempty"`
	TargetID      string          `json:"targetId,omitempty"`
	TargetKind    string          `json:"targetKind,omitempty"`
	RemoteSession string          `json:"remoteSession,omitempty"`
	RemoteRunDir  string          `json:"remoteRunDir,omitempty"`
	RemoteWorkDir string          `json:"remoteWorkDir,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

type sessionWorkerPayload struct {
	Kind     string          `json:"kind"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

type sessionWorkspacePayload struct {
	Root               string `json:"root"`
	CWD                string `json:"cwd"`
	SourceRoot         string `json:"sourceRoot"`
	WorkspaceName      string `json:"workspaceName"`
	Mode               string `json:"mode"`
	VCSType            string `json:"vcsType"`
	WorkerID           string `json:"workerId"`
	TaskID             string `json:"taskId"`
	TargetID           string `json:"targetId,omitempty"`
	TargetKind         string `json:"targetKind,omitempty"`
	SharedRoot         string `json:"sharedRoot,omitempty"`
	SharedArtifactsDir string `json:"sharedArtifactsDir,omitempty"`
	SharedWorkerDir    string `json:"sharedWorkerDir,omitempty"`
}

func questionIDForApprovalEvent(eventID int64) string {
	return fmt.Sprintf("approval_%d", eventID)
}

func applyQuestionNeeded(questions map[string]core.Question, event core.Event) error {
	var payload struct {
		Question string          `json:"question"`
		Reason   string          `json:"reason"`
		Metadata json.RawMessage `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode approval.needed: %w", err)
	}
	if payload.Metadata == nil {
		payload.Metadata = event.Payload
	}
	id := questionIDForApprovalEvent(event.ID)
	questions[id] = core.Question{
		ID:        id,
		TaskID:    event.TaskID,
		WorkerID:  event.WorkerID,
		Reason:    payload.Reason,
		Question:  payload.Question,
		CreatedAt: event.At,
		UpdatedAt: event.At,
		Metadata:  payload.Metadata,
	}
	return nil
}

func applyQuestionDecided(questions map[string]core.Question, event core.Event) error {
	var payload struct {
		Approved   *bool  `json:"approved,omitempty"`
		Answer     string `json:"answer,omitempty"`
		Question   string `json:"question,omitempty"`
		QuestionID string `json:"questionId,omitempty"`
		Reason     string `json:"reason,omitempty"`
		WorkerID   string `json:"workerId,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode approval.decided: %w", err)
	}
	workerID := firstNonEmpty(event.WorkerID, payload.WorkerID)
	var selectedID string
	var selectedAt time.Time
	if payload.QuestionID != "" {
		if question, ok := questions[payload.QuestionID]; ok && question.TaskID == event.TaskID && !question.Decided {
			selectedID = payload.QuestionID
		}
	}
	if selectedID == "" {
		for id, question := range questions {
			if question.TaskID != event.TaskID || question.Decided {
				continue
			}
			if workerID != "" && question.WorkerID != workerID {
				continue
			}
			if selectedID == "" || question.CreatedAt.After(selectedAt) {
				selectedID = id
				selectedAt = question.CreatedAt
			}
		}
	}
	if selectedID == "" && workerID != "" {
		for id, question := range questions {
			if question.TaskID != event.TaskID || question.Decided {
				continue
			}
			if selectedID == "" || question.CreatedAt.After(selectedAt) {
				selectedID = id
				selectedAt = question.CreatedAt
			}
		}
	}
	if selectedID == "" {
		return nil
	}
	question := questions[selectedID]
	question.Decided = true
	question.Approved = payload.Approved
	question.Answer = payload.Answer
	if question.Question == "" {
		question.Question = payload.Question
	}
	if question.Reason == "" {
		question.Reason = payload.Reason
	}
	question.UpdatedAt = event.At
	questions[selectedID] = question
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func upsertSession(sessions map[string]core.Session, workerID string, taskID string, at time.Time) core.Session {
	if workerID == "" {
		return core.Session{}
	}
	session := sessions[workerID]
	if session.ID == "" {
		session.ID = workerID
		session.WorkerID = workerID
		session.TaskID = taskID
		session.Status = core.WorkerQueued
		session.CreatedAt = at
	}
	if session.WorkerID == "" {
		session.WorkerID = workerID
	}
	if session.TaskID == "" {
		session.TaskID = taskID
	}
	if session.CreatedAt.IsZero() {
		session.CreatedAt = at
	}
	session.UpdatedAt = at
	return session
}

func applySessionExecutionPlanned(sessions map[string]core.Session, taskID string, at time.Time, payload sessionExecutionPayload) {
	if payload.WorkerID == "" {
		return
	}
	session := upsertSession(sessions, payload.WorkerID, taskID, at)
	session.NodeID = payload.NodeID
	session.WorkerKind = payload.WorkerKind
	session.SpawnID = payload.SpawnID
	session.Role = payload.Role
	session.TargetID = payload.TargetID
	session.TargetKind = payload.TargetKind
	session.RemoteSession = payload.RemoteSession
	session.RemoteRunDir = payload.RemoteRunDir
	session.RemoteWorkDir = payload.RemoteWorkDir
	session.Metadata = payload.Metadata
	sessions[payload.WorkerID] = session
}

func applySessionWorkerCreated(sessions map[string]core.Session, taskID string, workerID string, at time.Time, payload sessionWorkerPayload, workspaceMetadata json.RawMessage) {
	if workerID == "" {
		return
	}
	session := upsertSession(sessions, workerID, taskID, at)
	if payload.Kind != "" {
		session.WorkerKind = payload.Kind
	}
	session.Metadata = mergeMetadata(payload.Metadata, workspaceMetadata)
	if providerSessionID := stringFromMetadata(payload.Metadata, "providerSessionId"); providerSessionID != "" {
		session.ProviderSessionID = providerSessionID
	}
	sessions[workerID] = session
}

func applySessionWorkspacePrepared(sessions map[string]core.Session, taskID string, workerID string, at time.Time, payload sessionWorkspacePayload, raw json.RawMessage) {
	if workerID == "" {
		workerID = payload.WorkerID
	}
	if taskID == "" {
		taskID = payload.TaskID
	}
	if workerID == "" {
		return
	}
	session := upsertSession(sessions, workerID, taskID, at)
	session.WorkspaceRoot = payload.Root
	session.WorkspaceCWD = payload.CWD
	session.SourceRoot = payload.SourceRoot
	session.WorkspaceName = payload.WorkspaceName
	session.WorkspaceMode = payload.Mode
	session.VCSType = payload.VCSType
	session.SharedRoot = payload.SharedRoot
	session.SharedArtifactsDir = payload.SharedArtifactsDir
	session.SharedWorkerDir = payload.SharedWorkerDir
	if payload.TargetID != "" {
		session.TargetID = payload.TargetID
	}
	if payload.TargetKind != "" {
		session.TargetKind = payload.TargetKind
	}
	session.Metadata = mergeMetadata(session.Metadata, raw)
	sessions[workerID] = session
}

func updateSessionStatus(sessions map[string]core.Session, workerID string, taskID string, at time.Time, status core.WorkerStatus) {
	if workerID == "" {
		return
	}
	session := upsertSession(sessions, workerID, taskID, at)
	if status != "" {
		session.Status = status
	}
	if status == core.WorkerRunning && session.StartedAt == nil {
		startedAt := at
		session.StartedAt = &startedAt
	}
	if isTerminalWorkerStatus(status) && session.CompletedAt == nil {
		completedAt := at
		session.CompletedAt = &completedAt
	}
	sessions[workerID] = session
}

func touchSession(sessions map[string]core.Session, workerID string, taskID string, at time.Time) {
	if workerID == "" {
		return
	}
	session := sessions[workerID]
	if session.ID == "" || isTerminalWorkerStatus(session.Status) {
		return
	}
	session = upsertSession(sessions, workerID, taskID, at)
	sessions[workerID] = session
}

func touchSessionActivity(sessions map[string]core.Session, workerID string, taskID string, at time.Time, eventID int64, payload json.RawMessage) {
	if workerID == "" {
		return
	}
	session := sessions[workerID]
	if session.ID == "" || isTerminalWorkerStatus(session.Status) {
		return
	}
	session = upsertSession(sessions, workerID, taskID, at)
	label, action := compactWorkerOutputActivity(payload)
	session.CurrentActionLabel = label
	session.CurrentAction = action
	session.CurrentActionAt = &at
	session.CurrentActionEvent = eventID
	sessions[workerID] = session
}

func (p *readModelState) apply(event core.Event) error {
	p.ensure()
	switch event.Type {
	case core.EventTaskCreated:
		var payload struct {
			ProjectID string          `json:"projectId,omitempty"`
			Title     string          `json:"title"`
			Prompt    string          `json:"prompt"`
			Metadata  json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.created: %w", err)
		}
		projectID := payload.ProjectID
		if projectID == "" {
			projectID = projectIDFromMetadata(payload.Metadata)
		}
		p.Tasks[event.TaskID] = core.Task{
			ID:              event.TaskID,
			ProjectID:       projectID,
			WorkstreamID:    workstreamIDFromMetadata(payload.Metadata),
			Title:           payload.Title,
			Prompt:          payload.Prompt,
			Status:          core.TaskQueued,
			ObjectiveStatus: core.ObjectiveActive,
			ObjectivePhase:  "queued",
			CreatedAt:       event.At,
			UpdatedAt:       event.At,
			Metadata:        payload.Metadata,
		}
	case core.EventTaskUpdated:
		var payload struct {
			Title         string          `json:"title,omitempty"`
			Prompt        string          `json:"prompt,omitempty"`
			MetadataPatch json.RawMessage `json:"metadataPatch,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.updated: %w", err)
		}
		task, ok := p.Tasks[event.TaskID]
		if !ok {
			return nil
		}
		if payload.Title != "" {
			task.Title = payload.Title
		}
		if payload.Prompt != "" {
			task.Prompt = payload.Prompt
		}
		task.Metadata = mergeMetadataPatch(task.Metadata, payload.MetadataPatch)
		task.WorkstreamID = workstreamIDFromMetadata(task.Metadata)
		task.UpdatedAt = event.At
		p.Tasks[event.TaskID] = task
	case core.EventTaskStatus:
		var payload struct {
			Status core.TaskStatus `json:"status"`
			Error  string          `json:"error,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.status: %w", err)
		}
		task := p.Tasks[event.TaskID]
		task.Status = payload.Status
		task.Error = payload.Error
		switch payload.Status {
		case core.TaskSucceeded, core.TaskFailed, core.TaskCanceled:
			nextObjective := objectiveStatusForTaskStatus(payload.Status)
			if task.ObjectiveStatus == "" || task.ObjectiveStatus == core.ObjectiveActive || task.ObjectiveStatus != nextObjective {
				task.ObjectiveStatus = nextObjective
				task.ObjectivePhase = objectivePhaseForTaskStatus(payload.Status)
			}
		case core.TaskWaiting:
			if task.ObjectiveStatus == "" || task.ObjectiveStatus == core.ObjectiveActive {
				task.ObjectiveStatus = core.ObjectiveWaitingUser
				task.ObjectivePhase = objectivePhaseForTaskStatus(payload.Status)
			}
		default:
			if task.ObjectiveStatus == "" {
				task.ObjectiveStatus = objectiveStatusForTaskStatus(payload.Status)
				task.ObjectivePhase = objectivePhaseForTaskStatus(payload.Status)
			}
		}
		task.UpdatedAt = event.At
		p.Tasks[event.TaskID] = task
	case core.EventTaskObjective:
		var payload struct {
			Status core.ObjectiveStatus `json:"status"`
			Phase  string               `json:"phase,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.objective_updated: %w", err)
		}
		task := p.Tasks[event.TaskID]
		if payload.Status != "" {
			task.ObjectiveStatus = payload.Status
		}
		if payload.Phase != "" {
			task.ObjectivePhase = payload.Phase
		}
		task.UpdatedAt = event.At
		p.Tasks[event.TaskID] = task
	case core.EventTaskMilestone:
		var payload struct {
			Name     string          `json:"name"`
			Phase    string          `json:"phase,omitempty"`
			Summary  string          `json:"summary,omitempty"`
			Metadata json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.milestone_reached: %w", err)
		}
		task := p.Tasks[event.TaskID]
		task.Milestones = append(task.Milestones, core.TaskMilestone{
			Name:     payload.Name,
			Phase:    payload.Phase,
			Summary:  payload.Summary,
			At:       event.At,
			Metadata: payload.Metadata,
		})
		task.UpdatedAt = event.At
		p.Tasks[event.TaskID] = task
	case core.EventTaskWorkPlan:
		var payload core.WorkPlan
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.work_plan_updated: %w", err)
		}
		task := p.Tasks[event.TaskID]
		task.WorkPlan = &payload
		task.UpdatedAt = event.At
		p.Tasks[event.TaskID] = task
	case core.EventTaskArtifact:
		var payload struct {
			ID       string          `json:"id"`
			Kind     string          `json:"kind"`
			Name     string          `json:"name,omitempty"`
			URL      string          `json:"url,omitempty"`
			Ref      string          `json:"ref,omitempty"`
			Metadata json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.artifact_recorded: %w", err)
		}
		artifactID := taskArtifactSnapshotID(payload.ID, event.ID)
		task := p.Tasks[event.TaskID]
		task.Artifacts = upsertTaskArtifact(task.Artifacts, core.TaskArtifact{
			ID:        artifactID,
			Kind:      payload.Kind,
			Name:      payload.Name,
			URL:       payload.URL,
			Ref:       payload.Ref,
			CreatedAt: event.At,
			UpdatedAt: event.At,
			Metadata:  payload.Metadata,
		})
		task.UpdatedAt = event.At
		p.Tasks[event.TaskID] = task
		p.Artifacts[artifactID] = core.Artifact{
			ID:        artifactID,
			TaskID:    event.TaskID,
			Kind:      payload.Kind,
			Name:      payload.Name,
			URL:       payload.URL,
			Ref:       payload.Ref,
			CreatedAt: event.At,
			UpdatedAt: event.At,
			Metadata:  payload.Metadata,
		}
	case core.EventTaskCleared:
		p.ClearedTasks[event.TaskID] = true
	case core.EventTaskSteered:
		if err := applySteeringTaskSteered(p.Steering, event); err != nil {
			return err
		}
	case core.EventTaskPlanned, core.EventTaskReplanned:
		applyTaskSteeringApplied(p.Steering, event.TaskID, event.ID, event.At)
	case core.EventExecutionPlanned:
		var payload struct {
			NodeID        string          `json:"nodeId"`
			WorkerID      string          `json:"workerId,omitempty"`
			WorkerKind    string          `json:"workerKind"`
			PlanID        string          `json:"planId,omitempty"`
			ParentNodeID  string          `json:"parentNodeId,omitempty"`
			SpawnID       string          `json:"spawnId,omitempty"`
			Role          string          `json:"role,omitempty"`
			Reason        string          `json:"reason,omitempty"`
			TargetID      string          `json:"targetId,omitempty"`
			TargetKind    string          `json:"targetKind,omitempty"`
			RemoteSession string          `json:"remoteSession,omitempty"`
			RemoteRunDir  string          `json:"remoteRunDir,omitempty"`
			RemoteWorkDir string          `json:"remoteWorkDir,omitempty"`
			DependsOn     []string        `json:"dependsOn,omitempty"`
			Metadata      json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode execution.node_planned: %w", err)
		}
		node := core.ExecutionNode{
			ID:            payload.NodeID,
			TaskID:        event.TaskID,
			WorkerID:      payload.WorkerID,
			WorkerKind:    payload.WorkerKind,
			Status:        core.WorkerQueued,
			PlanID:        payload.PlanID,
			ParentNodeID:  payload.ParentNodeID,
			SpawnID:       payload.SpawnID,
			Role:          payload.Role,
			Reason:        payload.Reason,
			TargetID:      payload.TargetID,
			TargetKind:    payload.TargetKind,
			RemoteSession: payload.RemoteSession,
			RemoteRunDir:  payload.RemoteRunDir,
			RemoteWorkDir: payload.RemoteWorkDir,
			DependsOn:     payload.DependsOn,
			CreatedAt:     event.At,
			UpdatedAt:     event.At,
			Metadata:      payload.Metadata,
		}
		p.Nodes[payload.NodeID] = node
		if payload.WorkerID != "" {
			p.WorkerNodes[payload.WorkerID] = payload.NodeID
			applySessionExecutionPlanned(p.Sessions, event.TaskID, event.At, sessionExecutionPayload{
				NodeID:        payload.NodeID,
				WorkerID:      payload.WorkerID,
				WorkerKind:    payload.WorkerKind,
				SpawnID:       payload.SpawnID,
				Role:          payload.Role,
				TargetID:      payload.TargetID,
				TargetKind:    payload.TargetKind,
				RemoteSession: payload.RemoteSession,
				RemoteRunDir:  payload.RemoteRunDir,
				RemoteWorkDir: payload.RemoteWorkDir,
				Metadata:      payload.Metadata,
			})
		}
	case core.EventExecutionStatus:
		var payload struct {
			NodeID string            `json:"nodeId"`
			Status core.WorkerStatus `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode execution.node_status: %w", err)
		}
		node := p.Nodes[payload.NodeID]
		if node.ID != "" {
			node.Status = payload.Status
			node.UpdatedAt = event.At
			p.Nodes[payload.NodeID] = node
			if node.WorkerID != "" {
				updateSessionStatus(p.Sessions, node.WorkerID, node.TaskID, event.At, payload.Status)
			}
		}
	case core.EventWorkItemQueued:
		var payload struct {
			ID         string          `json:"id"`
			Kind       string          `json:"kind"`
			TargetKind string          `json:"targetKind,omitempty"`
			TargetID   string          `json:"targetId,omitempty"`
			Reason     string          `json:"reason,omitempty"`
			Prompt     string          `json:"prompt,omitempty"`
			Metadata   json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode work_item.queued: %w", err)
		}
		if payload.ID == "" {
			return nil
		}
		item := p.WorkItems[payload.ID]
		if item.ID == "" {
			item = core.WorkItem{
				ID:        payload.ID,
				TaskID:    event.TaskID,
				CreatedAt: event.At,
			}
		}
		item.Kind = payload.Kind
		item.Status = core.WorkItemQueued
		item.TargetKind = payload.TargetKind
		item.TargetID = payload.TargetID
		item.Reason = payload.Reason
		item.Prompt = payload.Prompt
		item.Error = ""
		item.UpdatedAt = event.At
		item.Metadata = payload.Metadata
		p.WorkItems[payload.ID] = item
	case core.EventWorkItemStarted:
		var payload struct {
			ID         string `json:"id"`
			WorkerID   string `json:"workerId,omitempty"`
			LeaseOwner string `json:"leaseOwner,omitempty"`
			LeaseUntil string `json:"leaseUntil,omitempty"`
			Attempt    int    `json:"attempt,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode work_item.started: %w", err)
		}
		item := p.WorkItems[payload.ID]
		if item.ID != "" {
			attempt := payload.Attempt
			if attempt <= 0 {
				attempt = item.Attempt + 1
			}
			var leaseUntil *time.Time
			if strings.TrimSpace(payload.LeaseUntil) != "" {
				parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(payload.LeaseUntil))
				if err != nil {
					return fmt.Errorf("decode work_item.started leaseUntil: %w", err)
				}
				leaseUntil = &parsed
			}
			item.Status = core.WorkItemRunning
			item.WorkerID = payload.WorkerID
			item.LeaseOwner = payload.LeaseOwner
			item.LeaseUntil = leaseUntil
			item.Attempt = attempt
			item.UpdatedAt = event.At
			p.WorkItems[payload.ID] = item
		}
	case core.EventWorkItemCompleted:
		var payload struct {
			ID       string              `json:"id"`
			Status   core.WorkItemStatus `json:"status"`
			WorkerID string              `json:"workerId,omitempty"`
			Error    string              `json:"error,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode work_item.completed: %w", err)
		}
		item := p.WorkItems[payload.ID]
		if item.ID != "" {
			if payload.Status != "" {
				item.Status = payload.Status
			}
			if payload.WorkerID != "" {
				item.WorkerID = payload.WorkerID
			}
			item.LeaseOwner = ""
			item.LeaseUntil = nil
			item.Error = payload.Error
			item.UpdatedAt = event.At
			p.WorkItems[payload.ID] = item
			applySteeringWorkItemCompleted(p.Steering, item, event.At)
		}
	case core.EventApprovalNeeded:
		if err := applyQuestionNeeded(p.Questions, event); err != nil {
			return err
		}
	case core.EventApprovalDecided:
		if err := applyQuestionDecided(p.Questions, event); err != nil {
			return err
		}
	case core.EventWorkerSteered:
		if err := applySteeringWorkerSteered(p.Steering, p.Workers, p.Nodes, event); err != nil {
			return err
		}
	case core.EventWorkerCreated:
		var payload struct {
			Kind        string          `json:"kind"`
			Command     []string        `json:"command,omitempty"`
			Prompt      string          `json:"prompt,omitempty"`
			PromptPath  string          `json:"promptPath,omitempty"`
			PromptError string          `json:"promptError,omitempty"`
			Metadata    json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode worker.created: %w", err)
		}
		metadata := mergeMetadata(payload.Metadata, p.WorkspaceMetadata[event.WorkerID])
		p.Workers[event.WorkerID] = core.Worker{
			ID:          event.WorkerID,
			TaskID:      event.TaskID,
			Kind:        payload.Kind,
			Status:      core.WorkerQueued,
			Command:     payload.Command,
			Prompt:      payload.Prompt,
			PromptPath:  payload.PromptPath,
			PromptError: payload.PromptError,
			CreatedAt:   event.At,
			UpdatedAt:   event.At,
			Metadata:    metadata,
		}
		applySessionWorkerCreated(p.Sessions, event.TaskID, event.WorkerID, event.At, sessionWorkerPayload{
			Kind:     payload.Kind,
			Metadata: payload.Metadata,
		}, p.WorkspaceMetadata[event.WorkerID])
		if nodeID := p.WorkerNodes[event.WorkerID]; nodeID != "" {
			node := p.Nodes[nodeID]
			node.WorkerKind = payload.Kind
			node.UpdatedAt = event.At
			p.Nodes[nodeID] = node
		}
	case core.EventWorkerWorkspace:
		p.WorkspaceMetadata[event.WorkerID] = event.Payload
		var payload sessionWorkspacePayload
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			applySessionWorkspacePrepared(p.Sessions, event.TaskID, event.WorkerID, event.At, payload, event.Payload)
		}
		worker := p.Workers[event.WorkerID]
		if worker.ID != "" {
			worker.Metadata = mergeMetadata(worker.Metadata, event.Payload)
			worker.UpdatedAt = event.At
			p.Workers[event.WorkerID] = worker
		}
	case core.EventWorkerStarted:
		worker := p.Workers[event.WorkerID]
		worker.Status = core.WorkerRunning
		worker.UpdatedAt = event.At
		p.Workers[event.WorkerID] = worker
		updateSessionStatus(p.Sessions, event.WorkerID, event.TaskID, event.At, core.WorkerRunning)
		if nodeID := p.WorkerNodes[event.WorkerID]; nodeID != "" {
			node := p.Nodes[nodeID]
			node.Status = core.WorkerRunning
			node.UpdatedAt = event.At
			p.Nodes[nodeID] = node
		}
	case core.EventWorkerOutput:
		worker := p.Workers[event.WorkerID]
		if worker.ID != "" && !isTerminalWorkerStatus(worker.Status) {
			worker.UpdatedAt = event.At
			p.Workers[event.WorkerID] = worker
		}
		touchSessionActivity(p.Sessions, event.WorkerID, event.TaskID, event.At, event.ID, event.Payload)
		if nodeID := p.WorkerNodes[event.WorkerID]; nodeID != "" {
			node := p.Nodes[nodeID]
			if node.ID != "" && !isTerminalWorkerStatus(node.Status) {
				node.UpdatedAt = event.At
				p.Nodes[nodeID] = node
			}
		}
	case core.EventWorkerCompleted:
		var payload struct {
			Status core.WorkerStatus `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode worker.completed: %w", err)
		}
		worker := p.Workers[event.WorkerID]
		worker.Status = payload.Status
		worker.UpdatedAt = event.At
		p.Workers[event.WorkerID] = worker
		updateSessionStatus(p.Sessions, event.WorkerID, event.TaskID, event.At, payload.Status)
		if nodeID := p.WorkerNodes[event.WorkerID]; nodeID != "" {
			node := p.Nodes[nodeID]
			node.Status = payload.Status
			node.UpdatedAt = event.At
			p.Nodes[nodeID] = node
		}
	case core.EventWorkerApplied:
		task := p.Tasks[event.TaskID]
		if task.ID != "" {
			task.AppliedWorkerID = event.WorkerID
			task.UpdatedAt = event.At
			p.Tasks[event.TaskID] = task
		}
	case core.EventPRPublished, core.EventPRUpdated:
		var payload struct {
			ID               string          `json:"id"`
			Repo             string          `json:"repo"`
			Number           int             `json:"number,omitempty"`
			URL              string          `json:"url"`
			Branch           string          `json:"branch"`
			Base             string          `json:"base"`
			Title            string          `json:"title"`
			State            string          `json:"state,omitempty"`
			Draft            bool            `json:"draft,omitempty"`
			ChecksStatus     string          `json:"checksStatus,omitempty"`
			ChecksConclusion string          `json:"checksConclusion,omitempty"`
			MergeStatus      string          `json:"mergeStatus,omitempty"`
			Mergeable        string          `json:"mergeable,omitempty"`
			ReviewStatus     string          `json:"reviewStatus,omitempty"`
			BranchOwner      string          `json:"branchOwner,omitempty"`
			BranchOwnerDir   string          `json:"branchOwnerDir,omitempty"`
			BranchHead       string          `json:"branchHead,omitempty"`
			UpdateLeaseOwner string          `json:"updateLeaseOwner,omitempty"`
			UpdateLeaseDir   string          `json:"updateLeaseDir,omitempty"`
			UpdateBaseHead   string          `json:"updateBaseHead,omitempty"`
			Metadata         json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode %s: %w", event.Type, err)
		}
		id := payload.ID
		if id == "" {
			id = fmt.Sprintf("%s#%d", payload.Repo, payload.Number)
		}
		next := core.PullRequest{
			ID:               id,
			TaskID:           event.TaskID,
			Repo:             payload.Repo,
			Number:           payload.Number,
			URL:              payload.URL,
			Branch:           payload.Branch,
			Base:             payload.Base,
			Title:            payload.Title,
			State:            payload.State,
			Draft:            payload.Draft,
			ChecksStatus:     payload.ChecksStatus,
			ChecksConclusion: payload.ChecksConclusion,
			MergeStatus:      payload.MergeStatus,
			Mergeable:        payload.Mergeable,
			ReviewStatus:     payload.ReviewStatus,
			BranchOwner:      payload.BranchOwner,
			BranchOwnerDir:   payload.BranchOwnerDir,
			BranchHead:       payload.BranchHead,
			UpdateLeaseOwner: payload.UpdateLeaseOwner,
			UpdateLeaseDir:   payload.UpdateLeaseDir,
			UpdateBaseHead:   payload.UpdateBaseHead,
			CreatedAt:        event.At,
			UpdatedAt:        event.At,
			Metadata:         payload.Metadata,
		}
		next = hydratePullRequestLeaseFields(next)
		id = resolvePullRequestSnapshotID(id, next, p.PullRequests, p.PullRequestAliases, p.PullRequestIdentities)
		next.ID = id
		if previous := p.PullRequests[id]; previous.ID != "" {
			next = mergePublishedPullRequest(previous, next)
		}
		p.PullRequests[id] = next
		refreshPullRequestFeedbackForPullRequest(p.PullRequestFeedback, next, event.At)
	case core.EventPRStatusChecked:
		var payload struct {
			ID               string          `json:"id"`
			State            string          `json:"state,omitempty"`
			Draft            bool            `json:"draft,omitempty"`
			ChecksStatus     string          `json:"checksStatus,omitempty"`
			ChecksConclusion string          `json:"checksConclusion,omitempty"`
			MergeStatus      string          `json:"mergeStatus,omitempty"`
			Mergeable        string          `json:"mergeable,omitempty"`
			ReviewStatus     string          `json:"reviewStatus,omitempty"`
			Metadata         json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode pull_request.status_checked: %w", err)
		}
		id := payload.ID
		if alias := p.PullRequestAliases[id]; alias != "" {
			id = alias
		}
		pr := p.PullRequests[id]
		if pr.ID != "" {
			if payload.State != "" {
				pr.State = payload.State
			}
			pr.Draft = payload.Draft
			if payload.ChecksStatus != "" {
				pr.ChecksStatus = payload.ChecksStatus
			}
			if payload.ChecksConclusion != "" {
				pr.ChecksConclusion = payload.ChecksConclusion
			}
			if payload.MergeStatus != "" {
				pr.MergeStatus = payload.MergeStatus
			}
			if payload.Mergeable != "" {
				pr.Mergeable = payload.Mergeable
			}
			if payload.ReviewStatus != "" {
				pr.ReviewStatus = payload.ReviewStatus
			}
			pr.UpdatedAt = event.At
			if len(payload.Metadata) > 0 {
				pr.Metadata = mergePullRequestMetadata(pr.Metadata, payload.Metadata)
			}
			p.PullRequests[id] = pr
			refreshPullRequestFeedbackForPullRequest(p.PullRequestFeedback, pr, event.At)
		}
	case core.EventPRBabysitter:
		var payload struct {
			ID               string `json:"id"`
			BabysitterTaskID string `json:"babysitterTaskId"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode pull_request.babysitter_started: %w", err)
		}
		pr := p.PullRequests[payload.ID]
		if pr.ID != "" {
			pr.BabysitterTaskID = payload.BabysitterTaskID
			pr.UpdatedAt = event.At
			p.PullRequests[payload.ID] = pr
			refreshPullRequestFeedbackForPullRequest(p.PullRequestFeedback, pr, event.At)
		}
	case core.EventPRFollowUp:
		if err := applyPullRequestFeedbackQueued(p.PullRequestFeedback, p.PullRequests, p.PullRequestAliases, event); err != nil {
			return err
		}
	case core.EventTaskAction:
		if err := applyPullRequestFeedbackAction(p.PullRequestFeedback, event); err != nil {
			return err
		}
		if entry, ok := memoryEntryFromTaskAction(p.Tasks, event); ok {
			p.MemoryEntries[entry.ID] = entry
		}
	}
	return nil
}

func memoryEntryFromTaskAction(tasks map[string]core.Task, event core.Event) (core.MemoryEntry, bool) {
	var payload struct {
		Kind     string          `json:"kind"`
		Status   string          `json:"status,omitempty"`
		Reason   string          `json:"reason,omitempty"`
		Summary  string          `json:"summary,omitempty"`
		WorkerID string          `json:"workerId,omitempty"`
		Metadata json.RawMessage `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return core.MemoryEntry{}, false
	}
	kind := strings.TrimSpace(payload.Kind)
	summary := strings.TrimSpace(nonEmptyString(payload.Summary, payload.Reason))
	if summary == "" {
		return core.MemoryEntry{}, false
	}
	important := kind == "worker_result_digest" || payload.Status == "failed" || payload.Status == "waiting" || payload.Status == "rejected" || highValueMemoryText(summary)
	if !important {
		return core.MemoryEntry{}, false
	}
	task := tasks[event.TaskID]
	return core.MemoryEntry{
		ID:            fmt.Sprintf("memory-%d", event.ID),
		ProjectID:     task.ProjectID,
		TaskID:        event.TaskID,
		Kind:          nonEmptyString(kind, "task_action"),
		SourceEventID: event.ID,
		SourceEvent:   string(event.Type),
		WorkerID:      nonEmptyString(payload.WorkerID, event.WorkerID),
		Summary:       summary,
		CreatedAt:     event.At,
		UpdatedAt:     event.At,
		Metadata:      payload.Metadata,
	}, true
}

func highValueMemoryText(text string) bool {
	lower := strings.ToLower(text)
	for _, marker := range []string{"decision:", "decided", "blocked", "blocker", "root cause", "baseline", "benchmark", "regression", "invariant"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func (s *SQLiteStore) snapshotFromReadModel(ctx context.Context, includeEvents bool) (core.Snapshot, error) {
	state, lastEventID, current, err := s.loadCurrentReadModel(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if !current {
		state, lastEventID, err = s.catchUpReadModel(ctx)
		if err != nil {
			return core.Snapshot{}, err
		}
	}
	if err := applyWorkerOutputWatermarks(ctx, s.db, &state, nil); err != nil {
		return core.Snapshot{}, err
	}
	var events []core.Event
	if includeEvents {
		events, err = s.eventsUpTo(ctx, lastEventID)
		if err != nil {
			return core.Snapshot{}, err
		}
	}
	return state.snapshot(lastEventID, events, includeEvents), nil
}

func (s *SQLiteStore) taskCardsFromReadModel(ctx context.Context) (core.Snapshot, error) {
	state, lastEventID, current, err := s.loadCurrentReadModel(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if !current {
		state, lastEventID, err = s.catchUpReadModel(ctx)
		if err != nil {
			return core.Snapshot{}, err
		}
	}
	activeTasks := activeProjectionTasks(state.Tasks, state.ClearedTasks)
	state.Workers = filterTasks(state.Workers, state.ClearedTasks, activeTasks, func(worker core.Worker) string { return worker.TaskID })
	state.Nodes = filterTasks(state.Nodes, state.ClearedTasks, activeTasks, func(node core.ExecutionNode) string { return node.TaskID })
	state.WorkItems = filterTasks(state.WorkItems, state.ClearedTasks, activeTasks, func(item core.WorkItem) string { return item.TaskID })
	for id, item := range state.WorkItems {
		if item.Status == core.WorkItemSucceeded || item.Status == core.WorkItemFailed || item.Status == core.WorkItemCanceled {
			delete(state.WorkItems, id)
		}
	}
	state.Questions = filterTasks(state.Questions, state.ClearedTasks, activeTasks, func(question core.Question) string { return question.TaskID })
	for id, question := range state.Questions {
		if question.Decided {
			delete(state.Questions, id)
		}
	}
	state.Sessions = filterTasks(state.Sessions, state.ClearedTasks, activeTasks, func(session core.Session) string { return session.TaskID })
	for id, session := range state.Sessions {
		if isTerminalWorkerStatus(session.Status) {
			delete(state.Sessions, id)
		}
	}
	state.PullRequests = filterTasks(state.PullRequests, state.ClearedTasks, activeTasks, func(pr core.PullRequest) string { return pr.TaskID })
	state.PullRequestFeedback = filterTasks(state.PullRequestFeedback, state.ClearedTasks, activeTasks, func(feedback core.PullRequestFeedback) string { return feedback.TaskID })
	for id, feedback := range state.PullRequestFeedback {
		if feedback.Status != "pending" {
			delete(state.PullRequestFeedback, id)
		}
	}
	state.Steering = filterTasks(state.Steering, state.ClearedTasks, activeTasks, func(item core.SteeringItem) string { return item.TaskID })
	for id, item := range state.Steering {
		if item.Status != "pending" {
			delete(state.Steering, id)
		}
	}
	if err := applyWorkerOutputWatermarks(ctx, s.db, &state, activeTasks); err != nil {
		return core.Snapshot{}, err
	}
	return state.taskCardsSnapshot(lastEventID), nil
}

func (s *SQLiteStore) taskAssignmentsFromReadModel(ctx context.Context, taskID string) (core.Snapshot, error) {
	lastEventID, err := s.ensureReadModelCurrent(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	state := newReadModelState()
	if err := loadProjectionTask(ctx, s.db, state.Tasks, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadClearedTask(ctx, s.db, state.ClearedTasks, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if state.ClearedTasks[taskID] || state.Tasks[taskID].ID == "" {
		return core.Snapshot{LastEventID: lastEventID, Events: snapshotResponseEvents(nil, false)}, nil
	}
	if err := loadProjectionWorkersForTask(ctx, s.db, state.Workers, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionExecutionNodesForTask(ctx, s.db, state.Nodes, taskID); err != nil {
		return core.Snapshot{}, err
	}
	for _, node := range state.Nodes {
		if node.WorkerID != "" {
			state.WorkerNodes[node.WorkerID] = node.ID
		}
	}
	if err := loadProjectionWorkItemsForTask(ctx, s.db, state.WorkItems, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionArtifactsForTask(ctx, s.db, state.Artifacts, taskID); err != nil {
		return core.Snapshot{}, err
	}
	mergeArtifactsFromTasks(state.Artifacts, state.Tasks)
	if err := loadProjectionQuestionsForTask(ctx, s.db, state.Questions, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionSessionsForTask(ctx, s.db, state.Sessions, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionPullRequestsForTask(ctx, s.db, state.PullRequests, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionPullRequestFeedbackForTask(ctx, s.db, state.PullRequestFeedback, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionSteeringForTask(ctx, s.db, state.Steering, taskID); err != nil {
		return core.Snapshot{}, err
	}
	if err := applyWorkerOutputWatermarksForTask(ctx, s.db, &state, taskID); err != nil {
		return core.Snapshot{}, err
	}
	return state.snapshot(lastEventID, nil, false), nil
}

func (s *SQLiteStore) ensureReadModelCurrent(ctx context.Context) (int64, error) {
	var lastEventID int64
	err := s.db.QueryRowContext(ctx, `
SELECT last_event_id
FROM projection_meta
WHERE id = 1`).Scan(&lastEventID)
	ok := true
	if errorsIsNoRows(err) {
		ok = false
	} else if err != nil {
		return 0, err
	}
	latestEventID, err := s.latestEventID(ctx)
	if err != nil {
		return 0, err
	}
	if ok && lastEventID == latestEventID {
		return lastEventID, nil
	}
	if !ok && latestEventID == 0 {
		return 0, nil
	}
	_, lastEventID, err = s.catchUpReadModel(ctx)
	if err != nil {
		return 0, err
	}
	return lastEventID, nil
}

func (s *SQLiteStore) loadCurrentReadModel(ctx context.Context) (readModelState, int64, bool, error) {
	state, lastEventID, ok, err := loadProjectionReadModel(ctx, s.db)
	if err != nil {
		return readModelState{}, 0, false, err
	}
	latestEventID, err := s.latestEventID(ctx)
	if err != nil {
		return readModelState{}, 0, false, err
	}
	if !ok {
		if latestEventID == 0 {
			return newReadModelState(), 0, true, nil
		}
		return readModelState{}, 0, false, nil
	}
	return state, lastEventID, lastEventID == latestEventID, nil
}

func (s *SQLiteStore) rebuildReadModel(ctx context.Context) (readModelState, int64, error) {
	var state readModelState
	var lastEventID int64
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		var err error
		state, lastEventID, err = rebuildReadModelTx(ctx, tx)
		return err
	})
	return state, lastEventID, err
}

func (s *SQLiteStore) catchUpReadModel(ctx context.Context) (readModelState, int64, error) {
	var state readModelState
	var lastEventID int64
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		var err error
		state, lastEventID, err = catchUpReadModelTx(ctx, tx)
		return err
	})
	return state, lastEventID, err
}

func updateProjectionReadModelTx(ctx context.Context, tx *sql.Tx, event core.Event) error {
	state, lastEventID, ok, err := loadProjectionReadModel(ctx, tx)
	if err != nil {
		return err
	}
	if !ok || lastEventID != event.ID-1 {
		_, _, err := catchUpReadModelTx(ctx, tx)
		return err
	}
	if event.Type == core.EventWorkerOutput {
		if err := saveWorkerOutputWatermark(ctx, tx, event); err != nil {
			return err
		}
		return advanceProjectionReadModel(ctx, tx, event.ID)
	}
	if err := state.apply(event); err != nil {
		return err
	}
	return saveProjectionReadModel(ctx, tx, state, event.ID)
}

func catchUpReadModelTx(ctx context.Context, tx *sql.Tx) (readModelState, int64, error) {
	state, lastEventID, ok, err := loadProjectionReadModel(ctx, tx)
	if err != nil {
		return readModelState{}, 0, err
	}
	latestEventID, err := latestEventIDFrom(ctx, tx)
	if err != nil {
		return readModelState{}, 0, err
	}
	if !ok {
		if latestEventID == 0 {
			return newReadModelState(), 0, nil
		}
		return rebuildReadModelTx(ctx, tx)
	}
	if lastEventID == latestEventID {
		return state, lastEventID, nil
	}
	events, err := projectionInputEvents(ctx, tx, lastEventID)
	if err != nil {
		return readModelState{}, 0, err
	}
	changedState := false
	for _, event := range events {
		if event.Type == core.EventWorkerOutput {
			if err := saveWorkerOutputWatermark(ctx, tx, event); err != nil {
				return readModelState{}, 0, err
			}
		} else {
			if err := state.apply(event); err != nil {
				return readModelState{}, 0, err
			}
			changedState = true
		}
		lastEventID = event.ID
	}
	if changedState {
		if err := saveProjectionReadModel(ctx, tx, state, lastEventID); err != nil {
			return readModelState{}, 0, err
		}
		return state, lastEventID, nil
	}
	if err := advanceProjectionReadModel(ctx, tx, lastEventID); err != nil {
		return readModelState{}, 0, err
	}
	return state, lastEventID, nil
}

func rebuildReadModelTx(ctx context.Context, tx *sql.Tx) (readModelState, int64, error) {
	events, err := projectionInputEvents(ctx, tx, 0)
	if err != nil {
		return readModelState{}, 0, err
	}
	state := newReadModelState()
	var lastEventID int64
	for _, event := range events {
		if err := state.apply(event); err != nil {
			return readModelState{}, 0, err
		}
		lastEventID = event.ID
	}
	if err := saveProjectionReadModel(ctx, tx, state, lastEventID); err != nil {
		return readModelState{}, 0, err
	}
	return state, lastEventID, nil
}

type projectionQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func latestEventIDFrom(ctx context.Context, q projectionQuerier) (int64, error) {
	var id sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(id) FROM events`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func loadProjectionReadModel(ctx context.Context, q projectionQuerier) (readModelState, int64, bool, error) {
	var lastEventID int64
	err := q.QueryRowContext(ctx, `
SELECT last_event_id
FROM projection_meta
WHERE id = 1`).Scan(&lastEventID)
	if errorsIsNoRows(err) {
		return readModelState{}, 0, false, nil
	}
	if err != nil {
		return readModelState{}, 0, false, err
	}
	state := newReadModelState()
	if err := loadProjectionTasks(ctx, q, state.Tasks); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionWorkers(ctx, q, state.Workers); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionExecutionNodes(ctx, q, state.Nodes); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionWorkItems(ctx, q, state.WorkItems); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionArtifacts(ctx, q, state.Artifacts); err != nil {
		return readModelState{}, 0, false, err
	}
	mergeArtifactsFromTasks(state.Artifacts, state.Tasks)
	if err := loadProjectionMemoryEntries(ctx, q, state.MemoryEntries); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionQuestions(ctx, q, state.Questions); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionSessions(ctx, q, state.Sessions); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionPullRequests(ctx, q, state.PullRequests); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionPullRequestFeedback(ctx, q, state.PullRequestFeedback); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadProjectionSteering(ctx, q, state.Steering); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadStringMap(ctx, q, `pull_request_aliases`, `alias`, `id`, state.PullRequestAliases); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadStringMap(ctx, q, `pull_request_identities`, `identity`, `id`, state.PullRequestIdentities); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadClearedTasks(ctx, q, `cleared_tasks`, state.ClearedTasks); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadStringMap(ctx, q, `worker_node_links`, `worker_id`, `node_id`, state.WorkerNodes); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadRawMessageMap(ctx, q, `worker_workspace_metadata`, `worker_id`, `data`, state.WorkspaceMetadata); err != nil {
		return readModelState{}, 0, false, err
	}
	return state, lastEventID, true, nil
}

func loadProjectionTasks(ctx context.Context, q projectionQuerier, out map[string]core.Task) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, project_id, workstream_id, title, prompt, status, error, objective_status, objective_phase,
	created_at, updated_at, metadata, applied_worker_id, milestones, work_plan, artifacts
FROM task_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		task, err := scanProjectionTask(rows)
		if err != nil {
			return err
		}
		out[task.ID] = task
	}
	return rows.Err()
}

func loadProjectionTask(ctx context.Context, q projectionQuerier, out map[string]core.Task, taskID string) error {
	row := q.QueryRowContext(ctx, `
SELECT id, project_id, workstream_id, title, prompt, status, error, objective_status, objective_phase,
	created_at, updated_at, metadata, applied_worker_id, milestones, work_plan, artifacts
FROM task_read_models
WHERE id = ?`, taskID)
	task, err := scanProjectionTask(row)
	if errorsIsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	out[task.ID] = task
	return nil
}

func scanProjectionTask(row interface {
	Scan(dest ...any) error
}) (core.Task, error) {
	var task core.Task
	var status string
	var objectiveStatus string
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	var milestones string
	var workPlan string
	var artifacts string
	if err := row.Scan(
		&task.ID,
		&task.ProjectID,
		&task.WorkstreamID,
		&task.Title,
		&task.Prompt,
		&status,
		&task.Error,
		&objectiveStatus,
		&task.ObjectivePhase,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
		&task.AppliedWorkerID,
		&milestones,
		&workPlan,
		&artifacts,
	); err != nil {
		return core.Task{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.Task{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.Task{}, err
	}
	if milestones != "" {
		if err := json.Unmarshal([]byte(milestones), &task.Milestones); err != nil {
			return core.Task{}, err
		}
	}
	if workPlan != "" {
		var plan core.WorkPlan
		if err := json.Unmarshal([]byte(workPlan), &plan); err != nil {
			return core.Task{}, err
		}
		task.WorkPlan = &plan
	}
	if artifacts != "" {
		if err := json.Unmarshal([]byte(artifacts), &task.Artifacts); err != nil {
			return core.Task{}, err
		}
	}
	task.Status = core.TaskStatus(status)
	task.ObjectiveStatus = core.ObjectiveStatus(objectiveStatus)
	task.CreatedAt = createdAt
	task.UpdatedAt = updatedAt
	if metadata != "" {
		task.Metadata = json.RawMessage(metadata)
	}
	return task, nil
}

func loadProjectionWorkers(ctx context.Context, q projectionQuerier, out map[string]core.Worker) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, command, prompt, prompt_path, prompt_error, created_at, updated_at, metadata
FROM worker_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		worker, err := scanProjectionWorker(rows)
		if err != nil {
			return err
		}
		out[worker.ID] = worker
	}
	return rows.Err()
}

func loadProjectionWorkersForTask(ctx context.Context, q projectionQuerier, out map[string]core.Worker, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, command, prompt, prompt_path, prompt_error, created_at, updated_at, metadata
FROM worker_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		worker, err := scanProjectionWorker(rows)
		if err != nil {
			return err
		}
		out[worker.ID] = worker
	}
	return rows.Err()
}

func scanProjectionWorker(row interface {
	Scan(dest ...any) error
}) (core.Worker, error) {
	var worker core.Worker
	var status string
	var command string
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	if err := row.Scan(
		&worker.ID,
		&worker.TaskID,
		&worker.Kind,
		&status,
		&command,
		&worker.Prompt,
		&worker.PromptPath,
		&worker.PromptError,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
	); err != nil {
		return core.Worker{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.Worker{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.Worker{}, err
	}
	if command != "" {
		if err := json.Unmarshal([]byte(command), &worker.Command); err != nil {
			return core.Worker{}, err
		}
	}
	worker.Status = core.WorkerStatus(status)
	worker.CreatedAt = createdAt
	worker.UpdatedAt = updatedAt
	if metadata != "" {
		worker.Metadata = json.RawMessage(metadata)
	}
	return worker, nil
}

func loadActiveProjectionWorkers(ctx context.Context, q projectionQuerier, out map[string]core.Worker) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, created_at, updated_at
FROM worker_read_models
WHERE status NOT IN (?, ?, ?)`, core.WorkerSucceeded, core.WorkerFailed, core.WorkerCanceled)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var worker core.Worker
		var status string
		var createdAtRaw string
		var updatedAtRaw string
		if err := rows.Scan(
			&worker.ID,
			&worker.TaskID,
			&worker.Kind,
			&status,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return err
		}
		createdAt, err := parseReadModelTime(createdAtRaw)
		if err != nil {
			return err
		}
		updatedAt, err := parseReadModelTime(updatedAtRaw)
		if err != nil {
			return err
		}
		worker.Status = core.WorkerStatus(status)
		worker.CreatedAt = createdAt
		worker.UpdatedAt = updatedAt
		out[worker.ID] = worker
	}
	return rows.Err()
}

func loadProjectionExecutionNodes(ctx context.Context, q projectionQuerier, out map[string]core.ExecutionNode) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, worker_kind, status, plan_id, parent_node_id, spawn_id, role,
	reason, target_id, target_kind, remote_session, remote_run_dir, remote_work_dir, depends_on,
	created_at, updated_at, metadata
FROM execution_node_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		node, err := scanProjectionExecutionNode(rows)
		if err != nil {
			return err
		}
		out[node.ID] = node
	}
	return rows.Err()
}

func loadProjectionExecutionNodesForTask(ctx context.Context, q projectionQuerier, out map[string]core.ExecutionNode, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, worker_kind, status, plan_id, parent_node_id, spawn_id, role,
	reason, target_id, target_kind, remote_session, remote_run_dir, remote_work_dir, depends_on,
	created_at, updated_at, metadata
FROM execution_node_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		node, err := scanProjectionExecutionNode(rows)
		if err != nil {
			return err
		}
		out[node.ID] = node
	}
	return rows.Err()
}

func scanProjectionExecutionNode(row interface {
	Scan(dest ...any) error
}) (core.ExecutionNode, error) {
	var node core.ExecutionNode
	var status string
	var dependsOn string
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	if err := row.Scan(
		&node.ID,
		&node.TaskID,
		&node.WorkerID,
		&node.WorkerKind,
		&status,
		&node.PlanID,
		&node.ParentNodeID,
		&node.SpawnID,
		&node.Role,
		&node.Reason,
		&node.TargetID,
		&node.TargetKind,
		&node.RemoteSession,
		&node.RemoteRunDir,
		&node.RemoteWorkDir,
		&dependsOn,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
	); err != nil {
		return core.ExecutionNode{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.ExecutionNode{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.ExecutionNode{}, err
	}
	if dependsOn != "" {
		if err := json.Unmarshal([]byte(dependsOn), &node.DependsOn); err != nil {
			return core.ExecutionNode{}, err
		}
	}
	node.Status = core.WorkerStatus(status)
	node.CreatedAt = createdAt
	node.UpdatedAt = updatedAt
	if metadata != "" {
		node.Metadata = json.RawMessage(metadata)
	}
	return node, nil
}

func loadActiveProjectionExecutionNodes(ctx context.Context, q projectionQuerier, out map[string]core.ExecutionNode) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, worker_kind, status, plan_id, parent_node_id, spawn_id, role,
	reason, target_id, target_kind, remote_session, remote_run_dir, remote_work_dir, created_at, updated_at
FROM execution_node_read_models
WHERE status NOT IN (?, ?, ?)`, core.WorkerSucceeded, core.WorkerFailed, core.WorkerCanceled)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var node core.ExecutionNode
		var status string
		var createdAtRaw string
		var updatedAtRaw string
		if err := rows.Scan(
			&node.ID,
			&node.TaskID,
			&node.WorkerID,
			&node.WorkerKind,
			&status,
			&node.PlanID,
			&node.ParentNodeID,
			&node.SpawnID,
			&node.Role,
			&node.Reason,
			&node.TargetID,
			&node.TargetKind,
			&node.RemoteSession,
			&node.RemoteRunDir,
			&node.RemoteWorkDir,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return err
		}
		createdAt, err := parseReadModelTime(createdAtRaw)
		if err != nil {
			return err
		}
		updatedAt, err := parseReadModelTime(updatedAtRaw)
		if err != nil {
			return err
		}
		node.Status = core.WorkerStatus(status)
		node.CreatedAt = createdAt
		node.UpdatedAt = updatedAt
		out[node.ID] = node
	}
	return rows.Err()
}

func loadProjectionWorkItems(ctx context.Context, q projectionQuerier, out map[string]core.WorkItem) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, target_kind, target_id, reason, prompt, worker_id, lease_owner, lease_until, attempt, error, created_at, updated_at, metadata
FROM work_item_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanProjectionWorkItem(rows)
		if err != nil {
			return err
		}
		out[item.ID] = item
	}
	return rows.Err()
}

func loadProjectionWorkItemsForTask(ctx context.Context, q projectionQuerier, out map[string]core.WorkItem, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, target_kind, target_id, reason, prompt, worker_id, lease_owner, lease_until, attempt, error, created_at, updated_at, metadata
FROM work_item_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanProjectionWorkItem(rows)
		if err != nil {
			return err
		}
		out[item.ID] = item
	}
	return rows.Err()
}

func scanProjectionWorkItem(row interface {
	Scan(dest ...any) error
}) (core.WorkItem, error) {
	var item core.WorkItem
	var status string
	var leaseUntilRaw string
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	if err := row.Scan(
		&item.ID,
		&item.TaskID,
		&item.Kind,
		&status,
		&item.TargetKind,
		&item.TargetID,
		&item.Reason,
		&item.Prompt,
		&item.WorkerID,
		&item.LeaseOwner,
		&leaseUntilRaw,
		&item.Attempt,
		&item.Error,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
	); err != nil {
		return core.WorkItem{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.WorkItem{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.WorkItem{}, err
	}
	item.Status = core.WorkItemStatus(status)
	if strings.TrimSpace(leaseUntilRaw) != "" {
		leaseUntil, err := parseReadModelTime(leaseUntilRaw)
		if err != nil {
			return core.WorkItem{}, err
		}
		item.LeaseUntil = &leaseUntil
	}
	item.CreatedAt = createdAt
	item.UpdatedAt = updatedAt
	if metadata != "" {
		item.Metadata = json.RawMessage(metadata)
	}
	return item, nil
}

func loadActiveProjectionWorkItems(ctx context.Context, q projectionQuerier, out map[string]core.WorkItem) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, target_kind, target_id, reason, worker_id, lease_owner, lease_until, attempt, created_at, updated_at
FROM work_item_read_models
WHERE status NOT IN (?, ?, ?)`, core.WorkItemSucceeded, core.WorkItemFailed, core.WorkItemCanceled)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item core.WorkItem
		var status string
		var leaseUntilRaw string
		var createdAtRaw string
		var updatedAtRaw string
		if err := rows.Scan(
			&item.ID,
			&item.TaskID,
			&item.Kind,
			&status,
			&item.TargetKind,
			&item.TargetID,
			&item.Reason,
			&item.WorkerID,
			&item.LeaseOwner,
			&leaseUntilRaw,
			&item.Attempt,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return err
		}
		createdAt, err := parseReadModelTime(createdAtRaw)
		if err != nil {
			return err
		}
		updatedAt, err := parseReadModelTime(updatedAtRaw)
		if err != nil {
			return err
		}
		item.Status = core.WorkItemStatus(status)
		if strings.TrimSpace(leaseUntilRaw) != "" {
			leaseUntil, err := parseReadModelTime(leaseUntilRaw)
			if err != nil {
				return err
			}
			item.LeaseUntil = &leaseUntil
		}
		item.CreatedAt = createdAt
		item.UpdatedAt = updatedAt
		out[item.ID] = item
	}
	return rows.Err()
}

func loadProjectionArtifacts(ctx context.Context, q projectionQuerier, out map[string]core.Artifact) error {
	return loadArtifactsFromTable(ctx, q, `artifact_read_models`, out)
}

func loadProjectionArtifactsForTask(ctx context.Context, q projectionQuerier, out map[string]core.Artifact, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, name, url, ref, created_at, updated_at, metadata
FROM artifact_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		artifact, err := scanProjectionArtifact(rows)
		if err != nil {
			return err
		}
		out[artifact.ID] = artifact
	}
	return rows.Err()
}

func loadProjectionMemoryEntries(ctx context.Context, q projectionQuerier, out map[string]core.MemoryEntry) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, project_id, task_id, kind, source_event_id, source_event, worker_id, summary, created_at, updated_at, metadata
FROM memory_entry_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var entry core.MemoryEntry
		var createdAtRaw string
		var updatedAtRaw string
		var metadata string
		if err := rows.Scan(
			&entry.ID,
			&entry.ProjectID,
			&entry.TaskID,
			&entry.Kind,
			&entry.SourceEventID,
			&entry.SourceEvent,
			&entry.WorkerID,
			&entry.Summary,
			&createdAtRaw,
			&updatedAtRaw,
			&metadata,
		); err != nil {
			return err
		}
		createdAt, err := parseReadModelTime(createdAtRaw)
		if err != nil {
			return err
		}
		updatedAt, err := parseReadModelTime(updatedAtRaw)
		if err != nil {
			return err
		}
		entry.CreatedAt = createdAt
		entry.UpdatedAt = updatedAt
		if metadata != "" {
			entry.Metadata = json.RawMessage(metadata)
		}
		out[entry.ID] = entry
	}
	return rows.Err()
}

func loadArtifactsFromTable(ctx context.Context, q projectionQuerier, table string, out map[string]core.Artifact) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, name, url, ref, created_at, updated_at, metadata
FROM `+table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		artifact, err := scanProjectionArtifact(rows)
		if err != nil {
			return err
		}
		out[artifact.ID] = artifact
	}
	return rows.Err()
}

func scanProjectionArtifact(row interface {
	Scan(dest ...any) error
}) (core.Artifact, error) {
	var artifact core.Artifact
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	if err := row.Scan(
		&artifact.ID,
		&artifact.TaskID,
		&artifact.Kind,
		&artifact.Name,
		&artifact.URL,
		&artifact.Ref,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
	); err != nil {
		return core.Artifact{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.Artifact{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.Artifact{}, err
	}
	artifact.CreatedAt = createdAt
	artifact.UpdatedAt = updatedAt
	if metadata != "" {
		artifact.Metadata = json.RawMessage(metadata)
	}
	return artifact, nil
}

func mergeArtifactsFromTasks(out map[string]core.Artifact, tasks map[string]core.Task) {
	for taskID, task := range tasks {
		for index, taskArtifact := range task.Artifacts {
			id := taskArtifact.ID
			if id == "" {
				id = fmt.Sprintf("%s-artifact-%d", taskID, index)
			}
			if _, ok := out[id]; ok {
				continue
			}
			out[id] = core.Artifact{
				ID:        id,
				TaskID:    taskID,
				Kind:      taskArtifact.Kind,
				Name:      taskArtifact.Name,
				URL:       taskArtifact.URL,
				Ref:       taskArtifact.Ref,
				CreatedAt: taskArtifact.CreatedAt,
				UpdatedAt: taskArtifact.UpdatedAt,
				Metadata:  taskArtifact.Metadata,
			}
		}
	}
}

func loadProjectionQuestions(ctx context.Context, q projectionQuerier, out map[string]core.Question) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, reason, question, answer, decided, approved, created_at, updated_at, metadata
FROM question_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		question, err := scanProjectionQuestion(rows, true)
		if err != nil {
			return err
		}
		out[question.ID] = question
	}
	return rows.Err()
}

func loadProjectionQuestionsForTask(ctx context.Context, q projectionQuerier, out map[string]core.Question, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, reason, question, answer, decided, approved, created_at, updated_at, metadata
FROM question_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		question, err := scanProjectionQuestion(rows, true)
		if err != nil {
			return err
		}
		out[question.ID] = question
	}
	return rows.Err()
}

func loadActiveProjectionQuestions(ctx context.Context, q projectionQuerier, out map[string]core.Question) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, reason, question, answer, decided, approved, created_at, updated_at, ''
FROM question_read_models
WHERE decided = 0`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		question, err := scanProjectionQuestion(rows, false)
		if err != nil {
			return err
		}
		out[question.ID] = question
	}
	return rows.Err()
}

func scanProjectionQuestion(rows interface {
	Scan(dest ...any) error
}, includeMetadata bool) (core.Question, error) {
	var question core.Question
	var decided int
	var approved string
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	if err := rows.Scan(
		&question.ID,
		&question.TaskID,
		&question.WorkerID,
		&question.Reason,
		&question.Question,
		&question.Answer,
		&decided,
		&approved,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
	); err != nil {
		return core.Question{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.Question{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.Question{}, err
	}
	question.Decided = decided != 0
	if approved == "true" {
		value := true
		question.Approved = &value
	} else if approved == "false" {
		value := false
		question.Approved = &value
	}
	question.CreatedAt = createdAt
	question.UpdatedAt = updatedAt
	if includeMetadata && metadata != "" {
		question.Metadata = json.RawMessage(metadata)
	}
	return question, nil
}

func loadProjectionSessions(ctx context.Context, q projectionQuerier, out map[string]core.Session) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, node_id, worker_kind, role, spawn_id, status, target_id, target_kind,
	remote_session, remote_run_dir, remote_work_dir, workspace_root, workspace_cwd, source_root,
	workspace_name, workspace_mode, vcs_type, shared_root, shared_artifacts_dir, shared_worker_dir,
	provider_session_id, current_action_label, current_action, current_action_at, current_action_event,
	created_at, started_at, updated_at,
	completed_at, metadata
FROM session_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		session, err := scanProjectionSession(rows)
		if err != nil {
			return err
		}
		out[session.ID] = session
	}
	return rows.Err()
}

func loadProjectionSessionsForTask(ctx context.Context, q projectionQuerier, out map[string]core.Session, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, node_id, worker_kind, role, spawn_id, status, target_id, target_kind,
	remote_session, remote_run_dir, remote_work_dir, workspace_root, workspace_cwd, source_root,
	workspace_name, workspace_mode, vcs_type, shared_root, shared_artifacts_dir, shared_worker_dir,
	provider_session_id, current_action_label, current_action, current_action_at, current_action_event,
	created_at, started_at, updated_at,
	completed_at, metadata
FROM session_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		session, err := scanProjectionSession(rows)
		if err != nil {
			return err
		}
		out[session.ID] = session
	}
	return rows.Err()
}

func loadActiveProjectionSessions(ctx context.Context, q projectionQuerier, out map[string]core.Session) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, node_id, worker_kind, role, spawn_id, status, target_id, target_kind,
	remote_session, remote_run_dir, remote_work_dir, workspace_root, workspace_cwd, source_root,
	workspace_name, workspace_mode, vcs_type, shared_root, shared_artifacts_dir, shared_worker_dir,
	provider_session_id, current_action_label, current_action, current_action_at, current_action_event,
	created_at, started_at, updated_at,
	completed_at, ''
FROM session_read_models
WHERE status NOT IN (?, ?, ?)`, core.WorkerSucceeded, core.WorkerFailed, core.WorkerCanceled)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		session, err := scanProjectionSession(rows)
		if err != nil {
			return err
		}
		out[session.ID] = session
	}
	return rows.Err()
}

func scanProjectionSession(rows interface {
	Scan(dest ...any) error
}) (core.Session, error) {
	var session core.Session
	var status string
	var createdAtRaw string
	var startedAtRaw string
	var updatedAtRaw string
	var completedAtRaw string
	var currentActionAtRaw string
	var metadata string
	if err := rows.Scan(
		&session.ID,
		&session.TaskID,
		&session.WorkerID,
		&session.NodeID,
		&session.WorkerKind,
		&session.Role,
		&session.SpawnID,
		&status,
		&session.TargetID,
		&session.TargetKind,
		&session.RemoteSession,
		&session.RemoteRunDir,
		&session.RemoteWorkDir,
		&session.WorkspaceRoot,
		&session.WorkspaceCWD,
		&session.SourceRoot,
		&session.WorkspaceName,
		&session.WorkspaceMode,
		&session.VCSType,
		&session.SharedRoot,
		&session.SharedArtifactsDir,
		&session.SharedWorkerDir,
		&session.ProviderSessionID,
		&session.CurrentActionLabel,
		&session.CurrentAction,
		&currentActionAtRaw,
		&session.CurrentActionEvent,
		&createdAtRaw,
		&startedAtRaw,
		&updatedAtRaw,
		&completedAtRaw,
		&metadata,
	); err != nil {
		return core.Session{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.Session{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.Session{}, err
	}
	startedAt, err := parseOptionalReadModelTime(startedAtRaw)
	if err != nil {
		return core.Session{}, err
	}
	completedAt, err := parseOptionalReadModelTime(completedAtRaw)
	if err != nil {
		return core.Session{}, err
	}
	currentActionAt, err := parseOptionalReadModelTime(currentActionAtRaw)
	if err != nil {
		return core.Session{}, err
	}
	session.Status = core.WorkerStatus(status)
	session.CreatedAt = createdAt
	session.StartedAt = startedAt
	session.UpdatedAt = updatedAt
	session.CompletedAt = completedAt
	session.CurrentActionAt = currentActionAt
	if metadata != "" {
		session.Metadata = json.RawMessage(metadata)
	}
	return session, nil
}

func loadProjectionPullRequests(ctx context.Context, q projectionQuerier, out map[string]core.PullRequest) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, repo, number, url, branch, base, title, state, draft, checks_status,
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id,
	branch_owner, branch_owner_dir, branch_head, update_lease_owner, update_lease_dir, update_base_head,
	created_at, updated_at, metadata
FROM pull_request_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		pr, err := scanProjectionPullRequest(rows)
		if err != nil {
			return err
		}
		out[pr.ID] = pr
	}
	return rows.Err()
}

func loadProjectionPullRequestsForTask(ctx context.Context, q projectionQuerier, out map[string]core.PullRequest, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, repo, number, url, branch, base, title, state, draft, checks_status,
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id,
	branch_owner, branch_owner_dir, branch_head, update_lease_owner, update_lease_dir, update_base_head,
	created_at, updated_at, metadata
FROM pull_request_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		pr, err := scanProjectionPullRequest(rows)
		if err != nil {
			return err
		}
		out[pr.ID] = pr
	}
	return rows.Err()
}

func scanProjectionPullRequest(row interface {
	Scan(dest ...any) error
}) (core.PullRequest, error) {
	var pr core.PullRequest
	var draft int
	var createdAtRaw string
	var updatedAtRaw string
	var metadata string
	if err := row.Scan(
		&pr.ID,
		&pr.TaskID,
		&pr.Repo,
		&pr.Number,
		&pr.URL,
		&pr.Branch,
		&pr.Base,
		&pr.Title,
		&pr.State,
		&draft,
		&pr.ChecksStatus,
		&pr.ChecksConclusion,
		&pr.MergeStatus,
		&pr.Mergeable,
		&pr.ReviewStatus,
		&pr.BabysitterTaskID,
		&pr.BranchOwner,
		&pr.BranchOwnerDir,
		&pr.BranchHead,
		&pr.UpdateLeaseOwner,
		&pr.UpdateLeaseDir,
		&pr.UpdateBaseHead,
		&createdAtRaw,
		&updatedAtRaw,
		&metadata,
	); err != nil {
		return core.PullRequest{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.PullRequest{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.PullRequest{}, err
	}
	pr.Draft = draft != 0
	pr.CreatedAt = createdAt
	pr.UpdatedAt = updatedAt
	if metadata != "" {
		pr.Metadata = json.RawMessage(metadata)
	}
	return pr, nil
}

func parseReadModelTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, value)
}

func parseOptionalReadModelTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := parseReadModelTime(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func formatOptionalReadModelTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func loadStringMap(ctx context.Context, q projectionQuerier, table string, keyColumn string, valueColumn string, out map[string]string) error {
	rows, err := q.QueryContext(ctx, `SELECT `+keyColumn+`, `+valueColumn+` FROM `+table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		out[key] = value
	}
	return rows.Err()
}

func loadRawMessageMap(ctx context.Context, q projectionQuerier, table string, keyColumn string, valueColumn string, out map[string]json.RawMessage) error {
	rows, err := q.QueryContext(ctx, `SELECT `+keyColumn+`, `+valueColumn+` FROM `+table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		out[key] = json.RawMessage(value)
	}
	return rows.Err()
}

func loadClearedTasks(ctx context.Context, q projectionQuerier, table string, out map[string]bool) error {
	rows, err := q.QueryContext(ctx, `SELECT task_id FROM `+table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			return err
		}
		out[taskID] = true
	}
	return rows.Err()
}

func loadClearedTask(ctx context.Context, q projectionQuerier, out map[string]bool, taskID string) error {
	var id string
	err := q.QueryRowContext(ctx, `SELECT task_id FROM cleared_tasks WHERE task_id = ?`, taskID).Scan(&id)
	if errorsIsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	out[id] = true
	return nil
}

func loadProjectionPullRequestFeedback(ctx context.Context, q projectionQuerier, out map[string]core.PullRequestFeedback) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, pull_request_id, event_id, attempt, status, reason, repo, number, url, branch, base,
	state, checks_status, merge_status, review_status, feedback_signature, feedback_body, prompt,
	created_at, updated_at, handled_at, metadata
FROM pull_request_feedback_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		feedback, err := scanProjectionPullRequestFeedback(rows)
		if err != nil {
			return err
		}
		out[feedback.ID] = feedback
	}
	return rows.Err()
}

func loadProjectionPullRequestFeedbackForTask(ctx context.Context, q projectionQuerier, out map[string]core.PullRequestFeedback, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, pull_request_id, event_id, attempt, status, reason, repo, number, url, branch, base,
	state, checks_status, merge_status, review_status, feedback_signature, feedback_body, prompt,
	created_at, updated_at, handled_at, metadata
FROM pull_request_feedback_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		feedback, err := scanProjectionPullRequestFeedback(rows)
		if err != nil {
			return err
		}
		out[feedback.ID] = feedback
	}
	return rows.Err()
}

func loadPendingProjectionPullRequestFeedback(ctx context.Context, q projectionQuerier, out map[string]core.PullRequestFeedback) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, pull_request_id, event_id, attempt, status, reason, repo, number, url, branch, base,
	state, checks_status, merge_status, review_status, feedback_signature, '', '',
	created_at, updated_at, handled_at, ''
FROM pull_request_feedback_read_models
WHERE status = 'pending'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		feedback, err := scanProjectionPullRequestFeedback(rows)
		if err != nil {
			return err
		}
		out[feedback.ID] = feedback
	}
	return rows.Err()
}

func scanProjectionPullRequestFeedback(rows interface {
	Scan(dest ...any) error
}) (core.PullRequestFeedback, error) {
	var feedback core.PullRequestFeedback
	var createdAtRaw string
	var updatedAtRaw string
	var handledAtRaw string
	var metadata string
	if err := rows.Scan(
		&feedback.ID,
		&feedback.TaskID,
		&feedback.PullRequestID,
		&feedback.EventID,
		&feedback.Attempt,
		&feedback.Status,
		&feedback.Reason,
		&feedback.Repo,
		&feedback.Number,
		&feedback.URL,
		&feedback.Branch,
		&feedback.Base,
		&feedback.State,
		&feedback.ChecksStatus,
		&feedback.MergeStatus,
		&feedback.ReviewStatus,
		&feedback.FeedbackSignature,
		&feedback.FeedbackBody,
		&feedback.Prompt,
		&createdAtRaw,
		&updatedAtRaw,
		&handledAtRaw,
		&metadata,
	); err != nil {
		return core.PullRequestFeedback{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.PullRequestFeedback{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.PullRequestFeedback{}, err
	}
	handledAt, err := parseOptionalReadModelTime(handledAtRaw)
	if err != nil {
		return core.PullRequestFeedback{}, err
	}
	feedback.CreatedAt = createdAt
	feedback.UpdatedAt = updatedAt
	feedback.HandledAt = handledAt
	if metadata != "" {
		feedback.Metadata = json.RawMessage(metadata)
	}
	return feedback, nil
}

func loadProjectionSteering(ctx context.Context, q projectionQuerier, out map[string]core.SteeringItem) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, node_id, worker_kind, role, spawn_id, candidate_worker_id, review_phase,
	target_kind, target_id, status, reason, message, created_at, updated_at, applied_at, metadata
FROM steering_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanProjectionSteering(rows)
		if err != nil {
			return err
		}
		out[item.ID] = item
	}
	return rows.Err()
}

func loadProjectionSteeringForTask(ctx context.Context, q projectionQuerier, out map[string]core.SteeringItem, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, node_id, worker_kind, role, spawn_id, candidate_worker_id, review_phase,
	target_kind, target_id, status, reason, message, created_at, updated_at, applied_at, metadata
FROM steering_read_models
WHERE task_id = ?`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanProjectionSteering(rows)
		if err != nil {
			return err
		}
		out[item.ID] = item
	}
	return rows.Err()
}

func loadPendingProjectionSteering(ctx context.Context, q projectionQuerier, out map[string]core.SteeringItem) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, node_id, worker_kind, role, spawn_id, candidate_worker_id, review_phase,
	target_kind, target_id, status, reason, message, created_at, updated_at, applied_at, ''
FROM steering_read_models
WHERE status = 'pending'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanProjectionSteering(rows)
		if err != nil {
			return err
		}
		out[item.ID] = item
	}
	return rows.Err()
}

func scanProjectionSteering(rows interface {
	Scan(dest ...any) error
}) (core.SteeringItem, error) {
	var item core.SteeringItem
	var createdAtRaw string
	var updatedAtRaw string
	var appliedAtRaw string
	var metadata string
	if err := rows.Scan(
		&item.ID,
		&item.TaskID,
		&item.WorkerID,
		&item.NodeID,
		&item.WorkerKind,
		&item.Role,
		&item.SpawnID,
		&item.CandidateWorkerID,
		&item.ReviewPhase,
		&item.TargetKind,
		&item.TargetID,
		&item.Status,
		&item.Reason,
		&item.Message,
		&createdAtRaw,
		&updatedAtRaw,
		&appliedAtRaw,
		&metadata,
	); err != nil {
		return core.SteeringItem{}, err
	}
	createdAt, err := parseReadModelTime(createdAtRaw)
	if err != nil {
		return core.SteeringItem{}, err
	}
	updatedAt, err := parseReadModelTime(updatedAtRaw)
	if err != nil {
		return core.SteeringItem{}, err
	}
	appliedAt, err := parseOptionalReadModelTime(appliedAtRaw)
	if err != nil {
		return core.SteeringItem{}, err
	}
	item.CreatedAt = createdAt
	item.UpdatedAt = updatedAt
	item.AppliedAt = appliedAt
	if metadata != "" {
		item.Metadata = json.RawMessage(metadata)
	}
	return item, nil
}

func saveProjectionReadModel(ctx context.Context, q projectionQuerier, state readModelState, lastEventID int64) error {
	state.ensure()
	if err := saveProjectionTasks(ctx, q, state.Tasks); err != nil {
		return err
	}
	if err := saveProjectionWorkers(ctx, q, state.Workers); err != nil {
		return err
	}
	if err := saveProjectionExecutionNodes(ctx, q, state.Nodes); err != nil {
		return err
	}
	if err := saveProjectionWorkItems(ctx, q, state.WorkItems); err != nil {
		return err
	}
	if err := saveProjectionArtifacts(ctx, q, state.Artifacts); err != nil {
		return err
	}
	if err := saveProjectionMemoryEntries(ctx, q, state.MemoryEntries); err != nil {
		return err
	}
	if err := saveProjectionQuestions(ctx, q, state.Questions); err != nil {
		return err
	}
	if err := saveProjectionSessions(ctx, q, state.Sessions); err != nil {
		return err
	}
	if err := saveProjectionPullRequests(ctx, q, state.PullRequests); err != nil {
		return err
	}
	if err := saveProjectionPullRequestFeedback(ctx, q, state.PullRequestFeedback); err != nil {
		return err
	}
	if err := saveProjectionSteering(ctx, q, state.Steering); err != nil {
		return err
	}
	if err := saveStringMap(ctx, q, `pull_request_aliases`, `alias`, `id`, state.PullRequestAliases); err != nil {
		return err
	}
	if err := saveStringMap(ctx, q, `pull_request_identities`, `identity`, `id`, state.PullRequestIdentities); err != nil {
		return err
	}
	if err := saveClearedTasks(ctx, q, `cleared_tasks`, state.ClearedTasks); err != nil {
		return err
	}
	if err := saveStringMap(ctx, q, `worker_node_links`, `worker_id`, `node_id`, state.WorkerNodes); err != nil {
		return err
	}
	if err := saveRawMessageMap(ctx, q, `worker_workspace_metadata`, `worker_id`, `data`, state.WorkspaceMetadata); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `
INSERT INTO projection_meta (id, last_event_id, updated_at)
VALUES (1, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	last_event_id = excluded.last_event_id,
	updated_at = excluded.updated_at`,
		lastEventID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func saveProjectionTasks(ctx context.Context, q projectionQuerier, tasks map[string]core.Task) error {
	seen := map[string]bool{}
	for _, task := range tasks {
		if task.ID == "" {
			continue
		}
		seen[task.ID] = true
		milestones, err := jsonString(task.Milestones, "[]")
		if err != nil {
			return err
		}
		workPlan := ""
		if task.WorkPlan != nil {
			workPlan, err = jsonString(task.WorkPlan, "")
			if err != nil {
				return err
			}
		}
		artifacts, err := jsonString(task.Artifacts, "[]")
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `
INSERT INTO task_read_models (
	id, project_id, workstream_id, title, prompt, status, error, objective_status, objective_phase,
	created_at, updated_at, metadata, applied_worker_id, milestones, work_plan, artifacts
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	project_id = excluded.project_id,
	workstream_id = excluded.workstream_id,
	title = excluded.title,
	prompt = excluded.prompt,
	status = excluded.status,
	error = excluded.error,
	objective_status = excluded.objective_status,
	objective_phase = excluded.objective_phase,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata,
	applied_worker_id = excluded.applied_worker_id,
	milestones = excluded.milestones,
	work_plan = excluded.work_plan,
	artifacts = excluded.artifacts`,
			task.ID,
			task.ProjectID,
			task.WorkstreamID,
			task.Title,
			task.Prompt,
			string(task.Status),
			task.Error,
			string(task.ObjectiveStatus),
			task.ObjectivePhase,
			task.CreatedAt.Format(time.RFC3339Nano),
			task.UpdatedAt.Format(time.RFC3339Nano),
			string(task.Metadata),
			task.AppliedWorkerID,
			milestones,
			workPlan,
			artifacts,
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `task_read_models`, `id`, seen)
}

func saveProjectionWorkers(ctx context.Context, q projectionQuerier, workers map[string]core.Worker) error {
	seen := map[string]bool{}
	for _, worker := range workers {
		if worker.ID == "" {
			continue
		}
		seen[worker.ID] = true
		command, err := jsonString(worker.Command, "[]")
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `
INSERT INTO worker_read_models (
	id, task_id, kind, status, command, prompt, prompt_path, prompt_error, created_at, updated_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	kind = excluded.kind,
	status = excluded.status,
	command = excluded.command,
	prompt = excluded.prompt,
	prompt_path = excluded.prompt_path,
	prompt_error = excluded.prompt_error,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata`,
			worker.ID,
			worker.TaskID,
			worker.Kind,
			string(worker.Status),
			command,
			worker.Prompt,
			worker.PromptPath,
			worker.PromptError,
			worker.CreatedAt.Format(time.RFC3339Nano),
			worker.UpdatedAt.Format(time.RFC3339Nano),
			string(worker.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `worker_read_models`, `id`, seen)
}

func saveProjectionExecutionNodes(ctx context.Context, q projectionQuerier, nodes map[string]core.ExecutionNode) error {
	seen := map[string]bool{}
	for _, node := range nodes {
		if node.ID == "" {
			continue
		}
		seen[node.ID] = true
		dependsOn, err := jsonString(node.DependsOn, "[]")
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `
INSERT INTO execution_node_read_models (
	id, task_id, worker_id, worker_kind, status, plan_id, parent_node_id, spawn_id, role,
	reason, target_id, target_kind, remote_session, remote_run_dir, remote_work_dir, depends_on,
	created_at, updated_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	worker_id = excluded.worker_id,
	worker_kind = excluded.worker_kind,
	status = excluded.status,
	plan_id = excluded.plan_id,
	parent_node_id = excluded.parent_node_id,
	spawn_id = excluded.spawn_id,
	role = excluded.role,
	reason = excluded.reason,
	target_id = excluded.target_id,
	target_kind = excluded.target_kind,
	remote_session = excluded.remote_session,
	remote_run_dir = excluded.remote_run_dir,
	remote_work_dir = excluded.remote_work_dir,
	depends_on = excluded.depends_on,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata`,
			node.ID,
			node.TaskID,
			node.WorkerID,
			node.WorkerKind,
			string(node.Status),
			node.PlanID,
			node.ParentNodeID,
			node.SpawnID,
			node.Role,
			node.Reason,
			node.TargetID,
			node.TargetKind,
			node.RemoteSession,
			node.RemoteRunDir,
			node.RemoteWorkDir,
			dependsOn,
			node.CreatedAt.Format(time.RFC3339Nano),
			node.UpdatedAt.Format(time.RFC3339Nano),
			string(node.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `execution_node_read_models`, `id`, seen)
}

func saveProjectionWorkItems(ctx context.Context, q projectionQuerier, items map[string]core.WorkItem) error {
	seen := map[string]bool{}
	for _, item := range items {
		if item.ID == "" {
			continue
		}
		seen[item.ID] = true
		if _, err := q.ExecContext(ctx, `
	INSERT INTO work_item_read_models (
		id, task_id, kind, status, target_kind, target_id, reason, prompt, worker_id, lease_owner, lease_until, attempt, error,
		created_at, updated_at, metadata
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
		task_id = excluded.task_id,
		kind = excluded.kind,
		status = excluded.status,
		target_kind = excluded.target_kind,
		target_id = excluded.target_id,
		reason = excluded.reason,
		prompt = excluded.prompt,
		worker_id = excluded.worker_id,
		lease_owner = excluded.lease_owner,
		lease_until = excluded.lease_until,
		attempt = excluded.attempt,
		error = excluded.error,
		created_at = excluded.created_at,
		updated_at = excluded.updated_at,
		metadata = excluded.metadata`,
			item.ID,
			item.TaskID,
			item.Kind,
			string(item.Status),
			item.TargetKind,
			item.TargetID,
			item.Reason,
			item.Prompt,
			item.WorkerID,
			item.LeaseOwner,
			formatOptionalReadModelTime(item.LeaseUntil),
			item.Attempt,
			item.Error,
			item.CreatedAt.Format(time.RFC3339Nano),
			item.UpdatedAt.Format(time.RFC3339Nano),
			string(item.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `work_item_read_models`, `id`, seen)
}

func saveProjectionArtifacts(ctx context.Context, q projectionQuerier, artifacts map[string]core.Artifact) error {
	return saveArtifactsToTable(ctx, q, `artifact_read_models`, artifacts)
}

func saveProjectionMemoryEntries(ctx context.Context, q projectionQuerier, entries map[string]core.MemoryEntry) error {
	seen := map[string]bool{}
	for _, entry := range entries {
		if entry.ID == "" {
			continue
		}
		seen[entry.ID] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO memory_entry_read_models (
	id, project_id, task_id, kind, source_event_id, source_event, worker_id, summary,
	created_at, updated_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	project_id = excluded.project_id,
	task_id = excluded.task_id,
	kind = excluded.kind,
	source_event_id = excluded.source_event_id,
	source_event = excluded.source_event,
	worker_id = excluded.worker_id,
	summary = excluded.summary,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata`,
			entry.ID,
			entry.ProjectID,
			entry.TaskID,
			entry.Kind,
			entry.SourceEventID,
			entry.SourceEvent,
			entry.WorkerID,
			entry.Summary,
			entry.CreatedAt.Format(time.RFC3339Nano),
			entry.UpdatedAt.Format(time.RFC3339Nano),
			string(entry.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `memory_entry_read_models`, `id`, seen)
}

func saveArtifactsToTable(ctx context.Context, q projectionQuerier, table string, artifacts map[string]core.Artifact) error {
	seen := map[string]bool{}
	for _, artifact := range artifacts {
		if artifact.ID == "" {
			continue
		}
		seen[artifact.ID] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO `+table+` (
	id, task_id, kind, name, url, ref, created_at, updated_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	kind = excluded.kind,
	name = excluded.name,
	url = excluded.url,
	ref = excluded.ref,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata`,
			artifact.ID,
			artifact.TaskID,
			artifact.Kind,
			artifact.Name,
			artifact.URL,
			artifact.Ref,
			artifact.CreatedAt.Format(time.RFC3339Nano),
			artifact.UpdatedAt.Format(time.RFC3339Nano),
			string(artifact.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, table, `id`, seen)
}

func saveProjectionQuestions(ctx context.Context, q projectionQuerier, questions map[string]core.Question) error {
	seen := map[string]bool{}
	for _, question := range questions {
		if question.ID == "" {
			continue
		}
		seen[question.ID] = true
		decided := 0
		if question.Decided {
			decided = 1
		}
		approved := ""
		if question.Approved != nil {
			if *question.Approved {
				approved = "true"
			} else {
				approved = "false"
			}
		}
		if _, err := q.ExecContext(ctx, `
	INSERT INTO question_read_models (
		id, task_id, worker_id, reason, question, answer, decided, approved, created_at, updated_at, metadata
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
		task_id = excluded.task_id,
		worker_id = excluded.worker_id,
		reason = excluded.reason,
		question = excluded.question,
		answer = excluded.answer,
		decided = excluded.decided,
		approved = excluded.approved,
		created_at = excluded.created_at,
		updated_at = excluded.updated_at,
		metadata = excluded.metadata`,
			question.ID,
			question.TaskID,
			question.WorkerID,
			question.Reason,
			question.Question,
			question.Answer,
			decided,
			approved,
			question.CreatedAt.Format(time.RFC3339Nano),
			question.UpdatedAt.Format(time.RFC3339Nano),
			string(question.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `question_read_models`, `id`, seen)
}

func saveProjectionSessions(ctx context.Context, q projectionQuerier, sessions map[string]core.Session) error {
	seen := map[string]bool{}
	for _, session := range sessions {
		if session.ID == "" {
			continue
		}
		seen[session.ID] = true
		if _, err := q.ExecContext(ctx, `
	INSERT INTO session_read_models (
		id, task_id, worker_id, node_id, worker_kind, role, spawn_id, status, target_id, target_kind,
		remote_session, remote_run_dir, remote_work_dir, workspace_root, workspace_cwd, source_root,
		workspace_name, workspace_mode, vcs_type, shared_root, shared_artifacts_dir, shared_worker_dir,
		provider_session_id, current_action_label, current_action, current_action_at, current_action_event,
		created_at, started_at, updated_at,
		completed_at, metadata
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
		task_id = excluded.task_id,
		worker_id = excluded.worker_id,
		node_id = excluded.node_id,
		worker_kind = excluded.worker_kind,
		role = excluded.role,
		spawn_id = excluded.spawn_id,
		status = excluded.status,
		target_id = excluded.target_id,
		target_kind = excluded.target_kind,
		remote_session = excluded.remote_session,
		remote_run_dir = excluded.remote_run_dir,
		remote_work_dir = excluded.remote_work_dir,
		workspace_root = excluded.workspace_root,
		workspace_cwd = excluded.workspace_cwd,
		source_root = excluded.source_root,
		workspace_name = excluded.workspace_name,
		workspace_mode = excluded.workspace_mode,
		vcs_type = excluded.vcs_type,
		shared_root = excluded.shared_root,
		shared_artifacts_dir = excluded.shared_artifacts_dir,
		shared_worker_dir = excluded.shared_worker_dir,
		provider_session_id = excluded.provider_session_id,
		current_action_label = excluded.current_action_label,
		current_action = excluded.current_action,
		current_action_at = excluded.current_action_at,
		current_action_event = excluded.current_action_event,
		created_at = excluded.created_at,
		started_at = excluded.started_at,
		updated_at = excluded.updated_at,
		completed_at = excluded.completed_at,
		metadata = excluded.metadata`,
			session.ID,
			session.TaskID,
			session.WorkerID,
			session.NodeID,
			session.WorkerKind,
			session.Role,
			session.SpawnID,
			string(session.Status),
			session.TargetID,
			session.TargetKind,
			session.RemoteSession,
			session.RemoteRunDir,
			session.RemoteWorkDir,
			session.WorkspaceRoot,
			session.WorkspaceCWD,
			session.SourceRoot,
			session.WorkspaceName,
			session.WorkspaceMode,
			session.VCSType,
			session.SharedRoot,
			session.SharedArtifactsDir,
			session.SharedWorkerDir,
			session.ProviderSessionID,
			session.CurrentActionLabel,
			session.CurrentAction,
			formatOptionalReadModelTime(session.CurrentActionAt),
			session.CurrentActionEvent,
			session.CreatedAt.Format(time.RFC3339Nano),
			formatOptionalReadModelTime(session.StartedAt),
			session.UpdatedAt.Format(time.RFC3339Nano),
			formatOptionalReadModelTime(session.CompletedAt),
			string(session.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `session_read_models`, `id`, seen)
}

func saveProjectionPullRequests(ctx context.Context, q projectionQuerier, pullRequests map[string]core.PullRequest) error {
	seen := map[string]bool{}
	for _, pr := range pullRequests {
		if pr.ID == "" {
			continue
		}
		seen[pr.ID] = true
		draft := 0
		if pr.Draft {
			draft = 1
		}
		if _, err := q.ExecContext(ctx, `
INSERT INTO pull_request_read_models (
	id, task_id, repo, number, url, branch, base, title, state, draft, checks_status,
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id,
	branch_owner, branch_owner_dir, branch_head, update_lease_owner, update_lease_dir, update_base_head,
	created_at, updated_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	repo = excluded.repo,
	number = excluded.number,
	url = excluded.url,
	branch = excluded.branch,
	base = excluded.base,
	title = excluded.title,
	state = excluded.state,
	draft = excluded.draft,
	checks_status = excluded.checks_status,
	checks_conclusion = excluded.checks_conclusion,
	merge_status = excluded.merge_status,
	mergeable = excluded.mergeable,
	review_status = excluded.review_status,
	babysitter_task_id = excluded.babysitter_task_id,
	branch_owner = excluded.branch_owner,
	branch_owner_dir = excluded.branch_owner_dir,
	branch_head = excluded.branch_head,
	update_lease_owner = excluded.update_lease_owner,
	update_lease_dir = excluded.update_lease_dir,
	update_base_head = excluded.update_base_head,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata`,
			pr.ID,
			pr.TaskID,
			pr.Repo,
			pr.Number,
			pr.URL,
			pr.Branch,
			pr.Base,
			pr.Title,
			pr.State,
			draft,
			pr.ChecksStatus,
			pr.ChecksConclusion,
			pr.MergeStatus,
			pr.Mergeable,
			pr.ReviewStatus,
			pr.BabysitterTaskID,
			pr.BranchOwner,
			pr.BranchOwnerDir,
			pr.BranchHead,
			pr.UpdateLeaseOwner,
			pr.UpdateLeaseDir,
			pr.UpdateBaseHead,
			pr.CreatedAt.Format(time.RFC3339Nano),
			pr.UpdatedAt.Format(time.RFC3339Nano),
			string(pr.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `pull_request_read_models`, `id`, seen)
}

func saveProjectionPullRequestFeedback(ctx context.Context, q projectionQuerier, feedbackRows map[string]core.PullRequestFeedback) error {
	seen := map[string]bool{}
	for _, feedback := range feedbackRows {
		if feedback.ID == "" {
			continue
		}
		seen[feedback.ID] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO pull_request_feedback_read_models (
	id, task_id, pull_request_id, event_id, attempt, status, reason, repo, number, url, branch, base,
	state, checks_status, merge_status, review_status, feedback_signature, feedback_body, prompt,
	created_at, updated_at, handled_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	pull_request_id = excluded.pull_request_id,
	event_id = excluded.event_id,
	attempt = excluded.attempt,
	status = excluded.status,
	reason = excluded.reason,
	repo = excluded.repo,
	number = excluded.number,
	url = excluded.url,
	branch = excluded.branch,
	base = excluded.base,
	state = excluded.state,
	checks_status = excluded.checks_status,
	merge_status = excluded.merge_status,
	review_status = excluded.review_status,
	feedback_signature = excluded.feedback_signature,
	feedback_body = excluded.feedback_body,
	prompt = excluded.prompt,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	handled_at = excluded.handled_at,
	metadata = excluded.metadata`,
			feedback.ID,
			feedback.TaskID,
			feedback.PullRequestID,
			feedback.EventID,
			feedback.Attempt,
			feedback.Status,
			feedback.Reason,
			feedback.Repo,
			feedback.Number,
			feedback.URL,
			feedback.Branch,
			feedback.Base,
			feedback.State,
			feedback.ChecksStatus,
			feedback.MergeStatus,
			feedback.ReviewStatus,
			feedback.FeedbackSignature,
			feedback.FeedbackBody,
			feedback.Prompt,
			feedback.CreatedAt.Format(time.RFC3339Nano),
			feedback.UpdatedAt.Format(time.RFC3339Nano),
			formatOptionalReadModelTime(feedback.HandledAt),
			string(feedback.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `pull_request_feedback_read_models`, `id`, seen)
}

func saveProjectionSteering(ctx context.Context, q projectionQuerier, steering map[string]core.SteeringItem) error {
	seen := map[string]bool{}
	for _, item := range steering {
		if item.ID == "" {
			continue
		}
		seen[item.ID] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO steering_read_models (
	id, task_id, worker_id, node_id, worker_kind, role, spawn_id, candidate_worker_id, review_phase,
	target_kind, target_id, status, reason, message, created_at, updated_at, applied_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	worker_id = excluded.worker_id,
	node_id = excluded.node_id,
	worker_kind = excluded.worker_kind,
	role = excluded.role,
	spawn_id = excluded.spawn_id,
	candidate_worker_id = excluded.candidate_worker_id,
	review_phase = excluded.review_phase,
	target_kind = excluded.target_kind,
	target_id = excluded.target_id,
	status = excluded.status,
	reason = excluded.reason,
	message = excluded.message,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	applied_at = excluded.applied_at,
	metadata = excluded.metadata`,
			item.ID,
			item.TaskID,
			item.WorkerID,
			item.NodeID,
			item.WorkerKind,
			item.Role,
			item.SpawnID,
			item.CandidateWorkerID,
			item.ReviewPhase,
			item.TargetKind,
			item.TargetID,
			item.Status,
			item.Reason,
			item.Message,
			item.CreatedAt.Format(time.RFC3339Nano),
			item.UpdatedAt.Format(time.RFC3339Nano),
			formatOptionalReadModelTime(item.AppliedAt),
			string(item.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `steering_read_models`, `id`, seen)
}

func saveStringMap(ctx context.Context, q projectionQuerier, table string, keyColumn string, valueColumn string, values map[string]string) error {
	seen := map[string]bool{}
	for key, value := range values {
		seen[key] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO `+table+` (`+keyColumn+`, `+valueColumn+`)
VALUES (?, ?)
ON CONFLICT(`+keyColumn+`) DO UPDATE SET
	`+valueColumn+` = excluded.`+valueColumn+`
WHERE `+valueColumn+` != excluded.`+valueColumn, key, value); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, table, keyColumn, seen)
}

func saveRawMessageMap(ctx context.Context, q projectionQuerier, table string, keyColumn string, valueColumn string, values map[string]json.RawMessage) error {
	seen := map[string]bool{}
	for key, value := range values {
		seen[key] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO `+table+` (`+keyColumn+`, `+valueColumn+`)
VALUES (?, ?)
ON CONFLICT(`+keyColumn+`) DO UPDATE SET
	`+valueColumn+` = excluded.`+valueColumn+`
WHERE `+valueColumn+` != excluded.`+valueColumn, key, string(value)); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, table, keyColumn, seen)
}

func saveClearedTasks(ctx context.Context, q projectionQuerier, table string, values map[string]bool) error {
	seen := map[string]bool{}
	for taskID, cleared := range values {
		if !cleared {
			continue
		}
		seen[taskID] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO `+table+` (task_id)
VALUES (?)
ON CONFLICT(task_id) DO NOTHING`, taskID); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, table, `task_id`, seen)
}

func deleteMissingRows(ctx context.Context, q projectionQuerier, table string, key string, keep map[string]bool) error {
	rows, err := q.QueryContext(ctx, `SELECT `+key+` FROM `+table)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if !keep[id] {
			stale = append(stale, id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range stale {
		if _, err := q.ExecContext(ctx, `DELETE FROM `+table+` WHERE `+key+` = ?`, id); err != nil {
			return err
		}
	}
	return nil
}

func advanceProjectionReadModel(ctx context.Context, q projectionQuerier, lastEventID int64) error {
	_, err := q.ExecContext(ctx, `
UPDATE projection_meta
SET last_event_id = ?, updated_at = ?
WHERE id = 1`,
		lastEventID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func saveWorkerOutputWatermark(ctx context.Context, q projectionQuerier, event core.Event) error {
	label, currentAction := compactWorkerOutputActivity(event.Payload)
	at := event.At.Format(time.RFC3339Nano)
	_, err := q.ExecContext(ctx, `
INSERT INTO worker_output_watermarks (worker_id, task_id, event_id, at, label, current_action)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(worker_id) DO UPDATE SET
	task_id = excluded.task_id,
	event_id = excluded.event_id,
	at = excluded.at,
	label = excluded.label,
	current_action = excluded.current_action
WHERE excluded.event_id > worker_output_watermarks.event_id`,
		event.WorkerID,
		event.TaskID,
		event.ID,
		at,
		label,
		currentAction,
	)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `
UPDATE session_read_models
SET current_action_label = ?,
	current_action = ?,
	current_action_at = ?,
	current_action_event = ?,
	updated_at = CASE WHEN status NOT IN (?, ?, ?) THEN ? ELSE updated_at END
WHERE worker_id = ? AND current_action_event < ?`,
		label,
		currentAction,
		at,
		event.ID,
		core.WorkerSucceeded,
		core.WorkerFailed,
		core.WorkerCanceled,
		at,
		event.WorkerID,
		event.ID,
	)
	return err
}

func compactWorkerOutputActivity(payload json.RawMessage) (string, string) {
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", ""
	}
	kind := strings.TrimSpace(stringFromAny(decoded["kind"]))
	stream := strings.TrimSpace(stringFromAny(decoded["stream"]))
	label := strings.Join(nonEmptyStrings(kind, stream), ":")
	text := strings.TrimSpace(stringFromAny(decoded["text"]))
	raw := mapFromAny(decoded["raw"])
	if len(raw) == 0 {
		raw = mapFromAny(decoded["rawResult"])
	}
	item := mapFromAny(raw["item"])
	if itemType := strings.TrimSpace(stringFromAny(item["type"])); itemType != "" {
		label = strings.Join(nonEmptyStrings(itemType, kind), ":")
	}
	if text == "" {
		for _, key := range []string{"text", "message", "description", "result", "command"} {
			if value := strings.TrimSpace(stringFromAny(item[key])); value != "" {
				text = value
				break
			}
		}
	}
	if text == "" {
		for _, key := range []string{"description", "result", "message"} {
			if value := strings.TrimSpace(stringFromAny(raw[key])); value != "" {
				text = value
				break
			}
		}
	}
	if label == "" {
		label = "output"
	}
	return truncateSessionAction(label, 80), truncateSessionAction(text, 600)
}

func stringFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func mapFromAny(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return nil
}

func nonEmptyStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func truncateSessionAction(value string, limit int) string {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func applyWorkerOutputWatermarks(ctx context.Context, q projectionQuerier, state *readModelState, taskIDs map[string]bool) error {
	rows, err := q.QueryContext(ctx, `
SELECT worker_id, task_id, event_id, at
FROM worker_output_watermarks
ORDER BY event_id ASC`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var workerID string
		var taskID string
		var eventID int64
		var atRaw string
		if err := rows.Scan(&workerID, &taskID, &eventID, &atRaw); err != nil {
			return err
		}
		if taskIDs != nil && !taskIDs[taskID] {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, atRaw)
		if err != nil {
			continue
		}
		worker := state.Workers[workerID]
		if worker.ID != "" && !isTerminalWorkerStatus(worker.Status) && at.After(worker.UpdatedAt) {
			worker.UpdatedAt = at
			state.Workers[workerID] = worker
		}
		if nodeID := state.WorkerNodes[workerID]; nodeID != "" {
			node := state.Nodes[nodeID]
			if node.ID != "" && !isTerminalWorkerStatus(node.Status) && at.After(node.UpdatedAt) {
				node.UpdatedAt = at
				state.Nodes[nodeID] = node
			}
		}
	}
	return rows.Err()
}

func applyWorkerOutputWatermarksForTask(ctx context.Context, q projectionQuerier, state *readModelState, taskID string) error {
	rows, err := q.QueryContext(ctx, `
SELECT worker_id, task_id, event_id, at
FROM worker_output_watermarks
WHERE task_id = ?
ORDER BY event_id ASC`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var workerID string
		var watermarkTaskID string
		var eventID int64
		var atRaw string
		if err := rows.Scan(&workerID, &watermarkTaskID, &eventID, &atRaw); err != nil {
			return err
		}
		at, err := time.Parse(time.RFC3339Nano, atRaw)
		if err != nil {
			continue
		}
		worker := state.Workers[workerID]
		if worker.ID != "" && !isTerminalWorkerStatus(worker.Status) && at.After(worker.UpdatedAt) {
			worker.UpdatedAt = at
			state.Workers[workerID] = worker
		}
		if nodeID := state.WorkerNodes[workerID]; nodeID != "" {
			node := state.Nodes[nodeID]
			if node.ID != "" && !isTerminalWorkerStatus(node.Status) && at.After(node.UpdatedAt) {
				node.UpdatedAt = at
				state.Nodes[nodeID] = node
			}
		}
		_ = eventID
		_ = watermarkTaskID
	}
	return rows.Err()
}

func projectionInputEvents(ctx context.Context, q projectionQuerier, afterID int64) ([]core.Event, error) {
	rows, err := q.QueryContext(ctx, `
WITH latest_worker_output AS (
	SELECT MAX(id) AS id
	FROM events
	WHERE id > ? AND type = 'worker.output'
	GROUP BY worker_id
)
SELECT
	id,
	at,
	type,
	task_id,
	worker_id,
	payload
FROM events
WHERE id > ? AND type != 'worker.output'
UNION ALL
SELECT
	id,
	at,
	type,
	task_id,
	worker_id,
	payload
FROM events
WHERE id IN (SELECT id FROM latest_worker_output)
ORDER BY id ASC`, afterID, afterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *SQLiteStore) eventsUpTo(ctx context.Context, maxID int64) ([]core.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, at, type, task_id, worker_id, payload
FROM events
WHERE id <= ?
ORDER BY id ASC`, maxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func errorsIsNoRows(err error) bool {
	return err == sql.ErrNoRows
}
