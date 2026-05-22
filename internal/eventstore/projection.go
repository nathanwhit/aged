package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"aged/internal/core"
)

type readModelState struct {
	Tasks                 map[string]core.Task          `json:"tasks"`
	Workers               map[string]core.Worker        `json:"workers"`
	Nodes                 map[string]core.ExecutionNode `json:"nodes"`
	PullRequests          map[string]core.PullRequest   `json:"pullRequests"`
	PullRequestAliases    map[string]string             `json:"pullRequestAliases"`
	PullRequestIdentities map[string]string             `json:"pullRequestIdentities"`
	ClearedTasks          map[string]bool               `json:"clearedTasks"`
	WorkerNodes           map[string]string             `json:"workerNodes"`
	WorkspaceMetadata     map[string]json.RawMessage    `json:"workspaceMetadata"`
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
	if p.PullRequests == nil {
		p.PullRequests = map[string]core.PullRequest{}
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
	return core.Snapshot{
		Tasks:               orderedTasks(filteredTasks),
		Workers:             orderedWorkers(filterClearedWorkers(p.Workers, p.ClearedTasks)),
		ExecutionNodes:      orderedExecutionNodes(filteredNodes),
		PullRequests:        orderedPullRequests(filterClearedPullRequests(p.PullRequests, p.ClearedTasks)),
		OrchestrationGraphs: orchestrationGraphs(filteredTasks, filteredNodes),
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
	workers = compactCardWorkers(workers)
	nodes = compactCardExecutionNodes(nodes)
	pullRequests = compactCardPullRequests(pullRequests)
	return core.Snapshot{
		Tasks:               orderedTasks(taskCards),
		Workers:             orderedWorkers(workers),
		ExecutionNodes:      orderedExecutionNodes(nodes),
		PullRequests:        orderedPullRequests(pullRequests),
		OrchestrationGraphs: orchestrationGraphs(taskCards, nodes),
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
		"completionMode",
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
	case core.EventTaskCandidate:
		var payload struct {
			WorkerID string `json:"workerId"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode task.final_candidate_selected: %w", err)
		}
		task := p.Tasks[event.TaskID]
		task.FinalCandidateWorkerID = payload.WorkerID
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
		task := p.Tasks[event.TaskID]
		task.Artifacts = upsertTaskArtifact(task.Artifacts, core.TaskArtifact{
			ID:        payload.ID,
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
	case core.EventTaskCleared:
		p.ClearedTasks[event.TaskID] = true
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
		if nodeID := p.WorkerNodes[event.WorkerID]; nodeID != "" {
			node := p.Nodes[nodeID]
			node.WorkerKind = payload.Kind
			node.UpdatedAt = event.At
			p.Nodes[nodeID] = node
		}
	case core.EventWorkerWorkspace:
		p.WorkspaceMetadata[event.WorkerID] = event.Payload
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
			CreatedAt:        event.At,
			UpdatedAt:        event.At,
			Metadata:         payload.Metadata,
		}
		id = resolvePullRequestSnapshotID(id, next, p.PullRequests, p.PullRequestAliases, p.PullRequestIdentities)
		next.ID = id
		if previous := p.PullRequests[id]; previous.ID != "" {
			next = mergePublishedPullRequest(previous, next)
		}
		p.PullRequests[id] = next
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
		}
	}
	return nil
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
	state, lastEventID, current, err := s.loadCurrentTaskCardReadModel(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if !current {
		state, lastEventID, err = s.catchUpReadModel(ctx)
		if err != nil {
			return core.Snapshot{}, err
		}
		state = compactSnapshotCardState(state)
	}
	activeTasks := activeProjectionTasks(state.Tasks, state.ClearedTasks)
	state.Workers = filterTasks(state.Workers, state.ClearedTasks, activeTasks, func(worker core.Worker) string { return worker.TaskID })
	state.Nodes = filterTasks(state.Nodes, state.ClearedTasks, activeTasks, func(node core.ExecutionNode) string { return node.TaskID })
	state.PullRequests = filterTasks(state.PullRequests, state.ClearedTasks, activeTasks, func(pr core.PullRequest) string { return pr.TaskID })
	if err := applyWorkerOutputWatermarks(ctx, s.db, &state, activeTasks); err != nil {
		return core.Snapshot{}, err
	}
	return state.taskCardsSnapshot(lastEventID), nil
}

func (s *SQLiteStore) loadCurrentTaskCardReadModel(ctx context.Context) (readModelState, int64, bool, error) {
	state, lastEventID, ok, err := loadTaskCardReadModel(ctx, s.db)
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
	return state, lastEventID, nil
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
		if err := advanceProjectionReadModel(ctx, tx, event.ID); err != nil {
			return err
		}
		return advanceTaskCardReadModel(ctx, tx, event.ID)
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
	if err := advanceTaskCardReadModel(ctx, tx, lastEventID); err != nil {
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
	if err := loadProjectionPullRequests(ctx, q, state.PullRequests); err != nil {
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
	created_at, updated_at, metadata, final_candidate_worker_id, applied_worker_id, milestones, work_plan, artifacts
FROM task_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var task core.Task
		var status string
		var objectiveStatus string
		var createdAtRaw string
		var updatedAtRaw string
		var metadata string
		var milestones string
		var workPlan string
		var artifacts string
		if err := rows.Scan(
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
			&task.FinalCandidateWorkerID,
			&task.AppliedWorkerID,
			&milestones,
			&workPlan,
			&artifacts,
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
		if milestones != "" {
			if err := json.Unmarshal([]byte(milestones), &task.Milestones); err != nil {
				return err
			}
		}
		if workPlan != "" {
			var plan core.WorkPlan
			if err := json.Unmarshal([]byte(workPlan), &plan); err != nil {
				return err
			}
			task.WorkPlan = &plan
		}
		if artifacts != "" {
			if err := json.Unmarshal([]byte(artifacts), &task.Artifacts); err != nil {
				return err
			}
		}
		task.Status = core.TaskStatus(status)
		task.ObjectiveStatus = core.ObjectiveStatus(objectiveStatus)
		task.CreatedAt = createdAt
		task.UpdatedAt = updatedAt
		if metadata != "" {
			task.Metadata = json.RawMessage(metadata)
		}
		out[task.ID] = task
	}
	return rows.Err()
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
		var worker core.Worker
		var status string
		var command string
		var createdAtRaw string
		var updatedAtRaw string
		var metadata string
		if err := rows.Scan(
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
		if command != "" {
			if err := json.Unmarshal([]byte(command), &worker.Command); err != nil {
				return err
			}
		}
		worker.Status = core.WorkerStatus(status)
		worker.CreatedAt = createdAt
		worker.UpdatedAt = updatedAt
		if metadata != "" {
			worker.Metadata = json.RawMessage(metadata)
		}
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
		var node core.ExecutionNode
		var status string
		var dependsOn string
		var createdAtRaw string
		var updatedAtRaw string
		var metadata string
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
			&dependsOn,
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
		if dependsOn != "" {
			if err := json.Unmarshal([]byte(dependsOn), &node.DependsOn); err != nil {
				return err
			}
		}
		node.Status = core.WorkerStatus(status)
		node.CreatedAt = createdAt
		node.UpdatedAt = updatedAt
		if metadata != "" {
			node.Metadata = json.RawMessage(metadata)
		}
		out[node.ID] = node
	}
	return rows.Err()
}

func loadProjectionPullRequests(ctx context.Context, q projectionQuerier, out map[string]core.PullRequest) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, repo, number, url, branch, base, title, state, draft, checks_status,
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id, created_at, updated_at, metadata
FROM pull_request_read_models`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var pr core.PullRequest
		var draft int
		var createdAtRaw string
		var updatedAtRaw string
		var metadata string
		if err := rows.Scan(
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
		pr.Draft = draft != 0
		pr.CreatedAt = createdAt
		pr.UpdatedAt = updatedAt
		if metadata != "" {
			pr.Metadata = json.RawMessage(metadata)
		}
		out[pr.ID] = pr
	}
	return rows.Err()
}

func loadTaskCardReadModel(ctx context.Context, q projectionQuerier) (readModelState, int64, bool, error) {
	var lastEventID int64
	err := q.QueryRowContext(ctx, `
SELECT last_event_id
FROM task_card_meta
WHERE id = 1`).Scan(&lastEventID)
	if errorsIsNoRows(err) {
		return readModelState{}, 0, false, nil
	}
	if err != nil {
		return readModelState{}, 0, false, err
	}
	state := newReadModelState()
	if err := loadTaskCardTasks(ctx, q, state.Tasks); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadTaskCardWorkers(ctx, q, state.Workers); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadTaskCardExecutionNodes(ctx, q, state.Nodes); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadTaskCardPullRequests(ctx, q, state.PullRequests); err != nil {
		return readModelState{}, 0, false, err
	}
	if err := loadStringMap(ctx, q, `task_card_worker_nodes`, `worker_id`, `node_id`, state.WorkerNodes); err != nil {
		return readModelState{}, 0, false, err
	}
	return state, lastEventID, true, nil
}

func loadTaskCardTasks(ctx context.Context, q projectionQuerier, out map[string]core.Task) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, project_id, workstream_id, title, status, error, objective_status, objective_phase,
	created_at, updated_at, metadata, final_candidate_worker_id, applied_worker_id
FROM task_cards`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var task core.Task
		var status string
		var objectiveStatus string
		var createdAtRaw string
		var updatedAtRaw string
		var metadata string
		if err := rows.Scan(
			&task.ID,
			&task.ProjectID,
			&task.WorkstreamID,
			&task.Title,
			&status,
			&task.Error,
			&objectiveStatus,
			&task.ObjectivePhase,
			&createdAtRaw,
			&updatedAtRaw,
			&metadata,
			&task.FinalCandidateWorkerID,
			&task.AppliedWorkerID,
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
		task.Status = core.TaskStatus(status)
		task.ObjectiveStatus = core.ObjectiveStatus(objectiveStatus)
		task.CreatedAt = createdAt
		task.UpdatedAt = updatedAt
		if metadata != "" {
			task.Metadata = json.RawMessage(metadata)
		}
		out[task.ID] = task
	}
	return rows.Err()
}

func loadTaskCardWorkers(ctx context.Context, q projectionQuerier, out map[string]core.Worker) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, kind, status, command, prompt_path, prompt_error, created_at, updated_at
FROM task_card_workers`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var worker core.Worker
		var status string
		var command string
		var createdAtRaw string
		var updatedAtRaw string
		if err := rows.Scan(
			&worker.ID,
			&worker.TaskID,
			&worker.Kind,
			&status,
			&command,
			&worker.PromptPath,
			&worker.PromptError,
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
		if command != "" {
			if err := json.Unmarshal([]byte(command), &worker.Command); err != nil {
				return err
			}
		}
		worker.Status = core.WorkerStatus(status)
		worker.CreatedAt = createdAt
		worker.UpdatedAt = updatedAt
		out[worker.ID] = worker
	}
	return rows.Err()
}

func loadTaskCardExecutionNodes(ctx context.Context, q projectionQuerier, out map[string]core.ExecutionNode) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, worker_id, worker_kind, status, plan_id, parent_node_id, spawn_id, role,
	reason, target_id, target_kind, remote_session, remote_run_dir, remote_work_dir, depends_on,
	created_at, updated_at
FROM task_card_execution_nodes`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var node core.ExecutionNode
		var status string
		var dependsOn string
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
			&dependsOn,
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
		if dependsOn != "" {
			if err := json.Unmarshal([]byte(dependsOn), &node.DependsOn); err != nil {
				return err
			}
		}
		node.Status = core.WorkerStatus(status)
		node.CreatedAt = createdAt
		node.UpdatedAt = updatedAt
		out[node.ID] = node
	}
	return rows.Err()
}

func loadTaskCardPullRequests(ctx context.Context, q projectionQuerier, out map[string]core.PullRequest) error {
	rows, err := q.QueryContext(ctx, `
SELECT id, task_id, repo, number, url, branch, base, title, state, draft, checks_status,
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id, created_at, updated_at
FROM task_card_pull_requests`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var pr core.PullRequest
		var draft int
		var createdAtRaw string
		var updatedAtRaw string
		if err := rows.Scan(
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
		pr.Draft = draft != 0
		pr.CreatedAt = createdAt
		pr.UpdatedAt = updatedAt
		out[pr.ID] = pr
	}
	return rows.Err()
}

func parseReadModelTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, value)
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
	if err := saveProjectionPullRequests(ctx, q, state.PullRequests); err != nil {
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
	if err != nil {
		return err
	}
	return saveTaskCardReadModel(ctx, q, state, lastEventID)
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
	created_at, updated_at, metadata, final_candidate_worker_id, applied_worker_id, milestones, work_plan, artifacts
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
	final_candidate_worker_id = excluded.final_candidate_worker_id,
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
			task.FinalCandidateWorkerID,
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
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id, created_at, updated_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
			pr.CreatedAt.Format(time.RFC3339Nano),
			pr.UpdatedAt.Format(time.RFC3339Nano),
			string(pr.Metadata),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `pull_request_read_models`, `id`, seen)
}

func saveTaskCardReadModel(ctx context.Context, q projectionQuerier, state readModelState, lastEventID int64) error {
	cardState := compactSnapshotCardState(state)
	if err := saveTaskCards(ctx, q, cardState.Tasks); err != nil {
		return err
	}
	if err := saveTaskCardWorkers(ctx, q, cardState.Workers); err != nil {
		return err
	}
	if err := saveTaskCardExecutionNodes(ctx, q, cardState.Nodes); err != nil {
		return err
	}
	if err := saveTaskCardPullRequests(ctx, q, cardState.PullRequests); err != nil {
		return err
	}
	if err := saveStringMap(ctx, q, `task_card_worker_nodes`, `worker_id`, `node_id`, cardState.WorkerNodes); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `
INSERT INTO task_card_meta (id, last_event_id, updated_at)
VALUES (1, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	last_event_id = excluded.last_event_id,
	updated_at = excluded.updated_at`,
		lastEventID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func saveTaskCards(ctx context.Context, q projectionQuerier, tasks map[string]core.Task) error {
	seen := map[string]bool{}
	for _, task := range tasks {
		if task.ID == "" {
			continue
		}
		seen[task.ID] = true
		if _, err := q.ExecContext(ctx, `
INSERT INTO task_cards (
	id, project_id, workstream_id, title, status, error, objective_status, objective_phase,
	created_at, updated_at, metadata, final_candidate_worker_id, applied_worker_id
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	project_id = excluded.project_id,
	workstream_id = excluded.workstream_id,
	title = excluded.title,
	status = excluded.status,
	error = excluded.error,
	objective_status = excluded.objective_status,
	objective_phase = excluded.objective_phase,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at,
	metadata = excluded.metadata,
	final_candidate_worker_id = excluded.final_candidate_worker_id,
	applied_worker_id = excluded.applied_worker_id`,
			task.ID,
			task.ProjectID,
			task.WorkstreamID,
			task.Title,
			string(task.Status),
			task.Error,
			string(task.ObjectiveStatus),
			task.ObjectivePhase,
			task.CreatedAt.Format(time.RFC3339Nano),
			task.UpdatedAt.Format(time.RFC3339Nano),
			string(task.Metadata),
			task.FinalCandidateWorkerID,
			task.AppliedWorkerID,
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `task_cards`, `id`, seen)
}

func saveTaskCardWorkers(ctx context.Context, q projectionQuerier, workers map[string]core.Worker) error {
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
INSERT INTO task_card_workers (
	id, task_id, kind, status, command, prompt_path, prompt_error, created_at, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	task_id = excluded.task_id,
	kind = excluded.kind,
	status = excluded.status,
	command = excluded.command,
	prompt_path = excluded.prompt_path,
	prompt_error = excluded.prompt_error,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at`,
			worker.ID,
			worker.TaskID,
			worker.Kind,
			string(worker.Status),
			command,
			worker.PromptPath,
			worker.PromptError,
			worker.CreatedAt.Format(time.RFC3339Nano),
			worker.UpdatedAt.Format(time.RFC3339Nano),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `task_card_workers`, `id`, seen)
}

func saveTaskCardExecutionNodes(ctx context.Context, q projectionQuerier, nodes map[string]core.ExecutionNode) error {
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
INSERT INTO task_card_execution_nodes (
	id, task_id, worker_id, worker_kind, status, plan_id, parent_node_id, spawn_id, role,
	reason, target_id, target_kind, remote_session, remote_run_dir, remote_work_dir, depends_on,
	created_at, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
	updated_at = excluded.updated_at`,
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
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `task_card_execution_nodes`, `id`, seen)
}

func saveTaskCardPullRequests(ctx context.Context, q projectionQuerier, pullRequests map[string]core.PullRequest) error {
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
INSERT INTO task_card_pull_requests (
	id, task_id, repo, number, url, branch, base, title, state, draft, checks_status,
	checks_conclusion, merge_status, mergeable, review_status, babysitter_task_id, created_at, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
	created_at = excluded.created_at,
	updated_at = excluded.updated_at`,
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
			pr.CreatedAt.Format(time.RFC3339Nano),
			pr.UpdatedAt.Format(time.RFC3339Nano),
		); err != nil {
			return err
		}
	}
	return deleteMissingRows(ctx, q, `task_card_pull_requests`, `id`, seen)
}

func compactSnapshotCardState(state readModelState) readModelState {
	state.ensure()
	out := newReadModelState()
	for id, cleared := range state.ClearedTasks {
		if cleared {
			out.ClearedTasks[id] = true
		}
	}
	for id, task := range state.Tasks {
		if state.ClearedTasks[id] {
			continue
		}
		out.Tasks[id] = compactTaskCard(task)
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		for workerID, worker := range state.Workers {
			if worker.TaskID == id {
				out.Workers[workerID] = compactCardWorker(worker)
			}
		}
		for nodeID, node := range state.Nodes {
			if node.TaskID == id {
				out.Nodes[nodeID] = compactCardExecutionNode(node)
				if node.WorkerID != "" {
					out.WorkerNodes[node.WorkerID] = nodeID
				}
			}
		}
		for prID, pr := range state.PullRequests {
			if pr.TaskID == id {
				out.PullRequests[prID] = compactCardPullRequest(pr)
			}
		}
	}
	for alias, id := range state.PullRequestAliases {
		if _, ok := out.PullRequests[id]; ok {
			out.PullRequestAliases[alias] = id
		}
	}
	for identity, id := range state.PullRequestIdentities {
		if _, ok := out.PullRequests[id]; ok {
			out.PullRequestIdentities[identity] = id
		}
	}
	return out
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

func advanceTaskCardReadModel(ctx context.Context, q projectionQuerier, lastEventID int64) error {
	_, err := q.ExecContext(ctx, `
UPDATE task_card_meta
SET last_event_id = ?, updated_at = ?
WHERE id = 1`,
		lastEventID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func saveWorkerOutputWatermark(ctx context.Context, q projectionQuerier, event core.Event) error {
	_, err := q.ExecContext(ctx, `
INSERT INTO worker_output_watermarks (worker_id, task_id, event_id, at)
VALUES (?, ?, ?, ?)
ON CONFLICT(worker_id) DO UPDATE SET
	task_id = excluded.task_id,
	event_id = excluded.event_id,
	at = excluded.at
WHERE excluded.event_id > worker_output_watermarks.event_id`,
		event.WorkerID,
		event.TaskID,
		event.ID,
		event.At.Format(time.RFC3339Nano),
	)
	return err
}

func applyWorkerOutputWatermarks(ctx context.Context, q projectionQuerier, state *readModelState, taskIDs map[string]bool) error {
	rows, err := q.QueryContext(ctx, `
SELECT worker_id, task_id, at
FROM worker_output_watermarks
ORDER BY event_id ASC`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var workerID string
		var taskID string
		var atRaw string
		if err := rows.Scan(&workerID, &taskID, &atRaw); err != nil {
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
	'{}' AS payload
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
