package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"aged/internal/core"
)

type snapshotProjectionState struct {
	Campaigns             map[string]core.Campaign      `json:"campaigns"`
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

func newSnapshotProjectionState() snapshotProjectionState {
	state := snapshotProjectionState{}
	state.ensure()
	return state
}

func (p *snapshotProjectionState) ensure() {
	if p.Campaigns == nil {
		p.Campaigns = map[string]core.Campaign{}
	}
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

func (p *snapshotProjectionState) snapshot(lastEventID int64, events []core.Event, includeEvents bool) core.Snapshot {
	p.ensure()
	filteredTasks := filterClearedTasks(p.Tasks, p.ClearedTasks)
	filteredNodes := filterClearedExecutionNodes(p.Nodes, p.ClearedTasks)
	campaigns := campaignsWithChildTasks(p.Campaigns, filteredTasks)
	return core.Snapshot{
		Campaigns:           orderedCampaigns(campaigns),
		Tasks:               orderedTasks(filteredTasks),
		Workers:             orderedWorkers(filterClearedWorkers(p.Workers, p.ClearedTasks)),
		ExecutionNodes:      orderedExecutionNodes(filteredNodes),
		PullRequests:        orderedPullRequests(filterClearedPullRequests(p.PullRequests, p.ClearedTasks)),
		OrchestrationGraphs: orchestrationGraphs(filteredTasks, filteredNodes),
		LastEventID:         lastEventID,
		Events:              snapshotResponseEvents(events, includeEvents),
	}
}

func (p *snapshotProjectionState) taskCardsSnapshot(lastEventID int64) core.Snapshot {
	p.ensure()
	filteredTasks := filterClearedTasks(p.Tasks, p.ClearedTasks)
	activeTasks := map[string]bool{}
	taskCards := make(map[string]core.Task, len(filteredTasks))
	for id, task := range filteredTasks {
		if isTerminalTaskStatus(task.Status) {
			task.Prompt = ""
			task.Milestones = nil
			task.Artifacts = nil
		} else {
			activeTasks[id] = true
		}
		taskCards[id] = task
	}
	workers := filterTasks(p.Workers, p.ClearedTasks, activeTasks, func(worker core.Worker) string { return worker.TaskID })
	nodes := filterTasks(p.Nodes, p.ClearedTasks, activeTasks, func(node core.ExecutionNode) string { return node.TaskID })
	pullRequests := filterTasks(p.PullRequests, p.ClearedTasks, activeTasks, func(pr core.PullRequest) string { return pr.TaskID })
	campaigns := campaignsWithChildTasks(p.Campaigns, taskCards)
	return core.Snapshot{
		Campaigns:           orderedCampaigns(campaigns),
		Tasks:               orderedTasks(taskCards),
		Workers:             orderedWorkers(workers),
		ExecutionNodes:      orderedExecutionNodes(nodes),
		PullRequests:        orderedPullRequests(pullRequests),
		OrchestrationGraphs: orchestrationGraphs(taskCards, nodes),
		LastEventID:         lastEventID,
	}
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

func (p *snapshotProjectionState) apply(event core.Event) error {
	p.ensure()
	switch event.Type {
	case core.EventCampaignCreated:
		var payload struct {
			ProjectID string          `json:"projectId,omitempty"`
			Title     string          `json:"title"`
			Prompt    string          `json:"prompt"`
			Metadata  json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode campaign.created: %w", err)
		}
		projectID := payload.ProjectID
		if projectID == "" {
			projectID = projectIDFromMetadata(payload.Metadata)
		}
		p.Campaigns[event.TaskID] = core.Campaign{
			ID:              event.TaskID,
			ProjectID:       projectID,
			RootTaskID:      stringFromMetadata(payload.Metadata, "rootTaskId"),
			Title:           payload.Title,
			Prompt:          payload.Prompt,
			Status:          core.CampaignActive,
			ObjectiveStatus: core.ObjectiveActive,
			ObjectivePhase:  "active",
			CreatedAt:       event.At,
			UpdatedAt:       event.At,
			Metadata:        payload.Metadata,
		}
	case core.EventCampaignUpdated:
		var payload struct {
			Title         string          `json:"title,omitempty"`
			Prompt        string          `json:"prompt,omitempty"`
			MetadataPatch json.RawMessage `json:"metadataPatch,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode campaign.updated: %w", err)
		}
		campaign := p.Campaigns[event.TaskID]
		if campaign.ID == "" {
			return nil
		}
		if payload.Title != "" {
			campaign.Title = payload.Title
		}
		if payload.Prompt != "" {
			campaign.Prompt = payload.Prompt
		}
		campaign.Metadata = mergeMetadataPatch(campaign.Metadata, payload.MetadataPatch)
		campaign.RootTaskID = stringFromMetadata(campaign.Metadata, "rootTaskId")
		campaign.UpdatedAt = event.At
		p.Campaigns[event.TaskID] = campaign
	case core.EventCampaignStatus:
		var payload struct {
			Status          core.CampaignStatus  `json:"status"`
			ObjectiveStatus core.ObjectiveStatus `json:"objectiveStatus,omitempty"`
			Phase           string               `json:"phase,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode campaign.status: %w", err)
		}
		campaign := p.Campaigns[event.TaskID]
		if campaign.ID == "" {
			return nil
		}
		if payload.Status != "" {
			campaign.Status = payload.Status
		}
		if payload.ObjectiveStatus != "" {
			campaign.ObjectiveStatus = payload.ObjectiveStatus
		} else {
			campaign.ObjectiveStatus = objectiveStatusForCampaignStatus(payload.Status)
		}
		if payload.Phase != "" {
			campaign.ObjectivePhase = payload.Phase
		}
		campaign.UpdatedAt = event.At
		p.Campaigns[event.TaskID] = campaign
	case core.EventCampaignWorkPlan:
		var payload core.WorkPlan
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode campaign.work_plan_updated: %w", err)
		}
		campaign := p.Campaigns[event.TaskID]
		if campaign.ID == "" {
			return nil
		}
		campaign.WorkPlan = &payload
		campaign.UpdatedAt = event.At
		p.Campaigns[event.TaskID] = campaign
	case core.EventCampaignArtifact:
		var payload struct {
			ID       string          `json:"id"`
			Kind     string          `json:"kind"`
			Name     string          `json:"name,omitempty"`
			URL      string          `json:"url,omitempty"`
			Ref      string          `json:"ref,omitempty"`
			Metadata json.RawMessage `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode campaign.artifact_recorded: %w", err)
		}
		campaign := p.Campaigns[event.TaskID]
		if campaign.ID == "" {
			return nil
		}
		campaign.Artifacts = upsertTaskArtifact(campaign.Artifacts, core.TaskArtifact{
			ID:        payload.ID,
			Kind:      payload.Kind,
			Name:      payload.Name,
			URL:       payload.URL,
			Ref:       payload.Ref,
			CreatedAt: event.At,
			UpdatedAt: event.At,
			Metadata:  payload.Metadata,
		})
		campaign.UpdatedAt = event.At
		p.Campaigns[event.TaskID] = campaign
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
			CampaignID:      campaignIDFromMetadata(payload.Metadata),
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
		task.CampaignID = campaignIDFromMetadata(task.Metadata)
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

func (s *SQLiteStore) snapshotFromProjection(ctx context.Context, includeEvents bool) (core.Snapshot, error) {
	state, lastEventID, current, err := s.loadCurrentSnapshotProjection(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if !current {
		state, lastEventID, err = s.rebuildSnapshotProjection(ctx)
		if err != nil {
			return core.Snapshot{}, err
		}
	}
	if err := applySnapshotWorkerOutputTimestamps(ctx, s.db, &state, nil); err != nil {
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

func (s *SQLiteStore) snapshotTaskCardsFromProjection(ctx context.Context) (core.Snapshot, error) {
	state, lastEventID, current, err := s.loadCurrentSnapshotProjection(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if !current {
		state, lastEventID, err = s.rebuildSnapshotProjection(ctx)
		if err != nil {
			return core.Snapshot{}, err
		}
	}
	activeTasks := activeProjectionTasks(state.Tasks, state.ClearedTasks)
	state.Workers = filterTasks(state.Workers, state.ClearedTasks, activeTasks, func(worker core.Worker) string { return worker.TaskID })
	state.Nodes = filterTasks(state.Nodes, state.ClearedTasks, activeTasks, func(node core.ExecutionNode) string { return node.TaskID })
	state.PullRequests = filterTasks(state.PullRequests, state.ClearedTasks, activeTasks, func(pr core.PullRequest) string { return pr.TaskID })
	if err := applySnapshotWorkerOutputTimestamps(ctx, s.db, &state, activeTasks); err != nil {
		return core.Snapshot{}, err
	}
	return state.taskCardsSnapshot(lastEventID), nil
}

func (s *SQLiteStore) loadCurrentSnapshotProjection(ctx context.Context) (snapshotProjectionState, int64, bool, error) {
	state, lastEventID, ok, err := loadSnapshotProjection(ctx, s.db)
	if err != nil {
		return snapshotProjectionState{}, 0, false, err
	}
	latestEventID, err := s.latestEventID(ctx)
	if err != nil {
		return snapshotProjectionState{}, 0, false, err
	}
	if !ok {
		if latestEventID == 0 {
			return newSnapshotProjectionState(), 0, true, nil
		}
		return snapshotProjectionState{}, 0, false, nil
	}
	return state, lastEventID, lastEventID == latestEventID, nil
}

func (s *SQLiteStore) rebuildSnapshotProjection(ctx context.Context) (snapshotProjectionState, int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return snapshotProjectionState{}, 0, err
	}
	defer tx.Rollback()

	state, lastEventID, err := rebuildSnapshotProjectionTx(ctx, tx)
	if err != nil {
		return snapshotProjectionState{}, 0, err
	}
	if err := tx.Commit(); err != nil {
		return snapshotProjectionState{}, 0, err
	}
	return state, lastEventID, nil
}

func updateSnapshotProjectionTx(ctx context.Context, tx *sql.Tx, event core.Event) error {
	state, lastEventID, ok, err := loadSnapshotProjection(ctx, tx)
	if err != nil {
		return err
	}
	if !ok || lastEventID != event.ID-1 {
		_, _, err := rebuildSnapshotProjectionTx(ctx, tx)
		return err
	}
	if event.Type == core.EventWorkerOutput {
		if err := saveSnapshotWorkerOutput(ctx, tx, event); err != nil {
			return err
		}
		return advanceSnapshotProjection(ctx, tx, event.ID)
	}
	if err := state.apply(event); err != nil {
		return err
	}
	return saveSnapshotProjection(ctx, tx, state, event.ID)
}

func rebuildSnapshotProjectionTx(ctx context.Context, tx *sql.Tx) (snapshotProjectionState, int64, error) {
	events, err := projectionInputEvents(ctx, tx, 0)
	if err != nil {
		return snapshotProjectionState{}, 0, err
	}
	state := newSnapshotProjectionState()
	var lastEventID int64
	for _, event := range events {
		if err := state.apply(event); err != nil {
			return snapshotProjectionState{}, 0, err
		}
		lastEventID = event.ID
	}
	if err := saveSnapshotProjection(ctx, tx, state, lastEventID); err != nil {
		return snapshotProjectionState{}, 0, err
	}
	return state, lastEventID, nil
}

type snapshotProjectionQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func loadSnapshotProjection(ctx context.Context, q snapshotProjectionQuerier) (snapshotProjectionState, int64, bool, error) {
	var lastEventID int64
	var data string
	err := q.QueryRowContext(ctx, `
SELECT last_event_id, state
FROM snapshot_projection
WHERE id = 1`).Scan(&lastEventID, &data)
	if errorsIsNoRows(err) {
		return snapshotProjectionState{}, 0, false, nil
	}
	if err != nil {
		return snapshotProjectionState{}, 0, false, err
	}
	state := newSnapshotProjectionState()
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return snapshotProjectionState{}, 0, false, err
	}
	state.ensure()
	return state, lastEventID, true, nil
}

func saveSnapshotProjection(ctx context.Context, q snapshotProjectionQuerier, state snapshotProjectionState, lastEventID int64) error {
	state.ensure()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `
INSERT INTO snapshot_projection (id, last_event_id, state, updated_at)
VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	last_event_id = excluded.last_event_id,
	state = excluded.state,
	updated_at = excluded.updated_at`,
		lastEventID,
		string(data),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func advanceSnapshotProjection(ctx context.Context, q snapshotProjectionQuerier, lastEventID int64) error {
	_, err := q.ExecContext(ctx, `
UPDATE snapshot_projection
SET last_event_id = ?, updated_at = ?
WHERE id = 1`,
		lastEventID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func saveSnapshotWorkerOutput(ctx context.Context, q snapshotProjectionQuerier, event core.Event) error {
	_, err := q.ExecContext(ctx, `
INSERT INTO snapshot_worker_outputs (worker_id, task_id, event_id, at)
VALUES (?, ?, ?, ?)
ON CONFLICT(worker_id) DO UPDATE SET
	task_id = excluded.task_id,
	event_id = excluded.event_id,
	at = excluded.at
WHERE excluded.event_id > snapshot_worker_outputs.event_id`,
		event.WorkerID,
		event.TaskID,
		event.ID,
		event.At.Format(time.RFC3339Nano),
	)
	return err
}

func applySnapshotWorkerOutputTimestamps(ctx context.Context, q snapshotProjectionQuerier, state *snapshotProjectionState, taskIDs map[string]bool) error {
	rows, err := q.QueryContext(ctx, `
SELECT worker_id, task_id, at
FROM snapshot_worker_outputs
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

func projectionInputEvents(ctx context.Context, q snapshotProjectionQuerier, afterID int64) ([]core.Event, error) {
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
