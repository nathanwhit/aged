package orchestrator

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
)

func (s *Service) TaskAssignments(ctx context.Context, taskID string) (core.TaskAssignmentsResponse, error) {
	snapshot, err := s.SnapshotSummary(ctx)
	if err != nil {
		return core.TaskAssignmentsResponse{}, err
	}
	return BuildTaskAssignments(snapshot, taskID)
}

func BuildTaskAssignments(snapshot core.Snapshot, taskID string) (core.TaskAssignmentsResponse, error) {
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.TaskAssignmentsResponse{}, eventstore.ErrNotFound
	}
	builder := newTaskAssignmentBuilder(snapshot, task.ID)
	builder.addSessions(snapshot.Sessions)
	builder.addWorkItems(snapshot.WorkItems)
	builder.addPullRequests(snapshot.PullRequests)
	builder.addPullRequestFeedback(snapshot.PullRequestFeedback)
	builder.addQuestions(snapshot.Questions)
	builder.addArtifacts(snapshot.Artifacts)
	builder.addSteering(snapshot.Steering)
	builder.addExecutionNodes(snapshot.ExecutionNodes)
	builder.addOrphanWorkers(snapshot.Workers)
	builder.sort()
	return core.TaskAssignmentsResponse{
		TaskID:      task.ID,
		Assignments: builder.rows,
	}, nil
}

type taskAssignmentBuilder struct {
	taskID           string
	rows             []core.TaskAssignment
	workersByID      map[string]core.Worker
	nodesByID        map[string]core.ExecutionNode
	nodesByWorker    map[string]core.ExecutionNode
	sessionsByID     map[string]core.Session
	sessionsByWorker map[string]core.Session
}

func newTaskAssignmentBuilder(snapshot core.Snapshot, taskID string) *taskAssignmentBuilder {
	builder := &taskAssignmentBuilder{
		taskID:           taskID,
		rows:             []core.TaskAssignment{},
		workersByID:      map[string]core.Worker{},
		nodesByID:        map[string]core.ExecutionNode{},
		nodesByWorker:    map[string]core.ExecutionNode{},
		sessionsByID:     map[string]core.Session{},
		sessionsByWorker: map[string]core.Session{},
	}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID {
			builder.workersByID[worker.ID] = worker
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID != taskID {
			continue
		}
		builder.nodesByID[node.ID] = node
		if node.WorkerID != "" {
			builder.nodesByWorker[node.WorkerID] = node
		}
	}
	for _, session := range snapshot.Sessions {
		if session.TaskID != taskID {
			continue
		}
		builder.sessionsByID[session.ID] = session
		if session.WorkerID != "" {
			builder.sessionsByWorker[session.WorkerID] = session
		}
	}
	return builder
}

func (b *taskAssignmentBuilder) addSessions(sessions []core.Session) {
	for _, session := range sessions {
		if session.TaskID != b.taskID {
			continue
		}
		row := core.TaskAssignment{
			ID:                 assignmentID("session", session.ID),
			TaskID:             session.TaskID,
			SourceKind:         "session",
			SourceID:           session.ID,
			Status:             string(session.Status),
			Kind:               nonEmpty(session.WorkerKind, "worker"),
			Role:               session.Role,
			WorkerID:           session.WorkerID,
			WorkerKind:         session.WorkerKind,
			NodeID:             session.NodeID,
			SessionID:          session.ID,
			TargetKind:         session.TargetKind,
			TargetID:           session.TargetID,
			SpawnID:            session.SpawnID,
			CurrentAction:      session.CurrentAction,
			CurrentActionLabel: session.CurrentActionLabel,
			CreatedAt:          session.CreatedAt,
			StartedAt:          session.StartedAt,
			UpdatedAt:          session.UpdatedAt,
			CompletedAt:        session.CompletedAt,
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addWorkItems(items []core.WorkItem) {
	for _, item := range items {
		if item.TaskID != b.taskID {
			continue
		}
		metadata := assignmentMetadata(item.Metadata)
		row := core.TaskAssignment{
			ID:         assignmentID("work_item", item.ID),
			TaskID:     item.TaskID,
			SourceKind: "work_item",
			SourceID:   item.ID,
			Status:     string(item.Status),
			Kind:       item.Kind,
			Role:       nonEmpty(stringMetadata(metadata, "role"), strings.TrimPrefix(item.Kind, "objective."), item.Kind),
			WorkerID:   item.WorkerID,
			WorkerKind: stringMetadata(metadata, "workerKind"),
			WorkItemID: item.ID,
			TargetKind: item.TargetKind,
			TargetID:   item.TargetID,
			DependsOn:  stringSliceMetadata(metadata, "dependsOn"),
			Reason:     nonEmpty(item.Reason, stringMetadata(metadata, "reason"), stringMetadata(metadata, "planRationale")),
			CreatedAt:  item.CreatedAt,
			UpdatedAt:  item.UpdatedAt,
		}
		if item.Status == core.WorkItemRunning || item.Status == core.WorkItemSucceeded || item.Status == core.WorkItemFailed || item.Status == core.WorkItemCanceled {
			row.StartedAt = timePtr(item.UpdatedAt)
		}
		if item.Status == core.WorkItemSucceeded || item.Status == core.WorkItemFailed || item.Status == core.WorkItemCanceled {
			row.CompletedAt = timePtr(item.UpdatedAt)
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addPullRequests(pullRequests []core.PullRequest) {
	for _, pr := range pullRequests {
		if pr.TaskID != b.taskID {
			continue
		}
		metadata := assignmentMetadata(pr.Metadata)
		row := core.TaskAssignment{
			ID:         assignmentID("pull_request", pr.ID),
			TaskID:     pr.TaskID,
			SourceKind: "pull_request",
			SourceID:   pr.ID,
			Status:     nonEmpty(pr.State, pr.ChecksStatus, pr.ReviewStatus, "published"),
			Kind:       "pull_request",
			WorkerID:   stringMetadata(metadata, "workerId"),
			WorkerKind: stringMetadata(metadata, "workerKind"),
			TargetKind: "pull_request",
			TargetID:   pr.ID,
			Reason:     nonEmpty(pr.Title, pr.URL),
			CreatedAt:  pr.CreatedAt,
			UpdatedAt:  pr.UpdatedAt,
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addPullRequestFeedback(feedbackRows []core.PullRequestFeedback) {
	for _, feedback := range feedbackRows {
		if feedback.TaskID != b.taskID {
			continue
		}
		metadata := assignmentMetadata(feedback.Metadata)
		row := core.TaskAssignment{
			ID:          assignmentID("pull_request_feedback", feedback.ID),
			TaskID:      feedback.TaskID,
			SourceKind:  "pull_request_feedback",
			SourceID:    feedback.ID,
			Status:      nonEmpty(feedback.Status, "pending"),
			Kind:        "pr.feedback",
			WorkerID:    stringMetadata(metadata, "workerId"),
			WorkerKind:  stringMetadata(metadata, "workerKind"),
			TargetKind:  "pull_request",
			TargetID:    feedback.PullRequestID,
			Reason:      nonEmpty(feedback.Reason, feedback.FeedbackSignature),
			CreatedAt:   feedback.CreatedAt,
			UpdatedAt:   feedback.UpdatedAt,
			CompletedAt: feedback.HandledAt,
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addQuestions(questions []core.Question) {
	for _, question := range questions {
		if question.TaskID != b.taskID {
			continue
		}
		status := "pending"
		var completedAt *time.Time
		if question.Decided {
			status = "answered"
			completedAt = timePtr(question.UpdatedAt)
		}
		row := core.TaskAssignment{
			ID:          assignmentID("question", question.ID),
			TaskID:      question.TaskID,
			SourceKind:  "question",
			SourceID:    question.ID,
			Status:      status,
			Kind:        "question",
			WorkerID:    question.WorkerID,
			TargetKind:  targetKindForWorker(question.WorkerID),
			TargetID:    question.WorkerID,
			Reason:      question.Reason,
			CreatedAt:   question.CreatedAt,
			UpdatedAt:   question.UpdatedAt,
			CompletedAt: completedAt,
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addArtifacts(artifacts []core.Artifact) {
	for _, artifact := range artifacts {
		if artifact.TaskID != b.taskID {
			continue
		}
		metadata := assignmentMetadata(artifact.Metadata)
		targetKind := nonEmpty(stringMetadata(metadata, "targetKind"), stringMetadata(metadata, "sourceKind"))
		targetID := nonEmpty(stringMetadata(metadata, "targetId"), stringMetadata(metadata, "sourceId"))
		if targetID == "" {
			targetID = nonEmpty(stringMetadata(metadata, "pullRequestID"), stringMetadata(metadata, "pullRequestId"))
			if targetID != "" {
				targetKind = "pull_request"
			}
		}
		row := core.TaskAssignment{
			ID:         assignmentID("artifact", artifact.ID),
			TaskID:     artifact.TaskID,
			SourceKind: "artifact",
			SourceID:   artifact.ID,
			Status:     "recorded",
			Kind:       artifact.Kind,
			WorkerID:   stringMetadata(metadata, "workerId"),
			WorkerKind: stringMetadata(metadata, "workerKind"),
			TargetKind: targetKind,
			TargetID:   targetID,
			Reason:     nonEmpty(artifact.Name, artifact.Ref, artifact.URL),
			CreatedAt:  artifact.CreatedAt,
			UpdatedAt:  artifact.UpdatedAt,
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addSteering(items []core.SteeringItem) {
	for _, item := range items {
		if item.TaskID != b.taskID {
			continue
		}
		row := core.TaskAssignment{
			ID:          assignmentID("steering", item.ID),
			TaskID:      item.TaskID,
			SourceKind:  "steering",
			SourceID:    item.ID,
			Status:      nonEmpty(item.Status, "pending"),
			Kind:        "steering",
			Role:        item.Role,
			WorkerID:    item.WorkerID,
			WorkerKind:  item.WorkerKind,
			NodeID:      item.NodeID,
			TargetKind:  item.TargetKind,
			TargetID:    item.TargetID,
			SpawnID:     item.SpawnID,
			Reason:      item.Reason,
			CreatedAt:   item.CreatedAt,
			UpdatedAt:   item.UpdatedAt,
			CompletedAt: item.AppliedAt,
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addExecutionNodes(nodes []core.ExecutionNode) {
	for _, node := range nodes {
		if node.TaskID != b.taskID {
			continue
		}
		row := core.TaskAssignment{
			ID:           assignmentID("execution_node", node.ID),
			TaskID:       node.TaskID,
			SourceKind:   "execution_node",
			SourceID:     node.ID,
			Status:       string(node.Status),
			Kind:         nonEmpty(node.WorkerKind, "execution_node"),
			Role:         node.Role,
			WorkerID:     node.WorkerID,
			WorkerKind:   node.WorkerKind,
			NodeID:       node.ID,
			TargetKind:   node.TargetKind,
			TargetID:     node.TargetID,
			ParentNodeID: node.ParentNodeID,
			SpawnID:      node.SpawnID,
			DependsOn:    node.DependsOn,
			Reason:       node.Reason,
			CreatedAt:    node.CreatedAt,
			UpdatedAt:    node.UpdatedAt,
		}
		if isTerminalWorkerStatus(node.Status) {
			row.CompletedAt = timePtr(node.UpdatedAt)
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) addOrphanWorkers(workers []core.Worker) {
	for _, worker := range workers {
		if worker.TaskID != b.taskID {
			continue
		}
		if _, ok := b.sessionsByWorker[worker.ID]; ok {
			continue
		}
		if _, ok := b.nodesByWorker[worker.ID]; ok {
			continue
		}
		row := core.TaskAssignment{
			ID:         assignmentID("worker", worker.ID),
			TaskID:     worker.TaskID,
			SourceKind: "worker",
			SourceID:   worker.ID,
			Status:     string(worker.Status),
			Kind:       worker.Kind,
			WorkerID:   worker.ID,
			WorkerKind: worker.Kind,
			CreatedAt:  worker.CreatedAt,
			UpdatedAt:  worker.UpdatedAt,
		}
		if isTerminalWorkerStatus(worker.Status) {
			row.CompletedAt = timePtr(worker.UpdatedAt)
		}
		b.append(row)
	}
}

func (b *taskAssignmentBuilder) append(row core.TaskAssignment) {
	b.hydrate(&row)
	b.rows = append(b.rows, row)
}

func (b *taskAssignmentBuilder) hydrate(row *core.TaskAssignment) {
	if row.WorkerID != "" {
		if worker, ok := b.workersByID[row.WorkerID]; ok {
			row.WorkerKind = nonEmpty(row.WorkerKind, worker.Kind)
		}
		if session, ok := b.sessionsByWorker[row.WorkerID]; ok {
			row.SessionID = nonEmpty(row.SessionID, session.ID)
			row.NodeID = nonEmpty(row.NodeID, session.NodeID)
			row.CurrentAction = nonEmpty(row.CurrentAction, session.CurrentAction)
			row.CurrentActionLabel = nonEmpty(row.CurrentActionLabel, session.CurrentActionLabel)
		}
		if node, ok := b.nodesByWorker[row.WorkerID]; ok {
			b.hydrateFromNode(row, node)
		}
	}
	if row.SessionID != "" {
		if session, ok := b.sessionsByID[row.SessionID]; ok {
			row.WorkerID = nonEmpty(row.WorkerID, session.WorkerID)
			row.WorkerKind = nonEmpty(row.WorkerKind, session.WorkerKind)
			row.NodeID = nonEmpty(row.NodeID, session.NodeID)
			row.TargetKind = nonEmpty(row.TargetKind, session.TargetKind)
			row.TargetID = nonEmpty(row.TargetID, session.TargetID)
			row.Role = nonEmpty(row.Role, session.Role)
			row.SpawnID = nonEmpty(row.SpawnID, session.SpawnID)
			row.CurrentAction = nonEmpty(row.CurrentAction, session.CurrentAction)
			row.CurrentActionLabel = nonEmpty(row.CurrentActionLabel, session.CurrentActionLabel)
		}
	}
	if row.NodeID != "" {
		if node, ok := b.nodesByID[row.NodeID]; ok {
			b.hydrateFromNode(row, node)
		}
	}
}

func (b *taskAssignmentBuilder) hydrateFromNode(row *core.TaskAssignment, node core.ExecutionNode) {
	row.NodeID = nonEmpty(row.NodeID, node.ID)
	row.WorkerID = nonEmpty(row.WorkerID, node.WorkerID)
	row.WorkerKind = nonEmpty(row.WorkerKind, node.WorkerKind)
	row.TargetKind = nonEmpty(row.TargetKind, node.TargetKind)
	row.TargetID = nonEmpty(row.TargetID, node.TargetID)
	row.ParentNodeID = nonEmpty(row.ParentNodeID, node.ParentNodeID)
	row.SpawnID = nonEmpty(row.SpawnID, node.SpawnID)
	row.Role = nonEmpty(row.Role, node.Role)
	row.Reason = nonEmpty(row.Reason, node.Reason)
	if len(row.DependsOn) == 0 {
		row.DependsOn = node.DependsOn
	}
}

func (b *taskAssignmentBuilder) sort() {
	sort.SliceStable(b.rows, func(i, j int) bool {
		left := b.rows[i]
		right := b.rows[j]
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		if left.SourceKind != right.SourceKind {
			return left.SourceKind < right.SourceKind
		}
		return left.SourceID < right.SourceID
	})
}

func assignmentID(kind string, id string) string {
	return kind + ":" + id
}

func assignmentMetadata(raw json.RawMessage) map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil
	}
	return metadata
}

func targetKindForWorker(workerID string) string {
	if workerID == "" {
		return ""
	}
	return "worker"
}

func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}
