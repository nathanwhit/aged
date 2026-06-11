package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
)

func (s *Service) TaskAssignments(ctx context.Context, taskID string) (core.TaskAssignmentsResponse, error) {
	snapshot, err := s.store.TaskAssignmentsSnapshot(ctx, taskID)
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
	builder.addDisplayRows(task, snapshot)
	builder.sortDisplayRows()
	return core.TaskAssignmentsResponse{
		TaskID:      task.ID,
		Assignments: builder.rows,
		DisplayRows: builder.displayRows,
	}, nil
}

type taskAssignmentBuilder struct {
	taskID           string
	rows             []core.TaskAssignment
	displayRows      []core.TaskAssignmentDisplayRow
	workersByID      map[string]core.Worker
	nodesByID        map[string]core.ExecutionNode
	nodesByWorker    map[string]core.ExecutionNode
	sessionsByID     map[string]core.Session
	sessionsByWorker map[string]core.Session
	pullRequestsByID map[string]core.PullRequest
	pendingFeedback  map[string]int
}

func newTaskAssignmentBuilder(snapshot core.Snapshot, taskID string) *taskAssignmentBuilder {
	builder := &taskAssignmentBuilder{
		taskID:           taskID,
		rows:             []core.TaskAssignment{},
		displayRows:      []core.TaskAssignmentDisplayRow{},
		workersByID:      map[string]core.Worker{},
		nodesByID:        map[string]core.ExecutionNode{},
		nodesByWorker:    map[string]core.ExecutionNode{},
		sessionsByID:     map[string]core.Session{},
		sessionsByWorker: map[string]core.Session{},
		pullRequestsByID: map[string]core.PullRequest{},
		pendingFeedback:  map[string]int{},
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
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID {
			builder.pullRequestsByID[pr.ID] = pr
		}
	}
	for _, feedback := range snapshot.PullRequestFeedback {
		if feedback.TaskID == taskID && strings.EqualFold(nonEmpty(feedback.Status, "pending"), "pending") && feedback.PullRequestID != "" {
			builder.pendingFeedback[feedback.PullRequestID]++
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

func (b *taskAssignmentBuilder) addDisplayRows(task core.Task, snapshot core.Snapshot) {
	b.addTaskDisplayRow(task)
	b.addQuestionDisplayRows(snapshot.Questions)
	b.addPullRequestFeedbackDisplayRows(snapshot.PullRequestFeedback)
	b.addSessionDisplayRows(snapshot.Sessions)
	b.addWorkItemDisplayRows(snapshot.WorkItems, task)
	b.addPullRequestDisplayRows(snapshot.PullRequests)
	b.addArtifactDisplayRows(snapshot.Artifacts, task)
	b.addSteeringDisplayRows(snapshot.Steering)
	b.addExecutionNodeDisplayRows(snapshot.ExecutionNodes)
	b.addOrphanWorkerDisplayRows(snapshot.Workers)
}

func (b *taskAssignmentBuilder) addTaskDisplayRow(task core.Task) {
	actions := taskDisplayActions(task)
	if task.Error != "" || task.Status == core.TaskFailed {
		b.displayRows = append(b.displayRows, core.TaskAssignmentDisplayRow{
			ID:             assignmentID("task_failure", task.ID),
			Kind:           "debug",
			Title:          "Task failure",
			Subtitle:       nonEmpty(humanizeAssignmentKey(task.ObjectivePhase), "Task status"),
			Status:         string(task.Status),
			Tone:           "danger",
			UpdatedAt:      nonZeroTime(task.UpdatedAt, task.CreatedAt),
			CurrentAction:  nonEmpty(task.Error, "The task failed without a detailed error."),
			Owner:          ownerForWorker(task.AppliedWorkerID, "Objective"),
			ProjectContext: task.ProjectID,
			Actions:        actions,
		})
		return
	}
	if isTerminalTaskStatus(task.Status) && len(actions) > 0 {
		currentAction := "Task is no longer active."
		if task.Status == core.TaskSucceeded {
			currentAction = "Objective output is complete."
		}
		b.displayRows = append(b.displayRows, core.TaskAssignmentDisplayRow{
			ID:             assignmentID("task_complete", task.ID),
			Kind:           "debug",
			Title:          "Task finished",
			Subtitle:       "Task lifecycle",
			Status:         string(task.Status),
			Tone:           toneForAssignmentStatus(string(task.Status)),
			UpdatedAt:      nonZeroTime(task.UpdatedAt, task.CreatedAt),
			CurrentAction:  currentAction,
			Owner:          ownerForWorker(task.AppliedWorkerID, "Objective"),
			ProjectContext: task.ProjectID,
			Actions:        actions,
		})
	}
}

func (b *taskAssignmentBuilder) addQuestionDisplayRows(questions []core.Question) {
	for _, question := range questions {
		if question.TaskID != b.taskID || question.Decided {
			continue
		}
		title := nonEmpty(question.Question, "Question needs an answer")
		b.displayRows = append(b.displayRows, core.TaskAssignmentDisplayRow{
			ID:            assignmentID("question", question.ID),
			Kind:          "question",
			Title:         title,
			Subtitle:      nonEmpty(humanizeAssignmentKey(question.Reason), "User input required"),
			Status:        "waiting_user",
			Tone:          "warning",
			UpdatedAt:     nonZeroTime(question.UpdatedAt, question.CreatedAt),
			CurrentAction: "Waiting for a response",
			Owner:         ownerForWorker(question.WorkerID, "Objective"),
			Selection: &core.TaskAssignmentSelection{
				Kind:       "question",
				QuestionID: question.ID,
			},
		})
	}
}

func (b *taskAssignmentBuilder) addPullRequestFeedbackDisplayRows(feedbackRows []core.PullRequestFeedback) {
	for _, feedback := range feedbackRows {
		if feedback.TaskID != b.taskID || !strings.EqualFold(nonEmpty(feedback.Status, "pending"), "pending") {
			continue
		}
		pr := b.pullRequestsByID[feedback.PullRequestID]
		if pr.ID != "" && isTerminalPullRequestState(pr.State) {
			continue
		}
		title := nonEmpty(prContext(feedback.Repo, feedback.Number, feedback.Branch), prContext(pr.Repo, pr.Number, pr.Branch), feedback.URL, feedback.PullRequestID, "Pull request")
		actions := []core.TaskAssignmentActionDescriptor{}
		if feedback.URL != "" {
			actions = append(actions, core.TaskAssignmentActionDescriptor{Kind: "open-pr", URL: feedback.URL})
		} else if pr.URL != "" {
			actions = append(actions, core.TaskAssignmentActionDescriptor{Kind: "open-pr", URL: pr.URL})
		}
		row := core.TaskAssignmentDisplayRow{
			ID:            assignmentID("feedback", feedback.ID),
			Kind:          "feedback",
			Title:         title,
			Subtitle:      nonEmpty(humanizeAssignmentKey(feedback.Reason), "Pull request feedback"),
			Status:        nonEmpty(feedback.Status, "pending"),
			Tone:          "warning",
			UpdatedAt:     nonZeroTime(feedback.UpdatedAt, feedback.CreatedAt),
			CurrentAction: nonEmpty(feedback.FeedbackBody, feedback.Prompt, "Follow-up work is queued."),
			Owner:         attemptOwner(feedback.Attempt),
			PRContext:     nonEmpty(prContext(feedback.Repo, feedback.Number, feedback.Branch), prContext(pr.Repo, pr.Number, pr.Branch)),
			Actions:       actions,
		}
		if feedback.PullRequestID != "" {
			row.Selection = &core.TaskAssignmentSelection{Kind: "pull_request", PullRequestID: feedback.PullRequestID}
		}
		b.displayRows = append(b.displayRows, row)
	}
}

func (b *taskAssignmentBuilder) addSessionDisplayRows(sessions []core.Session) {
	for _, session := range sessions {
		if session.TaskID != b.taskID || isTerminalWorkerStatus(session.Status) {
			continue
		}
		worker := b.workersByID[session.WorkerID]
		node := b.nodesByWorker[session.WorkerID]
		title := nonEmpty(humanizeAssignmentKey(session.Role), session.WorkerKind, worker.Kind, "Live session")
		subtitle := joinNonEmpty(" · ", session.RemoteSession, session.TargetID, session.WorkspaceName)
		if subtitle == "" {
			subtitle = shortAssignmentID(session.WorkerID)
		}
		actions := []core.TaskAssignmentActionDescriptor{
			{Kind: "inspect-session", SessionID: session.ID},
			{Kind: "cancel-session", SessionID: session.ID},
		}
		b.displayRows = append(b.displayRows, core.TaskAssignmentDisplayRow{
			ID:             assignmentID("session", session.ID),
			Kind:           "session",
			Title:          title,
			Subtitle:       subtitle,
			Status:         string(session.Status),
			Tone:           toneForAssignmentStatus(string(session.Status)),
			UpdatedAt:      nonZeroTime(session.UpdatedAt, nonZeroTimeFromPtr(session.StartedAt), session.CreatedAt),
			CurrentAction:  session.CurrentAction,
			Owner:          ownerForWorker(session.WorkerID, ""),
			Model:          nonEmpty(metadataDisplayString(worker.Metadata, "model"), metadataDisplayString(worker.Metadata, "brain"), metadataDisplayString(session.Metadata, "model")),
			ProjectContext: joinNonEmpty(" ", humanizeAssignmentKey(node.TargetKind), node.TargetID),
			Actions:        actions,
			Selection:      &core.TaskAssignmentSelection{Kind: "session", SessionID: session.ID},
		})
	}
}

func (b *taskAssignmentBuilder) addWorkItemDisplayRows(items []core.WorkItem, task core.Task) {
	for _, item := range items {
		if item.TaskID != b.taskID || !isDisplayWorkItemStatus(item.Status) {
			continue
		}
		row := core.TaskAssignmentDisplayRow{
			ID:             assignmentID("work", item.ID),
			Kind:           "work",
			Title:          humanizeAssignmentKey(item.Kind),
			Subtitle:       nonEmpty(item.Reason, joinNonEmpty(" · ", humanizeAssignmentKey(item.TargetKind), item.TargetID), "Work item"),
			Status:         string(item.Status),
			Tone:           toneForAssignmentStatus(string(item.Status)),
			UpdatedAt:      nonZeroTime(item.UpdatedAt, item.CreatedAt),
			CurrentAction:  nonEmpty(item.Error, item.Prompt),
			Owner:          nonEmpty(ownerForWorker(item.WorkerID, ""), leaseOwner(item.LeaseOwner)),
			ProjectContext: task.ProjectID,
			Selection:      &core.TaskAssignmentSelection{Kind: "work_item", WorkItemID: item.ID},
		}
		if item.Status == core.WorkItemQueued || item.Status == core.WorkItemRunning {
			row.Actions = []core.TaskAssignmentActionDescriptor{{Kind: "cancel-work-item", WorkItemID: item.ID}}
		}
		b.displayRows = append(b.displayRows, row)
	}
}

func (b *taskAssignmentBuilder) addPullRequestDisplayRows(pullRequests []core.PullRequest) {
	for _, pr := range pullRequests {
		if pr.TaskID != b.taskID || isTerminalPullRequestState(pr.State) {
			continue
		}
		context := prContext(pr.Repo, pr.Number, pr.Branch)
		title := nonEmpty(pr.Title, context, "Pull request")
		subtitle := nonEmpty(nonRedundantSubtitle(title, context), pullRequestBranchContext(pr), "Pull request")
		feedbackCount := b.pendingFeedback[pr.ID]
		status := nonEmpty(pr.ReviewStatus, pr.ChecksStatus, pr.State, "open")
		tone := toneForAssignmentStatus(status)
		currentAction := nonEmpty(pr.MergeStatus, pr.ChecksConclusion)
		if feedbackCount > 0 {
			tone = "warning"
			currentAction = pluralize(feedbackCount, "pending feedback item")
		}
		actions := []core.TaskAssignmentActionDescriptor{}
		if pr.URL != "" {
			actions = append(actions, core.TaskAssignmentActionDescriptor{Kind: "open-pr", URL: pr.URL})
		}
		actions = append(actions,
			core.TaskAssignmentActionDescriptor{Kind: "refresh-pr", PullRequestID: pr.ID},
			core.TaskAssignmentActionDescriptor{Kind: "babysit-pr", PullRequestID: pr.ID, Disabled: pr.BabysitterTaskID != ""},
		)
		b.displayRows = append(b.displayRows, core.TaskAssignmentDisplayRow{
			ID:            assignmentID("pr", pr.ID),
			Kind:          "pull_request",
			Title:         title,
			Subtitle:      subtitle,
			Status:        status,
			Tone:          tone,
			UpdatedAt:     nonZeroTime(pr.UpdatedAt, pr.CreatedAt),
			CurrentAction: currentAction,
			Owner:         ownerForPR(pr.BranchOwner),
			PRContext:     pullRequestBranchContext(pr),
			Actions:       actions,
			Selection:     &core.TaskAssignmentSelection{Kind: "pull_request", PullRequestID: pr.ID},
		})
	}
}

func (b *taskAssignmentBuilder) addArtifactDisplayRows(artifacts []core.Artifact, task core.Task) {
	for index, artifact := range artifacts {
		if artifact.TaskID != b.taskID || !isManagerVisibleArtifact(artifact.Kind) {
			continue
		}
		key := nonEmpty(artifact.ID, artifact.Ref, artifact.URL, fmt.Sprintf("%s-%d", humanizeAssignmentKey(artifact.Kind), index+1))
		row := core.TaskAssignmentDisplayRow{
			ID:             assignmentID("artifact", key),
			Kind:           "artifact",
			Title:          nonEmpty(artifact.Name, artifact.Ref, humanizeAssignmentKey(artifact.Kind)),
			Subtitle:       humanizeAssignmentKey(artifact.Kind),
			Status:         "available",
			Tone:           "good",
			UpdatedAt:      nonZeroTime(artifact.UpdatedAt, artifact.CreatedAt),
			CurrentAction:  nonEmpty(artifact.Ref, artifact.URL),
			Owner:          metadataDisplayString(artifact.Metadata, "workerId"),
			ProjectContext: task.ProjectID,
			Selection:      &core.TaskAssignmentSelection{Kind: "artifact", ArtifactID: key},
		}
		if artifact.URL != "" {
			row.Actions = []core.TaskAssignmentActionDescriptor{{Kind: "open-pr", URL: artifact.URL}}
		}
		b.displayRows = append(b.displayRows, row)
	}
}

func (b *taskAssignmentBuilder) addSteeringDisplayRows(items []core.SteeringItem) {
	for _, item := range items {
		if item.TaskID != b.taskID || !isActiveAssignmentStatus(nonEmpty(item.Status, "pending")) {
			continue
		}
		b.displayRows = append(b.displayRows, core.TaskAssignmentDisplayRow{
			ID:             assignmentID("steering", item.ID),
			Kind:           "debug",
			Title:          steeringAssignmentTitle(item),
			Subtitle:       nonEmpty(humanizeAssignmentKey(item.Reason), "Steering queued"),
			Status:         nonEmpty(item.Status, "pending"),
			Tone:           "info",
			UpdatedAt:      nonZeroTime(item.UpdatedAt, item.CreatedAt),
			CurrentAction:  item.Message,
			Owner:          nonEmpty(ownerForWorker(item.WorkerID, ""), humanizeAssignmentKey(item.TargetKind), "Objective"),
			ProjectContext: item.TargetID,
		})
	}
}

func (b *taskAssignmentBuilder) addExecutionNodeDisplayRows(nodes []core.ExecutionNode) {
	for _, node := range nodes {
		if node.TaskID != b.taskID || isTerminalWorkerStatus(node.Status) {
			continue
		}
		_, hasSession := b.sessionsByWorker[node.WorkerID]
		if hasSession {
			continue
		}
		row := core.TaskAssignmentDisplayRow{
			ID:             assignmentID("execution_node", node.ID),
			Kind:           "debug",
			Title:          nonEmpty(humanizeAssignmentKey(node.Role), node.WorkerKind, "Execution node"),
			Subtitle:       nonEmpty(node.Reason, "Execution node"),
			Status:         string(node.Status),
			Tone:           toneForAssignmentStatus(string(node.Status)),
			UpdatedAt:      nonZeroTime(node.UpdatedAt, node.CreatedAt),
			Owner:          ownerForWorker(node.WorkerID, "Unassigned"),
			ProjectContext: joinNonEmpty(" ", humanizeAssignmentKey(node.TargetKind), node.TargetID),
		}
		if node.WorkerID != "" && !isTerminalWorkerStatus(node.Status) {
			row.Actions = []core.TaskAssignmentActionDescriptor{{Kind: "cancel-worker", WorkerID: node.WorkerID}}
		}
		b.displayRows = append(b.displayRows, row)
	}
}

func (b *taskAssignmentBuilder) addOrphanWorkerDisplayRows(workers []core.Worker) {
	for _, worker := range workers {
		if worker.TaskID != b.taskID || isTerminalWorkerStatus(worker.Status) {
			continue
		}
		if _, ok := b.sessionsByWorker[worker.ID]; ok {
			continue
		}
		if _, ok := b.nodesByWorker[worker.ID]; ok {
			continue
		}
		row := core.TaskAssignmentDisplayRow{
			ID:            assignmentID("debug_worker", worker.ID),
			Kind:          "debug",
			Title:         nonEmpty(worker.Kind, "Worker"),
			Subtitle:      nonEmpty(worker.Prompt, "Worker without session details"),
			Status:        string(worker.Status),
			Tone:          toneForAssignmentStatus(string(worker.Status)),
			UpdatedAt:     nonZeroTime(worker.UpdatedAt, worker.CreatedAt),
			Owner:         ownerForWorker(worker.ID, ""),
			Model:         nonEmpty(metadataDisplayString(worker.Metadata, "model"), metadataDisplayString(worker.Metadata, "brain")),
			CurrentAction: worker.PromptError,
		}
		if !isTerminalWorkerStatus(worker.Status) {
			row.Actions = []core.TaskAssignmentActionDescriptor{{Kind: "cancel-worker", WorkerID: worker.ID}}
		}
		b.displayRows = append(b.displayRows, row)
	}
}

func (b *taskAssignmentBuilder) sortDisplayRows() {
	sort.SliceStable(b.displayRows, func(i, j int) bool {
		left := b.displayRows[i]
		right := b.displayRows[j]
		leftRank := assignmentDisplayRank(left)
		rightRank := assignmentDisplayRank(right)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.After(right.UpdatedAt)
		}
		return left.ID < right.ID
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

func taskDisplayActions(task core.Task) []core.TaskAssignmentActionDescriptor {
	actions := []core.TaskAssignmentActionDescriptor{}
	if task.Status == core.TaskFailed || task.Status == core.TaskCanceled {
		actions = append(actions, core.TaskAssignmentActionDescriptor{Kind: "retry-task", TaskID: task.ID})
	}
	if isTerminalTaskStatus(task.Status) {
		actions = append(actions, core.TaskAssignmentActionDescriptor{Kind: "clear-task", TaskID: task.ID})
	}
	return actions
}

func isDisplayWorkItemStatus(status core.WorkItemStatus) bool {
	return status == core.WorkItemQueued || status == core.WorkItemRunning
}

func isManagerVisibleArtifact(kind string) bool {
	normalized := strings.ToLower(strings.TrimSpace(kind))
	return normalized != "worker_log" && normalized != "github_pull_request"
}

func isActiveAssignmentStatus(status string) bool {
	normalized := strings.ToLower(strings.TrimSpace(status))
	return normalized == "pending" || normalized == "queued" || normalized == "running"
}

func toneForAssignmentStatus(status string) string {
	normalized := strings.ToLower(strings.TrimSpace(status))
	if normalized == "failed" || normalized == "canceled" || normalized == "abandoned" || strings.Contains(normalized, "failure") {
		return "danger"
	}
	if normalized == "waiting" || normalized == "waiting_user" || normalized == "pending" || normalized == "queued" {
		return "warning"
	}
	if normalized == "succeeded" || normalized == "satisfied" || normalized == "available" || normalized == "closed" {
		return "good"
	}
	return "info"
}

func assignmentDisplayRank(row core.TaskAssignmentDisplayRow) int {
	toneRank := map[string]int{"danger": 0, "warning": 1, "info": 2, "good": 3}
	kindRank := map[string]int{
		"question":     0,
		"feedback":     1,
		"session":      2,
		"work":         3,
		"pull_request": 4,
		"artifact":     5,
		"debug":        6,
	}
	return rankValue(toneRank, row.Tone, 3)*10 + rankValue(kindRank, row.Kind, 6)
}

func rankValue(values map[string]int, key string, fallback int) int {
	if value, ok := values[key]; ok {
		return value
	}
	return fallback
}

func metadataDisplayString(raw json.RawMessage, key string) string {
	metadata := assignmentMetadata(raw)
	if metadata == nil {
		return ""
	}
	switch value := metadata[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case float64:
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	case bool:
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	case []any:
		parts := make([]string, 0, len(value))
		for _, item := range value {
			if text := strings.TrimSpace(fmt.Sprintf("%v", item)); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, " ")
	default:
		return ""
	}
}

func humanizeAssignmentKey(key string) string {
	normalized := strings.TrimSpace(key)
	if normalized == "" {
		return ""
	}
	var expanded strings.Builder
	var previous rune
	for _, current := range normalized {
		switch {
		case current == '_' || current == '-' || current == '.':
			expanded.WriteRune(' ')
		case previous >= 'a' && previous <= 'z' && current >= 'A' && current <= 'Z':
			expanded.WriteRune(' ')
			expanded.WriteRune(current)
		default:
			expanded.WriteRune(current)
		}
		previous = current
	}
	parts := strings.Fields(expanded.String())
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func ownerForWorker(workerID string, fallback string) string {
	if strings.TrimSpace(workerID) == "" {
		return fallback
	}
	return "Worker " + shortAssignmentID(workerID)
}

func ownerForPR(owner string) string {
	if strings.TrimSpace(owner) == "" {
		return ""
	}
	return "Owner " + shortAssignmentID(owner)
}

func leaseOwner(owner string) string {
	if strings.TrimSpace(owner) == "" {
		return ""
	}
	return "Lease " + shortAssignmentID(owner)
}

func attemptOwner(attempt int) string {
	if attempt <= 0 {
		return ""
	}
	return fmt.Sprintf("Attempt %d", attempt)
}

func shortAssignmentID(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) <= 12 {
		return trimmed
	}
	return trimmed[:8]
}

func prContext(repo string, number int, branch string) string {
	if strings.TrimSpace(repo) != "" && number > 0 {
		return fmt.Sprintf("%s#%d", repo, number)
	}
	return joinNonEmpty(" · ", repo, branch)
}

func pullRequestBranchContext(pr core.PullRequest) string {
	return joinNonEmpty(" · ", prefixIfNonEmpty("base ", pr.Base), prefixIfNonEmpty("head ", pr.Branch))
}

func nonRedundantSubtitle(title string, subtitle string) string {
	if strings.EqualFold(strings.TrimSpace(title), strings.TrimSpace(subtitle)) {
		return ""
	}
	return subtitle
}

func prefixIfNonEmpty(prefix string, value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return prefix + value
}

func steeringAssignmentTitle(item core.SteeringItem) string {
	if item.TargetKind == "worker" && item.WorkerID != "" {
		return ownerForWorker(item.WorkerID, "Worker")
	}
	if item.TargetKind == "task" {
		return "Task steering"
	}
	return nonEmpty(humanizeAssignmentKey(item.Reason), "Steering")
}

func pluralize(count int, singular string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %ss", count, singular)
}

func joinNonEmpty(separator string, values ...string) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, separator)
}

func nonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func nonZeroTimeFromPtr(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
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
