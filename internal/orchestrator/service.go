package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"

	"github.com/google/uuid"
)

type WorkerChangesReview struct {
	WorkerID  string            `json:"workerId"`
	Workspace PreparedWorkspace `json:"workspace"`
	Changes   WorkspaceChanges  `json:"changes"`
}

type WorkerApplyResult struct {
	WorkerID      string                 `json:"workerId"`
	SourceRoot    string                 `json:"sourceRoot"`
	WorkspaceRoot string                 `json:"workspaceRoot"`
	Method        string                 `json:"method"`
	AppliedFiles  []WorkspaceChangedFile `json:"appliedFiles"`
	SkippedFiles  []WorkspaceChangedFile `json:"skippedFiles,omitempty"`
}

type ApplyPolicyRecommendation struct {
	TaskID     string           `json:"taskId"`
	Strategy   string           `json:"strategy"`
	Reason     string           `json:"reason"`
	Candidates []ApplyCandidate `json:"candidates"`
}

type ApplyCandidate struct {
	WorkerID     string                 `json:"workerId"`
	NodeID       string                 `json:"nodeId,omitempty"`
	WorkerKind   string                 `json:"workerKind"`
	Summary      string                 `json:"summary,omitempty"`
	ChangedFiles []WorkspaceChangedFile `json:"changedFiles,omitempty"`
	Applied      bool                   `json:"applied"`
}

type ClearTasksResult struct {
	Cleared []string `json:"cleared"`
}

type Service struct {
	store      eventstore.Store
	broker     *Broker
	brain      BrainProvider
	runners    map[string]worker.Runner
	workDir    string
	workspaces WorkspaceManager
	targets    *TargetRegistry
	sshRunner  SSHRunner

	mu         sync.Mutex
	cancels    map[string]context.CancelFunc
	tasks      map[string]string
	steering   map[string]chan string
	remoteRuns map[string]remoteRun
}

const maxDynamicReplanTurns = 4

type WorkerTurnResult struct {
	WorkerID string            `json:"workerId"`
	NodeID   string            `json:"nodeId,omitempty"`
	Status   core.WorkerStatus `json:"status"`
	Kind     string            `json:"kind"`
	Role     string            `json:"role,omitempty"`
	Summary  string            `json:"summary,omitempty"`
	Error    string            `json:"error,omitempty"`
	Changes  WorkspaceChanges  `json:"changes"`
}

func NewService(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string) *Service {
	return NewServiceWithWorkspaceManager(store, brain, runners, workDir, NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeIsolated, "", WorkspaceCleanupRetain))
}

func NewServiceWithWorkspaceManager(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string, workspaces WorkspaceManager) *Service {
	return NewServiceWithWorkspaceManagerAndTargets(store, brain, runners, workDir, workspaces, NewLocalTargetRegistry(), NewSSHRunner())
}

func NewServiceWithWorkspaceManagerAndTargets(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string, workspaces WorkspaceManager, targets *TargetRegistry, sshRunner SSHRunner) *Service {
	if workspaces == nil {
		workspaces = NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeIsolated, "", WorkspaceCleanupRetain)
	}
	if targets == nil {
		targets = NewLocalTargetRegistry()
	}
	return &Service{
		store:      store,
		broker:     NewBroker(),
		brain:      brain,
		runners:    runners,
		workDir:    workDir,
		workspaces: workspaces,
		targets:    targets,
		sshRunner:  sshRunner,
		cancels:    map[string]context.CancelFunc{},
		tasks:      map[string]string{},
		steering:   map[string]chan string{},
		remoteRuns: map[string]remoteRun{},
	}
}

func (s *Service) Snapshot(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if s.targets != nil {
		snapshot.Targets = s.targets.Snapshot()
	}
	return snapshot, nil
}

func (s *Service) RecoverRemoteWorkers(ctx context.Context) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	completed := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerCompleted {
			completed[event.WorkerID] = true
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TargetKind != string(TargetKindSSH) || node.WorkerID == "" || completed[node.WorkerID] {
			continue
		}
		if node.Status != core.WorkerRunning && node.Status != core.WorkerQueued {
			continue
		}
		target, ok := s.targets.Get(node.TargetID)
		if !ok {
			continue
		}
		run := remoteRun{
			Target:  target,
			Session: node.RemoteSession,
			RunDir:  node.RemoteRunDir,
			WorkDir: node.RemoteWorkDir,
			Status:  "running",
		}
		go s.recoverRemoteWorker(context.Background(), node, run)
	}
	return nil
}

func (s *Service) recoverRemoteWorker(ctx context.Context, node core.ExecutionNode, run remoteRun) {
	runState := &workerRunState{}
	sink := eventSink{service: s, taskID: node.TaskID, workerID: node.WorkerID, state: runState}
	status, err := s.sshRunner.Poll(ctx, run, worker.ParserForKind(node.WorkerKind), sink)
	workerStatus, statusErr := remoteStatusToWorkerStatus(status)
	if err != nil && !errors.Is(err, context.Canceled) {
		statusErr = err
		workerStatus = core.WorkerFailed
	}
	changes := WorkspaceChanges{
		Root:     run.RunDir,
		CWD:      run.WorkDir,
		Mode:     "remote",
		VCSType:  "ssh",
		DiffStat: "remote worker changes are reported through worker output",
	}
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   node.TaskID,
		WorkerID: node.WorkerID,
		Payload:  core.MustJSON(runState.completionPayload(workerStatus, statusErr, changes)),
	})
}

func (s *Service) Events(ctx context.Context, afterID int64, limit int) ([]core.Event, error) {
	return s.store.ListEvents(ctx, afterID, limit)
}

func (s *Service) Subscribe() (int, <-chan core.Event) {
	return s.broker.Subscribe()
}

func (s *Service) Unsubscribe(id int) {
	s.broker.Unsubscribe(id)
}

func (s *Service) CreateTask(ctx context.Context, req core.CreateTaskRequest) (core.Task, error) {
	if req.Title == "" {
		return core.Task{}, errors.New("title is required")
	}
	if req.Prompt == "" {
		return core.Task{}, errors.New("prompt is required")
	}

	taskID := uuid.NewString()
	created, err := s.append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    req.Title,
			"prompt":   req.Prompt,
			"metadata": map[string]any{},
		}),
	})
	if err != nil {
		return core.Task{}, err
	}

	task := core.Task{
		ID:        taskID,
		Title:     req.Title,
		Prompt:    req.Prompt,
		Status:    core.TaskQueued,
		CreatedAt: created.At,
		UpdatedAt: created.At,
	}

	go s.runTask(context.Background(), task)
	return task, nil
}

func (s *Service) SteerTask(ctx context.Context, taskID string, req core.SteeringRequest) error {
	if req.Message == "" {
		return errors.New("message is required")
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskSteered,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"message": req.Message,
		}),
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	for workerID, ch := range s.steering {
		if s.tasks[workerID] != taskID {
			continue
		}
		select {
		case ch <- req.Message:
		default:
		}
	}
	s.mu.Unlock()
	return err
}

func (s *Service) CancelWorker(ctx context.Context, workerID string) error {
	s.mu.Lock()
	cancel := s.cancels[workerID]
	remote := s.remoteRuns[workerID]
	s.mu.Unlock()
	if cancel == nil {
		return eventstore.ErrNotFound
	}
	if remote.Session != "" {
		_ = s.sshRunner.Cancel(ctx, remote)
	}
	cancel()
	return nil
}

func (s *Service) CancelTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	for workerID, cancel := range s.cancels {
		if s.tasks[workerID] == taskID {
			cancel()
		}
	}
	s.mu.Unlock()

	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	})
	return err
}

func (s *Service) ClearTask(ctx context.Context, taskID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, task := range snapshot.Tasks {
		if task.ID != taskID {
			continue
		}
		if !isTerminalTaskStatus(task.Status) {
			return errors.New("can only clear terminal tasks")
		}
		_, err := s.append(ctx, core.Event{
			Type:   core.EventTaskCleared,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"reason": "user_cleared",
			}),
		})
		return err
	}
	return eventstore.ErrNotFound
}

func (s *Service) ClearTerminalTasks(ctx context.Context) (ClearTasksResult, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ClearTasksResult{}, err
	}
	result := ClearTasksResult{Cleared: []string{}}
	for _, task := range snapshot.Tasks {
		if !isTerminalTaskStatus(task.Status) {
			continue
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskCleared,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"reason": "user_cleared_terminal",
			}),
		}); err != nil {
			return result, err
		}
		result.Cleared = append(result.Cleared, task.ID)
	}
	return result, nil
}

func isTerminalTaskStatus(status core.TaskStatus) bool {
	return status == core.TaskSucceeded || status == core.TaskFailed || status == core.TaskCanceled
}

func (s *Service) ReviewWorkerChanges(ctx context.Context, workerID string) (WorkerChangesReview, error) {
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return WorkerChangesReview{}, err
	}
	return WorkerChangesReview{
		WorkerID:  workerID,
		Workspace: workspace,
		Changes:   s.describeWorkspaceChanges(ctx, workspace),
	}, nil
}

func (s *Service) ApplyWorkerChanges(ctx context.Context, workerID string) (WorkerApplyResult, error) {
	if applied, err := s.workerChangesApplied(ctx, workerID); err != nil {
		return WorkerApplyResult{}, err
	} else if applied {
		return WorkerApplyResult{}, fmt.Errorf("worker changes already applied: %s", workerID)
	}
	review, err := s.ReviewWorkerChanges(ctx, workerID)
	if err != nil {
		return WorkerApplyResult{}, err
	}
	result, err := s.workspaces.ApplyChanges(ctx, review.Workspace, review.Changes)
	if err != nil {
		return WorkerApplyResult{}, err
	}
	result.WorkerID = workerID
	_, err = s.append(ctx, core.Event{
		Type:     core.EventWorkerApplied,
		TaskID:   review.Workspace.TaskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(result),
	})
	return result, err
}

func (s *Service) RecommendApplyPolicy(ctx context.Context, taskID string) (ApplyPolicyRecommendation, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ApplyPolicyRecommendation{}, err
	}
	candidates := applyCandidates(snapshot, taskID)
	recommendation := ApplyPolicyRecommendation{
		TaskID:     taskID,
		Strategy:   "none",
		Reason:     "no unapplied successful workers with source changes",
		Candidates: candidates,
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
	_, err = s.append(ctx, core.Event{
		Type:    core.EventApplyPolicy,
		TaskID:  taskID,
		Payload: core.MustJSON(recommendation),
	})
	return recommendation, err
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

func (s *Service) workerChangesApplied(ctx context.Context, workerID string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerApplied && event.WorkerID == workerID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) runTask(ctx context.Context, task core.Task) {
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return
	}

	plan, err := s.brain.Plan(ctx, task, nil)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if err := plan.Validate(); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}

	results := []WorkerTurnResult{}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	results = append(results, result)
	if !s.finishOrContinueTask(ctx, task.ID, result) {
		return
	}

	results, ok, err := s.runFollowUpWorkers(ctx, task, plan, results, result.NodeID)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if !ok {
		return
	}

	if !s.replanLoop(ctx, task, plan, results) {
		return
	}

	_ = s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
}

func (s *Service) runPlannedWorker(ctx context.Context, task core.Task, plan Plan) (WorkerTurnResult, error) {
	runner := s.runners[plan.WorkerKind]
	if runner == nil {
		return WorkerTurnResult{}, fmt.Errorf("unknown worker kind %q", plan.WorkerKind)
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	target, err := s.targets.Select(plan)
	if err != nil {
		return WorkerTurnResult{}, err
	}
	s.targets.Begin(target.ID)
	defer s.targets.Finish(target.ID)
	plan.Metadata["targetID"] = target.ID
	plan.Metadata["targetKind"] = string(target.Kind)
	if target.Kind == TargetKindSSH {
		return s.runSSHPlannedWorker(ctx, task, plan, runner, target)
	}

	workerID := uuid.NewString()
	nodeID := stringMetadata(plan.Metadata, "nodeID")
	if nodeID == "" {
		nodeID = uuid.NewString()
		plan.Metadata["nodeID"] = nodeID
	}
	planID := stringMetadata(plan.Metadata, "planID")
	if planID == "" {
		planID = uuid.NewString()
		plan.Metadata["planID"] = planID
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":       nodeID,
			"workerId":     workerID,
			"workerKind":   plan.WorkerKind,
			"planId":       planID,
			"parentNodeId": stringMetadata(plan.Metadata, "parentNodeID"),
			"spawnId":      stringMetadata(plan.Metadata, "spawnID"),
			"role":         stringMetadata(plan.Metadata, "spawnRole"),
			"reason":       stringMetadata(plan.Metadata, "spawnReason"),
			"targetId":     target.ID,
			"targetKind":   string(target.Kind),
			"dependsOn":    stringSliceMetadata(plan.Metadata, "dependsOn"),
			"metadata":     planMetadata(plan),
		}),
	}); err != nil {
		return WorkerTurnResult{}, err
	}
	workspace, err := s.workspaces.Prepare(ctx, WorkspaceSpec{
		TaskID:   task.ID,
		WorkerID: workerID,
		WorkDir:  s.workDir,
	})
	if err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(workspace),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	var steering chan string
	if runnerSupportsSteering(runner) {
		steering = make(chan string, 16)
	}
	spec := worker.Spec{
		ID:       workerID,
		TaskID:   task.ID,
		Kind:     plan.WorkerKind,
		Prompt:   plan.Prompt,
		WorkDir:  workspace.CWD,
		Steering: steering,
	}
	command := runner.BuildCommand(spec)
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     plan.WorkerKind,
			"command":  command,
			"metadata": planMetadata(plan),
		}),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[workerID] = cancel
	s.tasks[workerID] = task.ID
	if steering != nil {
		s.steering[workerID] = steering
	}
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		if ch := s.steering[workerID]; ch != nil {
			close(ch)
		}
		delete(s.cancels, workerID)
		delete(s.tasks, workerID)
		delete(s.steering, workerID)
		s.mu.Unlock()
	}()

	_ = s.setTaskStatus(ctx, task.ID, core.TaskRunning)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{}),
	})

	runState := &workerRunState{}
	err = runner.Run(workerCtx, spec, eventSink{service: s, taskID: task.ID, workerID: workerID, state: runState})
	if err != nil {
		status := core.WorkerFailed
		workspaceResult := WorkspaceResultFailed
		if errors.Is(workerCtx.Err(), context.Canceled) {
			status = core.WorkerCanceled
			workspaceResult = WorkspaceResultCanceled
		}
		changes := s.describeWorkspaceChanges(ctx, workspace)
		_, _ = s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: workerID,
			Payload:  core.MustJSON(runState.completionPayload(status, err, changes)),
		})
		_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, workspaceResult)
		return runState.turnResult(workerID, plan, status, err, changes), nil
	}

	if runState.isWaitingForInput() {
		changes := s.describeWorkspaceChanges(ctx, workspace)
		_, _ = s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: workerID,
			Payload:  core.MustJSON(runState.completionPayload(core.WorkerWaiting, nil, changes)),
		})
		return runState.turnResult(workerID, plan, core.WorkerWaiting, nil, changes), nil
	}

	changes := s.describeWorkspaceChanges(ctx, workspace)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(runState.completionPayload(core.WorkerSucceeded, nil, changes)),
	})
	_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, WorkspaceResultSucceeded)
	return runState.turnResult(workerID, plan, core.WorkerSucceeded, nil, changes), nil
}

func (s *Service) runSSHPlannedWorker(ctx context.Context, task core.Task, plan Plan, runner worker.Runner, target TargetConfig) (WorkerTurnResult, error) {
	workerID := uuid.NewString()
	nodeID := stringMetadata(plan.Metadata, "nodeID")
	if nodeID == "" {
		nodeID = uuid.NewString()
		plan.Metadata["nodeID"] = nodeID
	}
	planID := stringMetadata(plan.Metadata, "planID")
	if planID == "" {
		planID = uuid.NewString()
		plan.Metadata["planID"] = planID
	}
	steering := make(chan string, 16)
	spec := worker.Spec{
		ID:       workerID,
		TaskID:   task.ID,
		Kind:     plan.WorkerKind,
		Prompt:   plan.Prompt,
		WorkDir:  nonEmpty(target.WorkDir, s.workDir),
		Steering: steering,
	}
	command := runner.BuildCommand(spec)
	remoteRun := NewRemoteRun(target, spec)
	plan.Metadata["remoteSession"] = remoteRun.Session
	plan.Metadata["remoteRunDir"] = remoteRun.RunDir
	plan.Metadata["remoteWorkDir"] = remoteRun.WorkDir

	if _, err := s.append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        nodeID,
			"workerId":      workerID,
			"workerKind":    plan.WorkerKind,
			"planId":        planID,
			"parentNodeId":  stringMetadata(plan.Metadata, "parentNodeID"),
			"spawnId":       stringMetadata(plan.Metadata, "spawnID"),
			"role":          stringMetadata(plan.Metadata, "spawnRole"),
			"reason":        stringMetadata(plan.Metadata, "spawnReason"),
			"targetId":      target.ID,
			"targetKind":    string(target.Kind),
			"remoteSession": remoteRun.Session,
			"remoteRunDir":  remoteRun.RunDir,
			"remoteWorkDir": remoteRun.WorkDir,
			"dependsOn":     stringSliceMetadata(plan.Metadata, "dependsOn"),
			"metadata":      planMetadata(plan),
		}),
	}); err != nil {
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          remoteRun.RunDir,
			CWD:           remoteRun.WorkDir,
			SourceRoot:    remoteRun.WorkDir,
			WorkspaceName: remoteRun.Session,
			Mode:          "remote",
			VCSType:       "ssh",
			WorkerID:      workerID,
			TaskID:        task.ID,
		}),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     plan.WorkerKind,
			"command":  command,
			"metadata": planMetadata(plan),
		}),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[workerID] = cancel
	s.tasks[workerID] = task.ID
	s.steering[workerID] = steering
	s.remoteRuns[workerID] = remoteRun
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		close(s.steering[workerID])
		delete(s.cancels, workerID)
		delete(s.tasks, workerID)
		delete(s.steering, workerID)
		delete(s.remoteRuns, workerID)
		s.mu.Unlock()
	}()

	_ = s.setTaskStatus(ctx, task.ID, core.TaskRunning)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": target.ID, "session": remoteRun.Session}),
	})
	runState := &workerRunState{}
	sink := eventSink{service: s, taskID: task.ID, workerID: workerID, state: runState}
	if err := s.sshRunner.Start(workerCtx, remoteRun, command); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	status, err := s.sshRunner.Poll(workerCtx, remoteRun, worker.ParserForKind(plan.WorkerKind), sink)
	workerStatus, statusErr := remoteStatusToWorkerStatus(status)
	if err != nil && !errors.Is(err, context.Canceled) {
		statusErr = err
		workerStatus = core.WorkerFailed
	}
	if errors.Is(workerCtx.Err(), context.Canceled) {
		workerStatus = core.WorkerCanceled
		statusErr = context.Canceled
	}
	changes := WorkspaceChanges{
		Root:     remoteRun.RunDir,
		CWD:      remoteRun.WorkDir,
		Mode:     "remote",
		VCSType:  "ssh",
		Dirty:    false,
		DiffStat: "remote worker changes are reported through worker output",
	}
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(runState.completionPayload(workerStatus, statusErr, changes)),
	})
	return runState.turnResult(workerID, plan, workerStatus, statusErr, changes), nil
}

func (s *Service) finishOrContinueTask(ctx context.Context, taskID string, result WorkerTurnResult) bool {
	switch result.Status {
	case core.WorkerSucceeded:
		return true
	case core.WorkerWaiting:
		_ = s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	case core.WorkerCanceled:
		_ = s.setTaskStatus(ctx, taskID, core.TaskCanceled)
	default:
		_ = s.setTaskStatus(ctx, taskID, core.TaskFailed)
	}
	return false
}

func (s *Service) replanLoop(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) bool {
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		return true
	}
	for turn := 1; turn <= maxDynamicReplanTurns; turn++ {
		decision, err := replanner.Replan(ctx, task, OrchestrationState{
			InitialPlan: initial,
			Results:     results,
			Turn:        turn,
		})
		if err != nil {
			_ = s.failTask(ctx, task.ID, fmt.Errorf("dynamic replan failed: %w", err))
			return false
		}
		if err := decision.Validate(); err != nil {
			_ = s.failTask(ctx, task.ID, fmt.Errorf("invalid dynamic replan decision: %w", err))
			return false
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskReplanned,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"turn":     turn,
				"decision": decision,
			}),
		}); err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return false
		}
		switch decision.Action {
		case "complete":
			return true
		case "wait":
			_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
			return false
		case "fail":
			_ = s.failTask(ctx, task.ID, errors.New(nonEmpty(decision.Message, decision.Rationale, "dynamic replan failed task")))
			return false
		case "continue":
			next := *decision.Plan
			if next.Metadata == nil {
				next.Metadata = map[string]any{}
			}
			next.Metadata["dynamicReplanTurn"] = turn
			if _, err := s.append(ctx, core.Event{
				Type:    core.EventTaskPlanned,
				TaskID:  task.ID,
				Payload: core.MustJSON(next),
			}); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false
			}
			result, err := s.runPlannedWorker(ctx, task, next)
			if err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false
			}
			results = append(results, result)
			if !s.finishOrContinueTask(ctx, task.ID, result) {
				return false
			}
			var ok bool
			results, ok, err = s.runFollowUpWorkers(ctx, task, next, results, result.NodeID)
			if err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false
			}
			if !ok {
				return false
			}
		}
	}
	_ = s.failTask(ctx, task.ID, fmt.Errorf("dynamic replanning exceeded %d turns", maxDynamicReplanTurns))
	return false
}

type followUpNode struct {
	id    string
	index int
	spawn SpawnRequest
	deps  []string
}

func (s *Service) runFollowUpWorkers(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, parentNodeID string) ([]WorkerTurnResult, bool, error) {
	if len(initial.Spawns) == 0 {
		return results, true, nil
	}
	pending, err := followUpNodes(initial.Spawns)
	if err != nil {
		return results, false, err
	}
	completed := map[string]WorkerTurnResult{}
	for len(pending) > 0 {
		ready := readyFollowUps(pending, completed)
		if len(ready) == 0 {
			return results, false, fmt.Errorf("spawn dependency cycle or missing dependency")
		}
		waveResults, ok, err := s.runFollowUpWave(ctx, task, initial, ready, results, parentNodeID)
		results = append(results, waveResults...)
		for index, result := range waveResults {
			completed[ready[index].id] = result
			delete(pending, ready[index].id)
		}
		if err != nil {
			return results, false, err
		}
		if !ok {
			return results, false, nil
		}
	}
	return results, true, nil
}

func followUpNodes(spawns []SpawnRequest) (map[string]followUpNode, error) {
	nodes := map[string]followUpNode{}
	for index, spawn := range spawns {
		id := spawnID(spawn, index)
		if _, ok := nodes[id]; ok {
			return nil, fmt.Errorf("duplicate spawn id %q", id)
		}
		deps := make([]string, 0, len(spawn.DependsOn))
		for _, dep := range spawn.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep != "" {
				deps = append(deps, dep)
			}
		}
		nodes[id] = followUpNode{id: id, index: index, spawn: spawn, deps: deps}
	}
	for _, node := range nodes {
		for _, dep := range node.deps {
			if _, ok := nodes[dep]; !ok {
				return nil, fmt.Errorf("spawn %q depends on unknown spawn %q", node.id, dep)
			}
			if dep == node.id {
				return nil, fmt.Errorf("spawn %q depends on itself", node.id)
			}
		}
	}
	return nodes, nil
}

func spawnID(spawn SpawnRequest, index int) string {
	if strings.TrimSpace(spawn.ID) != "" {
		return strings.TrimSpace(spawn.ID)
	}
	return fmt.Sprintf("spawn-%d", index+1)
}

func readyFollowUps(pending map[string]followUpNode, completed map[string]WorkerTurnResult) []followUpNode {
	ready := []followUpNode{}
	for _, node := range pending {
		blocked := false
		for _, dep := range node.deps {
			if _, ok := completed[dep]; !ok {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, node)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return ready[i].index < ready[j].index
	})
	return ready
}

func (s *Service) runFollowUpWave(ctx context.Context, task core.Task, initial Plan, nodes []followUpNode, priorResults []WorkerTurnResult, parentNodeID string) ([]WorkerTurnResult, bool, error) {
	waveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type outcome struct {
		index  int
		result WorkerTurnResult
		err    error
	}
	outcomes := make(chan outcome, len(nodes))
	for index, node := range nodes {
		followUp := s.followUpPlan(task, initial, node.spawn, priorResults, node.index+2, node.id, node.deps, parentNodeID)
		if _, err := s.append(ctx, core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  task.ID,
			Payload: core.MustJSON(followUp),
		}); err != nil {
			return nil, false, err
		}
		go func(index int, plan Plan) {
			result, err := s.runPlannedWorker(waveCtx, task, plan)
			outcomes <- outcome{index: index, result: result, err: err}
		}(index, followUp)
	}

	ordered := make([]WorkerTurnResult, len(nodes))
	var firstErr error
	var firstProblem *WorkerTurnResult
	for range nodes {
		outcome := <-outcomes
		ordered[outcome.index] = outcome.result
		if outcome.err != nil && firstErr == nil {
			firstErr = outcome.err
			cancel()
			continue
		}
		if outcome.err == nil && outcome.result.Status != core.WorkerSucceeded && firstProblem == nil {
			result := outcome.result
			firstProblem = &result
			cancel()
		}
	}
	if firstErr != nil {
		return ordered, false, firstErr
	}
	if firstProblem != nil {
		s.finishOrContinueTask(ctx, task.ID, *firstProblem)
		return ordered, false, nil
	}
	return ordered, true, nil
}

func (s *Service) followUpPlan(task core.Task, initial Plan, spawn SpawnRequest, results []WorkerTurnResult, turn int, spawnID string, dependsOn []string, parentNodeID string) Plan {
	workerKind := s.workerKindForSpawn(spawn, initial.WorkerKind)
	prompt := buildFollowUpPrompt(task, spawn, results)
	return Plan{
		WorkerKind: workerKind,
		Prompt:     prompt,
		Rationale:  "follow-up worker scheduled from initial plan: " + spawn.Reason,
		Steps: []PlanStep{{
			Title:       "Run " + spawn.Role,
			Description: spawn.Reason,
		}},
		RequiredApprovals: []ApprovalRequest{},
		Spawns:            []SpawnRequest{},
		Metadata: map[string]any{
			"brain":           "orchestrator",
			"scheduler":       "orchestrator",
			"turn":            turn,
			"spawnID":         spawnID,
			"spawnRole":       spawn.Role,
			"spawnReason":     spawn.Reason,
			"dependsOn":       dependsOn,
			"parentNodeID":    parentNodeID,
			"parentRationale": initial.Rationale,
		},
	}
}

func (s *Service) workerKindForSpawn(spawn SpawnRequest, fallback string) string {
	if strings.TrimSpace(spawn.WorkerKind) != "" {
		if _, ok := s.runners[spawn.WorkerKind]; ok {
			return spawn.WorkerKind
		}
	}
	role := strings.ToLower(spawn.Role + " " + spawn.Reason)
	if strings.Contains(role, "review") || strings.Contains(role, "feedback") || strings.Contains(role, "critique") {
		if _, ok := s.runners["claude"]; ok {
			return "claude"
		}
	}
	if _, ok := s.runners["codex"]; ok {
		return "codex"
	}
	if _, ok := s.runners[fallback]; ok {
		return fallback
	}
	if _, ok := s.runners["mock"]; ok {
		return "mock"
	}
	for kind := range s.runners {
		return kind
	}
	return fallback
}

func buildFollowUpPrompt(task core.Task, spawn SpawnRequest, results []WorkerTurnResult) string {
	var builder strings.Builder
	builder.WriteString("# Orchestrator Follow-up Worker Prompt\n\n")
	builder.WriteString("Task: ")
	builder.WriteString(task.Title)
	builder.WriteString("\n\nOriginal user request:\n")
	builder.WriteString(task.Prompt)
	builder.WriteString("\n\nFollow-up role:\n")
	builder.WriteString(spawn.Role)
	builder.WriteString("\n\nReason for this follow-up:\n")
	builder.WriteString(spawn.Reason)
	builder.WriteString("\n\nPrior worker results:\n")
	for index, result := range results {
		builder.WriteString(fmt.Sprintf("\n%d. Worker %s status: %s\n", index+1, result.WorkerID, result.Status))
		if result.Summary != "" {
			builder.WriteString("Summary: ")
			builder.WriteString(result.Summary)
			builder.WriteString("\n")
		}
		if result.Error != "" {
			builder.WriteString("Error: ")
			builder.WriteString(result.Error)
			builder.WriteString("\n")
		}
		if len(result.Changes.ChangedFiles) > 0 {
			builder.WriteString("Changed files:\n")
			for _, file := range result.Changes.ChangedFiles {
				builder.WriteString("- ")
				if file.Status != "" {
					builder.WriteString(file.Status)
					builder.WriteString(" ")
				}
				builder.WriteString(file.Path)
				builder.WriteString("\n")
			}
		}
	}
	builder.WriteString("\nExecute only this follow-up role. Do not apply changes unless this role explicitly requires implementation.\n")
	builder.WriteString("\nReport with these markdown sections when applicable:\n")
	builder.WriteString("- Findings\n")
	builder.WriteString("- Commands Run\n")
	builder.WriteString("- Benchmark Results\n")
	builder.WriteString("- Changed Files\n")
	builder.WriteString("- Blockers\n")
	builder.WriteString("- Recommended Next Turns\n")
	builder.WriteString("\nFor benchmark or profiler work, include exact commands, baseline numbers, candidate numbers, sample count when known, and confidence in whether the change is a real improvement.\n")
	return builder.String()
}

func nonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Service) describeWorkspaceChanges(ctx context.Context, workspace PreparedWorkspace) WorkspaceChanges {
	changes, err := s.workspaces.DescribeChanges(ctx, workspace)
	if err != nil && changes.Error == "" {
		changes.Error = err.Error()
	}
	return changes
}

func (s *Service) workspaceForWorker(ctx context.Context, workerID string) (PreparedWorkspace, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerWorkspace || event.WorkerID != workerID {
			continue
		}
		var workspace PreparedWorkspace
		if err := json.Unmarshal(event.Payload, &workspace); err != nil {
			return PreparedWorkspace{}, fmt.Errorf("decode worker workspace: %w", err)
		}
		return workspace, nil
	}
	return PreparedWorkspace{}, eventstore.ErrNotFound
}

func (s *Service) cleanupWorkspace(ctx context.Context, taskID string, workerID string, workspace PreparedWorkspace, result WorkspaceResult) error {
	cleanup, err := s.workspaces.Cleanup(ctx, workspace, result)
	if err != nil && cleanup.Error == "" {
		cleanup.Error = err.Error()
	}
	_, appendErr := s.append(ctx, core.Event{
		Type:     core.EventWorkerCleanup,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(cleanup),
	})
	if appendErr != nil {
		return appendErr
	}
	return err
}

func (s *Service) setTaskStatus(ctx context.Context, taskID string, status core.TaskStatus) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": status,
		}),
	})
	return err
}

func (s *Service) setExecutionNodeStatus(ctx context.Context, taskID string, nodeID string, status core.WorkerStatus) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventExecutionStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"nodeId": nodeID,
			"status": status,
		}),
	})
	return err
}

func (s *Service) failTask(ctx context.Context, taskID string, err error) error {
	_, appendErr := s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
			"error":  err.Error(),
		}),
	})
	return appendErr
}

func planMetadata(plan Plan) map[string]any {
	metadata := map[string]any{}
	for _, key := range []string{
		"brain",
		"scheduler",
		"model",
		"fallbackReason",
		"parentRationale",
		"dependsOn",
		"nodeID",
		"parentNodeID",
		"planID",
		"targetID",
		"targetKind",
		"remoteSession",
		"remoteRunDir",
		"remoteWorkDir",
		"spawnID",
		"spawnReason",
		"spawnRole",
		"turn",
		"dynamicReplanTurn",
	} {
		if value, ok := plan.Metadata[key]; ok && value != nil && value != "" {
			metadata[key] = value
		}
	}
	if plan.Rationale != "" {
		metadata["rationale"] = plan.Rationale
	}
	if len(plan.Steps) > 0 {
		metadata["steps"] = plan.Steps
	}
	if len(plan.RequiredApprovals) > 0 {
		metadata["requiredApprovals"] = plan.RequiredApprovals
	}
	if len(plan.Spawns) > 0 {
		metadata["spawns"] = plan.Spawns
	}
	return metadata
}

func stringMetadata(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	switch value := metadata[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func stringSliceMetadata(metadata map[string]any, key string) []string {
	if metadata == nil {
		return nil
	}
	switch value := metadata[key].(type) {
	case []string:
		return value
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				out = append(out, strings.TrimSpace(text))
			}
		}
		return out
	default:
		return nil
	}
}

func runnerSupportsSteering(runner worker.Runner) bool {
	support, ok := runner.(worker.SteeringSupport)
	return ok && support.SupportsSteering()
}

func (s *Service) append(ctx context.Context, event core.Event) (core.Event, error) {
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	stored, err := s.store.Append(ctx, event)
	if err != nil {
		return core.Event{}, err
	}
	s.broker.Publish(stored)
	return stored, nil
}

type eventSink struct {
	service  *Service
	taskID   string
	workerID string
	state    *workerRunState
}

func (s eventSink) Event(ctx context.Context, event worker.Event) error {
	if s.state != nil {
		s.state.observe(event)
	}
	_, err := s.service.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   s.taskID,
		WorkerID: s.workerID,
		Payload:  core.MustJSON(event),
	})
	return err
}

type workerRunState struct {
	mu         sync.Mutex
	logCount   int
	summary    string
	lastError  string
	needsInput bool
	rawResult  []byte
}

func (s *workerRunState) observe(event worker.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Kind {
	case worker.EventResult:
		s.summary = event.Text
		if len(event.Raw) > 0 {
			s.rawResult = append(s.rawResult[:0], event.Raw...)
		}
	case worker.EventError:
		s.lastError = event.Text
	case worker.EventNeedsInput:
		s.needsInput = true
		if s.summary == "" {
			s.summary = event.Text
		}
	default:
		s.logCount++
	}
}

func (s *workerRunState) isWaitingForInput() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.needsInput
}

func (s *workerRunState) completionPayload(status core.WorkerStatus, runErr error, changes WorkspaceChanges) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := map[string]any{
		"status":           status,
		"logCount":         s.logCount,
		"needsInput":       s.needsInput,
		"workspaceChanges": changes,
	}
	if len(changes.ChangedFiles) > 0 {
		payload["changedFiles"] = changes.ChangedFiles
	}
	if s.summary != "" {
		payload["summary"] = s.summary
	}
	if s.lastError != "" {
		payload["error"] = s.lastError
	} else if runErr != nil {
		payload["error"] = runErr.Error()
	}
	if len(s.rawResult) > 0 {
		payload["rawResult"] = core.MustJSON(jsonRawMessage(s.rawResult))
	}
	return payload
}

func (s *workerRunState) turnResult(workerID string, plan Plan, status core.WorkerStatus, runErr error, changes WorkspaceChanges) WorkerTurnResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := WorkerTurnResult{
		WorkerID: workerID,
		Status:   status,
		Kind:     plan.WorkerKind,
		Summary:  s.summary,
		Changes:  changes,
	}
	if plan.Metadata != nil {
		if nodeID, ok := plan.Metadata["nodeID"].(string); ok {
			result.NodeID = nodeID
		}
		if role, ok := plan.Metadata["spawnRole"].(string); ok {
			result.Role = role
		}
	}
	if s.lastError != "" {
		result.Error = s.lastError
	} else if runErr != nil {
		result.Error = runErr.Error()
	}
	return result
}

type jsonRawMessage []byte

func (m jsonRawMessage) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return []byte("null"), nil
	}
	return m, nil
}
