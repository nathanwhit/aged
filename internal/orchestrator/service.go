package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type Service struct {
	store      eventstore.Store
	broker     *Broker
	brain      BrainProvider
	runners    map[string]worker.Runner
	workDir    string
	workspaces WorkspaceManager

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
	tasks   map[string]string
}

func NewService(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string) *Service {
	return NewServiceWithWorkspaceManager(store, brain, runners, workDir, NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeIsolated, "", WorkspaceCleanupRetain))
}

func NewServiceWithWorkspaceManager(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string, workspaces WorkspaceManager) *Service {
	if workspaces == nil {
		workspaces = NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeIsolated, "", WorkspaceCleanupRetain)
	}
	return &Service{
		store:      store,
		broker:     NewBroker(),
		brain:      brain,
		runners:    runners,
		workDir:    workDir,
		workspaces: workspaces,
		cancels:    map[string]context.CancelFunc{},
		tasks:      map[string]string{},
	}
}

func (s *Service) Snapshot(ctx context.Context) (core.Snapshot, error) {
	return s.store.Snapshot(ctx)
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
	return err
}

func (s *Service) CancelWorker(ctx context.Context, workerID string) error {
	s.mu.Lock()
	cancel := s.cancels[workerID]
	s.mu.Unlock()
	if cancel == nil {
		return eventstore.ErrNotFound
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

	runner := s.runners[plan.WorkerKind]
	if runner == nil {
		_ = s.failTask(ctx, task.ID, fmt.Errorf("unknown worker kind %q", plan.WorkerKind))
		return
	}

	workerID := uuid.NewString()
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	workspace, err := s.workspaces.Prepare(ctx, WorkspaceSpec{
		TaskID:   task.ID,
		WorkerID: workerID,
		WorkDir:  s.workDir,
	})
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(workspace),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	spec := worker.Spec{
		ID:      workerID,
		TaskID:  task.ID,
		Kind:    plan.WorkerKind,
		Prompt:  plan.Prompt,
		WorkDir: workspace.CWD,
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
		_ = s.failTask(ctx, task.ID, err)
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[workerID] = cancel
	s.tasks[workerID] = task.ID
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.cancels, workerID)
		delete(s.tasks, workerID)
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
		taskStatus := core.TaskFailed
		workspaceResult := WorkspaceResultFailed
		if errors.Is(workerCtx.Err(), context.Canceled) {
			status = core.WorkerCanceled
			taskStatus = core.TaskCanceled
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
		_ = s.setTaskStatus(ctx, task.ID, taskStatus)
		return
	}

	if runState.isWaitingForInput() {
		changes := s.describeWorkspaceChanges(ctx, workspace)
		_, _ = s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: workerID,
			Payload:  core.MustJSON(runState.completionPayload(core.WorkerWaiting, nil, changes)),
		})
		_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
		return
	}

	changes := s.describeWorkspaceChanges(ctx, workspace)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(runState.completionPayload(core.WorkerSucceeded, nil, changes)),
	})
	_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, WorkspaceResultSucceeded)
	_ = s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
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
	for _, key := range []string{"brain", "scheduler", "model", "fallbackReason"} {
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

type jsonRawMessage []byte

func (m jsonRawMessage) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return []byte("null"), nil
	}
	return m, nil
}
