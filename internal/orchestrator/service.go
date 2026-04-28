package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"

	"github.com/google/uuid"
)

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

	err = runner.Run(workerCtx, spec, eventSink{service: s, taskID: task.ID, workerID: workerID})
	if err != nil {
		status := core.WorkerFailed
		taskStatus := core.TaskFailed
		workspaceResult := WorkspaceResultFailed
		if errors.Is(workerCtx.Err(), context.Canceled) {
			status = core.WorkerCanceled
			taskStatus = core.TaskCanceled
			workspaceResult = WorkspaceResultCanceled
		}
		_, _ = s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status": status,
				"error":  err.Error(),
			}),
		})
		_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, workspaceResult)
		_ = s.setTaskStatus(ctx, task.ID, taskStatus)
		return
	}

	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
		}),
	})
	_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, WorkspaceResultSucceeded)
	_ = s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
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
}

func (s eventSink) Event(ctx context.Context, event worker.Event) error {
	_, err := s.service.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   s.taskID,
		WorkerID: s.workerID,
		Payload:  core.MustJSON(event),
	})
	return err
}
