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
	store       eventstore.Store
	broker      *Broker
	brain       BrainProvider
	assistant   AssistantProvider
	titles      TitleGenerator
	runners     map[string]worker.Runner
	workDir     string
	projects    *ProjectRegistry
	plugins     *PluginRegistry
	workspaces  WorkspaceManager
	targets     *TargetRegistry
	sshRunner   SSHRunner
	prPublisher PullRequestPublisher

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
	projects, err := NewDefaultProjectRegistry(workDir)
	if err != nil {
		projects, _ = NewProjectRegistry([]core.Project{{
			ID:          "default",
			Name:        "default",
			LocalPath:   workDir,
			VCS:         "auto",
			DefaultBase: "main",
		}}, "default")
	}
	return &Service{
		store:       store,
		broker:      NewBroker(),
		brain:       brain,
		runners:     runners,
		workDir:     workDir,
		projects:    projects,
		plugins:     NewPluginRegistry(builtinPlugins()),
		workspaces:  workspaces,
		targets:     targets,
		sshRunner:   sshRunner,
		prPublisher: NewLocalPullRequestPublisher(),
		cancels:     map[string]context.CancelFunc{},
		tasks:       map[string]string{},
		steering:    map[string]chan string{},
		remoteRuns:  map[string]remoteRun{},
	}
}

func (s *Service) SetAssistant(assistant AssistantProvider) {
	s.assistant = assistant
	if assistant != nil && s.titles == nil {
		s.titles = AssistantTitleGenerator{Assistant: assistant}
	}
}

func (s *Service) SetTitleGenerator(generator TitleGenerator) {
	s.titles = generator
}

func (s *Service) SetProjects(projects *ProjectRegistry) {
	if projects != nil {
		s.projects = projects
		s.workDir = projects.Default().LocalPath
	}
}

func (s *Service) SetPlugins(plugins *PluginRegistry) {
	if plugins != nil {
		s.plugins = plugins
	}
}

func (s *Service) SetPullRequestPublisher(publisher PullRequestPublisher) {
	s.prPublisher = publisher
}

func (s *Service) Snapshot(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if s.targets != nil {
		snapshot.Targets = s.targets.Snapshot()
	}
	if s.projects != nil {
		snapshot.Projects = s.projects.Snapshot()
	}
	if s.plugins != nil {
		snapshot.Plugins = s.plugins.Snapshot()
	}
	return snapshot, nil
}

func (s *Service) RecoverRemoteWorkers(ctx context.Context) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.cancelStaleLocalWorkers(ctx, snapshot); err != nil {
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

func (s *Service) cancelStaleLocalWorkers(ctx context.Context, snapshot core.Snapshot) error {
	nodesByWorker := map[string]core.ExecutionNode{}
	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID != "" {
			nodesByWorker[node.WorkerID] = node
		}
	}
	for _, worker := range snapshot.Workers {
		if isTerminalWorkerStatus(worker.Status) {
			continue
		}
		node := nodesByWorker[worker.ID]
		if node.TargetKind == string(TargetKindSSH) {
			continue
		}
		_, err := s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   worker.TaskID,
			WorkerID: worker.ID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerCanceled,
				"summary": "Local worker was marked canceled during daemon startup recovery.",
				"error":   "local worker did not have a recoverable process handle after daemon restart",
			}),
		})
		if err != nil {
			return err
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: worker.TaskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskCanceled,
			}),
		}); err != nil {
			return err
		}
	}
	return nil
}

func isTerminalWorkerStatus(status core.WorkerStatus) bool {
	return status == core.WorkerSucceeded || status == core.WorkerFailed || status == core.WorkerCanceled
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
	if req.Prompt == "" {
		return core.Task{}, errors.New("prompt is required")
	}
	title := strings.TrimSpace(req.Title)
	metadata, err := createTaskMetadata(req)
	if err != nil {
		return core.Task{}, err
	}
	if title == "" {
		title = s.generateTaskTitle(ctx, req.Prompt)
		metadata["titleGenerated"] = true
	}
	project, err := s.projects.Resolve(req)
	if err != nil {
		return core.Task{}, err
	}
	metadata["projectId"] = project.ID
	if req.Source != "" || req.ExternalID != "" {
		if req.Source == "" || req.ExternalID == "" {
			return core.Task{}, errors.New("source and externalId must be provided together")
		}
		if existing, ok, err := s.FindTaskByExternalID(ctx, req.Source, req.ExternalID); err != nil {
			return core.Task{}, err
		} else if ok {
			return existing, nil
		}
	}

	taskID := uuid.NewString()
	created, err := s.append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"projectId": project.ID,
			"title":     title,
			"prompt":    req.Prompt,
			"metadata":  metadata,
		}),
	})
	if err != nil {
		return core.Task{}, err
	}

	task := core.Task{
		ID:        taskID,
		ProjectID: project.ID,
		Title:     title,
		Prompt:    req.Prompt,
		Status:    core.TaskQueued,
		CreatedAt: created.At,
		UpdatedAt: created.At,
		Metadata:  core.MustJSON(metadata),
	}

	go s.runTask(context.Background(), task)
	return task, nil
}

func (s *Service) generateTaskTitle(ctx context.Context, prompt string) string {
	if s.titles != nil {
		if title, err := s.titles.GenerateTitle(ctx, prompt); err == nil && strings.TrimSpace(title) != "" {
			return title
		}
	}
	return fallbackTaskTitle(prompt)
}

func (s *Service) Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		return core.AssistantResponse{}, errors.New("message is required")
	}
	if strings.TrimSpace(req.ConversationID) == "" {
		req.ConversationID = uuid.NewString()
	}
	if _, err := s.append(ctx, core.Event{
		Type: core.EventAssistantAsked,
		Payload: core.MustJSON(map[string]any{
			"conversationId": req.ConversationID,
			"message":        req.Message,
			"context":        req.Context,
		}),
	}); err != nil {
		return core.AssistantResponse{}, err
	}
	assistant := s.assistant
	if assistant == nil {
		var ok bool
		assistant, ok = s.brain.(AssistantProvider)
		if !ok {
			return core.AssistantResponse{}, errors.New("assistant brain is not configured")
		}
	}
	response, err := assistant.Ask(ctx, req)
	if err != nil {
		return core.AssistantResponse{}, err
	}
	if strings.TrimSpace(response.ConversationID) == "" {
		response.ConversationID = req.ConversationID
	}
	if _, err := s.append(ctx, core.Event{
		Type: core.EventAssistantAnswered,
		Payload: core.MustJSON(map[string]any{
			"conversationId": response.ConversationID,
			"message":        response.Message,
			"metadata":       response.Metadata,
		}),
	}); err != nil {
		return core.AssistantResponse{}, err
	}
	return response, nil
}

func (s *Service) PublishTaskPullRequest(ctx context.Context, taskID string, req core.PublishPullRequestRequest) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	if !isTerminalTaskStatus(task.Status) {
		return core.PullRequest{}, errors.New("can only publish pull requests for terminal tasks")
	}
	workerID := strings.TrimSpace(req.WorkerID)
	project := s.projectForTask(task)
	sourceRoot := project.LocalPath
	if workerID == "" {
		candidates := applyCandidates(snapshot, taskID)
		unapplied := unappliedCandidates(candidates)
		switch len(unapplied) {
		case 0:
			if latest := latestAppliedWorker(snapshot, taskID); latest != "" {
				workerID = latest
			}
		case 1:
			workerID = unapplied[0].WorkerID
		default:
			return core.PullRequest{}, errors.New("multiple unapplied worker changes exist; provide workerId")
		}
	}
	if workerID != "" {
		applied, err := s.workerChangesApplied(ctx, workerID)
		if err != nil {
			return core.PullRequest{}, err
		}
		if !applied {
			result, err := s.ApplyWorkerChanges(ctx, workerID)
			if err != nil {
				return core.PullRequest{}, err
			}
			sourceRoot = result.SourceRoot
		} else if appliedRoot := appliedWorkerSourceRoot(snapshot, workerID); appliedRoot != "" {
			sourceRoot = appliedRoot
		} else if workspace, err := s.workspaceForWorker(ctx, workerID); err == nil && workspace.SourceRoot != "" {
			sourceRoot = workspace.SourceRoot
		}
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = task.Title
	}
	body := strings.TrimSpace(req.Body)
	if body == "" {
		body = fmt.Sprintf("Task: `%s`\n\n%s", task.ID, task.Prompt)
	}
	pr, err := s.prPublisher.Publish(ctx, PullRequestPublishSpec{
		TaskID:   taskID,
		WorkerID: workerID,
		WorkDir:  sourceRoot,
		Repo:     nonEmpty(req.Repo, project.Repo),
		Base:     nonEmpty(req.Base, project.DefaultBase),
		Branch:   req.Branch,
		Title:    title,
		Body:     body,
		Draft:    req.Draft,
		Metadata: map[string]any{
			"workerId":  workerID,
			"workDir":   sourceRoot,
			"projectId": project.ID,
		},
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	if pr.ID == "" {
		pr.ID = uuid.NewString()
	}
	pr.TaskID = taskID
	event, err := s.append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":           pr.ID,
			"repo":         pr.Repo,
			"number":       pr.Number,
			"url":          pr.URL,
			"branch":       pr.Branch,
			"base":         pr.Base,
			"title":        pr.Title,
			"state":        pr.State,
			"draft":        pr.Draft,
			"checksStatus": pr.ChecksStatus,
			"mergeStatus":  pr.MergeStatus,
			"reviewStatus": pr.ReviewStatus,
			"metadata":     pr.Metadata,
		}),
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	pr.CreatedAt = event.At
	pr.UpdatedAt = event.At
	return pr, nil
}

func (s *Service) RefreshPullRequest(ctx context.Context, prID string) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	pr, ok, err := s.findPullRequest(ctx, prID)
	if err != nil {
		return core.PullRequest{}, err
	}
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	checked, err := s.prPublisher.Inspect(ctx, pr)
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.ID = pr.ID
	checked.TaskID = pr.TaskID
	event, err := s.append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":           checked.ID,
			"state":        checked.State,
			"draft":        checked.Draft,
			"checksStatus": checked.ChecksStatus,
			"mergeStatus":  checked.MergeStatus,
			"reviewStatus": checked.ReviewStatus,
			"metadata":     checked.Metadata,
		}),
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.UpdatedAt = event.At
	return checked, nil
}

func (s *Service) StartPullRequestBabysitter(ctx context.Context, prID string) (core.Task, error) {
	pr, ok, err := s.findPullRequest(ctx, prID)
	if err != nil {
		return core.Task{}, err
	}
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	task, err := s.CreateTask(ctx, core.CreateTaskRequest{
		Title:      fmt.Sprintf("Babysit PR %s#%d", pr.Repo, pr.Number),
		Prompt:     pullRequestBabysitterPrompt(pr),
		Source:     "github-pr-babysitter",
		ExternalID: pr.ID,
		Metadata: core.MustJSON(map[string]any{
			"pullRequestId": pr.ID,
			"repo":          pr.Repo,
			"number":        pr.Number,
			"url":           pr.URL,
		}),
	})
	if err != nil {
		return core.Task{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventPRBabysitter,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":               pr.ID,
			"babysitterTaskId": task.ID,
		}),
	}); err != nil {
		return core.Task{}, err
	}
	return task, nil
}

func (s *Service) FindTaskByExternalID(ctx context.Context, source string, externalID string) (core.Task, bool, error) {
	source = strings.TrimSpace(source)
	externalID = strings.TrimSpace(externalID)
	if source == "" || externalID == "" {
		return core.Task{}, false, errors.New("source and externalId are required")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, false, err
	}
	for _, task := range snapshot.Tasks {
		taskSource, taskExternalID := taskExternalRef(task)
		if taskSource == source && taskExternalID == externalID {
			return task, true, nil
		}
	}
	return core.Task{}, false, nil
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
	snapshot, snapshotErr := s.store.Snapshot(ctx)
	if snapshotErr == nil && taskStatus(snapshot, taskID) == core.TaskWaiting {
		go s.resumeWaitingTask(context.Background(), taskID, req.Message)
	}
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
	return s.reviewWorkerChanges(ctx, workerID, true)
}

func (s *Service) reviewWorkerChanges(ctx context.Context, workerID string, includeDiff bool) (WorkerChangesReview, error) {
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return WorkerChangesReview{}, err
	}
	changes := s.describeWorkspaceChanges(ctx, workspace)
	if includeDiff && changes.Error == "" {
		if diff, err := s.describeWorkspaceDiff(ctx, workspace); err != nil {
			changes.Error = err.Error()
		} else {
			changes.Diff = strings.TrimSpace(diff)
		}
	}
	return WorkerChangesReview{
		WorkerID:  workerID,
		Workspace: workspace,
		Changes:   changes,
	}, nil
}

func (s *Service) ApplyWorkerChanges(ctx context.Context, workerID string) (WorkerApplyResult, error) {
	if applied, err := s.workerChangesApplied(ctx, workerID); err != nil {
		return WorkerApplyResult{}, err
	} else if applied {
		return WorkerApplyResult{}, fmt.Errorf("worker changes already applied: %s", workerID)
	}
	review, err := s.reviewWorkerChanges(ctx, workerID, false)
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

func appliedWorkerSourceRoot(snapshot core.Snapshot, workerID string) string {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerApplied || event.WorkerID != workerID {
			continue
		}
		var payload struct {
			SourceRoot string `json:"sourceRoot"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			return payload.SourceRoot
		}
	}
	return ""
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

func (s *Service) findPullRequest(ctx context.Context, prID string) (core.PullRequest, bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false, err
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == prID {
			return pr, true, nil
		}
	}
	return core.PullRequest{}, false, nil
}

func pullRequestBabysitterPrompt(pr core.PullRequest) string {
	return fmt.Sprintf(`Monitor GitHub pull request %s#%d until it is ready to merge.

Pull request URL: %s
Branch: %s
Base: %s

Repeatedly inspect CI status, review comments, and mergeability. If checks fail or review comments request changes, diagnose the issue, make the required code changes in the repo, and report what changed. If the PR is green and no action is needed, report that it is ready. Do not merge unless the user explicitly asks for merge.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base)
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
	if result.Status == core.WorkerWaiting {
		s.handleWorkerQuestion(ctx, task, plan, results, result)
		return
	}
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

func (s *Service) handleWorkerQuestion(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, waiting WorkerTurnResult) {
	question := nonEmpty(waiting.Summary, waiting.Error, "worker requested orchestrator input")
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventApprovalNeeded,
		TaskID:   task.ID,
		WorkerID: waiting.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"question": question,
			"summary":  waiting.Summary,
			"error":    waiting.Error,
			"reason":   "worker_needs_input",
		}),
	})
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
		return
	}
	decision, err := replanner.Replan(ctx, task, OrchestrationState{
		InitialPlan: initial,
		Results:     results,
		Turn:        1,
	})
	if err != nil {
		_ = s.failTask(ctx, task.ID, fmt.Errorf("question replan failed: %w", err))
		return
	}
	if err := decision.Validate(); err != nil {
		_ = s.failTask(ctx, task.ID, fmt.Errorf("invalid question replan decision: %w", err))
		return
	}
	switch decision.Action {
	case "continue":
		_, _ = s.append(ctx, core.Event{
			Type:   core.EventApprovalDecided,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"approved": true,
				"answer":   nonEmpty(decision.Message, decision.Rationale),
				"reason":   "autonomous_replan",
				"workerId": waiting.WorkerID,
			}),
		})
		if decision.Plan.Metadata == nil {
			decision.Plan.Metadata = map[string]any{}
		}
		decision.Plan.Metadata["parentNodeID"] = waiting.NodeID
		decision.Plan.Metadata["questionWorkerID"] = waiting.WorkerID
		_, _ = s.append(ctx, core.Event{
			Type:   core.EventTaskReplanned,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"action":    decision.Action,
				"rationale": decision.Rationale,
				"message":   decision.Message,
				"turn":      1,
			}),
		})
		_, _ = s.append(ctx, core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  task.ID,
			Payload: core.MustJSON(decision.Plan),
		})
		if result, err := s.runPlannedWorker(ctx, task, *decision.Plan); err != nil {
			_ = s.failTask(ctx, task.ID, err)
		} else if result.Status == core.WorkerWaiting {
			s.handleWorkerQuestion(ctx, task, *decision.Plan, append(results, result), result)
		} else if s.finishOrContinueTask(ctx, task.ID, result) {
			_ = s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
		}
	case "wait":
		_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
	case "complete":
		_ = s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
	case "fail":
		_ = s.failTask(ctx, task.ID, errors.New(nonEmpty(decision.Message, decision.Rationale, "worker question could not be answered")))
	}
}

func (s *Service) resumeWaitingTask(ctx context.Context, taskID string, feedback string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskWaiting {
		return
	}
	waitingWorkerID, question := latestWorkerQuestion(snapshot, taskID)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventApprovalDecided,
		TaskID:   taskID,
		WorkerID: waitingWorkerID,
		Payload: core.MustJSON(map[string]any{
			"approved": true,
			"answer":   feedback,
			"question": question,
			"reason":   "user_feedback",
		}),
	})
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return
	}
	steering := taskSteering(snapshot, taskID)
	steering = append(steering, fmt.Sprintf("Worker question: %s\nFeedback: %s", question, feedback))
	plan, err := s.brain.Plan(ctx, task, steering)
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if err := plan.Validate(); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["questionWorkerID"] = waitingWorkerID
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if result.Status == core.WorkerWaiting {
		s.handleWorkerQuestion(ctx, task, plan, []WorkerTurnResult{result}, result)
		return
	}
	if s.finishOrContinueTask(ctx, taskID, result) {
		_ = s.setTaskStatus(ctx, taskID, core.TaskSucceeded)
	}
}

func (s *Service) runPlannedWorker(ctx context.Context, task core.Task, plan Plan) (WorkerTurnResult, error) {
	runner := s.runners[plan.WorkerKind]
	if runner == nil {
		return WorkerTurnResult{}, fmt.Errorf("unknown worker kind %q", plan.WorkerKind)
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	project := s.projectForTask(task)
	plan.Metadata["projectId"] = project.ID
	if plan.Metadata["targetLabels"] == nil && len(project.TargetLabels) > 0 {
		plan.Metadata["targetLabels"] = project.TargetLabels
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
		WorkDir:  project.LocalPath,
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
	project := s.projectForTask(task)
	steering := make(chan string, 16)
	spec := worker.Spec{
		ID:       workerID,
		TaskID:   task.ID,
		Kind:     plan.WorkerKind,
		Prompt:   plan.Prompt,
		WorkDir:  nonEmpty(target.WorkDir, project.LocalPath),
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
	changes := s.sshRunner.DescribeChanges(ctx, remoteRun)
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

func taskStatus(snapshot core.Snapshot, taskID string) core.TaskStatus {
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return ""
	}
	return task.Status
}

func (s *Service) projectForTask(task core.Task) core.Project {
	if s.projects == nil {
		return core.Project{ID: "default", Name: "default", LocalPath: s.workDir, VCS: "auto", DefaultBase: "main"}
	}
	if task.ProjectID != "" {
		if project, ok := s.projects.Get(task.ProjectID); ok {
			return project
		}
	}
	if len(task.Metadata) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(task.Metadata, &metadata); err == nil {
			if projectID := stringMetadataValue(metadata["projectId"]); projectID != "" {
				if project, ok := s.projects.Get(projectID); ok {
					return project
				}
			}
			if repo := stringMetadataValue(metadata["repo"]); repo != "" {
				if project, ok := s.projects.FindByRepo(repo); ok {
					return project
				}
			}
		}
	}
	return s.projects.Default()
}

func createTaskMetadata(req core.CreateTaskRequest) (map[string]any, error) {
	metadata := map[string]any{}
	if len(req.Metadata) > 0 {
		if err := json.Unmarshal(req.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("metadata must be a JSON object: %w", err)
		}
		if metadata == nil {
			metadata = map[string]any{}
		}
	}
	if req.Source != "" {
		metadata["source"] = strings.TrimSpace(req.Source)
	}
	if req.ExternalID != "" {
		metadata["externalId"] = strings.TrimSpace(req.ExternalID)
	}
	if req.ProjectID != "" {
		metadata["projectId"] = strings.TrimSpace(req.ProjectID)
	}
	return metadata, nil
}

func taskExternalRef(task core.Task) (string, string) {
	if len(task.Metadata) == 0 {
		return "", ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return "", ""
	}
	return stringMetadataValue(metadata["source"]), stringMetadataValue(metadata["externalId"])
}

func stringMetadataValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func findTask(snapshot core.Snapshot, taskID string) (core.Task, bool) {
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return task, true
		}
	}
	return core.Task{}, false
}

func taskSteering(snapshot core.Snapshot, taskID string) []string {
	var out []string
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskSteered {
			continue
		}
		var payload struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && strings.TrimSpace(payload.Message) != "" {
			out = append(out, payload.Message)
		}
	}
	return out
}

func latestWorkerQuestion(snapshot core.Snapshot, taskID string) (string, string) {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventWorkerCompleted:
			var payload struct {
				NeedsInput bool   `json:"needsInput"`
				Summary    string `json:"summary"`
				Error      string `json:"error"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.NeedsInput {
				return event.WorkerID, nonEmpty(payload.Summary, payload.Error, "worker requested orchestrator input")
			}
		case core.EventWorkerOutput:
			var payload struct {
				Kind string `json:"kind"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.Kind == string(worker.EventNeedsInput) {
				return event.WorkerID, nonEmpty(payload.Text, "worker requested orchestrator input")
			}
		}
	}
	return "", "worker requested orchestrator input"
}

func (s *Service) describeWorkspaceChanges(ctx context.Context, workspace PreparedWorkspace) WorkspaceChanges {
	changes, err := s.workspaces.DescribeChanges(ctx, workspace)
	if err != nil && changes.Error == "" {
		changes.Error = err.Error()
	}
	return changes
}

func (s *Service) describeWorkspaceDiff(ctx context.Context, workspace PreparedWorkspace) (string, error) {
	type workspaceDiffer interface {
		DescribeDiff(context.Context, PreparedWorkspace) (string, error)
	}
	differ, ok := s.workspaces.(workspaceDiffer)
	if !ok {
		return "", nil
	}
	return differ.DescribeDiff(ctx, workspace)
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
