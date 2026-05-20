package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
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

var (
	standaloneNoPRPattern     = regexp.MustCompile(`\bno\s+pr\b`)
	errWorkerCallbackDeferred = errors.New("worker callback deferred")
)

const (
	taskCancelReasonStartupRecovery = "startup_worker_recovery"
	taskCancelReasonUser            = "user_requested"
)

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
	store         eventstore.Store
	broker        *Broker
	brain         BrainProvider
	assistant     AssistantProvider
	titles        TitleGenerator
	runners       map[string]worker.Runner
	baseRunners   map[string]worker.Runner
	pluginRunners map[string]struct{}
	workDir       string
	projects      *ProjectRegistry
	plugins       *PluginRegistry
	promptSets    *PromptSetRegistry
	pluginCtx     context.Context
	drivers       *DriverRegistry
	workspaces    WorkspaceManager
	targets       *TargetRegistry
	sshRunner     SSHRunner
	usageSource   ProviderUsageSource
	prPublisher   PullRequestPublisher
	remoteApply   func(context.Context, core.Project, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error)

	mu          sync.Mutex
	cancels     map[string]context.CancelFunc
	taskCancels map[string]context.CancelFunc
	taskRuns    map[string]string
	tasks       map[string]string
	steering    map[string]chan string
	remoteRuns  map[string]remoteRun
	workerCaps  map[string]worker.Capabilities

	steeringRestarts map[string]struct{}
}

const (
	maxCompletionPublishRecoveryAttempts  = 4
	maxConsecutiveUnproductiveReplanTurns = 4
)

var workerCompletedAppendRetryDelays = []time.Duration{
	0,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
}

func workerExecutionPrompt(prompt string, workspace PreparedWorkspace) string {
	cwd := strings.TrimSpace(workspace.CWD)
	sourceRoot := strings.TrimSpace(workspace.SourceRoot)
	if cwd == "" {
		return prompt
	}

	var b strings.Builder
	b.WriteString("# Execution Workspace\n\n")
	b.WriteString("Run every command from this execution workspace:\n")
	b.WriteString(cwd)
	b.WriteString("\n\n")
	if targetID := strings.TrimSpace(workspace.TargetID); targetID != "" {
		b.WriteString("Execution target id: ")
		b.WriteString(targetID)
		if targetKind := strings.TrimSpace(workspace.TargetKind); targetKind != "" {
			b.WriteString(" (")
			b.WriteString(targetKind)
			b.WriteString(")")
		}
		b.WriteString("\n\n")
	}
	b.WriteString("Edit only files under the execution workspace. ")
	if sourceRoot != "" && sourceRoot != cwd {
		b.WriteString("Do not edit the source checkout directly:\n")
		b.WriteString(sourceRoot)
		b.WriteString("\n\n")
		b.WriteString("If the worker task below names the source checkout or another local checkout path, treat that path as context only and translate the work to the execution workspace.\n\n")
	} else {
		b.WriteString("Use the current working directory as the repository root.\n\n")
	}
	if helper := strings.TrimSpace(workspaceCreateTaskHelperPath(workspace)); helper != "" {
		b.WriteString("# Aged Task Creation\n\n")
		b.WriteString("If this worker needs to delegate, fan out, or spawn follow-up aged tasks, use this helper:\n")
		b.WriteString(helper)
		b.WriteString("\n\n")
		b.WriteString("It reads the new task prompt from stdin. Example: `printf '%s\\n' \"Concrete task prompt\" | ")
		b.WriteString(helper)
		b.WriteString(" --title \"Follow-up\"`. When the worker task explicitly asks you to spawn or create aged tasks, queue those tasks with this helper instead of doing the delegated implementation yourself.\n\n")
	}
	if helper := strings.TrimSpace(workspacePublishPRHelperPath(workspace)); helper != "" {
		b.WriteString("# Aged Pull Request Publication\n\n")
		b.WriteString("If this worker needs to publish an intermediate pull request for its own changes, use this helper instead of `gh pr create`:\n")
		b.WriteString(helper)
		b.WriteString("\n\n")
		b.WriteString("It reads the pull request body from stdin and asks the original aged orchestrator to publish the current worker result. Example: `printf '%s\\n' \"Summary and validation\" | ")
		b.WriteString(helper)
		b.WriteString(" --title \"Short PR title\"`. Durable loops should use this helper so aged records and babysits the PR while the loop continues.\n\n")
	}
	b.WriteString("# Worker Task\n\n")
	b.WriteString(strings.TrimSpace(prompt))
	return b.String()
}

func workspaceAgedWorkerDir(workspace PreparedWorkspace) string {
	if strings.EqualFold(strings.TrimSpace(workspace.Mode), "remote") || strings.EqualFold(strings.TrimSpace(workspace.VCSType), "ssh") {
		return ""
	}
	workerID := strings.TrimSpace(workspace.WorkerID)
	if workerID == "" {
		workerID = "worker"
	}
	return filepath.Join(os.TempDir(), "aged-worker-callbacks", workerID)
}

func workspaceCreateTaskHelperPath(workspace PreparedWorkspace) string {
	base := workspaceAgedWorkerDir(workspace)
	if base == "" {
		return ""
	}
	return filepath.Join(base, "bin", "aged-create-task")
}

func workspacePublishPRHelperPath(workspace PreparedWorkspace) string {
	base := workspaceAgedWorkerDir(workspace)
	if base == "" {
		return ""
	}
	return filepath.Join(base, "bin", "aged-publish-pr")
}

func workspaceCallbackDir(workspace PreparedWorkspace) string {
	base := workspaceAgedWorkerDir(workspace)
	if base == "" {
		return ""
	}
	return filepath.Join(base, "callbacks")
}

func installLocalCreateTaskHelper(workspace PreparedWorkspace) (string, string, error) {
	helperPath := workspaceCreateTaskHelperPath(workspace)
	callbackDir := workspaceCallbackDir(workspace)
	if helperPath == "" || callbackDir == "" {
		return "", "", nil
	}
	if err := os.MkdirAll(filepath.Dir(helperPath), 0o755); err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(callbackDir, 0o755); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(helperPath, []byte(localCreateTaskHelperScript(callbackDir, workspace.TaskID, workspace.WorkerID)), 0o700); err != nil {
		return "", "", err
	}
	publishHelperPath := workspacePublishPRHelperPath(workspace)
	if publishHelperPath != "" {
		if err := os.WriteFile(publishHelperPath, []byte(localPublishPRHelperScript(callbackDir, workspace.TaskID, workspace.WorkerID)), 0o700); err != nil {
			return "", "", err
		}
	}
	return helperPath, callbackDir, nil
}

func localPublishPRHelperScript(callbackDir string, taskID string, workerID string) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	b.WriteString("if [ -z \"${AGED_WORKER_CALLBACK_DIR:-}\" ]; then AGED_WORKER_CALLBACK_DIR=")
	b.WriteString(shellQuote(callbackDir))
	b.WriteString("; fi\nexport AGED_WORKER_CALLBACK_DIR\n")
	b.WriteString("if [ -z \"${AGED_PARENT_TASK_ID:-}\" ]; then AGED_PARENT_TASK_ID=")
	b.WriteString(shellQuote(taskID))
	b.WriteString("; fi\nexport AGED_PARENT_TASK_ID\n")
	b.WriteString("if [ -z \"${AGED_PARENT_WORKER_ID:-}\" ]; then AGED_PARENT_WORKER_ID=")
	b.WriteString(shellQuote(workerID))
	b.WriteString("; fi\nexport AGED_PARENT_WORKER_ID\n")
	b.WriteString(remotePublishPRHelperScript())
	return b.String()
}

func localCreateTaskHelperScript(callbackDir string, taskID string, workerID string) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	b.WriteString("if [ -z \"${AGED_WORKER_CALLBACK_DIR:-}\" ]; then AGED_WORKER_CALLBACK_DIR=")
	b.WriteString(shellQuote(callbackDir))
	b.WriteString("; fi\nexport AGED_WORKER_CALLBACK_DIR\n")
	b.WriteString("if [ -z \"${AGED_PARENT_TASK_ID:-}\" ]; then AGED_PARENT_TASK_ID=")
	b.WriteString(shellQuote(taskID))
	b.WriteString("; fi\nexport AGED_PARENT_TASK_ID\n")
	b.WriteString("if [ -z \"${AGED_PARENT_WORKER_ID:-}\" ]; then AGED_PARENT_WORKER_ID=")
	b.WriteString(shellQuote(workerID))
	b.WriteString("; fi\nexport AGED_PARENT_WORKER_ID\n")
	b.WriteString(remoteCreateTaskHelperScript())
	return b.String()
}

func remoteWorkerExecutionPrompt(prompt string, workspace PreparedWorkspace) string {
	prompt = workerExecutionPrompt(prompt, workspace)
	var b strings.Builder
	b.WriteString("# Original Orchestrator\n\n")
	b.WriteString("This worker is running on a remote execution target under an existing aged orchestrator. Do not start a new aged daemon or orchestrator from this worker.\n\n")
	b.WriteString("To create follow-up work, use the `aged-create-task` helper on PATH. It reads the new task prompt from stdin and queues it for the original orchestrator over the existing SSH control channel. ")
	b.WriteString("When creating follow-up work, do not ask the follow-up task to open a draft pull request unless the user explicitly requested a draft PR; project configuration controls draft-by-default behavior. ")
	b.WriteString("To publish this worker result as an intermediate pull request, use the `aged-publish-pr` helper on PATH instead of `gh pr create`; it reads the pull request body from stdin and the orchestrator records the PR. ")
	b.WriteString("The remote environment also exports `AGED_PARENT_TASK_ID`, `AGED_PARENT_WORKER_ID`, and `AGED_WORKER_CALLBACK_DIR`.\n\n")
	b.WriteString(prompt)
	return b.String()
}

func retryWorkerExecutionPrompt(prompt string, previousWorkerID string, resumeSessionID string, steering []string, contextKind string) string {
	var b strings.Builder
	if contextKind == "durable_loop" {
		b.WriteString("# Continuation Context\n\n")
		b.WriteString("This durable loop iteration is continuing from a previous worker turn.\n")
	} else {
		b.WriteString("# Retry Context\n\n")
		b.WriteString("This is a retry of a previously failed or canceled worker turn.\n")
	}
	b.WriteString("Previous worker ID: ")
	b.WriteString(previousWorkerID)
	b.WriteString("\n")
	if strings.TrimSpace(resumeSessionID) != "" {
		b.WriteString("The worker provider session is being resumed when supported.\n")
	}
	if contextKind == "durable_loop" {
		b.WriteString("The execution workspace may already contain changes from that worker. Inspect the current workspace state first, preserve useful existing work, and continue from there instead of starting over.\n\n")
	} else {
		b.WriteString("The execution workspace may already contain partial changes from that worker. Inspect the current workspace state first, preserve useful existing work, and continue from there instead of starting over.\n\n")
	}
	if len(steering) > 0 {
		b.WriteString("# User Steering\n\n")
		b.WriteString("Apply this user steering on the resumed turn:\n")
		for _, message := range dedupeTrimmedStrings(steering) {
			b.WriteString("- ")
			b.WriteString(message)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString(prompt)
	return b.String()
}

type WorkerTurnResult struct {
	WorkerID     string            `json:"workerId"`
	NodeID       string            `json:"nodeId,omitempty"`
	Status       core.WorkerStatus `json:"status"`
	Kind         string            `json:"kind"`
	Role         string            `json:"role,omitempty"`
	SpawnID      string            `json:"spawnId,omitempty"`
	BaseWorkerID string            `json:"baseWorkerId,omitempty"`
	Summary      string            `json:"summary,omitempty"`
	Error        string            `json:"error,omitempty"`
	Changes      WorkspaceChanges  `json:"changes"`
}

type activeWorkerControl struct {
	ID           string
	Cancel       context.CancelFunc
	Capabilities worker.Capabilities
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
	service := &Service{
		store:            store,
		broker:           NewBroker(),
		brain:            brain,
		runners:          runners,
		baseRunners:      maps.Clone(runners),
		pluginRunners:    map[string]struct{}{},
		workDir:          workDir,
		projects:         projects,
		plugins:          NewPluginRegistry(builtinPlugins()),
		promptSets:       NewPromptSetRegistry(nil, ""),
		workspaces:       workspaces,
		targets:          targets,
		sshRunner:        sshRunner,
		prPublisher:      NewLocalPullRequestPublisher(),
		remoteApply:      applyRemotePatch,
		cancels:          map[string]context.CancelFunc{},
		taskCancels:      map[string]context.CancelFunc{},
		taskRuns:         map[string]string{},
		tasks:            map[string]string{},
		steering:         map[string]chan string{},
		remoteRuns:       map[string]remoteRun{},
		workerCaps:       map[string]worker.Capabilities{},
		steeringRestarts: map[string]struct{}{},
	}
	service.drivers = NewDriverRegistry(service)
	return service
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
		s.RefreshDrivers()
	}
}

func (s *Service) Drivers() *DriverRegistry {
	if s == nil {
		return nil
	}
	return s.drivers
}

func (s *Service) RefreshDrivers() {
	if s != nil && s.drivers != nil {
		_, _ = s.drivers.Refresh()
	}
}

func (s *Service) LoadProjects(ctx context.Context, seed *ProjectRegistry) error {
	if seed == nil {
		return errors.New("project seed registry is not configured")
	}
	projects, defaultID, err := s.store.ListProjects(ctx)
	if err != nil {
		return err
	}
	if len(projects) == 0 {
		defaultProject := seed.Default()
		for _, project := range seed.Snapshot() {
			if _, err := s.store.SaveProject(ctx, project, project.ID == defaultProject.ID); err != nil {
				return err
			}
		}
		projects, defaultID, err = s.store.ListProjects(ctx)
		if err != nil {
			return err
		}
	}
	registry, err := NewProjectRegistry(projects, defaultID)
	if err != nil {
		return err
	}
	s.SetProjects(registry)
	return nil
}

func (s *Service) CreateProject(ctx context.Context, project core.Project) (core.Project, error) {
	normalized, err := normalizeProject(project)
	if err != nil {
		return core.Project{}, err
	}
	if s.projects != nil {
		if _, exists := s.projects.Get(normalized.ID); exists {
			return core.Project{}, fmt.Errorf("project %q already exists", normalized.ID)
		}
	}
	saved, err := s.store.CreateProject(ctx, normalized)
	if err != nil {
		return core.Project{}, err
	}
	if s.projects == nil {
		registry, err := NewProjectRegistry([]core.Project{saved}, saved.ID)
		if err != nil {
			return core.Project{}, err
		}
		s.SetProjects(registry)
		s.RefreshDrivers()
		return saved, nil
	}
	if _, err := s.projects.Add(saved); err != nil {
		return core.Project{}, err
	}
	s.RefreshDrivers()
	return saved, nil
}

func (s *Service) UpdateProject(ctx context.Context, id string, project core.Project) (core.Project, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return core.Project{}, errors.New("project id is required")
	}
	project.ID = id
	normalized, err := normalizeProject(project)
	if err != nil {
		return core.Project{}, err
	}
	if s.projects == nil {
		return core.Project{}, errors.New("project registry is not configured")
	}
	if _, exists := s.projects.Get(id); !exists {
		return core.Project{}, eventstore.ErrNotFound
	}
	saved, err := s.store.SaveProject(ctx, normalized, false)
	if err != nil {
		return core.Project{}, err
	}
	if _, err := s.projects.Update(saved); err != nil {
		return core.Project{}, err
	}
	s.RefreshDrivers()
	return saved, nil
}

func (s *Service) DeleteProject(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("project id is required")
	}
	if s.projects == nil {
		return errors.New("project registry is not configured")
	}
	if _, exists := s.projects.Get(id); !exists {
		return eventstore.ErrNotFound
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, task := range snapshot.Tasks {
		if task.ProjectID == id && !isTerminalTaskStatus(task.Status) {
			return fmt.Errorf("cannot delete project %q while task %q is nonterminal", id, task.ID)
		}
	}
	if err := s.store.DeleteProject(ctx, id); err != nil {
		return err
	}
	if err := s.projects.Delete(id); err != nil {
		return err
	}
	s.RefreshDrivers()
	return nil
}

func (s *Service) ProjectHealth(ctx context.Context, id string) (core.ProjectHealth, error) {
	if s.projects == nil {
		return core.ProjectHealth{}, errors.New("project registry is not configured")
	}
	project, ok := s.projects.Get(id)
	if !ok {
		return core.ProjectHealth{}, eventstore.ErrNotFound
	}
	health := core.ProjectHealth{
		ProjectID:    project.ID,
		OK:           true,
		PathStatus:   "ok",
		VCSStatus:    "unknown",
		GitHubStatus: "not_configured",
		TargetStatus: "ok",
		CheckedAt:    time.Now().UTC(),
	}
	addError := func(message string) {
		health.OK = false
		health.Errors = append(health.Errors, message)
	}
	info, err := os.Stat(project.LocalPath)
	if err != nil {
		health.PathStatus = "missing"
		addError(err.Error())
		return health, nil
	}
	if !info.IsDir() {
		health.PathStatus = "not_directory"
		addError("localPath is not a directory")
		return health, nil
	}
	detectedVCS := detectProjectVCS(ctx, project.LocalPath)
	health.DetectedVCS = detectedVCS
	switch {
	case detectedVCS == "":
		health.VCSStatus = "not_detected"
		addError("no jj or git checkout detected")
	case project.VCS == "" || project.VCS == "auto" || project.VCS == detectedVCS:
		health.VCSStatus = "ok"
	default:
		health.VCSStatus = "mismatch"
		addError(fmt.Sprintf("configured vcs %q does not match detected %q", project.VCS, detectedVCS))
	}
	health.DetectedRepo = detectGitHubRepo(ctx, project.LocalPath)
	if project.Repo != "" || project.UpstreamRepo != "" {
		if health.DetectedRepo == "" {
			health.GitHubStatus = "repo_not_detected"
		} else {
			health.GitHubStatus = "repo_detected"
		}
		if authStatus, err := githubAuthStatus(ctx, project.LocalPath); err != nil {
			health.GitHubStatus = authStatus
			addError(err.Error())
		} else if health.GitHubStatus == "repo_detected" {
			health.GitHubStatus = "ok"
		} else {
			health.GitHubStatus = authStatus
		}
	}
	health.DetectedBase = detectDefaultBase(ctx, project.LocalPath, project.Repo)
	if project.DefaultBase == "" {
		health.DefaultBaseStatus = "missing"
		addError("defaultBase is not configured")
	} else if health.DetectedBase == "" || health.DetectedBase == project.DefaultBase {
		health.DefaultBaseStatus = "ok"
	} else {
		health.DefaultBaseStatus = "mismatch"
	}
	if len(project.TargetLabels) > 0 && s.targets != nil {
		metadata := map[string]any{"targetLabels": project.TargetLabels}
		applyRequirementsMetadata(metadata, project.Requirements)
		_, err := s.targets.Select(Plan{Metadata: metadata})
		if err != nil {
			health.TargetStatus = "no_matching_target"
			addError(err.Error())
		}
	} else if hasRequirements(project.Requirements) && s.targets != nil {
		metadata := map[string]any{}
		applyRequirementsMetadata(metadata, project.Requirements)
		_, err := s.targets.Select(Plan{Metadata: metadata})
		if err != nil {
			health.TargetStatus = "no_matching_target"
			addError(err.Error())
		}
	}
	return health, nil
}

func (s *Service) SetPlugins(plugins *PluginRegistry) {
	if plugins != nil {
		s.plugins = plugins
	}
}

func (s *Service) SetPromptSets(promptSets *PromptSetRegistry) {
	if promptSets != nil {
		s.promptSets = promptSets
	}
}

func (s *Service) SetProviderUsageSource(source ProviderUsageSource) {
	s.usageSource = source
}

func (s *Service) LoadPromptSets(ctx context.Context, seed *PromptSetRegistry) error {
	promptSets, defaultID, err := s.store.ListPromptSets(ctx)
	if err != nil {
		return err
	}
	var seedPromptSets []core.PromptSet
	if seed != nil {
		seedPromptSets = seed.Snapshot()
	}
	if defaultID == "" {
		for _, promptSet := range seedPromptSets {
			if promptSet.Default {
				defaultID = promptSet.ID
				break
			}
		}
	}
	promptSets = append(promptSets, seedPromptSets...)
	s.SetPromptSets(NewPromptSetRegistry(promptSets, defaultID))
	return nil
}

func (s *Service) RegisterPromptSet(ctx context.Context, promptSet core.PromptSet) (core.PromptSet, error) {
	makeDefault := promptSet.Default
	registered, err := s.registerPromptSetRuntime(promptSet, makeDefault)
	if err != nil {
		return core.PromptSet{}, err
	}
	registered.BuiltIn = false
	return s.store.SavePromptSet(ctx, registered, makeDefault)
}

func (s *Service) DeletePromptSet(ctx context.Context, id string) error {
	if err := s.store.DeletePromptSet(ctx, id); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	return s.promptSets.Delete(id)
}

func (s *Service) registerPromptSetRuntime(promptSet core.PromptSet, makeDefault bool) (core.PromptSet, error) {
	if s.promptSets == nil {
		s.promptSets = NewPromptSetRegistry(nil, "")
	}
	return s.promptSets.Register(promptSet, makeDefault)
}

func (s *Service) SetPluginRuntimeContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.pluginCtx = ctx
}

func (s *Service) LoadRegisteredTargets(ctx context.Context) error {
	targets, err := s.store.ListTargets(ctx)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if _, err := s.registerTargetRuntime(targetConfigFromCore(target)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) RegisterTarget(ctx context.Context, target core.TargetConfig) (core.TargetConfig, error) {
	registered, err := s.registerTargetRuntime(targetConfigFromCore(target))
	if err != nil {
		return core.TargetConfig{}, err
	}
	out := coreTargetConfig(registered)
	if _, err := s.store.SaveTarget(ctx, out); err != nil {
		return core.TargetConfig{}, err
	}
	s.RefreshTargetHealthFor(ctx, registered.ID)
	return out, nil
}

func (s *Service) DeleteTarget(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("target id is required")
	}
	if err := s.targets.Delete(id); err != nil {
		return err
	}
	if err := s.store.DeleteTarget(ctx, id); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	return nil
}

func (s *Service) registerTargetRuntime(target TargetConfig) (TargetConfig, error) {
	if s.targets == nil {
		s.targets = NewLocalTargetRegistry()
	}
	return s.targets.Register(target)
}

func (s *Service) LoadRegisteredPlugins(ctx context.Context) error {
	plugins, err := s.store.ListPlugins(ctx)
	if err != nil {
		return err
	}
	for _, plugin := range plugins {
		if s.plugins != nil && s.plugins.IsBuiltIn(plugin.ID) {
			continue
		}
		if _, err := s.registerPluginRuntime(plugin, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) RegisterPlugin(ctx context.Context, plugin core.Plugin) (core.Plugin, error) {
	registered, err := s.registerPluginRuntime(plugin, true)
	if err != nil {
		return core.Plugin{}, err
	}
	return s.store.SavePlugin(ctx, registered)
}

func (s *Service) DeletePlugin(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("plugin id is required")
	}
	if err := s.store.DeletePlugin(ctx, id); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	if err := s.plugins.Delete(id); err != nil {
		return err
	}
	s.syncPluginRunners()
	return nil
}

// syncPluginRunners reconciles s.runners with the current set of enabled
// runner plugins. Runner kinds that were previously contributed by a plugin
// but no longer appear (because the plugin was disabled, deleted, or had its
// protocol/kind/command changed) are removed, restoring any built-in/static
// runner of the same kind that was supplied at construction time.
func (s *Service) syncPluginRunners() {
	if s.runners == nil {
		s.runners = map[string]worker.Runner{}
	}
	if s.pluginRunners == nil {
		s.pluginRunners = map[string]struct{}{}
	}
	var current map[string]worker.Runner
	if s.plugins != nil {
		current = s.plugins.RunnerPlugins()
	}
	for kind := range s.pluginRunners {
		if _, stillPresent := current[kind]; stillPresent {
			continue
		}
		if base, ok := s.baseRunners[kind]; ok {
			s.runners[kind] = base
		} else {
			delete(s.runners, kind)
		}
		delete(s.pluginRunners, kind)
	}
	for kind, runner := range current {
		s.runners[kind] = runner
		s.pluginRunners[kind] = struct{}{}
	}
}

func (s *Service) registerPluginRuntime(plugin core.Plugin, probe bool) (core.Plugin, error) {
	if s.plugins == nil {
		s.plugins = NewPluginRegistry(nil)
	}
	registered, err := s.plugins.Register(plugin)
	if err != nil {
		return core.Plugin{}, err
	}
	if probe {
		s.plugins.Probe(context.Background())
		for _, current := range s.plugins.Snapshot() {
			if current.ID == registered.ID {
				registered = current
				break
			}
		}
	}
	s.syncPluginRunners()
	if registered.Kind == "driver" && registered.Enabled {
		ctx := s.pluginCtx
		if ctx == nil {
			ctx = context.Background()
		}
		s.plugins.StartDrivers(ctx)
	}
	return registered, nil
}

func (s *Service) SetRemotePatchApplier(applier func(context.Context, core.Project, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error)) {
	if applier != nil {
		s.remoteApply = applier
	}
}

func (s *Service) StartTargetProbes(ctx context.Context, interval time.Duration) {
	if s == nil || s.targets == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		s.RefreshTargetHealth(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.RefreshTargetHealth(ctx)
			}
		}
	}()
}

func (s *Service) RefreshTargetHealth(ctx context.Context) {
	if s == nil || s.targets == nil {
		return
	}
	for _, target := range s.targets.Configs() {
		s.refreshTargetHealth(ctx, target)
	}
}

func (s *Service) RefreshTargetHealthFor(ctx context.Context, id string) {
	if s == nil || s.targets == nil {
		return
	}
	target, ok := s.targets.Get(id)
	if !ok {
		return
	}
	s.refreshTargetHealth(ctx, target)
}

func (s *Service) refreshTargetHealth(ctx context.Context, target TargetConfig) {
	if target.Kind == TargetKindLocal {
		s.targets.UpdateHealth(target.ID, core.TargetHealth{
			Status:      "ok",
			CheckedAt:   time.Now().UTC(),
			Reachable:   true,
			Tmux:        true,
			RepoPresent: true,
		}, core.TargetResources{})
		return
	}
	if target.Kind != TargetKindSSH {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	health, resources := s.sshRunner.Probe(probeCtx, target)
	s.targets.UpdateHealth(target.ID, health, resources)
}

func (s *Service) Snapshot(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	return s.decorateSnapshot(snapshot), nil
}

func (s *Service) SnapshotSummary(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := s.store.SnapshotSummary(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	return s.decorateSnapshot(snapshot), nil
}

func (s *Service) SnapshotTaskCards(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := s.store.SnapshotTaskCards(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	return s.decorateSnapshot(snapshot), nil
}

func (s *Service) decorateSnapshot(snapshot core.Snapshot) core.Snapshot {
	if s.targets != nil {
		snapshot.Targets = s.targets.Snapshot()
	}
	if s.projects != nil {
		snapshot.Projects = s.projects.Snapshot()
	}
	if s.plugins != nil {
		snapshot.Plugins = s.plugins.Snapshot()
		if s.drivers != nil {
			snapshot.Plugins = s.drivers.DecoratePlugins(snapshot.Plugins)
		}
	}
	if s.promptSets != nil {
		snapshot.PromptSets = s.promptSets.Snapshot()
	}
	return snapshot
}

func (s *Service) RecoverRemoteWorkers(ctx context.Context) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.cancelStaleLocalWorkers(ctx, snapshot); err != nil {
		return err
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.recoverOrphanedPlanningTasks(ctx, snapshot); err != nil {
		return err
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.recoverOrphanedRunningGraphTasks(ctx, snapshot); err != nil {
		return err
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.retryStartupCanceledTasks(ctx, snapshot); err != nil {
		return err
	}
	snapshot, err = s.store.Snapshot(ctx)
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
			Target:   target,
			Session:  node.RemoteSession,
			RunDir:   node.RemoteRunDir,
			WorkDir:  node.RemoteWorkDir,
			TaskID:   node.TaskID,
			WorkerID: node.WorkerID,
			Status:   "running",
		}
		targetID := target.ID
		s.targets.Begin(targetID)
		go func() {
			defer s.targets.Finish(targetID)
			s.recoverRemoteWorker(context.Background(), node, run)
		}()
	}
	return nil
}

func (s *Service) recoverOrphanedPlanningTasks(ctx context.Context, snapshot core.Snapshot) error {
	for _, task := range snapshot.Tasks {
		if task.Status != core.TaskPlanning || taskHasActiveWorkers(snapshot, task.ID) {
			continue
		}
		if !taskPlanningStatusIsLatest(snapshot, task.ID) {
			continue
		}
		if resumingPullRequestFollowUp(snapshot, task.ID) {
			_, err := s.append(ctx, core.Event{
				Type:   core.EventTaskAction,
				TaskID: task.ID,
				Payload: core.MustJSON(map[string]any{
					"kind":   "startup_planning_recovery",
					"status": "resumed",
					"reason": "daemon restarted while pull request follow-up planning was in progress",
				}),
			})
			if err != nil {
				return err
			}
			if latestPullRequestFollowUpIsQueued(snapshot, task.ID) {
				if err := s.setTaskStatus(ctx, task.ID, core.TaskWaiting); err != nil {
					return err
				}
				s.startTaskRoutine(task.ID, func(taskCtx context.Context) {
					s.resumePullRequestFeedbackQueue(taskCtx, task.ID)
				})
			} else {
				s.startTaskRoutine(task.ID, func(taskCtx context.Context) {
					s.resumeLegacyPullRequestFollowUpPlanning(taskCtx, task.ID)
				})
			}
			continue
		}
		_, err := s.append(ctx, core.Event{
			Type:   core.EventTaskAction,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"kind":   "startup_planning_recovery",
				"status": "waiting",
				"reason": "daemon restarted while planning was in progress and no active worker could be recovered",
			}),
		})
		if err != nil {
			return err
		}
		if err := s.waitForUserAction(ctx, task.ID, "", "startup_planning_recovery", "Planning was interrupted by daemon restart before a plan or worker was recorded. Retry or steer the task to continue.", nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) recoverOrphanedRunningGraphTasks(ctx context.Context, snapshot core.Snapshot) error {
	for _, task := range snapshot.Tasks {
		if task.Status != core.TaskRunning || taskHasActiveWorkers(snapshot, task.ID) {
			continue
		}
		if !taskLatestStatusIs(snapshot, task.ID, core.TaskRunning) {
			continue
		}
		if plan, ok, planErr := retryPullRequestFollowUpPlan(snapshot, task.ID); planErr != nil {
			return planErr
		} else if ok {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   "startup_running_recovery",
				"status": "resumed",
				"reason": "daemon restarted while pull request follow-up was running with no active worker; retrying the follow-up plan",
			}); err != nil {
				return err
			}
			if err := s.markTaskRetryPlanning(ctx, task.ID); err != nil {
				return err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "retrying"
			s.startTaskRoutine(task.ID, func(taskCtx context.Context) {
				s.retryTask(taskCtx, task, plan)
			})
			continue
		}
		initial, results, err := retryGraphStateForTask(snapshot, task.ID)
		if err != nil || len(candidateResults(results)) == 0 {
			if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   "startup_running_recovery",
				"status": "waiting",
				"reason": "daemon restarted while task was running, but no active worker or recoverable graph state was found",
				"error":  errorString(err),
			}); actionErr != nil {
				return actionErr
			}
			if waitErr := s.waitForUserAction(ctx, task.ID, "", "startup_running_recovery", "Task was marked running after daemon restart, but no active worker could be recovered. Retry or steer the task to continue.", nil); waitErr != nil {
				return waitErr
			}
			continue
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   "startup_running_recovery",
			"status": "resumed",
			"reason": "daemon restarted while task was running with no active workers; resuming from persisted graph results",
		}); err != nil {
			return err
		}
		if err := s.markTaskRetryPlanning(ctx, task.ID); err != nil {
			return err
		}
		task.Status = core.TaskPlanning
		task.Error = ""
		task.ObjectiveStatus = core.ObjectiveActive
		task.ObjectivePhase = "retrying"
		s.startTaskRoutine(task.ID, func(taskCtx context.Context) {
			if strings.TrimSpace(task.FinalCandidateWorkerID) != "" {
				s.retryFinalCandidateTask(taskCtx, task, results)
				return
			}
			s.retryGraphTask(taskCtx, task, initial, results)
		})
	}
	return nil
}

func taskPlanningStatusIsLatest(snapshot core.Snapshot, taskID string) bool {
	return taskLatestStatusIs(snapshot, taskID, core.TaskPlanning)
}

func taskLatestStatusIs(snapshot core.Snapshot, taskID string, status core.TaskStatus) bool {
	latestStatusEvent := int64(0)
	latestMatchingStatusEvent := int64(0)
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskStatus {
			continue
		}
		latestStatusEvent = event.ID
		var payload struct {
			Status core.TaskStatus `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.Status == status {
			latestMatchingStatusEvent = event.ID
		}
	}
	return latestMatchingStatusEvent > 0 && latestMatchingStatusEvent == latestStatusEvent
}

func taskCanceledByStartupRecovery(snapshot core.Snapshot, taskID string) bool {
	latestStatusEvent := int64(0)
	latestCanceledStatusEvent := int64(0)
	latestCanceledReason := ""
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskStatus {
			continue
		}
		latestStatusEvent = event.ID
		var payload struct {
			Status core.TaskStatus `json:"status"`
			Reason string          `json:"reason,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Status != core.TaskCanceled {
			continue
		}
		latestCanceledStatusEvent = event.ID
		latestCanceledReason = payload.Reason
	}
	if latestCanceledStatusEvent == 0 || latestCanceledStatusEvent != latestStatusEvent {
		return false
	}
	if latestCanceledReason == taskCancelReasonStartupRecovery {
		return true
	}
	workerEvent := latestStartupCanceledWorkerEvent(snapshot, taskID)
	return workerEvent > 0 && workerEvent < latestCanceledStatusEvent && !taskStatusEventBetween(snapshot, taskID, workerEvent, latestCanceledStatusEvent)
}

func taskCanceledByUserAfterLatestSteeringRestart(snapshot core.Snapshot, taskID string) bool {
	var latestRestart int64
	var latestUserCancel int64
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskAction:
			var payload struct {
				Kind   string `json:"kind"`
				Status string `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				continue
			}
			if payload.Kind == "steering_restart" && payload.Status == "started" {
				latestRestart = event.ID
			}
		case core.EventTaskStatus:
			var payload struct {
				Status core.TaskStatus `json:"status"`
				Reason string          `json:"reason,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				continue
			}
			if payload.Status == core.TaskCanceled && payload.Reason == taskCancelReasonUser {
				latestUserCancel = event.ID
			}
		}
	}
	return latestRestart > 0 && latestUserCancel > latestRestart
}

func taskCanceledByUser(snapshot core.Snapshot, taskID string) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.Type != core.EventTaskStatus {
			continue
		}
		var payload struct {
			Status core.TaskStatus `json:"status"`
			Reason string          `json:"reason,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Status == core.TaskCanceled {
			return payload.Reason == taskCancelReasonUser
		}
		return false
	}
	return false
}

func latestStartupCanceledWorkerEvent(snapshot core.Snapshot, taskID string) int64 {
	var latest int64
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventWorkerCompleted {
			continue
		}
		var payload struct {
			Status  core.WorkerStatus `json:"status"`
			Summary string            `json:"summary,omitempty"`
			Error   string            `json:"error,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Status != core.WorkerCanceled {
			continue
		}
		if strings.Contains(payload.Summary, "daemon startup recovery") || strings.Contains(payload.Error, "recoverable process handle after daemon restart") {
			latest = event.ID
		}
	}
	return latest
}

func taskStatusEventBetween(snapshot core.Snapshot, taskID string, after int64, before int64) bool {
	for _, event := range snapshot.Events {
		if event.TaskID == taskID && event.Type == core.EventTaskStatus && event.ID > after && event.ID < before {
			return true
		}
	}
	return false
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
				"reason": taskCancelReasonStartupRecovery,
			}),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) retryStartupCanceledTasks(ctx context.Context, snapshot core.Snapshot) error {
	for _, task := range snapshot.Tasks {
		if task.Status != core.TaskCanceled || taskHasActiveWorkers(snapshot, task.ID) || !taskCanceledByStartupRecovery(snapshot, task.ID) {
			continue
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   "startup_auto_retry",
			"status": "retrying",
			"reason": "task was automatically canceled during daemon startup recovery",
		}); err != nil {
			return err
		}
		if _, err := s.RetryTask(ctx, task.ID); err != nil {
			if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   "startup_auto_retry",
				"status": "failed",
				"reason": "automatic retry after startup cancellation failed",
				"error":  err.Error(),
			}); actionErr != nil {
				return actionErr
			}
		}
	}
	return nil
}

func isTerminalWorkerStatus(status core.WorkerStatus) bool {
	return status == core.WorkerSucceeded || status == core.WorkerFailed || status == core.WorkerCanceled
}

func (s *Service) recoverRemoteWorker(ctx context.Context, node core.ExecutionNode, run remoteRun) {
	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[node.WorkerID] = cancel
	s.tasks[node.WorkerID] = node.TaskID
	s.remoteRuns[node.WorkerID] = run
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.cancels, node.WorkerID)
		delete(s.tasks, node.WorkerID)
		delete(s.remoteRuns, node.WorkerID)
		s.mu.Unlock()
	}()

	runState := &workerRunState{}
	sink := eventSink{service: s, taskID: node.TaskID, workerID: node.WorkerID, state: runState}
	sshRunner := s.sshRunner
	sshRunner.CallbackHandler = s.handleRemoteWorkerCallbacks
	status, err := sshRunner.Poll(workerCtx, run, worker.ParserForKind(node.WorkerKind), sink)
	workerStatus, statusErr := remoteStatusToWorkerStatus(status)
	if err != nil && !errors.Is(err, context.Canceled) {
		statusErr = err
		workerStatus = core.WorkerFailed
	}
	if errors.Is(workerCtx.Err(), context.Canceled) {
		workerStatus = core.WorkerCanceled
		statusErr = context.Canceled
	}
	changes := s.sshRunner.DescribeChanges(ctx, run)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   node.TaskID,
		WorkerID: node.WorkerID,
		Payload:  core.MustJSON(runState.completionPayload(workerStatus, statusErr, changes)),
	})
	_ = s.recordWorkerArtifacts(ctx, node.TaskID, node.WorkerID, node.WorkerKind, runState, changes)
	if workerStatus == core.WorkerCanceled {
		if snapshot, err := s.store.Snapshot(ctx); err == nil && !taskHasActiveWorkers(snapshot, node.TaskID) {
			_ = s.setTaskStatus(ctx, node.TaskID, core.TaskCanceled)
		}
		return
	}
	go s.resumeRecoveredRemoteTask(context.Background(), node.TaskID)
}

func (s *Service) Events(ctx context.Context, afterID int64, limit int) ([]core.Event, error) {
	return s.store.ListEvents(ctx, afterID, limit)
}

func (s *Service) TaskEvents(ctx context.Context, taskID string, limit int) ([]core.Event, error) {
	return s.store.ListTaskEvents(ctx, taskID, limit)
}

func (s *Service) Subscribe() (int, <-chan core.Event) {
	return s.broker.Subscribe()
}

func (s *Service) Unsubscribe(id int) {
	s.broker.Unsubscribe(id)
}

func (s *Service) startTaskRoutine(taskID string, fn func(context.Context)) {
	taskCtx, cancel := context.WithCancel(context.Background())
	runID := uuid.NewString()
	s.mu.Lock()
	if existing := s.taskCancels[taskID]; existing != nil {
		existing()
	}
	s.taskCancels[taskID] = cancel
	s.taskRuns[taskID] = runID
	s.mu.Unlock()

	go func() {
		defer func() {
			cancel()
			s.mu.Lock()
			if s.taskRuns[taskID] == runID {
				delete(s.taskCancels, taskID)
				delete(s.taskRuns, taskID)
			}
			s.mu.Unlock()
		}()
		fn(taskCtx)
	}()
}

func (s *Service) beginSteeringRestart(taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.steeringRestarts[taskID]; ok {
		return false
	}
	s.steeringRestarts[taskID] = struct{}{}
	return true
}

func (s *Service) finishSteeringRestart(taskID string) {
	s.mu.Lock()
	delete(s.steeringRestarts, taskID)
	s.mu.Unlock()
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
		ID:           taskID,
		ProjectID:    project.ID,
		WorkstreamID: strings.TrimSpace(stringMetadataValue(metadata["workstreamId"])),
		Title:        title,
		Prompt:       req.Prompt,
		Status:       core.TaskQueued,
		CreatedAt:    created.At,
		UpdatedAt:    created.At,
		Metadata:     core.MustJSON(metadata),
	}

	s.startTaskRoutine(task.ID, func(taskCtx context.Context) {
		s.runTask(taskCtx, task)
	})
	return task, nil
}

func (s *Service) UpdateTaskLoopConfig(ctx context.Context, taskID string, req core.UpdateLoopConfigRequest) (core.Task, error) {
	if req.LoopIntervalSeconds == nil && req.LoopPrompt == nil && req.RequiredTargetID == nil {
		return core.Task{}, errors.New("loop config update requires loopIntervalSeconds, loopPrompt, or requiredTargetID")
	}
	if req.LoopIntervalSeconds != nil && *req.LoopIntervalSeconds < 0 {
		return core.Task{}, errors.New("loopIntervalSeconds must be >= 0")
	}
	loopPrompt := ""
	if req.LoopPrompt != nil {
		loopPrompt = strings.TrimSpace(*req.LoopPrompt)
		if loopPrompt == "" {
			return core.Task{}, errors.New("loopPrompt must not be empty")
		}
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	if taskExecutionMode(task) != executionModeLoop {
		return core.Task{}, errors.New("task is not a durable loop")
	}
	if isTerminalTaskStatus(task.Status) {
		return core.Task{}, errors.New("cannot update a terminal task")
	}
	metadataPatch := make(map[string]any)
	action := map[string]any{
		"kind":   "loop_config_updated",
		"status": "updated",
	}
	if req.LoopIntervalSeconds != nil {
		metadataPatch["loopIntervalSeconds"] = *req.LoopIntervalSeconds
		action["loopIntervalSeconds"] = *req.LoopIntervalSeconds
	}
	if req.LoopPrompt != nil {
		metadataPatch["loopPrompt"] = loopPrompt
		action["loopPromptChanged"] = true
		action["loopPromptPreview"] = truncateText(loopPrompt, 200)
	}
	if req.RequiredTargetID != nil {
		requiredTargetID := strings.TrimSpace(*req.RequiredTargetID)
		metadataPatch["requiredTargetID"] = requiredTargetID
		if requiredTargetID == "" {
			action["requiredTargetIDCleared"] = true
		} else {
			action["requiredTargetID"] = requiredTargetID
		}
	}
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventTaskUpdated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"metadataPatch": metadataPatch,
		}),
	}); err != nil {
		return core.Task{}, err
	}
	if err := s.recordTaskAction(ctx, taskID, action); err != nil {
		return core.Task{}, err
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, err
	}
	task, ok = findTask(snapshot, taskID)
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
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
	if session := s.assistantSession(ctx, req.ConversationID); session.ProviderSessionID != "" {
		req.Provider = session.Provider
		req.ProviderSessionID = session.ProviderSessionID
	}
	if _, err := s.append(ctx, core.Event{
		Type: core.EventAssistantAsked,
		Payload: core.MustJSON(map[string]any{
			"conversationId":    req.ConversationID,
			"message":           req.Message,
			"context":           req.Context,
			"workDir":           req.WorkDir,
			"provider":          req.Provider,
			"providerSessionId": req.ProviderSessionID,
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
	metadata := assistantResponseMetadata(response)
	if _, err := s.append(ctx, core.Event{
		Type: core.EventAssistantAnswered,
		Payload: core.MustJSON(map[string]any{
			"conversationId":    response.ConversationID,
			"message":           response.Message,
			"provider":          response.Provider,
			"providerSessionId": response.ProviderSessionID,
			"metadata":          metadata,
		}),
	}); err != nil {
		return core.AssistantResponse{}, err
	}
	response.Metadata = metadata
	return response, nil
}

type assistantSession struct {
	Provider          string
	ProviderSessionID string
}

func (s *Service) assistantSession(ctx context.Context, conversationID string) assistantSession {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return assistantSession{}
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventAssistantAnswered {
			continue
		}
		var payload struct {
			ConversationID    string          `json:"conversationId"`
			Provider          string          `json:"provider"`
			ProviderSessionID string          `json:"providerSessionId"`
			Metadata          json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.ConversationID != conversationID {
			continue
		}
		provider := payload.Provider
		sessionID := payload.ProviderSessionID
		if sessionID == "" && len(payload.Metadata) > 0 {
			var metadata map[string]any
			if err := json.Unmarshal(payload.Metadata, &metadata); err == nil {
				provider = nonEmpty(provider, stringMetadataValue(metadata["assistant"]), stringMetadataValue(metadata["brain"]))
				sessionID = stringMetadataValue(metadata["providerSessionId"])
			}
		}
		if sessionID != "" {
			return assistantSession{Provider: provider, ProviderSessionID: sessionID}
		}
	}
	return assistantSession{}
}

func assistantResponseMetadata(response core.AssistantResponse) json.RawMessage {
	metadata := map[string]any{}
	if len(response.Metadata) > 0 && string(response.Metadata) != "null" {
		_ = json.Unmarshal(response.Metadata, &metadata)
	}
	if response.Provider != "" {
		metadata["assistant"] = response.Provider
	}
	if response.ProviderSessionID != "" {
		metadata["providerSessionId"] = response.ProviderSessionID
	}
	return core.MustJSON(metadata)
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
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, ok := findTask(snapshot, taskID); !ok {
		return eventstore.ErrNotFound
	}
	_, err = s.append(ctx, core.Event{
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
	activeWorkers := make([]activeWorkerControl, 0)
	for workerID, workerTaskID := range s.tasks {
		if workerTaskID == taskID {
			activeWorkers = append(activeWorkers, activeWorkerControl{
				ID:           workerID,
				Cancel:       s.cancels[workerID],
				Capabilities: s.workerCaps[workerID],
			})
		}
	}
	sort.Slice(activeWorkers, func(i, j int) bool {
		return activeWorkers[i].ID < activeWorkers[j].ID
	})
	deliveredWorkerIDs := map[string]bool{}
	for _, active := range activeWorkers {
		if !active.Capabilities.LiveSteering {
			continue
		}
		ch := s.steering[active.ID]
		if ch == nil {
			continue
		}
		select {
		case ch <- req.Message:
			deliveredWorkerIDs[active.ID] = true
		default:
		}
	}
	s.mu.Unlock()
	restartWorkers := make([]activeWorkerControl, 0)
	restartWorkerIDs := make([]string, 0)
	for _, active := range activeWorkers {
		if deliveredWorkerIDs[active.ID] {
			continue
		}
		restartWorkers = append(restartWorkers, active)
		restartWorkerIDs = append(restartWorkerIDs, active.ID)
	}
	snapshot, snapshotErr := s.store.Snapshot(ctx)
	if snapshotErr == nil && taskStatus(snapshot, taskID) == core.TaskWaiting {
		s.startTaskRoutine(taskID, func(taskCtx context.Context) {
			s.resumeWaitingTask(taskCtx, taskID, req.Message)
		})
	} else if snapshotErr == nil && len(restartWorkerIDs) > 0 {
		if !s.beginSteeringRestart(taskID) {
			_ = s.recordTaskAction(ctx, taskID, map[string]any{
				"kind":      "steering_restart",
				"status":    "skipped",
				"reason":    "steering restart already in progress",
				"message":   req.Message,
				"workerIds": restartWorkerIDs,
			})
			return nil
		}
		for _, active := range restartWorkers {
			if active.Cancel != nil {
				active.Cancel()
			}
			_ = s.CancelWorker(ctx, active.ID)
		}
		go s.restartRunningTaskWithSteering(context.Background(), taskID, req.Message, restartWorkerIDs)
	}
	return err
}

func (s *Service) SteerWorker(ctx context.Context, workerID string, req core.SteeringRequest) error {
	workerID = strings.TrimSpace(workerID)
	message := strings.TrimSpace(req.Message)
	if workerID == "" {
		return errors.New("workerId is required")
	}
	if message == "" {
		return errors.New("message is required")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	workerState, ok := findWorker(snapshot, workerID)
	if !ok {
		return eventstore.ErrNotFound
	}
	task, ok := findTask(snapshot, workerState.TaskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) {
		return nil
	}
	node := executionNodeForWorker(snapshot, workerID)
	if workerSteeringAlreadyPending(snapshot, task.ID, workerID, message) {
		return nil
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerSteered,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   workerID,
			"nodeId":     node.ID,
			"workerKind": nonEmpty(workerState.Kind, node.WorkerKind),
			"role":       node.Role,
			"spawnId":    node.SpawnID,
			"status":     "queued",
			"reason":     "user_worker_steering",
			"message":    message,
		}),
	}); err != nil {
		return err
	}
	if err := s.recordTaskMilestone(ctx, task.ID, "worker_steering_queued", "worker_steering", "Worker-specific steering queued for replanning.", map[string]any{
		"workerId": workerID,
		"nodeId":   node.ID,
	}); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "worker_steering", "Worker-specific steering is queued for replanning."); err != nil {
		return err
	}
	if task.Status == core.TaskWaiting {
		s.startTaskRoutine(task.ID, func(taskCtx context.Context) {
			s.resumeWorkerSteeringQueue(taskCtx, task.ID)
		})
	}
	return nil
}

func (s *Service) restartRunningTaskWithSteering(ctx context.Context, taskID string, message string, workerIDs []string) {
	defer s.finishSteeringRestart(taskID)
	_ = s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":      "steering_restart",
		"status":    "started",
		"reason":    "runner does not support live steering; canceling and resuming with user input",
		"message":   message,
		"workerIds": workerIDs,
	})
	for _, workerID := range workerIDs {
		if err := s.CancelWorker(ctx, workerID); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
			_ = s.recordTaskAction(ctx, taskID, map[string]any{
				"kind":     "steering_restart",
				"status":   "warning",
				"reason":   "worker cancellation failed",
				"workerId": workerID,
				"error":    err.Error(),
			})
		}
	}
	snapshot, err := s.waitForTaskWorkersStopped(ctx, taskID, 15*time.Second)
	if err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "steering_restart",
			"status": "failed",
			"reason": "timed out waiting for workers to stop",
			"error":  err.Error(),
		})
		_ = s.failTask(ctx, taskID, err)
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "steering_restart",
			"status": "failed",
			"reason": "task was not found after steering restart",
		})
		return
	}
	if task.Status == core.TaskSucceeded || taskCanceledByUser(snapshot, taskID) || taskCanceledByUserAfterLatestSteeringRestart(snapshot, taskID) {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "steering_restart",
			"status": "skipped",
			"reason": "task reached a terminal status before steering restart could resume it",
		})
		return
	}
	plan, err := retryPlanForTask(snapshot, taskID)
	if err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "steering_restart",
			"status": "failed",
			"reason": "retry plan could not be reconstructed",
			"error":  err.Error(),
		})
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "steering_restart",
			"status": "failed",
			"reason": "task could not be marked retrying",
			"error":  err.Error(),
		})
		return
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "retrying"
	_ = s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":      "steering_restart",
		"status":    "resumed",
		"reason":    "retrying canceled worker with retained workspace and steering",
		"workerIds": workerIDs,
	})
	s.retryTask(ctx, task, plan)
}

func (s *Service) waitForTaskWorkersStopped(ctx context.Context, taskID string, timeout time.Duration) (core.Snapshot, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var latest core.Snapshot
	for {
		snapshot, err := s.store.Snapshot(ctx)
		if err == nil {
			latest = snapshot
			status := taskStatus(snapshot, taskID)
			if !taskHasActiveWorkers(snapshot, taskID) && status != core.TaskQueued && status != core.TaskPlanning && status != core.TaskRunning {
				return snapshot, nil
			}
		}
		select {
		case <-ctx.Done():
			return latest, ctx.Err()
		case <-deadline.C:
			return latest, fmt.Errorf("task %s still has active workers after %s", taskID, timeout)
		case <-ticker.C:
		}
	}
}

func (s *Service) RetryTask(ctx context.Context, taskID string) (core.Task, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	if task.Status != core.TaskFailed && task.Status != core.TaskCanceled && !(task.Status == core.TaskSucceeded && taskExecutionMode(task) == executionModeLoop) {
		return core.Task{}, errors.New("can only retry failed or canceled tasks")
	}
	if _, err := s.projectForTask(task); err != nil {
		return core.Task{}, err
	}
	if taskExecutionMode(task) == executionModeLoop {
		if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
			return core.Task{}, err
		}
		task.Status = core.TaskPlanning
		task.Error = ""
		task.ObjectiveStatus = core.ObjectiveActive
		task.ObjectivePhase = "retrying"
		s.startTaskRoutine(taskID, func(taskCtx context.Context) {
			s.runDurableLoopTask(taskCtx, task)
		})
		return task, nil
	}
	if task.Status == core.TaskFailed {
		plan, ok, err := retryPullRequestFollowUpPlan(snapshot, taskID)
		if err != nil {
			return core.Task{}, err
		}
		if ok {
			if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
				return core.Task{}, err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "retrying"
			s.startTaskRoutine(taskID, func(taskCtx context.Context) {
				s.retryTask(taskCtx, task, plan)
			})
			return task, nil
		}
	}
	if task.Status == core.TaskFailed && s.retryFailedPublishPullRequestAction(ctx, task, snapshot) {
		task.Status = core.TaskPlanning
		task.Error = ""
		task.ObjectiveStatus = core.ObjectiveActive
		task.ObjectivePhase = "retrying"
		return task, nil
	}
	if strings.TrimSpace(task.FinalCandidateWorkerID) != "" {
		if _, results, graphErr := retryGraphStateForTask(snapshot, taskID); graphErr == nil {
			if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
				return core.Task{}, err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "retrying"
			s.startTaskRoutine(taskID, func(taskCtx context.Context) {
				s.retryFinalCandidateTask(taskCtx, task, results)
			})
			return task, nil
		}
	}
	if task.Status == core.TaskFailed {
		initial, results, graphErr := retryGraphStateForTask(snapshot, taskID)
		if graphErr == nil && taskFailureRecoverableFromGraph(snapshot, taskID, results) {
			if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
				return core.Task{}, err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "retrying"
			s.startTaskRoutine(taskID, func(taskCtx context.Context) {
				s.retryGraphTask(taskCtx, task, initial, results)
			})
			return task, nil
		}
	}
	if task.Status == core.TaskFailed && taskFailedDuringDynamicReplan(snapshot, taskID) {
		initial, results, err := retryGraphStateForTask(snapshot, taskID)
		if err != nil {
			return core.Task{}, err
		}
		if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
			return core.Task{}, err
		}
		task.Status = core.TaskPlanning
		task.Error = ""
		task.ObjectiveStatus = core.ObjectiveActive
		task.ObjectivePhase = "retrying"
		s.startTaskRoutine(taskID, func(taskCtx context.Context) {
			s.retryGraphTask(taskCtx, task, initial, results)
		})
		return task, nil
	}
	plan, err := retryPlanForTask(snapshot, taskID)
	if err != nil {
		return core.Task{}, err
	}
	if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
		return core.Task{}, err
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "retrying"
	s.startTaskRoutine(taskID, func(taskCtx context.Context) {
		s.retryTask(taskCtx, task, plan)
	})
	return task, nil
}

func (s *Service) retryFinalCandidateTask(ctx context.Context, task core.Task, results []WorkerTurnResult) {
	if err := s.completeTask(ctx, task.ID, results, task.FinalCandidateWorkerID, "retry final candidate publication"); err != nil {
		_ = s.failTask(ctx, task.ID, err)
	}
}

func (s *Service) resumeRecoveredRemoteTask(ctx context.Context, taskID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || isTerminalTaskStatus(task.Status) || taskHasActiveWorkers(snapshot, taskID) {
		return
	}
	if taskExecutionMode(task) == executionModeLoop {
		s.runDurableLoopTask(ctx, task)
		return
	}
	if plan, ok, err := retryPullRequestFollowUpPlan(snapshot, taskID); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	} else if ok {
		if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "recovering", "Resuming interrupted pull request follow-up work."); err != nil {
			return
		}
		if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
			return
		}
		task.Status = core.TaskPlanning
		task.Error = ""
		task.ObjectiveStatus = core.ObjectiveActive
		task.ObjectivePhase = "recovering"
		s.retryTask(ctx, task, plan)
		return
	}
	initial, results, err := retryGraphStateForTask(snapshot, taskID)
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "recovering", "Resuming task after recovered remote worker completion."); err != nil {
		return
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "recovering"
	if strings.TrimSpace(task.FinalCandidateWorkerID) != "" {
		s.retryFinalCandidateTask(ctx, task, results)
		return
	}
	s.retryGraphTask(ctx, task, initial, results)
}

func taskHasActiveWorkers(snapshot core.Snapshot, taskID string) bool {
	for _, activeWorker := range snapshot.Workers {
		if activeWorker.TaskID == taskID && !isTerminalWorkerStatus(activeWorker.Status) {
			return true
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && !isTerminalWorkerStatus(node.Status) {
			return true
		}
	}
	return false
}

func activeTaskWorkerIDs(snapshot core.Snapshot, taskID string) []string {
	seen := map[string]bool{}
	var workerIDs []string
	add := func(workerID string) {
		if workerID == "" || seen[workerID] {
			return
		}
		seen[workerID] = true
		workerIDs = append(workerIDs, workerID)
	}
	for _, activeWorker := range snapshot.Workers {
		if activeWorker.TaskID == taskID && !isTerminalWorkerStatus(activeWorker.Status) {
			add(activeWorker.ID)
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && !isTerminalWorkerStatus(node.Status) {
			add(node.WorkerID)
		}
	}
	sort.Strings(workerIDs)
	return workerIDs
}

func taskIDForWorker(snapshot core.Snapshot, workerID string) string {
	for _, activeWorker := range snapshot.Workers {
		if activeWorker.ID == workerID && activeWorker.TaskID != "" {
			return activeWorker.TaskID
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID == workerID && node.TaskID != "" {
			return node.TaskID
		}
	}
	return ""
}

func (s *Service) markTaskRetryPlanning(ctx context.Context, taskID string) error {
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "retrying", "Retrying task."); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, taskID, core.TaskPlanning)
}

func (s *Service) CancelWorker(ctx context.Context, workerID string) error {
	s.mu.Lock()
	cancel := s.cancels[workerID]
	remote := s.remoteRuns[workerID]
	taskID := s.tasks[workerID]
	s.mu.Unlock()
	if cancel == nil {
		return s.cancelPersistedWorker(ctx, workerID)
	}
	if strings.TrimSpace(taskID) == "" {
		if snapshot, err := s.store.Snapshot(ctx); err == nil {
			taskID = taskIDForWorker(snapshot, workerID)
		}
	}
	if remote.Session != "" {
		_ = s.sshRunner.Cancel(ctx, remote)
	}
	cancel()
	if strings.TrimSpace(taskID) != "" {
		_ = s.markLiveWorkerCanceled(ctx, taskID, workerID, remote)
	}
	return nil
}

func (s *Service) markLiveWorkerCanceled(ctx context.Context, taskID string, workerID string, remote remoteRun) error {
	if s.workerCompleted(ctx, taskID, workerID) {
		return nil
	}
	changes := WorkspaceChanges{}
	if remote.Session != "" {
		changes = WorkspaceChanges{
			Root:    remote.RunDir,
			CWD:     remote.WorkDir,
			Mode:    "remote",
			VCSType: "ssh",
		}
	}
	_, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":           core.WorkerCanceled,
			"summary":          "Worker was canceled from live daemon state.",
			"error":            "worker canceled by user request",
			"workspaceChanges": changes,
		}),
	})
	return err
}

func (s *Service) cancelPersistedWorker(ctx context.Context, workerID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	node, run, ok := s.persistedRemoteRun(snapshot, workerID)
	if ok {
		if run.Session != "" {
			_ = s.sshRunner.Cancel(ctx, run)
		}
		if _, err := s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   node.TaskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerCanceled,
				"summary": "Remote worker was canceled from persisted daemon state.",
				"error":   "remote worker did not have a live local cancellation handle",
				"workspaceChanges": WorkspaceChanges{
					Root:    run.RunDir,
					CWD:     run.WorkDir,
					Mode:    "remote",
					VCSType: "ssh",
				},
			}),
		}); err != nil {
			return err
		}
		return nil
	}

	for _, worker := range snapshot.Workers {
		if worker.ID != workerID || isTerminalWorkerStatus(worker.Status) {
			continue
		}
		if _, err := s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   worker.TaskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerCanceled,
				"summary": "Worker was canceled from persisted daemon state.",
				"error":   "worker did not have a live local cancellation handle",
			}),
		}); err != nil {
			return err
		}
		return nil
	}

	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID != workerID || isTerminalWorkerStatus(node.Status) {
			continue
		}
		if _, err := s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   node.TaskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerCanceled,
				"summary": "Worker was canceled from persisted execution node state.",
				"error":   "worker did not have a live local cancellation handle",
			}),
		}); err != nil {
			return err
		}
		return nil
	}
	return eventstore.ErrNotFound
}

func (s *Service) persistedRemoteRun(snapshot core.Snapshot, workerID string) (core.ExecutionNode, remoteRun, bool) {
	if s.targets == nil {
		return core.ExecutionNode{}, remoteRun{}, false
	}
	for i := len(snapshot.ExecutionNodes) - 1; i >= 0; i-- {
		node := snapshot.ExecutionNodes[i]
		if node.WorkerID != workerID || node.TargetKind != string(TargetKindSSH) || isTerminalWorkerStatus(node.Status) {
			continue
		}
		target, ok := s.targets.Get(node.TargetID)
		if !ok {
			return core.ExecutionNode{}, remoteRun{}, false
		}
		return node, remoteRun{
			Target:   target,
			Session:  node.RemoteSession,
			RunDir:   node.RemoteRunDir,
			WorkDir:  node.RemoteWorkDir,
			TaskID:   node.TaskID,
			WorkerID: node.WorkerID,
			Status:   "running",
		}, true
	}
	return core.ExecutionNode{}, remoteRun{}, false
}

func (s *Service) CancelTask(ctx context.Context, taskID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, ok := findTask(snapshot, taskID); !ok {
		return eventstore.ErrNotFound
	}

	canceledWorkers := map[string]bool{}
	var workerIDs []string
	s.mu.Lock()
	if cancel := s.taskCancels[taskID]; cancel != nil {
		cancel()
	}
	for workerID := range s.cancels {
		if s.tasks[workerID] == taskID {
			canceledWorkers[workerID] = true
			workerIDs = append(workerIDs, workerID)
		}
	}
	s.mu.Unlock()
	for _, workerID := range workerIDs {
		_ = s.CancelWorker(ctx, workerID)
	}
	for _, workerID := range activeTaskWorkerIDs(snapshot, taskID) {
		if canceledWorkers[workerID] {
			continue
		}
		_ = s.CancelWorker(ctx, workerID)
	}

	_, err = s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
			"reason": taskCancelReasonUser,
		}),
	})
	if err != nil {
		return err
	}
	return nil
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

func (s *Service) taskIsTerminal(ctx context.Context, taskID string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return isTerminalTaskStatus(task.Status), nil
		}
	}
	return false, nil
}

func (s *Service) taskArtifacts(ctx context.Context, taskID string) []core.TaskArtifact {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return append([]core.TaskArtifact{}, task.Artifacts...)
		}
	}
	return nil
}

func (s *Service) taskPullRequestStates(ctx context.Context, taskID string) []ReplanPullRequestState {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	states := []ReplanPullRequestState{}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID {
			continue
		}
		states = append(states, ReplanPullRequestState{
			ID:                   pr.ID,
			Repo:                 pr.Repo,
			Number:               pr.Number,
			URL:                  pr.URL,
			Branch:               pr.Branch,
			Base:                 pr.Base,
			Title:                pr.Title,
			State:                pr.State,
			Draft:                pr.Draft,
			ChecksStatus:         pr.ChecksStatus,
			ChecksConclusion:     pr.ChecksConclusion,
			MergeStatus:          pr.MergeStatus,
			Mergeable:            pr.Mergeable,
			ReviewStatus:         pr.ReviewStatus,
			ContinueAfterPublish: pullRequestContinuesTask(pr),
			PublicationPhase:     pullRequestMetadataString(pr, "publicationPhase"),
		})
	}
	return states
}

func canPublishPullRequestForTask(task core.Task) bool {
	return isTerminalTaskStatus(task.Status) || strings.TrimSpace(task.FinalCandidateWorkerID) != ""
}

func (s *Service) ReviewWorkerChanges(ctx context.Context, workerID string) (WorkerChangesReview, error) {
	return s.reviewWorkerChanges(ctx, workerID, true)
}

func (s *Service) reviewWorkerChanges(ctx context.Context, workerID string, includeDiff bool) (WorkerChangesReview, error) {
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return WorkerChangesReview{}, err
	}
	if workspace.VCSType == "ssh" {
		changes, err := s.completedWorkspaceChanges(ctx, workerID)
		if err != nil {
			return WorkerChangesReview{}, err
		}
		return WorkerChangesReview{
			WorkerID:  workerID,
			Workspace: workspace,
			Changes:   changes,
		}, nil
	}
	changes := s.describeWorkspaceChanges(ctx, workspace)
	if includeDiff && changes.Error == "" {
		if completed, err := s.completedWorkspaceChanges(ctx, workerID); err == nil && strings.TrimSpace(completed.Diff) != "" {
			changes.Diff = completed.Diff
		} else if diff, err := s.describeWorkspaceDiff(ctx, workspace); err != nil {
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
	var result WorkerApplyResult
	if review.Workspace.VCSType == "ssh" {
		project, err := s.projectForTaskID(ctx, review.Workspace.TaskID)
		if err != nil {
			return WorkerApplyResult{}, err
		}
		result, err = s.remoteApply(ctx, project, review.Workspace, review.Changes)
		if err != nil {
			return WorkerApplyResult{}, err
		}
	} else {
		result, err = s.workspaces.ApplyChanges(ctx, review.Workspace, review.Changes)
		if err != nil {
			return WorkerApplyResult{}, err
		}
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

func (s *Service) ApplyTaskResult(ctx context.Context, taskID string) (WorkerApplyResult, error) {
	return s.applyTaskResultWithRecovery(ctx, taskID, 0)
}

func (s *Service) applyTaskResultWithRecovery(ctx context.Context, taskID string, attempts int) (WorkerApplyResult, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return WorkerApplyResult{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return WorkerApplyResult{}, eventstore.ErrNotFound
	}
	if !isTerminalTaskStatus(task.Status) {
		return WorkerApplyResult{}, errors.New("can only apply terminal task results")
	}
	if task.FinalCandidateWorkerID == "" {
		return WorkerApplyResult{}, errors.New("task has no final candidate to apply")
	}
	result, err := s.ApplyWorkerChanges(ctx, task.FinalCandidateWorkerID)
	if err != nil {
		if recovered, recoverResult, recoverErr := s.recoverTaskApplyFailure(ctx, task, snapshot, err, attempts); recovered {
			return recoverResult, recoverErr
		}
	}
	return result, err
}

func (s *Service) recoverTaskApplyFailure(ctx context.Context, task core.Task, snapshot core.Snapshot, applyErr error, attempts int) (bool, WorkerApplyResult, error) {
	if attempts >= 1 || !isRecoverableApplyConflict(applyErr) {
		return false, WorkerApplyResult{}, nil
	}
	recovery := s.recoverFinalCandidateWithReplan(ctx, task.ID, snapshot, task.FinalCandidateWorkerID, applyErr, "local_apply_recovery", "after_apply_conflict", "local apply failed", nil)
	if !recovery.Handled {
		return false, WorkerApplyResult{}, nil
	}
	if recovery.Err != nil || !recovery.Completed {
		return true, WorkerApplyResult{}, recovery.Err
	}
	candidateWorkerID, reason, err := resolveFinalCandidate(recovery.Results, recovery.SelectedWorkerID)
	if err != nil {
		return true, WorkerApplyResult{}, s.waitForFinalCandidateResolution(ctx, task.ID, err)
	}
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventTaskCandidate,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"workerId": candidateWorkerID,
			"reason":   nonEmpty(reason, recovery.Reason, "local apply conflict recovered after replanning"),
		}),
	}); err != nil {
		return true, WorkerApplyResult{}, err
	}
	applyResult, err := s.ApplyWorkerChanges(ctx, candidateWorkerID)
	return true, applyResult, err
}

func (s *Service) RecommendApplyPolicy(ctx context.Context, taskID string) (ApplyPolicyRecommendation, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ApplyPolicyRecommendation{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return ApplyPolicyRecommendation{}, eventstore.ErrNotFound
	}
	candidates := applyCandidates(snapshot, taskID)
	recommendation := ApplyPolicyRecommendation{
		TaskID:     taskID,
		Strategy:   "none",
		Reason:     "no unapplied successful workers with source changes",
		Candidates: candidates,
	}
	if task.FinalCandidateWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == task.FinalCandidateWorkerID {
				if candidate.Applied {
					recommendation.Strategy = "already_applied"
					recommendation.Reason = "final task candidate has already been applied"
				} else {
					recommendation.Strategy = "apply_final"
					recommendation.Reason = "orchestrator selected a final task candidate"
				}
				return s.recordApplyPolicy(ctx, taskID, recommendation)
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
	return s.recordApplyPolicy(ctx, taskID, recommendation)
}

func (s *Service) recordApplyPolicy(ctx context.Context, taskID string, recommendation ApplyPolicyRecommendation) (ApplyPolicyRecommendation, error) {
	_, err := s.append(ctx, core.Event{
		Type:    core.EventApplyPolicy,
		TaskID:  taskID,
		Payload: core.MustJSON(recommendation),
	})
	return recommendation, err
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

func appliedWorkerSourceRootFromStore(ctx context.Context, store eventstore.Store, workerID string) string {
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		return ""
	}
	return appliedWorkerSourceRoot(snapshot, workerID)
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
	if taskExecutionMode(task) == executionModeLoop {
		s.runDurableLoopTask(ctx, task)
		return
	}
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
	normalizePlanReasoning(&plan)
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if plan.WorkPlan != nil {
		if err := s.updateTaskWorkPlan(ctx, task.ID, *plan.WorkPlan); err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return
		}
	}
	if ok, err := s.runImmediatePlanActions(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	} else if !ok {
		return
	}

	results := []WorkerTurnResult{}
	var ok bool
	followUpParentNodeID := ""
	if len(plan.Workers) > 0 {
		results, ok, err = s.runInitialWorkerGraph(ctx, task, plan)
		if err != nil {
			if s.waitForRecoverableError(ctx, task.ID, "", err) {
				return
			}
			_ = s.failTask(ctx, task.ID, err)
			return
		}
		if !ok {
			return
		}
	} else {
		result, err := s.runPlannedWorker(ctx, task, plan)
		if err != nil {
			if s.waitForRecoverableError(ctx, task.ID, "", err) {
				return
			}
			if s.recoverWorkerFailureWithReplan(ctx, task, plan, results, err) {
				return
			}
			_ = s.failTask(ctx, task.ID, err)
			return
		}
		results = append(results, result)
		if result.Status == core.WorkerWaiting {
			s.handleWorkerQuestion(ctx, task, plan, results, result)
			return
		}
		if result.Status == core.WorkerFailed && s.recoverWorkerFailureWithReplan(ctx, task, plan, results, nil) {
			return
		}
		if !s.finishOrContinueTask(ctx, task.ID, result) {
			return
		}
		followUpParentNodeID = result.NodeID
	}
	if ok, nextResults, err := s.runDeferredPlanWork(ctx, task, plan, results, followUpParentNodeID); err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	} else if !ok {
		return
	} else {
		results = nextResults
	}

	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
	if !replanOK {
		return
	}

	_ = s.completeTask(ctx, task.ID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) recoverWorkerFailureWithReplan(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, err error) bool {
	if _, ok := s.brain.(ReplanProvider); !ok {
		return false
	}
	if err != nil {
		results = append(results, failedFollowUpResult(initial, err))
	}
	if len(results) == 0 {
		return false
	}
	failure := results[len(results)-1]
	if failure.Status != core.WorkerFailed {
		return false
	}
	if blocker, ok := classifyUserRecoverableBlocker(nonEmpty(failure.Error, failure.Summary)); ok {
		if s.recoverableWorkerFailureCanRetryOnAlternateTarget(ctx, task, initial, failure, blocker) {
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":     "worker_failure_recovery",
				"when":     "after_worker_failure",
				"reason":   "Worker failed due to a target-local setup issue; asking the orchestrator to retry on another eligible target.",
				"workerId": failure.WorkerID,
				"status":   "started",
				"error":    failure.Error,
			})
			ok, selectedWorkerID, reason, results := s.replanLoop(ctx, task, initial, results)
			if !ok {
				return true
			}
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":     "worker_failure_recovery",
				"when":     "after_worker_failure",
				"reason":   nonEmpty(reason, "Orchestrator selected a recovery result."),
				"workerId": selectedWorkerID,
				"status":   "completed",
			})
			_ = s.completeTask(ctx, task.ID, results, selectedWorkerID, reason)
			return true
		}
		_ = s.waitForUserAction(ctx, task.ID, failure.WorkerID, blocker.Reason, blocker.Question, map[string]any{
			"summary":    blocker.Summary,
			"workerKind": failure.Kind,
			"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
			"error":      failure.Error,
		})
		return true
	}
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":     "worker_failure_recovery",
		"when":     "after_worker_failure",
		"reason":   "Worker failed; asking the orchestrator to repair, retry, or consolidate instead of failing the task immediately.",
		"workerId": failure.WorkerID,
		"status":   "started",
		"error":    failure.Error,
	})
	ok, selectedWorkerID, reason, results := s.replanLoop(ctx, task, initial, results)
	if !ok {
		return true
	}
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":     "worker_failure_recovery",
		"when":     "after_worker_failure",
		"reason":   nonEmpty(reason, "Orchestrator selected a recovery result."),
		"workerId": selectedWorkerID,
		"status":   "completed",
	})
	_ = s.completeTask(ctx, task.ID, results, selectedWorkerID, reason)
	return true
}

func (s *Service) handleWorkerQuestion(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, waiting WorkerTurnResult) {
	question := nonEmpty(waiting.Summary, waiting.Error, "worker requested orchestrator input")
	_ = s.recordUserActionNeeded(ctx, task.ID, waiting.WorkerID, "worker_needs_input", question, map[string]any{
		"summary": waiting.Summary,
		"error":   waiting.Error,
	})
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		_ = s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingUser, "approval_needed", question)
		_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
		return
	}
	decision, err := replanner.Replan(ctx, task, OrchestrationState{
		InitialPlan:   initial,
		Results:       results,
		ContextLedger: s.taskContextLedger(ctx, task.ID),
		Artifacts:     s.taskArtifacts(ctx, task.ID),
		PullRequests:  s.taskPullRequestStates(ctx, task.ID),
		TaskSteering:  s.taskSteering(ctx, task.ID),
		Turn:          1,
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
		if shouldInheritLatestCandidate(decision.Plan.Metadata) {
			if baseWorkerID := latestCandidateWorkerID(results); baseWorkerID != "" {
				decision.Plan.Metadata["baseWorkerID"] = baseWorkerID
			}
		}
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
		nextResults, ok, err := s.runPlanWorkerSet(ctx, task, *decision.Plan, results, waiting.NodeID)
		if err != nil {
			if s.waitForRecoverableError(ctx, task.ID, waiting.WorkerID, err) {
				return
			}
			_ = s.failTask(ctx, task.ID, err)
			return
		}
		if !ok {
			return
		}
		if ok, updatedResults, err := s.runDeferredPlanWork(ctx, task, *decision.Plan, nextResults, waiting.NodeID); err != nil {
			if s.waitForRecoverableError(ctx, task.ID, waiting.WorkerID, err) {
				return
			}
			_ = s.failTask(ctx, task.ID, err)
		} else if ok {
			nextResults = updatedResults
			replanOK, finalCandidateWorkerID, finalCandidateReason, nextResults := s.replanLoop(ctx, task, *decision.Plan, nextResults)
			if !replanOK {
				return
			}
			_ = s.completeTask(ctx, task.ID, nextResults, nonEmpty(decision.FinalCandidateWorkerID, finalCandidateWorkerID), nonEmpty(decision.Rationale, finalCandidateReason))
		}
	case "wait":
		_ = s.waitForUserAction(ctx, task.ID, waiting.WorkerID, "orchestrator_wait", nonEmpty(decision.Message, decision.Rationale, question), map[string]any{
			"rationale": decision.Rationale,
		})
	case "complete":
		_ = s.completeTask(ctx, task.ID, results, decision.FinalCandidateWorkerID, decision.Rationale)
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
	if s.retryWaitingPublishPullRequestAction(ctx, task, snapshot) {
		return
	}
	if s.retryWaitingFinalCandidatePublication(ctx, task, snapshot) {
		return
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "replanning", "Resuming task with user feedback."); err != nil {
		return
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return
	}
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "replanning"
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
	if resumingPullRequestFollowUp(snapshot, taskID) {
		if pr, ok := latestPullRequestFollowUp(snapshot, taskID); ok {
			plan = annotatePullRequestFollowUpPlan(plan, pr)
		}
		plan = normalizePullRequestFollowUpPlan(plan)
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
	results, ok, err := s.runPlanWorkerSet(ctx, task, plan, nil, "")
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if !ok {
		return
	}
	if ok, nextResults, err := s.runDeferredPlanWork(ctx, task, plan, results, ""); err != nil {
		if s.waitForRecoverableError(ctx, taskID, "", err) {
			return
		}
		_ = s.failTask(ctx, taskID, err)
	} else if ok {
		results = nextResults
		replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
		if !replanOK {
			return
		}
		_ = s.completeTask(ctx, taskID, results, finalCandidateWorkerID, finalCandidateReason)
	}
}

func (s *Service) resumeLegacyPullRequestFollowUpPlanning(ctx context.Context, taskID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskPlanning {
		return
	}
	plan, err := s.brain.Plan(ctx, task, taskSteering(snapshot, taskID))
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if err := plan.Validate(); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if pr, ok := latestPullRequestFollowUp(snapshot, taskID); ok {
		plan = annotatePullRequestFollowUpPlan(plan, pr)
	}
	plan = normalizePullRequestFollowUpPlan(plan)
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	results, ok, err := s.runPlanWorkerSet(ctx, task, plan, nil, "")
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if !ok {
		return
	}
	if ok, nextResults, err := s.runDeferredPlanWork(ctx, task, plan, results, ""); err != nil {
		if s.waitForRecoverableError(ctx, taskID, "", err) {
			return
		}
		_ = s.failTask(ctx, taskID, err)
	} else if ok {
		results = nextResults
		replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
		if !replanOK {
			return
		}
		_ = s.completeTask(ctx, taskID, results, finalCandidateWorkerID, finalCandidateReason)
	}
}

func (s *Service) resumePullRequestFeedbackQueue(ctx context.Context, taskID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskWaiting {
		return
	}
	if len(pendingPullRequestFeedback(snapshot, taskID)) == 0 {
		return
	}
	initial, results, err := retryGraphStateForTask(snapshot, taskID)
	if err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "pull_request_feedback_queue",
			"status": "waiting",
			"reason": "queued pull request feedback could not resume automatically",
			"error":  err.Error(),
		})
		return
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "pr_needs_work"
	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, initial, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, taskID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) resumeWorkerSteeringQueue(ctx context.Context, taskID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskWaiting {
		return
	}
	if len(pendingWorkerSteering(snapshot, taskID)) == 0 {
		return
	}
	initial, results, err := retryGraphStateForTask(snapshot, taskID)
	if err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "worker_steering_queue",
			"status": "waiting",
			"reason": "queued worker steering could not resume automatically",
			"error":  err.Error(),
		})
		return
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "worker_steering"
	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, initial, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, taskID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) retryWaitingFinalCandidatePublication(ctx context.Context, task core.Task, snapshot core.Snapshot) bool {
	candidateWorkerID := strings.TrimSpace(task.FinalCandidateWorkerID)
	if candidateWorkerID == "" || task.ObjectiveStatus != core.ObjectiveWaitingUser || task.ObjectivePhase != "approval_needed" {
		return false
	}
	if !latestApprovalNeededMatches(snapshot, task.ID, candidateWorkerID, "ssh_signing_agent_failed") {
		return false
	}
	_, results, err := retryGraphStateForTask(snapshot, task.ID)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return true
	}
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "retrying", "Retrying completion pull request publication after user remediation."); err != nil {
		return true
	}
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return true
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "retrying"
	s.retryFinalCandidateTask(ctx, task, results)
	return true
}

func latestApprovalNeededMatches(snapshot core.Snapshot, taskID string, workerID string, reason string) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventApprovalNeeded || event.TaskID != taskID {
			continue
		}
		if strings.TrimSpace(event.WorkerID) != strings.TrimSpace(workerID) {
			return false
		}
		var payload struct {
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false
		}
		return payload.Reason == reason
	}
	return false
}

func (s *Service) retryTask(ctx context.Context, task core.Task, plan Plan) {
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	results, ok, err := s.runPlanWorkerSet(ctx, task, plan, nil, "")
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if !ok {
		return
	}
	if ok, nextResults, err := s.runDeferredPlanWork(ctx, task, plan, results, ""); err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	} else if !ok {
		return
	} else {
		results = nextResults
	}
	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, task.ID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) runPlanWorkerSet(ctx context.Context, task core.Task, plan Plan, priorResults []WorkerTurnResult, parentNodeID string) ([]WorkerTurnResult, bool, error) {
	results := append([]WorkerTurnResult{}, priorResults...)
	_ = parentNodeID
	if len(plan.Workers) > 0 {
		graphResults, ok, err := s.runInitialWorkerGraph(ctx, task, plan)
		results = append(results, graphResults...)
		if err != nil || !ok {
			return results, ok, err
		}
		return results, true, nil
	}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		return results, false, err
	}
	results = append(results, result)
	if result.Status == core.WorkerWaiting {
		s.handleWorkerQuestion(ctx, task, plan, results, result)
		return results, false, nil
	}
	if !s.finishOrContinueTask(ctx, task.ID, result) {
		return results, false, nil
	}
	return results, true, nil
}

func (s *Service) retryGraphTask(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) {
	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, initial, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, task.ID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) runPlannedWorker(ctx context.Context, task core.Task, plan Plan) (WorkerTurnResult, error) {
	normalizePlanReasoning(&plan)
	plan = s.rebalancePlanWorkerKind(ctx, plan)
	runner := s.runners[plan.WorkerKind]
	if runner == nil {
		return WorkerTurnResult{}, fmt.Errorf("unknown worker kind %q", plan.WorkerKind)
	}
	project, err := s.projectForTask(task)
	if err != nil {
		return WorkerTurnResult{}, err
	}
	plan.Metadata["projectId"] = project.ID
	if requested := targetLabels(plan.Metadata); len(requested) > 0 {
		plan.Metadata["ignoredTargetLabels"] = requested
		plan.Metadata["targetSelectionPolicy"] = "scheduler target labels are ignored; placement is selected by task or project policy"
		delete(plan.Metadata, "targetLabels")
	}
	if requestedID := requiredTargetID(plan.Metadata); requestedID != "" {
		plan.Metadata["ignoredRequiredTargetID"] = requestedID
		plan.Metadata["targetSelectionPolicy"] = "scheduler required target id is ignored; placement is selected by task or project policy"
		delete(plan.Metadata, "requiredTargetID")
	}
	if requestedRequirements := targetRequirements(plan.Metadata); hasRequirements(requestedRequirements) {
		plan.Metadata["ignoredRequiredMemoryMB"] = requestedRequirements.MemoryMB
		plan.Metadata["ignoredRequiredStorageMB"] = requestedRequirements.StorageMB
		plan.Metadata["targetSelectionPolicy"] = "scheduler target requirements are ignored; placement is selected by task or project policy"
		delete(plan.Metadata, "requiredMemoryMB")
		delete(plan.Metadata, "requiredStorageMB")
		delete(plan.Metadata, "requiredDiskMB")
	}
	if labels := taskTargetLabels(task); len(labels) > 0 {
		plan.Metadata["targetLabels"] = labels
		plan.Metadata["targetSelectionSource"] = "task"
	} else if len(project.TargetLabels) > 0 {
		plan.Metadata["targetLabels"] = project.TargetLabels
		plan.Metadata["targetSelectionSource"] = "project"
	}
	if requirements := taskTargetRequirements(task); hasRequirements(requirements) {
		applyRequirementsMetadata(plan.Metadata, requirements)
		plan.Metadata["targetRequirementsSource"] = "task"
	} else if hasRequirements(project.Requirements) {
		applyRequirementsMetadata(plan.Metadata, project.Requirements)
		plan.Metadata["targetRequirementsSource"] = "project"
	}
	if requiredID := taskRequiredTargetID(task); requiredID != "" {
		plan.Metadata["requiredTargetID"] = requiredID
		plan.Metadata["targetSelectionSource"] = "task_required"
	}
	target, err := s.selectExecutionTarget(ctx, plan)
	if err != nil {
		return WorkerTurnResult{}, err
	}
	plan.Metadata["targetID"] = target.ID
	plan.Metadata["targetKind"] = string(target.Kind)
	if target.Kind == TargetKindSSH {
		s.targets.Begin(target.ID)
		result, err := s.runSSHPlannedWorker(ctx, task, plan, runner, target)
		s.targets.Finish(target.ID)
		if err == nil || !isRemotePreStartFallbackError(err) {
			return result, err
		}
		if requiredTargetID(plan.Metadata) != "" {
			return result, err
		}
		fallback, fallbackErr := s.targets.SelectLocalFallback()
		if fallbackErr != nil {
			return result, err
		}
		plan.Metadata["fallbackFromTargetID"] = target.ID
		plan.Metadata["fallbackFromTargetKind"] = string(target.Kind)
		plan.Metadata["fallbackReason"] = err.Error()
		plan.Metadata["targetID"] = fallback.ID
		plan.Metadata["targetKind"] = string(fallback.Kind)
		target = fallback
	}
	s.targets.Begin(target.ID)
	defer s.targets.Finish(target.ID)

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
	retryFromWorkerID := stringMetadata(plan.Metadata, "retryFromWorkerID")
	resumeSessionID := stringMetadata(plan.Metadata, "retryResumeSessionID")
	workspace, reusedWorkspace, workspaceErr := s.retryWorkspace(ctx, task.ID, workerID, retryFromWorkerID)
	if workspaceErr != nil {
		plan.Metadata["retryWorkspaceReused"] = false
		plan.Metadata["retryWorkspaceError"] = workspaceErr.Error()
	}
	if reusedWorkspace {
		plan.Metadata["retryWorkspaceReused"] = true
		plan.Metadata["retryWorkspaceCWD"] = workspace.CWD
	} else if retryFromWorkerID != "" {
		plan.Metadata["retryWorkspaceReused"] = false
	}
	workspaceSpec := WorkspaceSpec{
		TaskID:    task.ID,
		WorkerID:  workerID,
		WorkDir:   project.LocalPath,
		TaskTitle: task.Title,
	}
	if !reusedWorkspace {
		workspaceBaseRef := stringMetadata(plan.Metadata, "workspaceBaseRef")
		baseRevision, err := syncedProjectWorkspaceBaseRevision(ctx, project)
		if workspaceBaseRef != "" {
			baseRevision, err = syncedProjectWorkspaceRefRevision(ctx, project, workspaceBaseRef)
		}
		if err != nil {
			_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
			return WorkerTurnResult{}, err
		}
		workspaceSpec.BaseRevision = baseRevision
	}
	if strings.TrimSpace(workspaceSpec.BaseRevision) != "" {
		plan.Metadata["workspaceBaseRevision"] = workspaceSpec.BaseRevision
	}
	if !reusedWorkspace {
		if baseWorkerID := candidateBaseWorkerID(plan.Metadata); baseWorkerID != "" {
			baseSpec, err := s.baseWorkspaceSpec(ctx, workspaceSpec, baseWorkerID)
			if err != nil {
				_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
				return WorkerTurnResult{}, err
			}
			workspaceSpec = baseSpec
			plan.Metadata["baseWorkspaceCWD"] = workspaceSpec.BaseWorkDir
			plan.Metadata["baseRevision"] = workspaceSpec.BaseRevision
		}
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
	if !reusedWorkspace {
		workspace, err = s.workspaces.Prepare(ctx, WorkspaceSpec{
			TaskID:        workspaceSpec.TaskID,
			WorkerID:      workspaceSpec.WorkerID,
			WorkDir:       workspaceSpec.WorkDir,
			BaseWorkDir:   workspaceSpec.BaseWorkDir,
			BaseRevision:  workspaceSpec.BaseRevision,
			TaskTitle:     workspaceSpec.TaskTitle,
			WorkerSummary: workspaceSpec.WorkerSummary,
		})
		if err != nil {
			_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
			return WorkerTurnResult{}, err
		}
		if baseWorkerID := candidateBaseWorkerID(plan.Metadata); baseWorkerID != "" && workspaceSpec.BaseWorkDir == "" && workspaceSpec.BaseRevision == "" {
			patch, baseChanges, err := s.workerHandoffPatch(ctx, baseWorkerID)
			if err != nil {
				_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
				return WorkerTurnResult{}, err
			}
			if strings.TrimSpace(patch) != "" {
				if err := applyGitPatchToWorkspace(ctx, workspace.CWD, patch); err != nil {
					if boolMetadata(plan.Metadata, "allowBasePatchConflicts") {
						plan.Metadata["baseHandoff"] = "patch_conflict"
						plan.Metadata["basePatchApplied"] = false
						plan.Metadata["basePatchConflicted"] = true
						plan.Metadata["basePatchConflictError"] = err.Error()
					} else {
						_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
						return WorkerTurnResult{}, fmt.Errorf("apply base worker patch in local workspace: %w", err)
					}
				} else {
					plan.Metadata["baseHandoff"] = "patch"
					plan.Metadata["basePatchApplied"] = true
				}
				plan.Metadata["baseChangedFiles"] = len(baseChanges.ChangedFiles)
			} else {
				plan.Metadata["baseHandoff"] = "empty_patch"
			}
		}
	}
	workspace.TargetID = target.ID
	workspace.TargetKind = string(target.Kind)
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(workspace),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	helperPath, callbackDir, err := installLocalCreateTaskHelper(workspace)
	if err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, fmt.Errorf("install local worker task helper: %w", err)
	}
	if helperPath != "" {
		plan.Metadata["createTaskHelperPath"] = helperPath
		plan.Metadata["workerCallbackDir"] = callbackDir
	}
	capabilities := worker.RunnerCapabilities(runner)
	var steering chan string
	if capabilities.LiveSteering {
		steering = make(chan string, 16)
	}
	retrySteering := stringSliceMetadata(plan.Metadata, "retrySteering")
	workerPrompt := plan.Prompt
	prompt := workerExecutionPrompt(workerPrompt, workspace)
	if reusedWorkspace {
		if !capabilities.ResumeSession {
			resumeSessionID = ""
			delete(plan.Metadata, "retryResumeSessionID")
		}
		prompt = retryWorkerExecutionPrompt(prompt, retryFromWorkerID, resumeSessionID, retrySteering, stringMetadata(plan.Metadata, "retryContextKind"))
	} else {
		resumeSessionID = ""
		delete(plan.Metadata, "retryResumeSessionID")
		if retryFromWorkerID != "" && len(retrySteering) > 0 {
			prompt = retryWorkerExecutionPrompt(prompt, retryFromWorkerID, "", retrySteering, stringMetadata(plan.Metadata, "retryContextKind"))
		}
	}
	spec := worker.Spec{
		ID:              workerID,
		TaskID:          task.ID,
		Kind:            plan.WorkerKind,
		Prompt:          prompt,
		WorkDir:         workspace.CWD,
		ResumeSessionID: resumeSessionID,
		ReasoningEffort: plan.ReasoningEffort,
		TargetID:        target.ID,
		TargetKind:      string(target.Kind),
		Steering:        steering,
	}
	command := runner.BuildCommand(spec)
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     plan.WorkerKind,
			"command":  command,
			"prompt":   spec.Prompt,
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
	s.workerCaps[workerID] = capabilities
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
		delete(s.workerCaps, workerID)
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
		changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
		if s.workerCompleted(context.Background(), task.ID, workerID) {
			status = core.WorkerCanceled
			err = context.Canceled
		} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(status, err, changes)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		s.drainLocalWorkerCallbacks(ctx, task.ID, workerID, callbackDir)
		_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, workspaceResult)
		return runState.turnResult(workerID, plan, status, err, changes), nil
	}

	if runState.isWaitingForInput() {
		changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
		status := core.WorkerWaiting
		var statusErr error
		if s.workerCompleted(context.Background(), task.ID, workerID) {
			status = core.WorkerCanceled
			statusErr = context.Canceled
		} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(status, statusErr, changes)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		s.drainLocalWorkerCallbacks(ctx, task.ID, workerID, callbackDir)
		return runState.turnResult(workerID, plan, status, statusErr, changes), nil
	}

	changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
	status := core.WorkerSucceeded
	var statusErr error
	if s.workerCompleted(context.Background(), task.ID, workerID) {
		status = core.WorkerCanceled
		statusErr = context.Canceled
	} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(status, statusErr, changes)); completionErr != nil {
		return WorkerTurnResult{}, completionErr
	}
	_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
	s.drainLocalWorkerCallbacks(ctx, task.ID, workerID, callbackDir)
	_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, WorkspaceResultSucceeded)
	return runState.turnResult(workerID, plan, status, statusErr, changes), nil
}

func (s *Service) rebalancePlanWorkerKind(ctx context.Context, plan Plan) Plan {
	if s == nil || s.usageSource == nil {
		return plan
	}
	kind := strings.TrimSpace(plan.WorkerKind)
	if kind != "codex" && kind != "claude" {
		return plan
	}
	alternate := "claude"
	if kind == "claude" {
		alternate = "codex"
	}
	if s.runners[kind] == nil || s.runners[alternate] == nil {
		return plan
	}
	snapshot := s.usageSource.Snapshot(ctx)
	currentUsage, currentOK := snapshot.Providers[kind]
	alternateUsage, alternateOK := snapshot.Providers[alternate]
	currentPressure, currentKnown := providerUsagePressure(currentUsage)
	alternatePressure, alternateKnown := providerUsagePressure(alternateUsage)
	if !currentOK || !alternateOK || !currentKnown || !alternateKnown {
		return plan
	}
	if alternatePressure+providerUsageSwitchMargin > currentPressure && currentPressure < 95 {
		return plan
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["usageAwareScheduling"] = true
	plan.Metadata["usageOriginalWorkerKind"] = kind
	plan.Metadata["usageSelectedWorkerKind"] = alternate
	plan.Metadata["usageSelectionReason"] = fmt.Sprintf("%s usage pressure %d%%, %s usage pressure %d%%", kind, currentPressure, alternate, alternatePressure)
	plan.Metadata["usageCurrentPressure"] = currentPressure
	plan.Metadata["usageAlternatePressure"] = alternatePressure
	plan.WorkerKind = alternate
	return plan
}

func (s *Service) selectExecutionTarget(ctx context.Context, plan Plan) (TargetConfig, error) {
	requiredID := requiredTargetID(plan.Metadata)
	if requiredID != "" {
		target, err := s.targets.Select(plan)
		if err != nil {
			return TargetConfig{}, fmt.Errorf("required execution target %q is unavailable: %w", requiredID, err)
		}
		return target, nil
	}
	if retryTargetID := stringMetadata(plan.Metadata, "retryTargetID"); retryTargetID != "" {
		retryPlan := plan
		retryPlan.Metadata = copyPlanMetadata(plan.Metadata)
		retryPlan.Metadata["requiredTargetID"] = retryTargetID
		target, err := s.targets.Select(retryPlan)
		if err == nil {
			return target, nil
		}
		fallback, fallbackErr := s.targets.Select(plan)
		if fallbackErr != nil {
			return TargetConfig{}, err
		}
		recordRetryTargetFallback(plan, retryTargetID, fallback.ID, err)
		return fallback, nil
	}
	if retryFromWorkerID := stringMetadata(plan.Metadata, "retryFromWorkerID"); retryFromWorkerID != "" {
		lookup, lookupErr := s.executionTargetForWorker(ctx, retryFromWorkerID, plan)
		if lookupErr != nil {
			return TargetConfig{}, lookupErr
		}
		if lookup.targetID != "" && lookup.selectErr == nil {
			return lookup.target, nil
		}
		if lookup.targetID != "" && lookup.selectErr != nil {
			fallback, fallbackErr := s.targets.Select(plan)
			if fallbackErr != nil {
				return TargetConfig{}, lookup.selectErr
			}
			recordRetryTargetFallback(plan, lookup.targetID, fallback.ID, lookup.selectErr)
			return fallback, nil
		}
	}
	target, err := s.targets.Select(plan)
	if err == nil {
		return target, nil
	}
	if len(targetLabels(plan.Metadata)) == 0 {
		if fallback, fallbackErr := s.targets.SelectLocalFallback(); fallbackErr == nil {
			recordRetryTargetFallback(plan, "", fallback.ID, err)
			return fallback, nil
		}
	}
	return TargetConfig{}, err
}

type previousTargetLookup struct {
	target    TargetConfig
	targetID  string
	selectErr error
}

func (s *Service) executionTargetForWorker(ctx context.Context, workerID string, plan Plan) (previousTargetLookup, error) {
	var result previousTargetLookup
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return result, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return result, err
	}
	for i := len(snapshot.ExecutionNodes) - 1; i >= 0; i-- {
		node := snapshot.ExecutionNodes[i]
		if node.WorkerID != workerID || strings.TrimSpace(node.TargetID) == "" {
			continue
		}
		target, selectErr := s.selectTargetIDWithPlan(node.TargetID, plan)
		result.target = target
		result.targetID = node.TargetID
		result.selectErr = selectErr
		return result, nil
	}
	info := workerExecutionInfo(snapshot, workerID)
	if info == nil {
		return result, nil
	}
	targetID, _ := info["targetId"].(string)
	if strings.TrimSpace(targetID) == "" {
		targetID, _ = info["targetID"].(string)
	}
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return result, nil
	}
	target, selectErr := s.selectTargetIDWithPlan(targetID, plan)
	result.target = target
	result.targetID = targetID
	result.selectErr = selectErr
	return result, nil
}

func (s *Service) selectTargetIDWithPlan(targetID string, plan Plan) (TargetConfig, error) {
	targetPlan := plan
	targetPlan.Metadata = copyPlanMetadata(plan.Metadata)
	targetPlan.Metadata["requiredTargetID"] = targetID
	return s.targets.Select(targetPlan)
}

func recordRetryTargetFallback(plan Plan, fromTargetID, toTargetID string, cause error) {
	if plan.Metadata == nil {
		return
	}
	if fromTargetID != "" {
		plan.Metadata["retryTargetFallbackFromID"] = fromTargetID
	}
	if toTargetID != "" {
		plan.Metadata["retryTargetFallbackToID"] = toTargetID
	}
	if cause != nil {
		plan.Metadata["retryTargetFallbackReason"] = cause.Error()
	}
}

func isRemotePreStartFallbackError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "prepare remote checkout:")
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
	project, err := s.projectForTask(task)
	if err != nil {
		return WorkerTurnResult{}, err
	}
	remoteSourceDir, err := resolveRemoteCheckout(project, target)
	if err != nil {
		return WorkerTurnResult{}, fmt.Errorf("prepare remote checkout: %w", err)
	}
	remoteWorkDir := remoteSourceDir
	retryFromWorkerID := stringMetadata(plan.Metadata, "retryFromWorkerID")
	resumeSessionID := stringMetadata(plan.Metadata, "retryResumeSessionID")
	requireFreshWorkspace := strings.EqualFold(stringMetadata(plan.Metadata, "workspaceReusePolicy"), "fresh") || boolMetadata(plan.Metadata, "freshWorkspace")
	reusedWorkspace := false
	if retryFromWorkerID != "" && !requireFreshWorkspace {
		if retryWorkDir, err := s.remoteRetryWorkDir(ctx, target, retryFromWorkerID); err != nil {
			plan.Metadata["retryWorkspaceReused"] = false
			plan.Metadata["retryWorkspaceError"] = err.Error()
			resumeSessionID = ""
			delete(plan.Metadata, "retryResumeSessionID")
		} else {
			remoteWorkDir = retryWorkDir
			reusedWorkspace = true
			plan.Metadata["retryWorkspaceReused"] = true
			plan.Metadata["retryWorkspaceCWD"] = remoteWorkDir
		}
	}
	baseWorkerID := candidateBaseWorkerID(plan.Metadata)
	if !reusedWorkspace && baseWorkerID != "" && !requireFreshWorkspace {
		if sameTarget, _ := s.workerRanOnTarget(ctx, baseWorkerID, target.ID); sameTarget {
			if baseWorkDir, err := s.remoteRetryWorkDir(ctx, target, baseWorkerID); err == nil {
				remoteWorkDir = baseWorkDir
				reusedWorkspace = true
				plan.Metadata["baseWorkspaceCWD"] = remoteWorkDir
			} else {
				plan.Metadata["baseWorkspaceReuseError"] = err.Error()
			}
		}
	}
	remoteRun := NewRemoteRun(target, worker.Spec{ID: workerID, TaskID: task.ID, WorkDir: remoteWorkDir})
	if !reusedWorkspace {
		checkoutBase := project.DefaultBase
		checkoutBaseRef := projectWorkspaceBaseCommit(ctx, project)
		if workspaceBaseRef := stringMetadata(plan.Metadata, "workspaceBaseRef"); workspaceBaseRef != "" {
			checkoutBase = pullRequestWorkspaceBranch(workspaceBaseRef)
			synced, err := syncedProjectWorkspaceRefRevision(ctx, project, workspaceBaseRef)
			if err != nil {
				return WorkerTurnResult{}, err
			}
			if synced != "" {
				plan.Metadata["workspaceBaseRevision"] = synced
				checkoutBaseRef = projectWorkspaceRefCommit(ctx, project, synced)
			}
		}
		checkoutLog, err := s.sshRunner.PrepareCheckout(ctx, target, RemoteCheckoutSpec{
			RepoURL:     projectCloneURL(project),
			WorkDir:     remoteSourceDir,
			DefaultBase: checkoutBase,
			BaseRef:     checkoutBaseRef,
		})
		if err != nil {
			return WorkerTurnResult{}, fmt.Errorf("prepare remote checkout: %w: %s", err, checkoutLog)
		}
		if checkoutLog != "" {
			plan.Metadata["remoteCheckout"] = checkoutLog
		}
		remoteWorkDir = path.Join(remoteRun.RunDir, "repo")
		worktreeLog, err := s.sshRunner.PrepareWorktree(ctx, target, remoteSourceDir, remoteWorkDir)
		if err != nil {
			return WorkerTurnResult{}, fmt.Errorf("prepare remote worktree: %w: %s", err, worktreeLog)
		}
		remoteRun.WorkDir = remoteWorkDir
		plan.Metadata["remoteSourceDir"] = remoteSourceDir
		if worktreeLog != "" {
			plan.Metadata["remoteWorktree"] = worktreeLog
		}
		if baseWorkerID != "" {
			patch, baseChanges, err := s.workerHandoffPatch(ctx, baseWorkerID)
			if err != nil {
				return WorkerTurnResult{}, err
			}
			if strings.TrimSpace(patch) != "" {
				if err := s.sshRunner.ApplyPatch(ctx, target, remoteWorkDir, remoteRun.RunDir, patch); err != nil {
					if boolMetadata(plan.Metadata, "allowBasePatchConflicts") {
						plan.Metadata["baseHandoff"] = "patch_conflict"
						plan.Metadata["basePatchApplied"] = false
						plan.Metadata["basePatchConflicted"] = true
						plan.Metadata["basePatchConflictError"] = err.Error()
					} else {
						return WorkerTurnResult{}, fmt.Errorf("apply base worker patch on remote target: %w", err)
					}
				} else {
					plan.Metadata["baseHandoff"] = "patch"
					plan.Metadata["basePatchApplied"] = true
				}
				plan.Metadata["baseChangedFiles"] = len(baseChanges.ChangedFiles)
			} else {
				plan.Metadata["baseHandoff"] = "empty_patch"
			}
		}
	}
	workspace := PreparedWorkspace{
		Root:       remoteWorkDir,
		CWD:        remoteWorkDir,
		SourceRoot: remoteSourceDir,
		Mode:       "remote",
		VCSType:    "ssh",
		WorkerID:   workerID,
		TaskID:     task.ID,
		TargetID:   target.ID,
		TargetKind: string(target.Kind),
	}
	capabilities := worker.RunnerCapabilities(runner)
	capabilities.LiveSteering = false
	spec := worker.Spec{
		ID:              workerID,
		TaskID:          task.ID,
		Kind:            plan.WorkerKind,
		Prompt:          remoteWorkerExecutionPrompt(plan.Prompt, workspace),
		WorkDir:         remoteWorkDir,
		ResumeSessionID: resumeSessionID,
		ReasoningEffort: plan.ReasoningEffort,
		TargetID:        target.ID,
		TargetKind:      string(target.Kind),
	}
	if reusedWorkspace {
		if !capabilities.ResumeSession {
			resumeSessionID = ""
			delete(plan.Metadata, "retryResumeSessionID")
			spec.ResumeSessionID = ""
		}
		spec.Prompt = retryWorkerExecutionPrompt(spec.Prompt, retryFromWorkerID, resumeSessionID, stringSliceMetadata(plan.Metadata, "retrySteering"), stringMetadata(plan.Metadata, "retryContextKind"))
	} else if retryFromWorkerID != "" {
		retrySteering := stringSliceMetadata(plan.Metadata, "retrySteering")
		if len(retrySteering) > 0 {
			spec.Prompt = retryWorkerExecutionPrompt(spec.Prompt, retryFromWorkerID, "", retrySteering, stringMetadata(plan.Metadata, "retryContextKind"))
		}
	}
	command := runner.BuildCommand(spec)
	workspace.Root = remoteRun.RunDir
	workspace.CWD = remoteRun.WorkDir
	workspace.SourceRoot = remoteSourceDir
	workspace.WorkspaceName = remoteRun.Session
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
		Payload:  core.MustJSON(workspace),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":       plan.WorkerKind,
			"command":    command,
			"prompt":     spec.Prompt,
			"promptPath": remotePromptPath(remoteRun),
			"metadata":   planMetadata(plan),
		}),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[workerID] = cancel
	s.tasks[workerID] = task.ID
	s.remoteRuns[workerID] = remoteRun
	s.workerCaps[workerID] = capabilities
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.cancels, workerID)
		delete(s.tasks, workerID)
		delete(s.steering, workerID)
		delete(s.remoteRuns, workerID)
		delete(s.workerCaps, workerID)
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
	stdin, err := sshWorkerStdin(runner, spec, command, capabilities)
	if err != nil {
		changes := remoteWorkerStartFailureChanges(workspace)
		if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(core.WorkerFailed, err, changes)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		return runState.turnResult(workerID, plan, core.WorkerFailed, err, changes), nil
	}
	if err := s.sshRunner.Start(workerCtx, remoteRun, command, stdin); err != nil {
		changes := remoteWorkerStartFailureChanges(workspace)
		if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(core.WorkerFailed, err, changes)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		return runState.turnResult(workerID, plan, core.WorkerFailed, err, changes), nil
	}
	sshRunner := s.sshRunner
	sshRunner.CallbackHandler = s.handleRemoteWorkerCallbacks
	status, err := sshRunner.Poll(workerCtx, remoteRun, worker.ParserForKind(plan.WorkerKind), sink)
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
	if s.workerCompleted(context.Background(), task.ID, workerID) {
		workerStatus = core.WorkerCanceled
		statusErr = context.Canceled
	} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(workerStatus, statusErr, changes)); completionErr != nil {
		return WorkerTurnResult{}, completionErr
	}
	_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
	sshRunner.CallbackHandler = s.handleRemoteWorkerCallbacks
	if err := sshRunner.drainRemoteCallbacks(ctx, remoteRun, sink); err != nil {
		_ = sink.Event(ctx, worker.Event{Kind: worker.EventError, Stream: "stderr", Text: "failed to drain terminal remote worker callbacks: " + err.Error()})
	}
	return runState.turnResult(workerID, plan, workerStatus, statusErr, changes), nil
}

func sshWorkerStdin(runner worker.Runner, spec worker.Spec, command []string, capabilities worker.Capabilities) (string, error) {
	if provider, ok := runner.(worker.RemoteStdinProvider); ok {
		return provider.RemoteStdin(spec)
	}
	if capabilities.PromptStdin || worker.CommandUsesPromptStdin(command) {
		return spec.Prompt, nil
	}
	return "", nil
}

func (s *Service) handleRemoteWorkerCallbacks(ctx context.Context, run remoteRun, callbacks []RemoteWorkerCallback) error {
	for _, callback := range callbacks {
		switch nonEmpty(callback.Type, "create_task") {
		case "create_task":
			if err := s.handleRemoteCreateTaskCallback(ctx, run, callback); err != nil {
				return err
			}
		case "publish_pull_request":
			if !s.workerCompleted(ctx, run.TaskID, run.WorkerID) {
				return errWorkerCallbackDeferred
			}
			if err := s.handleWorkerPublishPullRequestCallback(ctx, run.TaskID, run.WorkerID, callback, "remote"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported remote worker callback %q from %s", callback.Type, callback.ID)
		}
	}
	return nil
}

func (s *Service) workerCompleted(ctx context.Context, taskID string, workerID string) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerCompleted && event.TaskID == taskID && event.WorkerID == workerID {
			return true
		}
	}
	return false
}

func (s *Service) appendWorkerCompleted(ctx context.Context, taskID string, workerID string, payload map[string]any) error {
	if s.workerCompleted(context.Background(), taskID, workerID) {
		return s.recordWorkerCompletedDigest(ctx, taskID, workerID, payload)
	}
	event := core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(payload),
	}
	var lastErr error
	for attempt, delay := range workerCompletedAppendRetryDelays {
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return fmt.Errorf("append worker.completed for worker %s: %w", workerID, ctx.Err())
			case <-timer.C:
			}
		}
		if s.workerCompleted(context.Background(), taskID, workerID) {
			return nil
		}
		if _, err := s.append(ctx, event); err != nil {
			lastErr = err
			if attempt < len(workerCompletedAppendRetryDelays)-1 {
				slog.Warn("failed to append worker.completed; retrying", "taskID", taskID, "workerID", workerID, "attempt", attempt+1, "error", err)
			}
			continue
		}
		return s.recordWorkerCompletedDigest(ctx, taskID, workerID, payload)
	}
	return fmt.Errorf("append worker.completed for worker %s: %w", workerID, lastErr)
}

func (s *Service) recordWorkerCompletedDigest(ctx context.Context, taskID string, workerID string, payload map[string]any) error {
	if s.workerCompletedDigestRecorded(ctx, taskID, workerID) {
		return nil
	}
	metadata := workerCompletedDigestMetadata(workerID, payload)
	if len(metadata) == 0 {
		return nil
	}
	reason := stringMetadata(metadata, "summary")
	if reason == "" {
		reason = stringMetadata(metadata, "error")
	}
	if reason == "" {
		reason = "worker completed with status " + stringMetadata(metadata, "status")
	}
	_, err := s.append(ctx, core.Event{
		Type:     core.EventTaskAction,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "worker_result_digest",
			"status":   "recorded",
			"reason":   reason,
			"workerId": workerID,
			"metadata": metadata,
		}),
	})
	return err
}

func (s *Service) workerCompletedDigestRecorded(ctx context.Context, taskID string, workerID string) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.WorkerID != workerID || event.Type != core.EventTaskAction {
			continue
		}
		var payload struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(event.Payload, &payload) == nil && payload.Kind == "worker_result_digest" {
			return true
		}
	}
	return false
}

func workerCompletedDigestMetadata(workerID string, payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return map[string]any{"workerId": workerID, "error": "could not encode worker completion payload: " + err.Error()}
	}
	var decoded struct {
		Status           core.WorkerStatus      `json:"status"`
		Summary          string                 `json:"summary,omitempty"`
		Error            string                 `json:"error,omitempty"`
		LogCount         int                    `json:"logCount,omitempty"`
		NeedsInput       bool                   `json:"needsInput,omitempty"`
		ChangedFiles     []WorkspaceChangedFile `json:"changedFiles,omitempty"`
		WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges,omitempty"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return map[string]any{"workerId": workerID, "error": "could not decode worker completion payload: " + err.Error()}
	}
	changedFiles := decoded.ChangedFiles
	if len(changedFiles) == 0 {
		changedFiles = decoded.WorkspaceChanges.ChangedFiles
	}
	if len(changedFiles) > 20 {
		omitted := len(changedFiles) - 20
		changedFiles = append(append([]WorkspaceChangedFile{}, changedFiles[:20]...), WorkspaceChangedFile{
			Path:   fmt.Sprintf("... %d additional changed files omitted ...", omitted),
			Status: "omitted",
		})
	}
	artifactMetadata := []map[string]any{}
	for _, artifact := range decoded.WorkspaceChanges.Artifacts {
		item := map[string]any{
			"id":   artifact.ID,
			"kind": artifact.Kind,
			"name": artifact.Name,
			"path": artifact.Path,
		}
		if artifact.Content != "" {
			item["contentBytes"] = len(artifact.Content)
			if len(artifact.Content) <= replanPromptTinyArtifactContentBytes {
				item["content"] = artifact.Content
			}
		}
		artifactMetadata = append(artifactMetadata, item)
	}
	metadata := map[string]any{
		"workerId":     workerID,
		"status":       decoded.Status,
		"summary":      truncateStringForPrompt(decoded.Summary, 2000),
		"error":        truncateStringForPrompt(decoded.Error, 2000),
		"logCount":     decoded.LogCount,
		"needsInput":   decoded.NeedsInput,
		"dirty":        decoded.WorkspaceChanges.Dirty,
		"changedFiles": changedFiles,
	}
	if decoded.WorkspaceChanges.DiffStat != "" {
		metadata["diffStat"] = truncateStringForPrompt(decoded.WorkspaceChanges.DiffStat, 1000)
	}
	if decoded.WorkspaceChanges.Error != "" {
		metadata["workspaceError"] = truncateStringForPrompt(decoded.WorkspaceChanges.Error, 2000)
	}
	if len(artifactMetadata) > 0 {
		metadata["artifacts"] = artifactMetadata
	}
	return metadata
}

func remoteWorkerStartFailureChanges(workspace PreparedWorkspace) WorkspaceChanges {
	return WorkspaceChanges{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
	}
}

func (s *Service) handleRemoteCreateTaskCallback(ctx context.Context, run remoteRun, callback RemoteWorkerCallback) error {
	prompt := strings.TrimSpace(callback.Prompt)
	if prompt == "" {
		return fmt.Errorf("remote worker callback %s has empty prompt", callback.ID)
	}
	projectID, err := s.workerCallbackProjectID(ctx, run.TaskID, callback.ProjectID)
	if err != nil {
		return fmt.Errorf("resolve project for remote callback %s: %w", callback.ID, err)
	}
	metadata := map[string]any{
		"source":           "remote_worker",
		"parentTaskId":     nonEmpty(callback.ParentTaskID, run.TaskID),
		"parentWorkerId":   nonEmpty(callback.ParentWorkerID, run.WorkerID),
		"remoteTargetId":   run.Target.ID,
		"remoteSession":    run.Session,
		"remoteCallbackId": callback.ID,
	}
	req := core.CreateTaskRequest{
		ProjectID:  projectID,
		Title:      strings.TrimSpace(callback.Title),
		Prompt:     prompt,
		Source:     "remote-worker",
		ExternalID: nonEmpty(run.WorkerID, run.Session) + ":" + callback.ID,
		Metadata:   core.MustJSON(metadata),
	}
	req, err = NormalizeCreateTaskRequest(req)
	if err != nil {
		return fmt.Errorf("normalize task from remote callback %s: %w", callback.ID, err)
	}
	if _, err := s.CreateTask(ctx, req); err != nil {
		return fmt.Errorf("create task from remote callback %s: %w", callback.ID, err)
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   run.TaskID,
		WorkerID: run.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"kind":       "log",
			"stream":     "stdout",
			"text":       "remote worker queued follow-up task: " + prompt,
			"callbackId": callback.ID,
		}),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) drainLocalWorkerCallbacks(ctx context.Context, taskID string, workerID string, callbackDir string) {
	callbackDir = strings.TrimSpace(callbackDir)
	if callbackDir == "" {
		return
	}
	callbacks, files, err := readLocalWorkerCallbackFiles(callbackDir)
	if err != nil {
		_, _ = s.append(context.Background(), core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"kind":   "error",
				"stream": "stderr",
				"text":   "failed to drain local worker callbacks: " + err.Error(),
			}),
		})
		return
	}
	if len(callbacks) == 0 {
		return
	}
	if err := s.handleLocalWorkerCallbacks(ctx, taskID, workerID, callbacks); err != nil {
		_, _ = s.append(context.Background(), core.Event{
			Type:     core.EventWorkerOutput,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"kind":   "error",
				"stream": "stderr",
				"text":   "failed to handle local worker callbacks: " + err.Error(),
			}),
		})
		return
	}
	for _, file := range files {
		_ = os.Remove(file)
	}
}

func readLocalWorkerCallbackFiles(callbackDir string) ([]RemoteWorkerCallback, []string, error) {
	matches, err := filepath.Glob(filepath.Join(callbackDir, "*.json"))
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(matches)
	callbacks := make([]RemoteWorkerCallback, 0, len(matches))
	for _, file := range matches {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, nil, err
		}
		callback, err := decodeRemoteCallback(filepath.Base(file), string(data))
		if err != nil {
			return nil, nil, err
		}
		callbacks = append(callbacks, callback)
	}
	return callbacks, matches, nil
}

func (s *Service) handleLocalWorkerCallbacks(ctx context.Context, taskID string, workerID string, callbacks []RemoteWorkerCallback) error {
	for _, callback := range callbacks {
		switch nonEmpty(callback.Type, "create_task") {
		case "create_task":
			if err := s.handleLocalCreateTaskCallback(ctx, taskID, workerID, callback); err != nil {
				return err
			}
		case "publish_pull_request":
			if err := s.handleWorkerPublishPullRequestCallback(ctx, taskID, workerID, callback, "local"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported local worker callback %q from %s", callback.Type, callback.ID)
		}
	}
	return nil
}

func (s *Service) handleLocalCreateTaskCallback(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback) error {
	prompt := strings.TrimSpace(callback.Prompt)
	if prompt == "" {
		return fmt.Errorf("local worker callback %s has empty prompt", callback.ID)
	}
	projectID, err := s.workerCallbackProjectID(ctx, taskID, callback.ProjectID)
	if err != nil {
		return fmt.Errorf("resolve project for local callback %s: %w", callback.ID, err)
	}
	metadata := map[string]any{
		"source":         "local_worker",
		"parentTaskId":   nonEmpty(callback.ParentTaskID, taskID),
		"parentWorkerId": nonEmpty(callback.ParentWorkerID, workerID),
		"callbackId":     callback.ID,
	}
	req := core.CreateTaskRequest{
		ProjectID:  projectID,
		Title:      strings.TrimSpace(callback.Title),
		Prompt:     prompt,
		Source:     "local-worker",
		ExternalID: nonEmpty(workerID, taskID) + ":" + callback.ID,
		Metadata:   core.MustJSON(metadata),
	}
	req, err = NormalizeCreateTaskRequest(req)
	if err != nil {
		return fmt.Errorf("normalize task from local callback %s: %w", callback.ID, err)
	}
	if _, err := s.CreateTask(ctx, req); err != nil {
		return fmt.Errorf("create task from local callback %s: %w", callback.ID, err)
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":       "log",
			"stream":     "stdout",
			"text":       "local worker queued follow-up task: " + prompt,
			"callbackId": callback.ID,
		}),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) workerCallbackProjectID(ctx context.Context, parentTaskID string, explicitProjectID string) (string, error) {
	projectID := strings.TrimSpace(explicitProjectID)
	if projectID != "" {
		return projectID, nil
	}
	project, err := s.projectForTaskID(ctx, parentTaskID)
	if err != nil {
		return "", err
	}
	return project.ID, nil
}

func (s *Service) handleWorkerPublishPullRequestCallback(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback, source string) error {
	body := strings.TrimSpace(callback.Body)
	if body == "" {
		return fmt.Errorf("%s worker callback %s has empty pull request body", source, callback.ID)
	}
	publishWorkerID := nonEmpty(callback.ParentWorkerID, workerID)
	if ready, err := s.workerPublishCallbackCandidateReady(ctx, taskID, publishWorkerID, callback, source); err != nil {
		return err
	} else if !ready {
		return nil
	}
	pr, err := s.PublishTaskPullRequest(ctx, taskID, core.PublishPullRequestRequest{
		WorkerID:             publishWorkerID,
		Repo:                 strings.TrimSpace(callback.Repo),
		Base:                 strings.TrimSpace(callback.Base),
		Branch:               strings.TrimSpace(callback.Branch),
		Title:                strings.TrimSpace(callback.Title),
		Body:                 body,
		Draft:                callback.Draft,
		ContinueAfterPublish: callback.ContinueAfterPublish,
	})
	if err != nil {
		return fmt.Errorf("publish pull request from %s callback %s: %w", source, callback.ID, err)
	}
	if callback.ContinueAfterPublish {
		if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "continuing_after_pr", "Pull request opened from worker callback; objective continues looking for more results."); err != nil {
			return err
		}
		if err := s.setTaskStatus(ctx, taskID, core.TaskRunning); err != nil {
			return err
		}
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":                 "log",
			"stream":               "stdout",
			"text":                 source + " worker published pull request: " + pr.URL,
			"callbackId":           callback.ID,
			"pullRequestId":        pr.ID,
			"url":                  pr.URL,
			"continueAfterPublish": callback.ContinueAfterPublish,
		}),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) workerPublishCallbackCandidateReady(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback, source string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return false, eventstore.ErrNotFound
	}
	resolvedWorkerID, err := resolvePullRequestWorkerID(snapshot, task, workerID)
	if err == nil && strings.TrimSpace(resolvedWorkerID) != "" {
		return true, nil
	}
	if err != nil && !errors.Is(err, errPullRequestWorkerNotPublishable) {
		return false, err
	}
	reason := "worker callback requested pull request publication, but the selected worker is not a successful changed candidate"
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":       "publish_pull_request",
		"when":       "worker_callback",
		"source":     source,
		"callbackId": callback.ID,
		"workerId":   workerID,
		"status":     "skipped",
		"error":      reason,
		"inputs": map[string]any{
			"repo":                 strings.TrimSpace(callback.Repo),
			"base":                 strings.TrimSpace(callback.Base),
			"branch":               strings.TrimSpace(callback.Branch),
			"title":                strings.TrimSpace(callback.Title),
			"draft":                callback.Draft,
			"continueAfterPublish": callback.ContinueAfterPublish,
		},
	}); err != nil {
		return false, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":       "log",
			"stream":     "stdout",
			"text":       source + " worker skipped pull request publication: no successful worker with candidate changes",
			"callbackId": callback.ID,
			"reason":     reason,
		}),
	}); err != nil {
		return false, err
	}
	return false, nil
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
		if blocker, ok := classifyUserRecoverableBlocker(nonEmpty(result.Error, result.Summary)); ok {
			_ = s.waitForUserAction(ctx, taskID, result.WorkerID, blocker.Reason, blocker.Question, map[string]any{
				"summary":    blocker.Summary,
				"workerKind": result.Kind,
				"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
				"error":      result.Error,
			})
			return false
		}
		_ = s.setTaskStatus(ctx, taskID, core.TaskFailed)
	}
	return false
}

func (s *Service) finishOrContinueResults(ctx context.Context, taskID string, results []WorkerTurnResult) bool {
	for _, result := range results {
		if !s.finishOrContinueTask(ctx, taskID, result) {
			return false
		}
	}
	return true
}

func (s *Service) completeTask(ctx context.Context, taskID string, results []WorkerTurnResult, selectedWorkerID string, reason string) error {
	return s.completeTaskWithPublishRecovery(ctx, taskID, results, selectedWorkerID, reason, publishRecoveryState{})
}

type publishRecoveryState struct {
	Attempts               int
	BlockedFinalCandidates map[string]string
}

func (s *Service) completeTaskWithPublishRecovery(ctx context.Context, taskID string, results []WorkerTurnResult, selectedWorkerID string, reason string, recoveryState publishRecoveryState) error {
	candidateWorkerID, candidateReason, err := resolveFinalCandidate(results, selectedWorkerID)
	if err != nil {
		return s.waitForFinalCandidateResolution(ctx, taskID, err)
	}
	if candidateWorkerID != "" {
		if strings.TrimSpace(reason) == "" {
			reason = candidateReason
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskCandidate,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"workerId": candidateWorkerID,
				"reason":   reason,
			}),
		}); err != nil {
			return err
		}
		if err := s.recordTaskMilestone(ctx, taskID, "candidate_ready", "candidate_ready", "Final candidate selected.", map[string]any{
			"workerId": candidateWorkerID,
			"reason":   reason,
		}); err != nil {
			return err
		}
	}
	if pr, ok := s.openPullRequestForTask(ctx, taskID); ok && !s.retryingCompletionPullRequestPublication(ctx, taskID) {
		if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "pr_open", pullRequestObjectiveSummary(pr, "pr_open")); err != nil {
			return err
		}
		return s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	}
	if candidateWorkerID != "" && s.taskCompletionMode(ctx, taskID) == "github" {
		if handled, recoverErr := s.recoverUnpublishableCompletionCandidate(ctx, taskID, results, candidateWorkerID, reason); handled {
			return recoverErr
		}
		publicationAction := completionPublicationReviewAction(s.latestCompletionPullRequestBody(ctx, taskID))
		if ready, rejectionReason, err := s.reviewCompletionPublicationReadiness(ctx, taskID, publicationAction, results, candidateWorkerID); err != nil {
			return err
		} else if !ready {
			if handled, recoverErr := s.recoverPublicationReadinessRejectedCandidate(ctx, taskID, results, candidateWorkerID, rejectionReason); handled {
				return recoverErr
			}
			return nil
		}
		review, err := s.reviewCandidateBeforePullRequest(ctx, taskID, results, candidateWorkerID, "completion")
		if err != nil {
			return err
		}
		results = review.Results
		if !review.Ready {
			if handled, recoverErr := s.recoverCodeReviewBlockedCandidate(ctx, taskID, results, candidateWorkerID, review.Reason); handled {
				return recoverErr
			}
			return nil
		}
		if _, err := s.PublishTaskPullRequest(ctx, taskID, core.PublishPullRequestRequest{
			WorkerID: candidateWorkerID,
			Body:     stringMetadata(publicationAction.Inputs, "body"),
		}); err != nil {
			publishErr := fmt.Errorf("publish completion pull request: %w", err)
			if handled, recoverErr := s.recoverCompletionPublishFailure(ctx, taskID, results, candidateWorkerID, publishErr, recoveryState); handled {
				return recoverErr
			}
			_ = s.failTask(ctx, taskID, publishErr)
			return err
		}
		return s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveSatisfied, "satisfied", "Local task result is complete."); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, taskID, core.TaskSucceeded)
}

type codeReviewGateResult struct {
	Ready          bool
	Results        []WorkerTurnResult
	Reason         string
	ReviewWorkerID string
}

func (s *Service) reviewCandidateBeforePullRequest(ctx context.Context, taskID string, results []WorkerTurnResult, candidateWorkerID string, phase string) (codeReviewGateResult, error) {
	out := codeReviewGateResult{Ready: true, Results: results}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return out, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return out, eventstore.ErrNotFound
	}
	project, err := s.projectForTask(task)
	if err != nil {
		return out, err
	}
	policy := normalizedReviewPolicy(project.ReviewPolicy)
	if !reviewPolicyEnabledForPhase(policy, phase) {
		return out, nil
	}
	if candidateAlreadyPassedCodeReview(snapshot, taskID, candidateWorkerID, phase) {
		return out, nil
	}
	if attempts := codeReviewAttempts(snapshot, taskID, candidateWorkerID, phase); attempts >= policy.MaxAttempts {
		reason := fmt.Sprintf("code review gate reached the configured max attempts (%d) for candidate %s", policy.MaxAttempts, candidateWorkerID)
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":              "code_review_gate",
			"phase":             phase,
			"status":            "waiting",
			"candidateWorkerId": candidateWorkerID,
			"reason":            reason,
			"attempts":          attempts,
		})
		_ = s.waitForUserAction(ctx, taskID, candidateWorkerID, "code_review_gate", reason, map[string]any{
			"candidateWorkerId": candidateWorkerID,
			"phase":             phase,
			"attempts":          attempts,
		})
		out.Ready = false
		out.Reason = reason
		return out, nil
	}
	candidate, ok := workerResultByID(results, candidateWorkerID)
	if !ok {
		return out, nil
	}
	plan := Plan{
		WorkerKind:      s.codeReviewWorkerKind(policy, task, candidate),
		Prompt:          s.codeReviewGatePrompt(task, candidate, policy, phase),
		ReasoningEffort: "high",
		Rationale:       "project review policy requires code review before pull request publication",
		Metadata: map[string]any{
			"baseWorkerID":      candidateWorkerID,
			"codeReviewGate":    true,
			"reviewPhase":       phase,
			"spawnID":           "code-review-gate",
			"spawnRole":         "review",
			"spawnReason":       "Project review policy requires an independent code review before publishing this candidate.",
			"candidateWorkerID": candidateWorkerID,
		},
	}
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		return out, err
	}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		return out, err
	}
	out.Results = append(results, result)
	out.ReviewWorkerID = result.WorkerID
	if result.Status != core.WorkerSucceeded {
		out.Ready = false
		out.Reason = nonEmpty(result.Error, result.Summary, "code review worker did not complete successfully")
		_ = s.recordCodeReviewGateResult(ctx, taskID, candidateWorkerID, phase, result, "failed", out.Reason)
		return out, nil
	}
	if codeReviewBlocksPublication(result, policy) {
		out.Ready = false
		out.Reason = nonEmpty(result.Summary, "code review requested changes")
		_ = s.recordCodeReviewGateResult(ctx, taskID, candidateWorkerID, phase, result, "blocked", out.Reason)
		return out, nil
	}
	out.Ready = true
	out.Reason = nonEmpty(result.Summary, "code review approved publication")
	_ = s.recordCodeReviewGateResult(ctx, taskID, candidateWorkerID, phase, result, "passed", out.Reason)
	return out, nil
}

func normalizedReviewPolicy(policy core.ReviewPolicy) core.ReviewPolicy {
	policy.BlockingSeverities = normalizeReviewSeverities(policy.BlockingSeverities)
	policy.ReviewerKinds = uniqueNonEmptyStrings(policy.ReviewerKinds)
	policy.PromptSetID = strings.TrimSpace(policy.PromptSetID)
	policy.Instructions = strings.TrimSpace(policy.Instructions)
	if policy.Enabled {
		if !policy.BeforeCompletionPR && !policy.BeforeIntermediatePR {
			policy.BeforeCompletionPR = true
			policy.BeforeIntermediatePR = true
		}
		if len(policy.BlockingSeverities) == 0 {
			policy.BlockingSeverities = []string{"P0", "P1"}
		}
		if policy.MaxAttempts <= 0 {
			policy.MaxAttempts = 2
		}
	}
	return policy
}

func reviewPolicyEnabledForPhase(policy core.ReviewPolicy, phase string) bool {
	if !policy.Enabled {
		return false
	}
	switch phase {
	case "completion":
		return policy.BeforeCompletionPR
	case "intermediate":
		return policy.BeforeIntermediatePR
	default:
		return false
	}
}

func (s *Service) codeReviewWorkerKind(policy core.ReviewPolicy, task core.Task, candidate WorkerTurnResult) string {
	for _, kind := range policy.ReviewerKinds {
		if _, ok := s.runners[kind]; ok {
			return kind
		}
	}
	for _, kind := range []string{"claude", "codex", candidate.Kind} {
		kind = strings.TrimSpace(kind)
		if kind == "" {
			continue
		}
		if _, ok := s.runners[kind]; ok {
			return kind
		}
	}
	for kind := range s.runners {
		return kind
	}
	return candidate.Kind
}

func (s *Service) codeReviewGatePrompt(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string) string {
	if provider, ok := s.brain.(CodeReviewPromptProvider); ok {
		if prompt := strings.TrimSpace(provider.CodeReviewPrompt(task, candidate, policy, phase)); prompt != "" {
			return prompt
		}
	}
	return buildCodeReviewGatePrompt(task, candidate, policy, phase)
}

func buildCodeReviewGatePrompt(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string) string {
	var builder strings.Builder
	builder.WriteString("# Pre-publication Code Review\n\n")
	builder.WriteString("Review the selected candidate before aged publishes it as a pull request. This is a blocking code review, not a task-completion readiness check.\n\n")
	builder.WriteString("Original user request:\n")
	builder.WriteString(task.Prompt)
	builder.WriteString("\n\nCandidate worker:\n")
	builder.WriteString(candidate.WorkerID)
	builder.WriteString("\n\nPublication phase:\n")
	builder.WriteString(phase)
	builder.WriteString("\n\nBlocking severities:\n")
	builder.WriteString(strings.Join(policy.BlockingSeverities, ", "))
	if strings.TrimSpace(policy.Instructions) != "" {
		builder.WriteString("\n\nProject-specific review instructions:\n")
		builder.WriteString(policy.Instructions)
	}
	builder.WriteString("\n\nReview requirements:\n")
	builder.WriteString("- Inspect the actual diff and surrounding code in the workspace.\n")
	builder.WriteString("- Look for correctness bugs, lifecycle/state regressions, missing regression coverage, unsafe assumptions, and mismatches between the PR claim and the implemented/tested behavior.\n")
	builder.WriteString("- Treat missing tests as blocking when the changed behavior is risky or the PR explicitly claims coverage for a path that is not actually tested.\n")
	builder.WriteString("- Do not make code changes. Report findings only.\n\n")
	builder.WriteString("Candidate summary:\n")
	builder.WriteString(nonEmpty(candidate.Summary, "(none)"))
	builder.WriteString("\n\nCandidate error:\n")
	builder.WriteString(nonEmpty(candidate.Error, "(none)"))
	builder.WriteString("\n\nChanged files:\n")
	for _, file := range candidate.Changes.ChangedFiles {
		builder.WriteString("- ")
		if file.Status != "" {
			builder.WriteString(file.Status)
			builder.WriteString(" ")
		}
		builder.WriteString(file.Path)
		builder.WriteString("\n")
	}
	if len(candidate.Changes.ChangedFiles) == 0 {
		builder.WriteString("- (none reported)\n")
	}
	builder.WriteString("\nRespond in markdown with exactly these sections:\n")
	builder.WriteString("Decision: approve OR request_changes\n")
	builder.WriteString("Findings:\n")
	builder.WriteString("Commands Run:\n")
	builder.WriteString("Residual Risk:\n\n")
	builder.WriteString("Use severity labels like P0, P1, P2, or P3. Any finding at a configured blocking severity should use `Decision: request_changes`.\n")
	return builder.String()
}

func codeReviewBlocksPublication(result WorkerTurnResult, policy core.ReviewPolicy) bool {
	text := strings.Join([]string{result.Summary, result.Error}, "\n")
	normalized := strings.Join(strings.Fields(strings.ToLower(text)), " ")
	if normalized == "" {
		return false
	}
	if decision, ok := codeReviewDecision(text); ok {
		return decision == "request_changes"
	}
	if strings.Contains(normalized, "changes requested") ||
		strings.Contains(normalized, "request changes") {
		return true
	}
	return containsBlockingSeverity(normalized, policy.BlockingSeverities)
}

func codeReviewDecision(text string) (string, bool) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if !strings.HasPrefix(lower, "decision:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(lower, "decision:"))
		value = strings.Trim(value, "`*_ ")
		value = strings.ReplaceAll(value, "-", "_")
		value = strings.Join(strings.Fields(value), " ")
		switch value {
		case "approve", "approved":
			return "approve", true
		case "request_changes", "request changes", "changes requested":
			return "request_changes", true
		default:
			return "", false
		}
	}
	return "", false
}

func containsBlockingSeverity(text string, severities []string) bool {
	for _, severity := range severities {
		severity = strings.ToLower(strings.TrimSpace(severity))
		if severity == "" {
			continue
		}
		pattern := regexp.MustCompile(`(^|[^a-z0-9])` + regexp.QuoteMeta(severity) + `([^a-z0-9]|$)`)
		if pattern.MatchString(text) {
			return true
		}
	}
	return false
}

func (s *Service) recordCodeReviewGateResult(ctx context.Context, taskID string, candidateWorkerID string, phase string, result WorkerTurnResult, status string, reason string) error {
	return s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":              "code_review_gate",
		"phase":             phase,
		"status":            status,
		"candidateWorkerId": candidateWorkerID,
		"reviewWorkerId":    result.WorkerID,
		"reason":            truncateStringForPrompt(reason, 2000),
	})
}

func candidateAlreadyPassedCodeReview(snapshot core.Snapshot, taskID string, candidateWorkerID string, phase string) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.Type != core.EventTaskAction {
			continue
		}
		var payload struct {
			Kind              string `json:"kind"`
			Phase             string `json:"phase"`
			Status            string `json:"status"`
			CandidateWorkerID string `json:"candidateWorkerId"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind != "code_review_gate" || payload.Phase != phase || payload.CandidateWorkerID != candidateWorkerID {
			continue
		}
		return payload.Status == "passed"
	}
	return false
}

func codeReviewAttempts(snapshot core.Snapshot, taskID string, candidateWorkerID string, phase string) int {
	attempts := 0
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskAction {
			continue
		}
		var payload struct {
			Kind              string `json:"kind"`
			Phase             string `json:"phase"`
			Status            string `json:"status"`
			CandidateWorkerID string `json:"candidateWorkerId"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == "code_review_gate" && payload.Phase == phase && payload.CandidateWorkerID == candidateWorkerID && (payload.Status == "blocked" || payload.Status == "failed") {
			attempts++
		}
	}
	return attempts
}

func (s *Service) recoverCodeReviewBlockedCandidate(ctx context.Context, taskID string, results []WorkerTurnResult, candidateWorkerID string, reason string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return true, err
	}
	recovery := s.recoverFinalCandidateWithReplan(ctx, taskID, snapshot, candidateWorkerID, errors.New(reason), "code_review_recovery", "before_completion_pr", "code review blocked publication", map[string]string{candidateWorkerID: reason})
	if !recovery.Handled {
		_ = s.waitForUserAction(ctx, taskID, candidateWorkerID, "code_review_gate", "Code review blocked publication.\n\n"+reason+"\n\nSteer the task to fix the findings, select a different candidate, or publish manually.", map[string]any{
			"error": reason,
		})
		return true, nil
	}
	if recovery.Err != nil || !recovery.Completed {
		return true, recovery.Err
	}
	return true, s.completeTaskWithPublishRecovery(ctx, taskID, recovery.Results, recovery.SelectedWorkerID, recovery.Reason, publishRecoveryState{})
}

func (s *Service) retryingCompletionPullRequestPublication(ctx context.Context, taskID string) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	task, ok := findTask(snapshot, taskID)
	return ok && taskCompletionModeFromTask(task) == "github" && task.ObjectivePhase == "retrying"
}

func (s *Service) latestCompletionPullRequestBody(ctx context.Context, taskID string) string {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ""
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.Type != core.EventTaskReplanned {
			continue
		}
		var payload struct {
			Decision ReplanDecision `json:"decision"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Decision.Action != "complete" {
			continue
		}
		return strings.TrimSpace(payload.Decision.PullRequestBody)
	}
	return ""
}

func (s *Service) recoverUnpublishableCompletionCandidate(ctx context.Context, taskID string, results []WorkerTurnResult, candidateWorkerID string, completionReason string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return true, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return true, eventstore.ErrNotFound
	}
	candidate, ok := workerResultByID(results, candidateWorkerID)
	if !ok {
		return false, nil
	}
	blockReason, blocked := s.completionReadinessBlockReason(ctx, task, candidate, completionReason)
	if !blocked {
		return false, nil
	}
	recovery := s.recoverCompletionReadinessWithReplan(ctx, taskID, snapshot, candidateWorkerID, errors.New(blockReason), true)
	if !recovery.Handled {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":     "completion_publish_readiness_recovery",
			"when":     "before_publish",
			"reason":   "Final candidate is not ready for publication.",
			"workerId": candidateWorkerID,
			"status":   "waiting",
			"error":    blockReason,
		})
		_ = s.waitForUserAction(ctx, taskID, candidateWorkerID, "publish_readiness", "The selected final candidate is not ready to publish as a completion pull request.\n\n"+blockReason+"\n\nSteer the task to continue, select a different final candidate, or explicitly publish anyway.", map[string]any{
			"error": blockReason,
		})
		return true, nil
	}
	if recovery.Err != nil || !recovery.Completed {
		return true, recovery.Err
	}
	return true, s.completeTaskWithPublishRecovery(ctx, taskID, recovery.Results, recovery.SelectedWorkerID, recovery.Reason, publishRecoveryState{})
}

func (s *Service) recoverCompletionReadinessWithReplan(ctx context.Context, taskID string, snapshot core.Snapshot, candidateWorkerID string, failureErr error, allowBlockedCandidateValidation bool) finalCandidateRecoveryResult {
	if _, ok := s.brain.(ReplanProvider); !ok {
		return finalCandidateRecoveryResult{}
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return finalCandidateRecoveryResult{Handled: true, Err: eventstore.ErrNotFound}
	}
	initial, results, err := retryGraphStateForTask(snapshot, taskID)
	if err != nil {
		return finalCandidateRecoveryResult{}
	}
	candidate, ok := workerResultByID(results, candidateWorkerID)
	if !ok || strings.TrimSpace(candidate.Kind) == "" {
		return finalCandidateRecoveryResult{}
	}
	results = annotateFinalCandidateFailure(results, candidateWorkerID, "completion publish readiness failed", failureErr)
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":     "completion_publish_readiness_recovery",
		"when":     "before_publish",
		"reason":   "Final candidate does not satisfy the task objective yet; asking the orchestrator to continue or select a different candidate.",
		"workerId": candidateWorkerID,
		"status":   "started",
		"error":    failureErr.Error(),
	}); err != nil {
		return finalCandidateRecoveryResult{Handled: true, Err: err}
	}
	blockedFinalCandidates := map[string]string{candidateWorkerID: failureErr.Error()}
	ok, selectedWorkerID, reason, results := s.replanLoopWithOptions(ctx, task, initial, results, replanLoopOptions{
		BlockedFinalCandidates:          blockedFinalCandidates,
		AllowBlockedCandidateValidation: allowBlockedCandidateValidation,
		RecoveryHint:                    fmt.Sprintf("completion readiness failed for worker %s: %s. Do not complete with this blocked candidate unless a successful validation worker confirms it. Continue with the next worker turn that can satisfy the task objective, select a different final candidate, or wait if the objective is no longer actionable.", candidateWorkerID, failureErr.Error()),
	})
	if !ok {
		return finalCandidateRecoveryResult{Handled: true, Results: results}
	}
	if selectedWorkerID == "" {
		if candidateWorkerID, candidateReason, err := resolveFinalCandidate(results, ""); err == nil {
			selectedWorkerID = candidateWorkerID
			reason = nonEmpty(reason, candidateReason)
		}
	}
	if selectedWorkerID == "" {
		return finalCandidateRecoveryResult{Handled: true, Results: results}
	}
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":     "completion_publish_readiness_recovery",
		"when":     "before_publish",
		"reason":   nonEmpty(reason, "Completion readiness recovery selected a new candidate."),
		"workerId": selectedWorkerID,
		"status":   "completed",
	}); err != nil {
		return finalCandidateRecoveryResult{Handled: true, Err: err}
	}
	return finalCandidateRecoveryResult{
		Handled:          true,
		Completed:        true,
		SelectedWorkerID: selectedWorkerID,
		Reason:           reason,
		Results:          results,
	}
}

func (s *Service) recoverCompletionPublishFailure(ctx context.Context, taskID string, results []WorkerTurnResult, candidateWorkerID string, publishErr error, recoveryState publishRecoveryState) (bool, error) {
	if !isRecoverablePublishConflict(publishErr) {
		return false, nil
	}
	if recoveryState.Attempts >= maxCompletionPublishRecoveryAttempts {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":     "completion_publish_recovery",
			"when":     "after_publish_conflict",
			"reason":   "Completion publish recovery reached the retry limit.",
			"workerId": candidateWorkerID,
			"status":   "waiting",
			"error":    publishErr.Error(),
		})
		_ = s.waitForUserAction(ctx, taskID, candidateWorkerID, "publish_conflict_recovery_limit", "Publishing the completion pull request still conflicts after multiple orchestrated repair attempts. Steer the task with how to resolve the remaining conflicts or publish manually from the retained worker workspace.", map[string]any{
			"error":                   publishErr.Error(),
			"blockedFinalCandidates":  sortedMapKeys(recoveryState.BlockedFinalCandidates),
			"publishRecoveryAttempts": recoveryState.Attempts,
		})
		return true, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	blockedFinalCandidates := maps.Clone(recoveryState.BlockedFinalCandidates)
	if blockedFinalCandidates == nil {
		blockedFinalCandidates = map[string]string{}
	}
	blockedFinalCandidates[candidateWorkerID] = publishErr.Error()
	recovery := s.recoverFinalCandidateWithReplan(ctx, taskID, snapshot, candidateWorkerID, publishErr, "completion_publish_recovery", "after_publish_conflict", "completion publish failed", blockedFinalCandidates)
	if !recovery.Handled {
		return false, nil
	}
	if recovery.Err != nil || !recovery.Completed {
		return true, recovery.Err
	}
	return true, s.completeTaskWithPublishRecovery(ctx, taskID, recovery.Results, recovery.SelectedWorkerID, recovery.Reason, publishRecoveryState{
		Attempts:               recoveryState.Attempts + 1,
		BlockedFinalCandidates: blockedFinalCandidates,
	})
}

type finalCandidateRecoveryResult struct {
	Handled          bool
	Completed        bool
	SelectedWorkerID string
	Reason           string
	Results          []WorkerTurnResult
	Err              error
}

func (s *Service) recoverFinalCandidateWithReplan(ctx context.Context, taskID string, snapshot core.Snapshot, candidateWorkerID string, failureErr error, actionKind string, actionWhen string, failureLabel string, blockedFinalCandidates map[string]string) finalCandidateRecoveryResult {
	if _, ok := s.brain.(ReplanProvider); !ok {
		return finalCandidateRecoveryResult{}
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return finalCandidateRecoveryResult{Handled: true, Err: eventstore.ErrNotFound}
	}
	initial, results, err := retryGraphStateForTask(snapshot, taskID)
	if err != nil {
		return finalCandidateRecoveryResult{}
	}
	candidate, ok := workerResultByID(results, candidateWorkerID)
	if !ok || strings.TrimSpace(candidate.Kind) == "" {
		return finalCandidateRecoveryResult{}
	}
	results = annotateFinalCandidateFailure(results, candidateWorkerID, failureLabel, failureErr)
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":     actionKind,
		"when":     actionWhen,
		"reason":   "Final candidate finalization failed; asking the orchestrator to choose a recovery plan.",
		"workerId": candidateWorkerID,
		"status":   "started",
		"error":    failureErr.Error(),
	}); err != nil {
		return finalCandidateRecoveryResult{Handled: true, Err: err}
	}
	blockedFinalCandidates = maps.Clone(blockedFinalCandidates)
	if blockedFinalCandidates == nil {
		blockedFinalCandidates = map[string]string{}
	}
	blockedFinalCandidates[candidateWorkerID] = failureErr.Error()
	ok, selectedWorkerID, reason, results := s.replanLoopWithOptions(ctx, task, initial, results, replanLoopOptions{
		BlockedFinalCandidates:         blockedFinalCandidates,
		AllowBlockedBasePatchConflicts: true,
		RecoveryHint:                   fmt.Sprintf("%s for worker %s. Do not complete with a blocked final candidate. Schedule a repair or consolidation worker that starts from the blocked worker changes, resolves conflicts against the current checkout, and produces a new candidate.", failureLabel, candidateWorkerID),
		RequiredRepairWorkerID:         candidateWorkerID,
		RequiredRepairReason:           failureErr.Error(),
		FinalizationRecovery:           true,
	})
	if !ok {
		return finalCandidateRecoveryResult{Handled: true, Results: results}
	}
	if selectedWorkerID == "" {
		if candidateWorkerID, candidateReason, err := resolveFinalCandidate(results, ""); err == nil {
			selectedWorkerID = candidateWorkerID
			reason = nonEmpty(reason, candidateReason)
		}
	}
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":       actionKind,
		"when":       actionWhen,
		"reason":     nonEmpty(reason, "Orchestrator selected a recovery result."),
		"workerId":   selectedWorkerID,
		"baseWorker": candidateWorkerID,
		"status":     "completed",
	}); err != nil {
		return finalCandidateRecoveryResult{Handled: true, Err: err}
	}
	return finalCandidateRecoveryResult{
		Handled:          true,
		Completed:        true,
		SelectedWorkerID: selectedWorkerID,
		Reason:           reason,
		Results:          results,
	}
}

func annotateFinalCandidateFailure(results []WorkerTurnResult, candidateWorkerID string, label string, failureErr error) []WorkerTurnResult {
	out := make([]WorkerTurnResult, len(results))
	copy(out, results)
	for index := range out {
		if out[index].WorkerID != candidateWorkerID {
			continue
		}
		message := strings.TrimSpace(label + ": " + failureErr.Error())
		if strings.TrimSpace(out[index].Error) == "" {
			out[index].Error = message
		} else {
			out[index].Error = strings.TrimSpace(out[index].Error + "\n" + message)
		}
		return out
	}
	return out
}

func isRecoverablePublishConflict(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "remote patch has conflicts") ||
		strings.Contains(lower, "patch does not apply") ||
		strings.Contains(lower, "3-way apply failed") ||
		(strings.Contains(lower, "applied patch") && strings.Contains(lower, "conflicts"))
}

func isRecoverableApplyConflict(err error) bool {
	if err == nil {
		return false
	}
	if isRecoverablePublishConflict(err) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "conflict") ||
		strings.Contains(lower, "could not apply") ||
		strings.Contains(lower, "merge git worker commit") ||
		strings.Contains(lower, "create jj merge revision")
}

func workerResultByID(results []WorkerTurnResult, workerID string) (WorkerTurnResult, bool) {
	for _, result := range results {
		if result.WorkerID == workerID {
			return result, true
		}
	}
	return WorkerTurnResult{}, false
}

func workerResultByReference(results []WorkerTurnResult, workerRef string) (WorkerTurnResult, bool) {
	workerRef = strings.TrimSpace(workerRef)
	if workerRef == "" {
		return WorkerTurnResult{}, false
	}
	if result, ok := workerResultByID(results, workerRef); ok {
		return result, true
	}
	for i := len(results) - 1; i >= 0; i-- {
		if results[i].SpawnID == workerRef {
			return results[i], true
		}
	}
	return WorkerTurnResult{}, false
}

func planActionWorkerID(results []WorkerTurnResult, workerRef string) string {
	workerRef = strings.TrimSpace(workerRef)
	if workerRef == "" {
		return latestCandidateWorkerID(results)
	}
	if result, ok := workerResultByReference(results, workerRef); ok {
		return result.WorkerID
	}
	return workerRef
}

func (s *Service) runImmediatePlanActions(ctx context.Context, task core.Task, plan Plan) (bool, error) {
	for _, action := range plan.Actions {
		if strings.TrimSpace(action.When) != "immediate" {
			continue
		}
		keepGoing, _, err := s.executePlanAction(ctx, task, action, nil)
		if err != nil || !keepGoing {
			return keepGoing, err
		}
	}
	return true, nil
}

func (s *Service) runPlanActions(ctx context.Context, task core.Task, plan Plan, results []WorkerTurnResult) (bool, []WorkerTurnResult, error) {
	for _, action := range plan.Actions {
		if strings.TrimSpace(action.When) == "immediate" {
			continue
		}
		if strings.TrimSpace(action.Kind) == "publish_pull_request" {
			workerID := ""
			if strings.TrimSpace(action.WorkerID) != "" {
				workerID = planActionWorkerID(results, action.WorkerID)
			}
			if blocker, ok := publicationBlockedByFollowUpFinding(results, workerID); ok {
				if err := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":           "publish_pull_request_blocked_by_follow_up",
					"when":           nonEmpty(action.When, "after_success"),
					"reason":         blocker.Reason,
					"actionReason":   action.Reason,
					"workerId":       blocker.WorkerID,
					"spawnId":        blocker.SpawnID,
					"role":           blocker.Role,
					"status":         "rejected",
					"findingSummary": blocker.Summary,
				}); err != nil {
					return false, results, err
				}
				continue
			}
		}
		keepGoing, nextResults, err := s.executePlanAction(ctx, task, action, results)
		results = nextResults
		if err != nil || !keepGoing {
			return keepGoing, results, err
		}
	}
	return true, results, nil
}

func (s *Service) runDeferredPlanWork(ctx context.Context, task core.Task, plan Plan, results []WorkerTurnResult, parentNodeID string) (bool, []WorkerTurnResult, error) {
	earlyActions, remainingActions := splitPreFollowUpActions(plan.Actions, results)
	if len(earlyActions) == 0 {
		nextResults, ok, err := s.runFollowUpWorkers(ctx, task, plan, results, parentNodeID)
		if err != nil || !ok {
			return ok, nextResults, err
		}
		return s.runPlanActions(ctx, task, plan, nextResults)
	}
	beforePRs, err := s.taskPullRequestCount(ctx, task.ID)
	if err != nil {
		return false, results, err
	}
	ok, nextResults, err := s.runPlanActions(ctx, task, planWithActions(plan, earlyActions), results)
	if err != nil || !ok {
		return ok, nextResults, err
	}
	if containsPublishPullRequestAction(earlyActions) {
		afterPRs, err := s.taskPullRequestCount(ctx, task.ID)
		if err != nil {
			return false, nextResults, err
		}
		if afterPRs <= beforePRs {
			return true, nextResults, nil
		}
	}
	nextResults, ok, err = s.runFollowUpWorkers(ctx, task, plan, nextResults, parentNodeID)
	if err != nil || !ok {
		return ok, nextResults, err
	}
	return s.runPlanActions(ctx, task, planWithActions(plan, remainingActions), nextResults)
}

func splitPreFollowUpActions(actions []PlanAction, results []WorkerTurnResult) ([]PlanAction, []PlanAction) {
	early := []PlanAction{}
	remaining := []PlanAction{}
	for _, action := range actions {
		if strings.TrimSpace(action.When) == "immediate" {
			continue
		}
		if strings.TrimSpace(action.WorkerID) != "" {
			if _, ok := workerResultByReference(results, action.WorkerID); ok {
				early = append(early, action)
				continue
			}
		}
		remaining = append(remaining, action)
	}
	return early, remaining
}

func planWithActions(plan Plan, actions []PlanAction) Plan {
	plan.Actions = actions
	return plan
}

func containsPublishPullRequestAction(actions []PlanAction) bool {
	for _, action := range actions {
		if strings.TrimSpace(action.Kind) == "publish_pull_request" {
			return true
		}
	}
	return false
}

func (s *Service) taskPullRequestCount(ctx context.Context, taskID string) (int, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID {
			count++
		}
	}
	return count, nil
}

type followUpPublicationBlocker struct {
	WorkerID string
	SpawnID  string
	Role     string
	Summary  string
	Reason   string
}

func publicationBlockedByFollowUpFinding(results []WorkerTurnResult, candidateWorkerID string) (followUpPublicationBlocker, bool) {
	start := 0
	candidateWorkerID = strings.TrimSpace(candidateWorkerID)
	if candidateWorkerID != "" {
		start = len(results)
		for i, result := range results {
			if result.WorkerID == candidateWorkerID {
				start = i + 1
			}
		}
	}
	for _, result := range results[start:] {
		if result.Status != core.WorkerSucceeded {
			continue
		}
		if !isReviewOrEvaluatorFollowUp(result) {
			continue
		}
		if !resultRequiresFollowUp(result.Summary) {
			continue
		}
		return followUpPublicationBlocker{
			WorkerID: result.WorkerID,
			SpawnID:  result.SpawnID,
			Role:     result.Role,
			Summary:  truncateStringForPrompt(strings.TrimSpace(result.Summary), 1000),
			Reason:   "review/evaluator follow-up reported substantive findings that require another worker turn before publishing",
		}, true
	}
	return followUpPublicationBlocker{}, false
}

func isReviewOrEvaluatorFollowUp(result WorkerTurnResult) bool {
	if strings.TrimSpace(result.SpawnID) == "" && strings.TrimSpace(result.Role) == "" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{result.Kind, result.Role, result.SpawnID}, " "))
	for _, marker := range []string{"review", "evaluator", "evaluate", "validation", "validator", "critique", "audit"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func resultRequiresFollowUp(summary string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(summary), " "))
	if normalized == "" {
		return false
	}
	for _, clean := range []string{
		"no findings",
		"no issues",
		"no blockers",
		"looks good",
		"approved",
		"clean review",
		"non-blocking",
		"informational only",
	} {
		if strings.Contains(normalized, clean) && !containsRequiredFollowUpPhrase(normalized) {
			return false
		}
	}
	if containsRequiredFollowUpPhrase(normalized) {
		return true
	}
	for _, rawLine := range strings.Split(summary, "\n") {
		line := strings.ToLower(strings.Join(strings.Fields(rawLine), " "))
		if line == "" {
			continue
		}
		if !strings.Contains(line, "finding") && !strings.Contains(line, "issue") && !strings.Contains(line, "blocker") {
			continue
		}
		for _, severity := range []string{"critical", "high", "medium", "major"} {
			if lineContainsReviewSeverity(line, severity) {
				return true
			}
		}
	}
	return false
}

func lineContainsReviewSeverity(line string, severity string) bool {
	for _, marker := range []string{
		severity + ":",
		severity + " -",
		severity + " issue",
		severity + " finding",
		severity + " blocker",
		"[" + severity + "]",
		"severity: " + severity,
	} {
		if strings.Contains(line, marker) {
			return true
		}
	}
	return false
}

func containsRequiredFollowUpPhrase(normalized string) bool {
	for _, phrase := range []string{
		"requires follow-up",
		"require follow-up",
		"needs follow-up",
		"need follow-up",
		"required follow-up",
		"must fix",
		"needs to be fixed",
		"need to be fixed",
		"requires another worker",
		"schedule another worker",
		"request changes",
		"changes requested",
		"do not publish",
		"not ready to publish",
	} {
		if strings.Contains(normalized, phrase) {
			return true
		}
	}
	return false
}

func (s *Service) executePlanAction(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult) (bool, []WorkerTurnResult, error) {
	switch strings.TrimSpace(action.Kind) {
	case "publish_pull_request":
		workerID := planActionWorkerID(results, action.WorkerID)
		if workerID == "" {
			return false, results, s.waitForMissingPublishCandidate(ctx, task, action, results)
		}
		req := publishPullRequestRequestFromAction(action)
		req.WorkerID = workerID
		if ready, err := s.reviewPlanPublicationReadiness(ctx, task, action, results, workerID); err != nil {
			return false, results, err
		} else if !ready {
			return true, results, nil
		}
		review, err := s.reviewCandidateBeforePullRequest(ctx, task.ID, results, workerID, "intermediate")
		if err != nil {
			return false, results, err
		}
		results = review.Results
		if !review.Ready {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":              action.Kind,
				"when":              nonEmpty(action.When, "after_success"),
				"reason":            "project review policy blocked pull request publication",
				"inputs":            action.Inputs,
				"workerId":          workerID,
				"reviewWorkerId":    review.ReviewWorkerID,
				"status":            "skipped",
				"candidateWorkerId": workerID,
				"error":             review.Reason,
			}); err != nil {
				return false, results, err
			}
			return true, results, nil
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":     action.Kind,
			"when":     nonEmpty(action.When, "after_success"),
			"reason":   action.Reason,
			"inputs":   action.Inputs,
			"workerId": workerID,
			"status":   "started",
		}); err != nil {
			return false, results, err
		}
		recordCompletedAction := func(published core.PullRequest) error {
			return s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":          action.Kind,
				"when":          nonEmpty(action.When, "after_success"),
				"reason":        action.Reason,
				"inputs":        action.Inputs,
				"workerId":      workerID,
				"pullRequestId": published.ID,
				"url":           published.URL,
			})
		}
		_, err = s.publishTaskPullRequest(ctx, task.ID, req, recordCompletedAction)
		if err != nil {
			if s.waitForRecoverableError(ctx, task.ID, workerID, err) {
				_ = s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":     action.Kind,
					"when":     nonEmpty(action.When, "after_success"),
					"reason":   action.Reason,
					"inputs":   action.Inputs,
					"workerId": workerID,
					"status":   "waiting",
					"error":    err.Error(),
				})
				return false, results, nil
			}
			return false, results, err
		}
		if boolMetadata(action.Inputs, "continueAfterPublish") {
			if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "continuing_after_pr", "Pull request opened; objective continues looking for more results."); err != nil {
				return false, results, err
			}
			return true, results, nil
		}
		return false, results, s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
	case "update_pull_request":
		workerID := planActionWorkerID(results, action.WorkerID)
		if workerID == "" {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   action.Kind,
				"when":   nonEmpty(action.When, "after_success"),
				"reason": action.Reason,
				"inputs": action.Inputs,
				"status": "skipped",
				"error":  "no successful changed candidate worker to update pull request",
			}); err != nil {
				return false, results, err
			}
			return true, results, nil
		}
		if result, ok := workerResultByID(results, workerID); ok && !resultHasCandidateChanges(result) {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":     action.Kind,
				"when":     nonEmpty(action.When, "after_success"),
				"reason":   action.Reason,
				"inputs":   action.Inputs,
				"workerId": workerID,
				"status":   "skipped",
				"error":    "follow-up worker produced no candidate changes to update pull request",
			}); err != nil {
				return false, results, err
			}
			return true, results, nil
		}
		pr, err := s.pullRequestForUpdateAction(ctx, task.ID, action)
		if err != nil {
			if errors.Is(err, errTerminalPullRequest) {
				if err := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":   action.Kind,
					"when":   nonEmpty(action.When, "after_success"),
					"reason": action.Reason,
					"inputs": action.Inputs,
					"status": "skipped",
					"error":  "pull request is already closed or merged; publish a fresh pull request for new candidate work",
				}); err != nil {
					return false, results, err
				}
				return true, results, nil
			}
			return false, results, err
		}
		req := updatePullRequestRequestFromAction(action)
		req.WorkerID = workerID
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":          action.Kind,
			"when":          nonEmpty(action.When, "after_success"),
			"reason":        action.Reason,
			"inputs":        action.Inputs,
			"workerId":      workerID,
			"pullRequestId": pr.ID,
			"status":        "started",
		}); err != nil {
			return false, results, err
		}
		updated, err := s.UpdateTaskPullRequest(ctx, task.ID, pr, req)
		if err != nil {
			if s.waitForRecoverableError(ctx, task.ID, workerID, err) {
				_ = s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":          action.Kind,
					"when":          nonEmpty(action.When, "after_success"),
					"reason":        action.Reason,
					"inputs":        action.Inputs,
					"workerId":      workerID,
					"pullRequestId": pr.ID,
					"status":        "waiting",
					"error":         err.Error(),
				})
				return false, results, nil
			}
			return false, results, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":          action.Kind,
			"when":          nonEmpty(action.When, "after_success"),
			"reason":        action.Reason,
			"inputs":        action.Inputs,
			"workerId":      workerID,
			"pullRequestId": updated.ID,
			"url":           updated.URL,
		}); err != nil {
			return false, results, err
		}
		return true, results, nil
	case "watch_pull_requests":
		req := watchPullRequestsRequestFromAction(action)
		if s.watchRequestTargetsTerminalPullRequest(ctx, task.ID, req) {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   action.Kind,
				"when":   nonEmpty(action.When, "after_success"),
				"reason": action.Reason,
				"inputs": action.Inputs,
				"status": "skipped",
				"error":  "pull request is already closed or merged",
			}); err != nil {
				return false, results, err
			}
			return true, results, nil
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
			"status": "started",
		}); err != nil {
			return false, results, err
		}
		prs, err := s.WatchPullRequests(ctx, task.ID, req)
		if err != nil {
			return false, results, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":             action.Kind,
			"when":             nonEmpty(action.When, "after_success"),
			"reason":           action.Reason,
			"inputs":           action.Inputs,
			"pullRequestCount": len(prs),
		}); err != nil {
			return false, results, err
		}
		return false, results, nil
	case "create_tasks":
		created, err := s.createChildTasksFromAction(ctx, task, action)
		if err != nil {
			return false, results, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":             action.Kind,
			"when":             nonEmpty(action.When, "after_success"),
			"reason":           action.Reason,
			"inputs":           action.Inputs,
			"createdTaskIds":   taskIDs(created),
			"createdTaskCount": len(created),
		}); err != nil {
			return false, results, err
		}
		return true, results, nil
	case "wait_external":
		phase := stringMetadata(action.Inputs, "phase")
		if phase == "" {
			phase = "waiting_external"
		}
		summary := stringMetadata(action.Inputs, "summary")
		if summary == "" {
			summary = action.Reason
		}
		if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingExternal, phase, summary); err != nil {
			return false, results, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
		}); err != nil {
			return false, results, err
		}
		return false, results, s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
	case "ask_user":
		question := stringMetadata(action.Inputs, "question")
		if question == "" {
			question = stringMetadata(action.Inputs, "summary")
		}
		if question == "" {
			question = action.Reason
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
		}); err != nil {
			return false, results, err
		}
		return false, results, s.waitForUserAction(ctx, task.ID, strings.TrimSpace(action.WorkerID), "ask_user", question, map[string]any{
			"summary":    stringMetadata(action.Inputs, "summary"),
			"target":     stringMetadata(action.Inputs, "target"),
			"project":    stringMetadata(action.Inputs, "project"),
			"resumeHint": stringMetadata(action.Inputs, "resumeHint"),
			"commands":   stringSliceMetadata(action.Inputs, "commands"),
		})
	default:
		return true, results, nil
	}
}

func (s *Service) waitForMissingPublishCandidate(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult) error {
	reason := "publish_pull_request action has no successful worker with candidate changes"
	metadata := map[string]any{
		"actionKind": action.Kind,
		"actionWhen": nonEmpty(action.When, "after_success"),
		"inputs":     action.Inputs,
	}
	question := "The plan tried to publish a pull request, but no successful worker produced publishable changes. Steer the task with the correct project or workspace, select another worker result, or retry after fixing the setup."
	workerID := ""
	if result, ok := latestSuccessfulWorkerResult(results); ok {
		workerID = result.WorkerID
		metadata["workerId"] = result.WorkerID
		metadata["workerKind"] = result.Kind
		if summary := strings.TrimSpace(result.Summary); summary != "" {
			truncated := truncateStringForPrompt(summary, 2000)
			metadata["workerSummary"] = truncated
			question = "The worker completed without publishable changes, so the planned pull request cannot be opened.\n\nLatest worker summary:\n" + truncated + "\n\nSteer the task with the correct project or workspace, select another worker result, or retry after fixing the setup."
		}
		if result.Error != "" {
			metadata["workerError"] = result.Error
		}
	}
	if err := s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":   action.Kind,
		"when":   nonEmpty(action.When, "after_success"),
		"reason": action.Reason,
		"inputs": action.Inputs,
		"status": "waiting",
		"error":  reason,
	}); err != nil {
		return err
	}
	return s.waitForUserAction(ctx, task.ID, workerID, "missing_publish_candidate", question, metadata)
}

type childTaskSpec struct {
	Title          string   `json:"title"`
	Prompt         string   `json:"prompt"`
	WorkstreamID   string   `json:"workstreamId,omitempty"`
	CompletionMode string   `json:"completionMode,omitempty"`
	DependsOn      []string `json:"dependsOn,omitempty"`
}

func (s *Service) createChildTasksFromAction(ctx context.Context, parent core.Task, action PlanAction) ([]core.Task, error) {
	rawTasks := anySliceMetadata(action.Inputs, "tasks")
	if len(rawTasks) == 0 {
		return nil, errors.New("create_tasks requires at least one task")
	}
	created := make([]core.Task, 0, len(rawTasks))
	for index, raw := range rawTasks {
		spec, err := childTaskSpecFromInput(raw)
		if err != nil {
			return nil, fmt.Errorf("create_tasks inputs.tasks[%d]: %w", index, err)
		}
		metadata := map[string]any{"parentTaskId": parent.ID}
		if spec.WorkstreamID != "" {
			metadata["workstreamId"] = spec.WorkstreamID
		}
		if len(spec.DependsOn) > 0 {
			metadata["dependsOn"] = spec.DependsOn
		}
		switch strings.ToLower(strings.TrimSpace(spec.CompletionMode)) {
		case "":
		case "github":
			metadata["completionMode"] = "github"
		case "local":
			metadata["completionMode"] = "local"
		default:
			return nil, fmt.Errorf("completionMode must be github or local")
		}
		req, err := NormalizeCreateTaskRequest(core.CreateTaskRequest{
			ProjectID:    parent.ProjectID,
			WorkstreamID: spec.WorkstreamID,
			Title:        spec.Title,
			Prompt:       spec.Prompt,
			Source:       "task-child",
			ExternalID:   parent.ID + ":" + nonEmpty(spec.WorkstreamID, fmt.Sprintf("task-%d", index+1)),
			Metadata:     core.MustJSON(metadata),
		})
		if err != nil {
			return nil, err
		}
		task, err := s.CreateTask(ctx, req)
		if err != nil {
			return nil, err
		}
		created = append(created, task)
	}
	return created, nil
}

func childTaskSpecFromInput(raw any) (childTaskSpec, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return childTaskSpec{}, err
	}
	var spec childTaskSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return childTaskSpec{}, err
	}
	spec.Title = strings.TrimSpace(spec.Title)
	spec.Prompt = strings.TrimSpace(spec.Prompt)
	spec.WorkstreamID = strings.TrimSpace(spec.WorkstreamID)
	spec.CompletionMode = strings.TrimSpace(spec.CompletionMode)
	spec.DependsOn = trimStringSlice(spec.DependsOn)
	if spec.Title == "" {
		return childTaskSpec{}, errors.New("title is required")
	}
	if spec.Prompt == "" {
		return childTaskSpec{}, errors.New("prompt is required")
	}
	return spec, nil
}

func trimStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func taskIDs(tasks []core.Task) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return ids
}

func latestSuccessfulWorkerResult(results []WorkerTurnResult) (WorkerTurnResult, bool) {
	for i := len(results) - 1; i >= 0; i-- {
		if results[i].Status == core.WorkerSucceeded {
			return results[i], true
		}
	}
	return WorkerTurnResult{}, false
}

func completionPublicationReviewAction(body string) PlanAction {
	return PlanAction{
		Kind:   "publish_pull_request",
		When:   "before_completion",
		Reason: "publish completion pull request",
		Inputs: map[string]any{
			"body": strings.TrimSpace(body),
		},
	}
}

func (s *Service) reviewCompletionPublicationReadiness(ctx context.Context, taskID string, action PlanAction, results []WorkerTurnResult, workerID string) (bool, string, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, "", err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return false, "", eventstore.ErrNotFound
	}
	return s.reviewTaskPublicationReadiness(ctx, task, action, results, workerID)
}

func (s *Service) reviewPlanPublicationReadiness(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult, workerID string) (bool, error) {
	ready, _, err := s.reviewTaskPublicationReadiness(ctx, task, action, results, workerID)
	return ready, err
}

func (s *Service) reviewTaskPublicationReadiness(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult, workerID string) (bool, string, error) {
	reviewer, ok := s.brain.(PublicationReviewProvider)
	if !ok {
		return true, "", nil
	}
	candidate, ok := workerResultByID(results, workerID)
	if !ok {
		return false, "", fmt.Errorf("publish_pull_request action selected unknown worker %s", workerID)
	}
	review, err := reviewer.ReviewPublication(ctx, task, candidate, action)
	if err != nil {
		if recordErr := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":     "publish_pull_request_readiness_review",
			"when":     nonEmpty(action.When, "after_success"),
			"reason":   "Publication readiness review failed; continuing with the planned action.",
			"workerId": workerID,
			"status":   "ignored",
			"error":    err.Error(),
		}); recordErr != nil {
			return false, "", recordErr
		}
		return true, "", nil
	}
	if review.Ready {
		return true, "", nil
	}
	reason := strings.TrimSpace(review.Reason)
	if reason == "" {
		reason = "candidate is not ready to publish as a pull request"
	}
	return false, reason, s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":            "publish_pull_request_readiness_rejected",
		"when":            nonEmpty(action.When, "after_success"),
		"reason":          reason,
		"actionReason":    action.Reason,
		"workerId":        workerID,
		"status":          "rejected",
		"candidateStatus": candidate.Status,
	})
}

func (s *Service) recoverPublicationReadinessRejectedCandidate(ctx context.Context, taskID string, results []WorkerTurnResult, candidateWorkerID string, reason string) (bool, error) {
	if strings.TrimSpace(reason) == "" {
		reason = "publication readiness review rejected completion pull request"
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return true, err
	}
	recovery := s.recoverCompletionReadinessWithReplan(ctx, taskID, snapshot, candidateWorkerID, errors.New(reason), false)
	if !recovery.Handled {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":     "completion_publish_readiness_recovery",
			"when":     "before_publish",
			"reason":   "Publication readiness review blocked completion pull request.",
			"workerId": candidateWorkerID,
			"status":   "waiting",
			"error":    reason,
		})
		_ = s.waitForUserAction(ctx, taskID, candidateWorkerID, "publish_readiness_review", "Publication readiness review blocked the completion pull request.\n\n"+reason+"\n\nSteer the task to continue, select a different final candidate, or explicitly publish anyway.", map[string]any{
			"error": reason,
		})
		return true, nil
	}
	if recovery.Err != nil || !recovery.Completed {
		return true, recovery.Err
	}
	return true, s.completeTaskWithPublishRecovery(ctx, taskID, recovery.Results, recovery.SelectedWorkerID, recovery.Reason, publishRecoveryState{})
}
func (s *Service) recordTaskAction(ctx context.Context, taskID string, payload map[string]any) error {
	_, err := s.append(ctx, core.Event{
		Type:    core.EventTaskAction,
		TaskID:  taskID,
		Payload: core.MustJSON(payload),
	})
	return err
}

func (s *Service) recordRejectedReplanCompletion(ctx context.Context, taskID string, turn int, decision ReplanDecision, reason string) error {
	return s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":                   "replan_completion_rejected",
		"status":                 "rejected",
		"turn":                   turn,
		"reason":                 reason,
		"workerId":               decision.FinalCandidateWorkerID,
		"replanAction":           decision.Action,
		"replanRationale":        decision.Rationale,
		"finalCandidateWorkerId": decision.FinalCandidateWorkerID,
	})
}

func (s *Service) waitForUserAction(ctx context.Context, taskID string, workerID string, reason string, question string, metadata map[string]any) error {
	if err := s.recordUserActionNeeded(ctx, taskID, workerID, reason, question, metadata); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingUser, "approval_needed", question); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, taskID, core.TaskWaiting)
}

func (s *Service) recordUserActionNeeded(ctx context.Context, taskID string, workerID string, reason string, question string, metadata map[string]any) error {
	payload := map[string]any{
		"question": nonEmpty(question, "The orchestrator needs user input before continuing."),
		"reason":   nonEmpty(reason, "user_input_required"),
	}
	for key, value := range metadata {
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				payload[key] = typed
			}
		case []string:
			if len(typed) > 0 {
				payload[key] = typed
			}
		default:
			if value != nil {
				payload[key] = value
			}
		}
	}
	_, err := s.append(ctx, core.Event{
		Type:     core.EventApprovalNeeded,
		TaskID:   taskID,
		WorkerID: strings.TrimSpace(workerID),
		Payload:  core.MustJSON(payload),
	})
	return err
}

func (s *Service) waitForRecoverableError(ctx context.Context, taskID string, workerID string, err error) bool {
	if err == nil {
		return false
	}
	blocker, ok := classifyUserRecoverableBlocker(err.Error())
	if !ok {
		return false
	}
	_ = s.waitForUserAction(ctx, taskID, workerID, blocker.Reason, blocker.Question, map[string]any{
		"summary":    blocker.Summary,
		"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
		"error":      err.Error(),
	})
	return true
}

func (s *Service) recoverableWorkerFailureCanRetryOnAlternateTarget(ctx context.Context, task core.Task, plan Plan, result WorkerTurnResult, blocker userRecoverableBlocker) bool {
	targetID := s.workerTargetID(ctx, result.WorkerID)
	if targetID == "" {
		return false
	}
	if !s.targets.MarkWorkerKindUnavailable(targetID, result.Kind, nonEmpty(result.Error, result.Summary, blocker.Summary)) {
		return false
	}
	probe := plan
	probe.Metadata = maps.Clone(plan.Metadata)
	delete(probe.Metadata, "targetID")
	delete(probe.Metadata, "targetKind")
	if _, err := s.selectExecutionTarget(ctx, probe); err != nil {
		return false
	}
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":       "worker_target_fallback",
		"when":       "after_worker_failure",
		"reason":     blocker.Summary,
		"workerId":   result.WorkerID,
		"workerKind": result.Kind,
		"targetId":   targetID,
		"status":     "continued",
		"error":      result.Error,
	})
	return true
}

func (s *Service) workerTargetID(ctx context.Context, workerID string) string {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return ""
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ""
	}
	for i := len(snapshot.ExecutionNodes) - 1; i >= 0; i-- {
		node := snapshot.ExecutionNodes[i]
		if node.WorkerID == workerID {
			return strings.TrimSpace(node.TargetID)
		}
	}
	return ""
}

type userRecoverableBlocker struct {
	Reason   string
	Summary  string
	Question string
}

func classifyUserRecoverableBlocker(text string) (userRecoverableBlocker, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return userRecoverableBlocker{}, false
	}
	lower := strings.ToLower(trimmed)
	checks := []struct {
		reason  string
		summary string
		any     []string
	}{
		{
			reason:  "missing_tool",
			summary: "A required command or tool is missing from the execution environment.",
			any:     []string{"command not found", "executable file not found", "no execution target matches labels", "no such file or directory: \"perf\"", "exec: \"perf\"", "exec: \"go\"", "exec: \"npm\"", "exec: \"deno\"", "exec: \"cargo\""},
		},
		{
			reason:  "permission_denied",
			summary: "The worker is blocked by operating-system or sandbox permissions.",
			any:     []string{"permission denied", "operation not permitted", "not permitted", "requires root", "must be root", "sudo:"},
		},
		{
			reason:  "worker_auth_required",
			summary: "The worker cannot authenticate to its model provider.",
			any:     []string{"missing bearer or basic authentication", "unexpected status 401 unauthorized", "401 unauthorized, url: wss://api.openai.com/v1/responses", "401 unauthorized, url: https://api.openai.com/v1/responses"},
		},
		{
			reason:  "worker_privilege_mismatch",
			summary: "The worker command cannot run with the target user's current privileges.",
			any:     []string{"--dangerously-skip-permissions cannot be used with root/sudo privileges"},
		},
		{
			reason:  "profiler_setup_required",
			summary: "Profiling is blocked by VM or kernel profiling configuration.",
			any:     []string{"perf_event_paranoid", "kernel.perf_event", "perf_event_open", "failed to open perf", "debug symbols", "dwarf"},
		},
		{
			reason:  "ssh_signing_agent_failed",
			summary: "PR publication is blocked by the local SSH signing agent.",
			any:     []string{"signing error", "sign_and_send_pubkey", "ssh sign failed", "1password: agent returned an error", "failed to fill whole buffer", "could not write object of type commit"},
		},
		{
			reason:  "ssh_setup_required",
			summary: "Remote execution is blocked by SSH authentication or host setup.",
			any:     []string{"permission denied (publickey)", "host key verification failed", "no route to host", "could not resolve hostname", "connection refused"},
		},
		{
			reason:  "repo_setup_required",
			summary: "Repository checkout or access is missing on the target environment.",
			any:     []string{"repository not found", "not a git repository", "workdir is not inside a supported vcs workspace", "missing repository", "clone"},
		},
		{
			reason:  "github_workflow_scope_required",
			summary: "GitHub rejected the push because the configured token cannot update workflow files.",
			any:     []string{"refusing to allow an oauth app to create or update workflow", "without `workflow` scope", "without workflow scope"},
		},
	}
	for _, check := range checks {
		for _, needle := range check.any {
			if strings.Contains(lower, needle) {
				return userRecoverableBlocker{
					Reason:   check.reason,
					Summary:  check.summary,
					Question: fmt.Sprintf("%s\n\nBlocked error:\n%s\n\nPlease fix this setup issue in the target environment, then reply with what changed so the orchestrator can continue.", check.summary, trimmed),
				}, true
			}
		}
	}
	return userRecoverableBlocker{}, false
}

func (s *Service) waitForFinalCandidateResolution(ctx context.Context, taskID string, err error) error {
	_, _ = s.append(ctx, core.Event{
		Type:   core.EventApprovalNeeded,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"question": "Final candidate selection is ambiguous or points at a non-applyable worker. Retry after steering the orchestrator to consolidate or select the intended candidate.",
			"reason":   "final_candidate_selection",
			"error":    err.Error(),
		}),
	})
	if statusErr := s.setTaskStatus(ctx, taskID, core.TaskWaiting); statusErr != nil {
		return statusErr
	}
	if objectiveErr := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingUser, "approval_needed", "Final candidate selection needs user or orchestrator approval."); objectiveErr != nil {
		return objectiveErr
	}
	return err
}

func replanMadeProgress(before []WorkerTurnResult, after []WorkerTurnResult) bool {
	if len(after) <= len(before) {
		return false
	}
	for _, result := range after[len(before):] {
		if result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
			return true
		}
	}
	return false
}

func validatesBlockedCandidate(results []WorkerTurnResult, selectedWorkerID string, blockedWorkerID string) bool {
	selectedWorkerID = strings.TrimSpace(selectedWorkerID)
	blockedWorkerID = strings.TrimSpace(blockedWorkerID)
	if selectedWorkerID == "" || blockedWorkerID == "" || selectedWorkerID == blockedWorkerID {
		return false
	}
	byID := map[string]WorkerTurnResult{}
	for _, result := range results {
		byID[result.WorkerID] = result
	}
	selected, ok := byID[selectedWorkerID]
	if !ok || selected.Status != core.WorkerSucceeded || resultHasCandidateChanges(selected) {
		return false
	}
	seen := map[string]bool{selectedWorkerID: true}
	current := selected
	for strings.TrimSpace(current.BaseWorkerID) != "" {
		parentID := current.BaseWorkerID
		if seen[parentID] {
			return false
		}
		seen[parentID] = true
		if parentID == blockedWorkerID {
			return true
		}
		parent, ok := byID[parentID]
		if !ok {
			return false
		}
		current = parent
	}
	return false
}

func sortedMapKeys(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func (s *Service) completionReadinessBlockReason(ctx context.Context, task core.Task, candidate WorkerTurnResult, completionReason string) (string, bool) {
	reviewer, ok := s.brain.(CompletionReviewProvider)
	if !ok {
		return "", false
	}
	review, err := reviewer.ReviewCompletion(ctx, task, candidate, completionReason)
	if err != nil {
		_ = s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":     "completion_readiness_review",
			"when":     "before_finalization",
			"reason":   "Completion readiness review failed; continuing with the replanner decision.",
			"workerId": candidate.WorkerID,
			"status":   "ignored",
			"error":    err.Error(),
		})
		return "", false
	}
	if review.Ready {
		return "", false
	}
	reason := strings.TrimSpace(review.Reason)
	if reason == "" {
		reason = "selected final candidate does not satisfy the task objective yet"
	}
	return reason, true
}

func (s *Service) taskCompletionMode(ctx context.Context, taskID string) string {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return "local"
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return "local"
	}
	return taskCompletionModeFromTask(task)
}

func taskCompletionModeFromTask(task core.Task) string {
	var metadata map[string]any
	if len(task.Metadata) > 0 {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	switch strings.ToLower(strings.TrimSpace(stringMetadataValue(metadata["completionMode"]))) {
	case "github":
		return "github"
	default:
		return "local"
	}
}

type replanLoopOptions struct {
	BlockedFinalCandidates          map[string]string
	AllowBlockedBasePatchConflicts  bool
	AllowBlockedCandidateValidation bool
	RecoveryHint                    string
	RequiredRepairWorkerID          string
	RequiredRepairReason            string
	FinalizationRecovery            bool
}

func (s *Service) replanLoop(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) (bool, string, string, []WorkerTurnResult) {
	return s.replanLoopWithOptions(ctx, task, initial, results, replanLoopOptions{})
}

func (s *Service) replanLoopWithOptions(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, options replanLoopOptions) (bool, string, string, []WorkerTurnResult) {
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		return true, "", "", results
	}
	blockedFinalCandidates := map[string]string{}
	for workerID, reason := range options.BlockedFinalCandidates {
		blockedFinalCandidates[workerID] = reason
	}
	recoveryHint := options.RecoveryHint
	stalledTurns := 0
	currentWorkPlan := initial.WorkPlan
	for turn := 1; ; turn++ {
		if terminal, err := s.taskIsTerminal(ctx, task.ID); err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return false, "", "", results
		} else if terminal && !options.FinalizationRecovery {
			return false, "", "", results
		}
		if stalledTurns >= maxConsecutiveUnproductiveReplanTurns {
			recoveryOptions := options
			recoveryOptions.BlockedFinalCandidates = blockedFinalCandidates
			recoveryOptions.RecoveryHint = recoveryHint
			return s.recoverReplanLimit(ctx, task, turn, results, recoveryOptions)
		}
		blockedFinalCandidateIDs := sortedMapKeys(blockedFinalCandidates)
		decision, err := replanner.Replan(ctx, task, OrchestrationState{
			InitialPlan:                initial,
			WorkPlan:                   currentWorkPlan,
			Results:                    results,
			ContextLedger:              s.taskContextLedger(ctx, task.ID),
			Artifacts:                  s.taskArtifacts(ctx, task.ID),
			PullRequests:               s.taskPullRequestStates(ctx, task.ID),
			TaskSteering:               s.taskSteering(ctx, task.ID),
			PendingPullRequestFeedback: s.pendingPullRequestFeedback(ctx, task.ID),
			PendingWorkerSteering:      s.pendingWorkerSteering(ctx, task.ID),
			Turn:                       turn,
			BlockedFinalCandidateIDs:   blockedFinalCandidateIDs,
			RecoveryHint:               recoveryHint,
		})
		if err != nil {
			if ctx.Err() != nil {
				return false, "", "", results
			}
			recoveryOptions := options
			recoveryOptions.BlockedFinalCandidates = blockedFinalCandidates
			recoveryOptions.RecoveryHint = recoveryHint
			return s.recoverReplanError(ctx, task, turn, results, fmt.Errorf("dynamic replan failed: %w", err), recoveryOptions)
		}
		if err := decision.Validate(); err != nil {
			recoveryOptions := options
			recoveryOptions.BlockedFinalCandidates = blockedFinalCandidates
			recoveryOptions.RecoveryHint = recoveryHint
			return s.recoverReplanError(ctx, task, turn, results, fmt.Errorf("invalid dynamic replan decision: %w", err), recoveryOptions)
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
			return false, "", "", results
		}
		if decision.WorkPlan != nil {
			if err := s.updateTaskWorkPlan(ctx, task.ID, *decision.WorkPlan); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			currentWorkPlan = decision.WorkPlan
		}
		switch decision.Action {
		case "complete":
			if pending := s.pendingPullRequestFeedback(ctx, task.ID); len(pending) > 0 {
				reason := "queued pull request feedback must be handled before completing the task"
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", "", results
				}
				stalledTurns++
				continue
			}
			if pending := s.pendingWorkerSteering(ctx, task.ID); len(pending) > 0 {
				reason := "queued worker steering must be handled before completing the task"
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", "", results
				}
				stalledTurns++
				continue
			}
			if taskCompletionModeFromTask(task) != "github" {
				if candidateWorkerID, _, err := resolveFinalCandidate(results, decision.FinalCandidateWorkerID); err == nil && candidateWorkerID != "" {
					if candidate, ok := workerResultByID(results, candidateWorkerID); ok {
						if reason, blocked := s.completionReadinessBlockReason(ctx, task, candidate, decision.Rationale); blocked {
							blockedFinalCandidates[candidateWorkerID] = reason
							recoveryHint = reason + " Do not complete with this blocked candidate. Continue with the next worker turn that can satisfy the task objective, select a different final candidate, or wait if the objective is no longer actionable."
							if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
								_ = s.failTask(ctx, task.ID, err)
								return false, "", "", results
							}
							stalledTurns++
							continue
						}
					}
				}
			}
			if len(blockedFinalCandidates) > 0 {
				candidateWorkerID, _, candidateErr := resolveFinalCandidate(results, decision.FinalCandidateWorkerID)
				if candidateErr != nil || candidateWorkerID == "" {
					if options.AllowBlockedCandidateValidation {
						for blockedWorkerID := range blockedFinalCandidates {
							if validatesBlockedCandidate(results, decision.FinalCandidateWorkerID, blockedWorkerID) {
								return true, blockedWorkerID, nonEmpty(decision.Rationale, "successful validation worker confirmed the blocked candidate"), results
							}
						}
					}
					if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, "recovery requires a new final candidate because the previous candidate failed finalization"); err != nil {
						_ = s.failTask(ctx, task.ID, err)
						return false, "", "", results
					}
					stalledTurns++
					continue
				}
				if reason := blockedFinalCandidates[candidateWorkerID]; strings.TrimSpace(reason) != "" {
					if options.AllowBlockedCandidateValidation && validatesBlockedCandidate(results, decision.FinalCandidateWorkerID, candidateWorkerID) {
						return true, candidateWorkerID, nonEmpty(decision.Rationale, "successful validation worker confirmed the blocked candidate"), results
					}
					if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, "blocked final candidate "+candidateWorkerID+" already failed finalization: "+reason); err != nil {
						_ = s.failTask(ctx, task.ID, err)
						return false, "", "", results
					}
					stalledTurns++
					continue
				}
			}
			return true, decision.FinalCandidateWorkerID, decision.Rationale, results
		case "wait":
			_ = s.waitForUserAction(ctx, task.ID, "", "orchestrator_wait", nonEmpty(decision.Message, decision.Rationale, "The orchestrator needs user input before continuing."), map[string]any{
				"turn":      turn,
				"rationale": decision.Rationale,
			})
			return false, "", "", results
		case "fail":
			_ = s.failTask(ctx, task.ID, errors.New(nonEmpty(decision.Message, decision.Rationale, "dynamic replan failed task")))
			return false, "", "", results
		case "continue":
			if terminal, err := s.taskIsTerminal(ctx, task.ID); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			} else if terminal && !options.FinalizationRecovery {
				return false, "", "", results
			}
			beforeResults := results
			next := *decision.Plan
			if pr, ok := s.firstPendingPullRequestFeedback(ctx, task.ID); ok {
				next = annotatePullRequestFollowUpPlan(next, pr)
				next = normalizePullRequestFollowUpPlan(next)
			}
			if steering, ok := s.firstPendingWorkerSteering(ctx, task.ID); ok {
				next = annotateWorkerSteeringPlan(next, steering)
			}
			if next.Metadata == nil {
				next.Metadata = map[string]any{}
			}
			next.Metadata["dynamicReplanTurn"] = turn
			if strings.TrimSpace(options.RequiredRepairWorkerID) != "" {
				next = forceConflictRepairPlan(task, next, options.RequiredRepairWorkerID, nonEmpty(options.RequiredRepairReason, recoveryHint), blockedFinalCandidates)
			}
			if shouldInheritLatestCandidate(next.Metadata) {
				if baseWorkerID := latestCandidateWorkerID(results); baseWorkerID != "" {
					next.Metadata["baseWorkerID"] = baseWorkerID
				}
			}
			if options.AllowBlockedBasePatchConflicts {
				baseWorkerID := candidateBaseWorkerID(next.Metadata)
				if _, blocked := blockedFinalCandidates[baseWorkerID]; blocked {
					next.Metadata["allowBasePatchConflicts"] = true
					next.Metadata["recoveryBaseWorkerID"] = baseWorkerID
					next.Metadata["recoveryHint"] = recoveryHint
				}
			}
			normalizePlanReasoning(&next)
			if _, err := s.append(ctx, core.Event{
				Type:    core.EventTaskPlanned,
				TaskID:  task.ID,
				Payload: core.MustJSON(next),
			}); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			var nextResults []WorkerTurnResult
			var ok bool
			if len(next.Workers) > 0 {
				nextResults, ok, err = s.runInitialWorkerGraph(ctx, task, next)
			} else {
				var result WorkerTurnResult
				result, err = s.runPlannedWorker(ctx, task, next)
				if err == nil {
					nextResults = append(nextResults, result)
					ok = true
				}
			}
			if err != nil {
				if ctx.Err() != nil {
					return false, "", "", results
				}
				if s.waitForRecoverableError(ctx, task.ID, "", err) {
					return false, "", "", results
				}
				results = append(results, failedFollowUpResult(next, err))
				_ = s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":   "worker_failure_recovery",
					"when":   "during_dynamic_replan",
					"reason": "Dynamic replan worker setup failed; continuing the replan loop with the failure as context.",
					"status": "continued",
					"error":  err.Error(),
				})
				stalledTurns++
				continue
			}
			if !ok {
				results = append(results, nextResults...)
				return false, "", "", results
			}
			results = append(results, nextResults...)
			if terminal, err := s.taskIsTerminal(ctx, task.ID); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			} else if terminal && !options.FinalizationRecovery {
				return false, "", "", results
			}
			if failed := firstWorkerResultWithStatus(nextResults, core.WorkerFailed); failed.WorkerID != "" {
				if blocker, ok := classifyUserRecoverableBlocker(nonEmpty(failed.Error, failed.Summary)); ok {
					if s.recoverableWorkerFailureCanRetryOnAlternateTarget(ctx, task, next, failed, blocker) {
						stalledTurns++
						continue
					}
					_ = s.waitForUserAction(ctx, task.ID, failed.WorkerID, blocker.Reason, blocker.Question, map[string]any{
						"summary":    blocker.Summary,
						"workerKind": failed.Kind,
						"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
						"error":      failed.Error,
					})
					return false, "", "", results
				}
				_ = s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":     "worker_failure_recovery",
					"when":     "during_dynamic_replan",
					"reason":   "Dynamic replan worker failed; continuing the replan loop with the failure as context.",
					"workerId": failed.WorkerID,
					"status":   "continued",
					"error":    failed.Error,
				})
				stalledTurns++
				continue
			}
			if !s.finishOrContinueResults(ctx, task.ID, nextResults) {
				return false, "", "", results
			}
			latest := latestWorkerResult(nextResults)
			if options.FinalizationRecovery {
				reason := nonEmpty(latest.Summary, next.Rationale, "finalization recovery worker produced a new candidate")
				return true, latest.WorkerID, reason, results
			}
			if ok, nextResults, err := s.runDeferredPlanWork(ctx, task, next, results, latest.NodeID); err != nil {
				if ctx.Err() != nil {
					return false, "", "", results
				}
				if s.waitForRecoverableError(ctx, task.ID, latest.WorkerID, err) {
					return false, "", "", results
				}
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			} else if !ok {
				return false, "", "", results
			} else {
				results = nextResults
			}
			if replanMadeProgress(beforeResults, results) {
				stalledTurns = 0
			} else {
				stalledTurns++
			}
		}
	}
}

func (s *Service) recoverReplanError(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, replanErr error, options replanLoopOptions) (bool, string, string, []WorkerTurnResult) {
	return s.recoverReplanFallback(ctx, task, turn, results, replanErr, options, replanFallbackConfig{
		CompleteReasonPrefix: "fallback completion after replanner error",
		CompleteMessage:      "The replanner returned an invalid decision, so aged used the deterministic final-candidate fallback.",
		WaitRationale:        "replanner returned an invalid decision and deterministic final-candidate fallback is ambiguous",
		WaitQuestion:         "Dynamic replanning failed and final candidate selection is ambiguous. Provide steering or retry after resolving the competing candidates.",
		WaitReason:           "dynamic_replan_error",
		WaitObjective:        "Dynamic replanning needs user steering before continuing.",
	})
}

func (s *Service) recoverReplanLimit(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, options replanLoopOptions) (bool, string, string, []WorkerTurnResult) {
	replanErr := fmt.Errorf("dynamic replanning reached %d consecutive turns without productive progress", maxConsecutiveUnproductiveReplanTurns)
	return s.recoverReplanFallback(ctx, task, turn, results, replanErr, options, replanFallbackConfig{
		CompleteReasonPrefix: "fallback completion after dynamic replanning stalled",
		CompleteMessage:      "Dynamic replanning stopped making productive progress, so aged used the deterministic final-candidate fallback instead of failing the task.",
		WaitRationale:        "dynamic replanning stopped making productive progress and deterministic final-candidate fallback is ambiguous",
		WaitQuestion:         "Dynamic replanning stopped making productive progress and final candidate selection is ambiguous. Provide steering or retry after resolving the competing candidates.",
		WaitReason:           "dynamic_replan_limit",
		WaitObjective:        "Dynamic replanning stopped making productive progress and needs user steering before continuing.",
	})
}

type replanFallbackConfig struct {
	CompleteReasonPrefix string
	CompleteMessage      string
	WaitRationale        string
	WaitQuestion         string
	WaitReason           string
	WaitObjective        string
}

func (s *Service) recoverReplanFallback(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, replanErr error, options replanLoopOptions, config replanFallbackConfig) (bool, string, string, []WorkerTurnResult) {
	if pendingReason := s.pendingReplanQueueBlockReason(ctx, task.ID); pendingReason != "" {
		s.waitForReplanFallback(ctx, task, turn, replanErr, config, pendingReason)
		return false, "", "", results
	}
	if isReplanContextWindowError(replanErr) {
		s.waitForReplanContextOverflow(ctx, task, turn, replanErr, results)
		return false, "", "", results
	}
	candidateWorkerID, candidateReason, candidateErr := resolveFinalCandidate(results, "")
	if candidateErr == nil && candidateWorkerID != "" {
		if reason := options.BlockedFinalCandidates[candidateWorkerID]; strings.TrimSpace(reason) != "" {
			candidateErr = fmt.Errorf("fallback final candidate %s is blocked: %s", candidateWorkerID, reason)
		} else {
			reason := config.CompleteReasonPrefix + ": " + replanErr.Error()
			if candidateReason != "" {
				reason += "; " + candidateReason
			}
			if _, err := s.append(ctx, core.Event{
				Type:   core.EventTaskReplanned,
				TaskID: task.ID,
				Payload: core.MustJSON(map[string]any{
					"turn": turn,
					"decision": ReplanDecision{
						Action:                 "complete",
						FinalCandidateWorkerID: candidateWorkerID,
						Rationale:              reason,
						Message:                config.CompleteMessage,
					},
					"fallback": true,
					"error":    replanErr.Error(),
				}),
			}); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			return true, candidateWorkerID, reason, results
		}
	}
	if taskCompletionModeFromTask(task) != "github" && candidateWorkerID == "" {
		if workerID, workerReason := deterministicLocalNoChangeCompletionWorker(results); workerID != "" {
			reason := config.CompleteReasonPrefix + ": " + replanErr.Error()
			if workerReason != "" {
				reason += "; " + workerReason
			}
			if _, err := s.append(ctx, core.Event{
				Type:   core.EventTaskReplanned,
				TaskID: task.ID,
				Payload: core.MustJSON(map[string]any{
					"turn": turn,
					"decision": ReplanDecision{
						Action:                 "complete",
						FinalCandidateWorkerID: workerID,
						Rationale:              reason,
						Message:                config.CompleteMessage,
					},
					"fallback": true,
					"error":    replanErr.Error(),
				}),
			}); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			return true, workerID, reason, results
		}
	}
	if candidateWorkerID, candidateReason := latestCandidateLeafExcluding(results, options.BlockedFinalCandidates); candidateWorkerID != "" {
		reason := config.CompleteReasonPrefix + ": " + replanErr.Error()
		if candidateReason != "" {
			reason += "; " + candidateReason
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskReplanned,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"turn": turn,
				"decision": ReplanDecision{
					Action:                 "complete",
					FinalCandidateWorkerID: candidateWorkerID,
					Rationale:              reason,
					Message:                config.CompleteMessage,
				},
				"fallback": true,
				"error":    replanErr.Error(),
			}),
		}); err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return false, "", "", results
		}
		return true, candidateWorkerID, reason, results
	}
	candidateError := "no deterministic final candidate available"
	if candidateErr != nil {
		candidateError = candidateErr.Error()
	}
	s.waitForReplanFallback(ctx, task, turn, replanErr, config, candidateError)
	return false, "", "", results
}

func (s *Service) pendingReplanQueueBlockReason(ctx context.Context, taskID string) string {
	var reasons []string
	if pending := s.pendingPullRequestFeedback(ctx, taskID); len(pending) > 0 {
		reasons = append(reasons, "queued pull request feedback must be handled before deterministic fallback completion")
	}
	if pending := s.pendingWorkerSteering(ctx, taskID); len(pending) > 0 {
		reasons = append(reasons, "queued worker steering must be handled before deterministic fallback completion")
	}
	return strings.Join(reasons, "; ")
}

func (s *Service) waitForReplanFallback(ctx context.Context, task core.Task, turn int, replanErr error, config replanFallbackConfig, candidateError string) {
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventTaskReplanned,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"turn": turn,
			"decision": ReplanDecision{
				Action:    "wait",
				Rationale: config.WaitRationale,
				Message:   replanErr.Error(),
			},
			"fallback":       true,
			"error":          replanErr.Error(),
			"candidateError": candidateError,
		}),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if err := s.waitForUserAction(ctx, task.ID, "", config.WaitReason, config.WaitQuestion, map[string]any{
		"error":          replanErr.Error(),
		"candidateError": candidateError,
		"objective":      config.WaitObjective,
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
	}
}

func (s *Service) waitForReplanContextOverflow(ctx context.Context, task core.Task, turn int, replanErr error, results []WorkerTurnResult) {
	digest := latestCompactReplanDigest(results)
	message := "Replan context too large. aged could not fit the compact orchestration state into the model context window, so it paused instead of selecting an old final candidate."
	if digest != "" {
		message += "\n\nLatest compact digest:\n" + digest
	}
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventTaskReplanned,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"turn": turn,
			"decision": ReplanDecision{
				Action:    "wait",
				Rationale: "replan context too large",
				Message:   message,
			},
			"fallback":       true,
			"error":          replanErr.Error(),
			"candidateError": "replan context too large",
			"compactDigest":  digest,
		}),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if err := s.waitForUserAction(ctx, task.ID, "", "dynamic_replan_context_too_large", message, map[string]any{
		"error":         replanErr.Error(),
		"objective":     "Replanning paused because the prompt exceeded the model context window.",
		"compactDigest": digest,
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
	}
}

func latestCompactReplanDigest(results []WorkerTurnResult) string {
	if len(results) == 0 {
		return ""
	}
	result := DefaultReplanPromptBudgeter().compactWorkerResult(results[len(results)-1])
	parts := []string{
		"workerId=" + result.WorkerID,
		"status=" + string(result.Status),
		"kind=" + result.Kind,
	}
	if result.Role != "" {
		parts = append(parts, "role="+result.Role)
	}
	if result.Summary != "" {
		parts = append(parts, "summary="+truncateStringForPrompt(result.Summary, 1200))
	}
	if result.Error != "" {
		parts = append(parts, "error="+truncateStringForPrompt(result.Error, 1200))
	}
	if len(result.Changes.ChangedFiles) > 0 {
		files := []string{}
		for _, file := range result.Changes.ChangedFiles {
			files = append(files, file.Path+":"+file.Status)
		}
		parts = append(parts, "changedFiles="+strings.Join(files, ", "))
	}
	if result.Changes.DiffStat != "" {
		parts = append(parts, "diffStat="+result.Changes.DiffStat)
	}
	return strings.Join(parts, "\n")
}

func isReplanContextWindowError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"context window",
		"ran out of room",
		"too many tokens",
		"maximum context length",
		"context length exceeded",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func deterministicLocalNoChangeCompletionWorker(results []WorkerTurnResult) (string, string) {
	if len(candidateResults(results)) != 0 {
		return "", ""
	}
	successful := []WorkerTurnResult{}
	for _, result := range results {
		if result.Status == core.WorkerSucceeded {
			successful = append(successful, result)
		}
	}
	if len(successful) != 1 {
		return "", ""
	}
	return successful[0].WorkerID, "only successful worker and local completion does not require candidate changes"
}

func annotateWorkerSteeringPlan(plan Plan, item WorkerSteeringItem) Plan {
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["retryFromWorkerID"] = item.WorkerID
	plan.Metadata["retrySteering"] = dedupeTrimmedStrings(append(stringSliceMetadata(plan.Metadata, "retrySteering"), item.Message))
	plan.Metadata["retryContextKind"] = "worker_steering"
	plan.Metadata["steeredWorkerID"] = item.WorkerID
	if item.NodeID != "" {
		plan.Metadata["steeredNodeID"] = item.NodeID
	}
	if item.Role != "" {
		plan.Metadata["steeredWorkerRole"] = item.Role
	}
	if item.SpawnID != "" {
		plan.Metadata["steeredWorkerSpawnID"] = item.SpawnID
	}
	return plan
}

func forceConflictRepairPlan(task core.Task, plan Plan, blockedWorkerID string, repairReason string, blocked map[string]string) Plan {
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	originalPrompt := strings.TrimSpace(plan.Prompt)
	plan.Prompt = buildConflictRepairPrompt(task, blockedWorkerID, repairReason, sortedMapKeys(blocked), originalPrompt)
	plan.Rationale = nonEmpty(plan.Rationale, "Repair blocked final candidate so it applies cleanly.")
	plan.Metadata["baseWorkerID"] = blockedWorkerID
	plan.Metadata["allowBasePatchConflicts"] = true
	plan.Metadata["recoveryBaseWorkerID"] = blockedWorkerID
	plan.Metadata["recoveryHint"] = repairReason
	plan.Metadata["forcedConflictRepair"] = true
	plan.Metadata["workspaceReusePolicy"] = "fresh"
	return plan
}

func buildConflictRepairPrompt(task core.Task, blockedWorkerID string, repairReason string, blockedWorkerIDs []string, originalPrompt string) string {
	var builder strings.Builder
	builder.WriteString("# Conflict Repair Task\n\n")
	builder.WriteString("The previous final candidate could not be published or applied because its patch conflicts with the current checkout.\n\n")
	builder.WriteString("Blocked worker ID: ")
	builder.WriteString(blockedWorkerID)
	builder.WriteString("\n")
	if strings.TrimSpace(repairReason) != "" {
		builder.WriteString("Failure:\n")
		builder.WriteString(strings.TrimSpace(repairReason))
		builder.WriteString("\n\n")
	}
	if len(blockedWorkerIDs) > 0 {
		builder.WriteString("Already blocked final candidates:\n")
		for _, id := range blockedWorkerIDs {
			builder.WriteString("- ")
			builder.WriteString(id)
			builder.WriteString("\n")
		}
		builder.WriteString("\n")
	}
	builder.WriteString("Original task:\n")
	builder.WriteString(task.Title)
	builder.WriteString("\n\n")
	builder.WriteString(task.Prompt)
	builder.WriteString("\n\n")
	builder.WriteString("Your only job in this turn is to produce a new candidate that preserves the blocked worker's intended changes while resolving the conflicts against the current checkout. Do not run a review-only pass. Do not merely validate the old candidate. Make the code changes needed for the repaired candidate, remove conflict markers if any, and run the focused tests needed to show the repaired patch is applyable.\n")
	if originalPrompt != "" {
		builder.WriteString("\nScheduler's original recovery request, for context only:\n")
		builder.WriteString(originalPrompt)
		builder.WriteString("\n")
	}
	return builder.String()
}

type followUpNode struct {
	id    string
	index int
	spawn SpawnRequest
	deps  []string
}

type initialWorkerNode struct {
	id     string
	index  int
	worker WorkerRequest
	deps   []string
}

func (s *Service) runInitialWorkerGraph(ctx context.Context, task core.Task, plan Plan) ([]WorkerTurnResult, bool, error) {
	pending, err := initialWorkerNodes(plan.Workers)
	if err != nil {
		return nil, false, err
	}
	results := []WorkerTurnResult{}
	completed := map[string]WorkerTurnResult{}
	for len(pending) > 0 {
		ready := readyInitialWorkers(pending, completed)
		if len(ready) == 0 {
			return results, false, fmt.Errorf("initial worker dependency cycle or missing dependency")
		}
		waveResults, ok, err := s.runInitialWorkerWave(ctx, task, plan, ready, results)
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

func initialWorkerNodes(workers []WorkerRequest) (map[string]initialWorkerNode, error) {
	nodes := map[string]initialWorkerNode{}
	for index, worker := range workers {
		id := workerRequestID(worker, index)
		if _, ok := nodes[id]; ok {
			return nil, fmt.Errorf("duplicate worker id %q", id)
		}
		deps := make([]string, 0, len(worker.DependsOn))
		for _, dep := range worker.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep != "" {
				deps = append(deps, dep)
			}
		}
		nodes[id] = initialWorkerNode{id: id, index: index, worker: worker, deps: deps}
	}
	for _, node := range nodes {
		for _, dep := range node.deps {
			if _, ok := nodes[dep]; !ok {
				return nil, fmt.Errorf("worker %q depends on unknown worker %q", node.id, dep)
			}
			if dep == node.id {
				return nil, fmt.Errorf("worker %q depends on itself", node.id)
			}
		}
	}
	return nodes, nil
}

func readyInitialWorkers(pending map[string]initialWorkerNode, completed map[string]WorkerTurnResult) []initialWorkerNode {
	ready := []initialWorkerNode{}
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

func (s *Service) runInitialWorkerWave(ctx context.Context, task core.Task, initial Plan, nodes []initialWorkerNode, priorResults []WorkerTurnResult) ([]WorkerTurnResult, bool, error) {
	waveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type outcome struct {
		index  int
		plan   Plan
		result WorkerTurnResult
		err    error
	}
	outcomes := make(chan outcome, len(nodes))
	for index, node := range nodes {
		workerPlan := s.initialWorkerPlan(initial, node.worker, priorResults, node.index+1, node.id, node.deps)
		if workerPlan.Metadata == nil {
			workerPlan.Metadata = map[string]any{}
		}
		if stringMetadata(workerPlan.Metadata, "nodeID") == "" {
			workerPlan.Metadata["nodeID"] = uuid.NewString()
		}
		if stringMetadata(workerPlan.Metadata, "planID") == "" {
			workerPlan.Metadata["planID"] = uuid.NewString()
		}
		go func(index int, plan Plan) {
			result, err := s.runPlannedWorker(waveCtx, task, plan)
			outcomes <- outcome{index: index, plan: plan, result: result, err: err}
		}(index, workerPlan)
	}

	ordered := make([]WorkerTurnResult, len(nodes))
	plans := make([]Plan, len(nodes))
	for range nodes {
		outcome := <-outcomes
		plans[outcome.index] = outcome.plan
		if outcome.err != nil {
			ordered[outcome.index] = failedFollowUpResult(outcome.plan, outcome.err)
			continue
		}
		ordered[outcome.index] = outcome.result
	}
	allResults := append(append([]WorkerTurnResult{}, priorResults...), ordered...)
	for index, result := range ordered {
		if result.Status == core.WorkerCanceled {
			_ = s.setTaskStatus(ctx, task.ID, core.TaskCanceled)
			return ordered, false, nil
		}
		if result.Status == core.WorkerWaiting {
			s.handleWorkerQuestion(ctx, task, plans[index], allResults, result)
			return ordered, false, nil
		}
	}
	return ordered, true, nil
}

func (s *Service) initialWorkerPlan(initial Plan, worker WorkerRequest, results []WorkerTurnResult, turn int, workerID string, dependsOn []string) Plan {
	reasoningEffort := normalizeReasoningEffort(nonEmpty(worker.ReasoningEffort, initial.ReasoningEffort))
	role := nonEmpty(worker.Role, workerID)
	reason := nonEmpty(worker.Reason, "initial worker scheduled by the scheduler")
	metadata := copyPlanMetadata(initial.Metadata)
	metadata["initialWorker"] = true
	metadata["scheduledWorkerID"] = workerID
	metadata["spawnID"] = workerID
	metadata["spawnRole"] = role
	metadata["spawnReason"] = reason
	metadata["dependsOn"] = dependsOn
	metadata["turn"] = turn
	metadata["parentRationale"] = initial.Rationale
	if baseWorkerID := latestCandidateWorkerIDForDependencies(results, dependsOn); baseWorkerID != "" {
		metadata["baseWorkerID"] = baseWorkerID
	}
	if reasoningEffort != "" {
		metadata["reasoningEffort"] = reasoningEffort
	}
	return Plan{
		WorkerKind:      worker.WorkerKind,
		Prompt:          buildInitialWorkerPrompt(worker.Prompt, results, dependsOn),
		ReasoningEffort: reasoningEffort,
		Rationale:       "initial worker scheduled from plan: " + reason,
		Steps: []PlanStep{{
			Title:       "Run " + role,
			Description: reason,
		}},
		RequiredApprovals: []ApprovalRequest{},
		Spawns:            []SpawnRequest{},
		Metadata:          metadata,
	}
}

func buildInitialWorkerPrompt(prompt string, results []WorkerTurnResult, dependsOn []string) string {
	if len(dependsOn) == 0 {
		return prompt
	}
	deps := map[string]bool{}
	for _, dep := range dependsOn {
		deps[strings.TrimSpace(dep)] = true
	}
	var builder strings.Builder
	builder.WriteString("Dependency worker results:\n")
	for _, result := range results {
		if !deps[result.SpawnID] {
			continue
		}
		builder.WriteString("\n- ")
		builder.WriteString(nonEmpty(result.SpawnID, result.WorkerID))
		builder.WriteString(" status: ")
		builder.WriteString(string(result.Status))
		if result.Summary != "" {
			builder.WriteString("\n  Summary: ")
			builder.WriteString(result.Summary)
		}
		if result.Error != "" {
			builder.WriteString("\n  Error: ")
			builder.WriteString(result.Error)
		}
		if len(result.Changes.ChangedFiles) > 0 {
			builder.WriteString("\n  Changed files:")
			for _, file := range result.Changes.ChangedFiles {
				builder.WriteString("\n  - ")
				if file.Status != "" {
					builder.WriteString(file.Status)
					builder.WriteString(" ")
				}
				builder.WriteString(file.Path)
			}
		}
		builder.WriteString("\n")
	}
	builder.WriteString("\nWorker instructions:\n")
	builder.WriteString(prompt)
	return builder.String()
}

func latestCandidateWorkerIDForDependencies(results []WorkerTurnResult, dependsOn []string) string {
	if len(dependsOn) == 0 {
		return ""
	}
	deps := map[string]bool{}
	for _, dep := range dependsOn {
		deps[strings.TrimSpace(dep)] = true
	}
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		if deps[result.SpawnID] && result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
			return result.WorkerID
		}
	}
	return ""
}

func (s *Service) runFollowUpWorkers(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, parentNodeID string) ([]WorkerTurnResult, bool, error) {
	if len(initial.Spawns) == 0 {
		return results, true, nil
	}
	completed := completedWorkerDependencies(results)
	pending, err := followUpNodes(initial.Spawns, completed)
	if err != nil {
		return results, false, err
	}
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

func completedWorkerDependencies(results []WorkerTurnResult) map[string]WorkerTurnResult {
	completed := map[string]WorkerTurnResult{}
	for _, result := range results {
		if id := strings.TrimSpace(result.SpawnID); id != "" {
			completed[id] = result
		}
		if id := strings.TrimSpace(result.WorkerID); id != "" {
			completed[id] = result
		}
	}
	return completed
}

func followUpNodes(spawns []SpawnRequest, completed map[string]WorkerTurnResult) (map[string]followUpNode, error) {
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
				if _, ok := completed[dep]; !ok {
					return nil, fmt.Errorf("spawn %q depends on unknown spawn %q", node.id, dep)
				}
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
		plan   Plan
		result WorkerTurnResult
		err    error
	}
	outcomes := make(chan outcome, len(nodes))
	for index, node := range nodes {
		followUp := s.followUpPlan(task, initial, node.spawn, priorResults, node.index+2, node.id, node.deps, parentNodeID)
		if followUp.Metadata == nil {
			followUp.Metadata = map[string]any{}
		}
		if stringMetadata(followUp.Metadata, "nodeID") == "" {
			followUp.Metadata["nodeID"] = uuid.NewString()
		}
		if stringMetadata(followUp.Metadata, "planID") == "" {
			followUp.Metadata["planID"] = uuid.NewString()
		}
		if _, err := s.append(ctx, core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  task.ID,
			Payload: core.MustJSON(followUp),
		}); err != nil {
			return nil, false, err
		}
		go func(index int, plan Plan) {
			result, err := s.runPlannedWorker(waveCtx, task, plan)
			outcomes <- outcome{index: index, plan: plan, result: result, err: err}
		}(index, followUp)
	}

	ordered := make([]WorkerTurnResult, len(nodes))
	for range nodes {
		outcome := <-outcomes
		if outcome.err != nil {
			ordered[outcome.index] = failedFollowUpResult(outcome.plan, outcome.err)
			continue
		}
		ordered[outcome.index] = outcome.result
	}
	for _, result := range ordered {
		if result.Status == core.WorkerCanceled {
			_ = s.setTaskStatus(ctx, task.ID, core.TaskCanceled)
			return ordered, false, nil
		}
	}
	return ordered, true, nil
}

func failedFollowUpResult(plan Plan, err error) WorkerTurnResult {
	result := WorkerTurnResult{
		NodeID:       stringMetadata(plan.Metadata, "nodeID"),
		Status:       core.WorkerFailed,
		Kind:         plan.WorkerKind,
		Role:         stringMetadata(plan.Metadata, "spawnRole"),
		SpawnID:      stringMetadata(plan.Metadata, "spawnID"),
		BaseWorkerID: stringMetadata(plan.Metadata, "baseWorkerID"),
		Summary:      "Follow-up worker setup failed before execution.",
		Error:        err.Error(),
	}
	return result
}

func (s *Service) followUpPlan(task core.Task, initial Plan, spawn SpawnRequest, results []WorkerTurnResult, turn int, spawnID string, dependsOn []string, parentNodeID string) Plan {
	workerKind := s.workerKindForSpawn(spawn, initial.WorkerKind)
	prompt := buildFollowUpPrompt(task, spawn, results)
	reasoningEffort := normalizeReasoningEffort(nonEmpty(spawn.ReasoningEffort, initial.ReasoningEffort))
	baseWorkerID := latestCandidateWorkerID(results)
	plan := Plan{
		WorkerKind:      workerKind,
		Prompt:          prompt,
		ReasoningEffort: reasoningEffort,
		Rationale:       "follow-up worker scheduled from initial plan: " + spawn.Reason,
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
	if baseWorkerID != "" {
		plan.Metadata["baseWorkerID"] = baseWorkerID
	}
	if reasoningEffort != "" {
		plan.Metadata["reasoningEffort"] = reasoningEffort
	}
	return plan
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

func artifactMetadataString(raw json.RawMessage, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return ""
	}
	return strings.TrimSpace(stringMetadataValue(metadata[key]))
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

func (s *Service) projectForTask(task core.Task) (core.Project, error) {
	if s.projects == nil {
		if projectID := explicitProjectIDForTask(task); projectID != "" {
			return core.Project{}, fmt.Errorf("unknown projectId %q", projectID)
		}
		return core.Project{ID: "default", Name: "default", LocalPath: s.workDir, VCS: "auto", DefaultBase: "main"}, nil
	}
	if projectID := explicitProjectIDForTask(task); projectID != "" {
		if project, ok := s.projects.Get(projectID); ok {
			return project, nil
		}
		return core.Project{}, fmt.Errorf("unknown projectId %q", projectID)
	}
	if len(task.Metadata) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(task.Metadata, &metadata); err == nil {
			if repo := stringMetadataValue(metadata["repo"]); repo != "" {
				if project, ok := s.projects.findByMetadataRepo(metadata, repo); ok {
					return project, nil
				}
			}
		}
	}
	return s.projects.Default(), nil
}

func explicitProjectIDForTask(task core.Task) string {
	if projectID := strings.TrimSpace(task.ProjectID); projectID != "" {
		return projectID
	}
	if len(task.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return ""
	}
	return strings.TrimSpace(stringMetadataValue(metadata["projectId"]))
}

func projectCloneURL(project core.Project) string {
	repo := strings.TrimSpace(nonEmpty(project.UpstreamRepo, project.Repo))
	if repo == "" {
		return ""
	}
	if strings.Contains(repo, "://") || strings.HasPrefix(repo, "git@") || strings.HasSuffix(repo, ".git") {
		return repo
	}
	if strings.Count(repo, "/") == 1 {
		return "https://github.com/" + repo + ".git"
	}
	return repo
}

func projectWorkspaceBaseRevision(ctx context.Context, project core.Project) string {
	base := strings.TrimSpace(project.DefaultBase)
	if base == "" || strings.TrimSpace(project.LocalPath) == "" {
		return ""
	}
	if strings.HasPrefix(base, "refs/") || gitCommitRefExists(ctx, project.LocalPath, base) && !looksLikeBranchName(base) {
		return base
	}
	for _, ref := range []string{
		"refs/remotes/upstream/" + base,
		"refs/remotes/origin/" + base,
		"refs/heads/" + base,
		base,
	} {
		if gitCommitRefExists(ctx, project.LocalPath, ref) {
			return ref
		}
	}
	return ""
}

func projectWorkspaceRefRevision(ctx context.Context, project core.Project, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" || strings.TrimSpace(project.LocalPath) == "" {
		return ""
	}
	if strings.HasPrefix(ref, "refs/") {
		if gitCommitRefExists(ctx, project.LocalPath, ref) {
			return ref
		}
		return ""
	}
	branch := pullRequestWorkspaceBranch(ref)
	for _, remote := range projectFetchRemotes(project) {
		remoteRef := "refs/remotes/" + remote + "/" + branch
		if gitCommitRefExists(ctx, project.LocalPath, remoteRef) {
			return remoteRef
		}
	}
	for _, candidate := range []string{
		"refs/heads/" + branch,
		branch,
		ref,
	} {
		if gitCommitRefExists(ctx, project.LocalPath, candidate) {
			return candidate
		}
	}
	return ""
}

func syncedProjectWorkspaceBaseRevision(ctx context.Context, project core.Project) (string, error) {
	base := strings.TrimSpace(project.DefaultBase)
	if base == "" || strings.TrimSpace(project.LocalPath) == "" {
		return "", nil
	}
	if strings.HasPrefix(base, "refs/") || gitCommitRefExists(ctx, project.LocalPath, base) && !looksLikeBranchName(base) {
		return base, nil
	}
	if _, err := runCommand(ctx, project.LocalPath, "git", "rev-parse", "--show-toplevel"); err != nil {
		return projectWorkspaceBaseRevision(ctx, project), nil
	}
	ref, err := syncGitProjectBaseBranch(ctx, project.LocalPath, base)
	if err != nil {
		return "", err
	}
	return ref, nil
}

func syncedProjectWorkspaceRefRevision(ctx context.Context, project core.Project, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" || strings.TrimSpace(project.LocalPath) == "" {
		return "", nil
	}
	if strings.HasPrefix(ref, "refs/") || looksLikeHexObjectID(ref) {
		if gitCommitRefExists(ctx, project.LocalPath, ref) {
			return ref, nil
		}
	}
	if _, err := runCommand(ctx, project.LocalPath, "git", "rev-parse", "--show-toplevel"); err != nil {
		return projectWorkspaceRefRevision(ctx, project, ref), nil
	}
	branch := pullRequestWorkspaceBranch(ref)
	if branch == "" {
		return "", nil
	}
	var lastErr error
	for _, remote := range projectFetchRemotes(project) {
		remoteRef := "refs/remotes/" + remote + "/" + branch
		refspec := "+refs/heads/" + branch + ":" + remoteRef
		if _, err := runCommand(ctx, project.LocalPath, "git", "fetch", "--prune", remote, refspec); err != nil {
			lastErr = err
			continue
		}
		if gitCommitRefExists(ctx, project.LocalPath, remoteRef) {
			return remoteRef, nil
		}
	}
	if existing := projectWorkspaceRefRevision(ctx, project, ref); existing != "" {
		return existing, nil
	}
	if lastErr != nil {
		return "", fmt.Errorf("sync git branch %q: %w", branch, lastErr)
	}
	return "", fmt.Errorf("sync git branch %q: no configured remote contains branch", branch)
}

func syncGitProjectBaseBranch(ctx context.Context, dir string, base string) (string, error) {
	remote := ""
	branch := base
	if gitRemoteExists(ctx, dir, "upstream") {
		remote = "upstream"
	} else if gitCommitRefExists(ctx, dir, "refs/heads/"+base) {
		upstreamRemote, upstreamBranch, err := gitBranchUpstream(ctx, dir, base)
		if err != nil {
			if !errors.Is(err, errGitBranchUpstreamNotConfigured) {
				return "", fmt.Errorf("sync git base branch %q: %w", base, err)
			}
			if !gitCommitRefExists(ctx, dir, "refs/remotes/origin/"+base) {
				return "", fmt.Errorf("sync git base branch %q: %w", base, err)
			}
			remote = "origin"
		} else {
			remote = upstreamRemote
			branch = upstreamBranch
		}
	} else if gitRemoteExists(ctx, dir, "origin") {
		remote = "origin"
	}
	if strings.TrimSpace(remote) == "" || strings.TrimSpace(branch) == "" {
		return "", fmt.Errorf("sync git base branch %q: upstream tracking branch is not configured", base)
	}
	remoteRef := "refs/remotes/" + remote + "/" + branch
	refspec := "+refs/heads/" + branch + ":" + remoteRef
	if _, err := runCommand(ctx, dir, "git", "fetch", "--prune", remote, refspec); err != nil {
		return "", fmt.Errorf("fetch git base branch %q from %s: %w", base, remote, err)
	}
	if !gitCommitRefExists(ctx, dir, remoteRef) {
		return "", fmt.Errorf("sync git base branch %q: fetched ref %s is not a commit", base, remoteRef)
	}
	return remoteRef, nil
}

func gitRemoteExists(ctx context.Context, dir string, remote string) bool {
	if strings.TrimSpace(remote) == "" {
		return false
	}
	_, err := runCommand(ctx, dir, "git", "remote", "get-url", remote)
	return err == nil
}

var errGitBranchUpstreamNotConfigured = errors.New("upstream tracking branch is not configured")

func gitBranchUpstream(ctx context.Context, dir string, branch string) (string, string, error) {
	remote, err := runCommand(ctx, dir, "git", "config", "--get", "branch."+branch+".remote")
	if err != nil {
		return "", "", errGitBranchUpstreamNotConfigured
	}
	merge, err := runCommand(ctx, dir, "git", "config", "--get", "branch."+branch+".merge")
	if err != nil {
		return "", "", errGitBranchUpstreamNotConfigured
	}
	remote = strings.TrimSpace(remote)
	merge = strings.TrimSpace(merge)
	if remote == "" || remote == "." || merge == "" {
		return "", "", errGitBranchUpstreamNotConfigured
	}
	upstreamBranch := strings.TrimPrefix(merge, "refs/heads/")
	if upstreamBranch == "" || upstreamBranch == merge {
		return "", "", fmt.Errorf("unsupported upstream merge ref %q", merge)
	}
	return remote, upstreamBranch, nil
}

func projectWorkspaceBaseCommit(ctx context.Context, project core.Project) string {
	ref := projectWorkspaceBaseRevision(ctx, project)
	if ref == "" {
		return ""
	}
	return projectWorkspaceRefCommit(ctx, project, ref)
}

func projectWorkspaceRefCommit(ctx context.Context, project core.Project, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	out, err := runCommand(ctx, project.LocalPath, "git", "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func gitCommitRefExists(ctx context.Context, dir string, ref string) bool {
	if strings.TrimSpace(dir) == "" || strings.TrimSpace(ref) == "" {
		return false
	}
	_, err := runCommand(ctx, dir, "git", "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	return err == nil
}

func looksLikeBranchName(ref string) bool {
	ref = strings.TrimSpace(ref)
	return ref != "" && !strings.Contains(ref, "/") && !looksLikeHexObjectID(ref)
}

func looksLikeHexObjectID(ref string) bool {
	if len(ref) < 7 || len(ref) > 40 {
		return false
	}
	for _, char := range ref {
		if (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F') {
			continue
		}
		return false
	}
	return true
}

func pullRequestWorkspaceBranch(ref string) string {
	ref = strings.TrimSpace(ref)
	if _, branch, ok := strings.Cut(ref, ":"); ok {
		ref = branch
	}
	return strings.TrimSpace(strings.TrimPrefix(ref, "refs/heads/"))
}

func projectFetchRemotes(project core.Project) []string {
	return uniqueNonEmptyStrings([]string{
		project.PushRemote,
		"origin",
		"upstream",
	})
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]bool{}
	result := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func createTaskMetadata(req core.CreateTaskRequest) (map[string]any, error) {
	metadata, err := createTaskMetadataMap(req.Metadata)
	if err != nil {
		return nil, err
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
	if req.WorkstreamID != "" {
		metadata["workstreamId"] = strings.TrimSpace(req.WorkstreamID)
	}
	return metadata, nil
}

func NormalizeCreateTaskRequest(req core.CreateTaskRequest) (core.CreateTaskRequest, error) {
	metadata, err := createTaskMetadataMap(req.Metadata)
	if err != nil {
		return core.CreateTaskRequest{}, err
	}
	ensureDefaultTaskCompletionMode(metadata, req.Prompt)
	req.Metadata = core.MustJSON(metadata)
	return req, nil
}

func createTaskMetadataMap(raw json.RawMessage) (map[string]any, error) {
	metadata := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, fmt.Errorf("metadata must be a JSON object: %w", err)
		}
		if metadata == nil {
			metadata = map[string]any{}
		}
	}
	return metadata, nil
}

func ensureDefaultTaskCompletionMode(metadata map[string]any, prompt string) {
	if taskMetadataExecutionMode(metadata) == executionModeLoop {
		return
	}
	if stringMetadataValue(metadata["completionMode"]) == "" {
		if promptRequestsLocalCompletion(prompt) {
			metadata["completionMode"] = "local"
			metadata["completionModeInferred"] = true
		} else {
			metadata["completionMode"] = "github"
			if promptRequestsGitHubCompletion(prompt) {
				metadata["completionModeInferred"] = true
			}
		}
	}
}

func taskMetadataExecutionMode(metadata map[string]any) string {
	switch strings.ToLower(strings.TrimSpace(stringMetadataValue(metadata["executionMode"]))) {
	case "loop", "durable_loop", "agent_loop":
		return executionModeLoop
	default:
		return "orchestrated"
	}
}

func promptRequestsLocalCompletion(prompt string) bool {
	lower := strings.ToLower(prompt)
	return strings.Contains(lower, "local-only") ||
		strings.Contains(lower, "local only") ||
		strings.Contains(lower, "local completion") ||
		strings.Contains(lower, "complete locally") ||
		strings.Contains(lower, "without a pr") ||
		strings.Contains(lower, "without pr") ||
		strings.Contains(lower, "do not open a pr") ||
		strings.Contains(lower, "do not open pr") ||
		strings.Contains(lower, "don't open a pr") ||
		strings.Contains(lower, "don't open pr") ||
		standaloneNoPRPattern.MatchString(lower)
}

func promptRequestsGitHubCompletion(prompt string) bool {
	lower := strings.ToLower(prompt)
	return strings.Contains(lower, "open pr") ||
		strings.Contains(lower, "open a pr") ||
		strings.Contains(lower, "open pull request") ||
		strings.Contains(lower, "open a pull request") ||
		strings.Contains(lower, "create pr") ||
		strings.Contains(lower, "create a pr") ||
		strings.Contains(lower, "create pull request") ||
		strings.Contains(lower, "make sure it gets merged") ||
		strings.Contains(lower, "until merged") ||
		strings.Contains(lower, "babysit")
}

func taskTargetLabels(task core.Task) map[string]string {
	if len(task.Metadata) == 0 {
		return nil
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return nil
	}
	return targetLabels(metadata)
}

func taskTargetRequirements(task core.Task) core.ProjectRequirements {
	if len(task.Metadata) == 0 {
		return core.ProjectRequirements{}
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return core.ProjectRequirements{}
	}
	return targetRequirements(metadata)
}

func taskRequiredTargetID(task core.Task) string {
	if len(task.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return ""
	}
	return requiredTargetID(metadata)
}

func applyRequirementsMetadata(metadata map[string]any, requirements core.ProjectRequirements) {
	if requirements.MemoryMB > 0 {
		metadata["requiredMemoryMB"] = requirements.MemoryMB
	} else {
		delete(metadata, "requiredMemoryMB")
	}
	if requirements.StorageMB > 0 {
		metadata["requiredStorageMB"] = requirements.StorageMB
	} else {
		delete(metadata, "requiredStorageMB")
	}
	delete(metadata, "requiredDiskMB")
}

func hasRequirements(requirements core.ProjectRequirements) bool {
	return requirements.MemoryMB > 0 || requirements.StorageMB > 0
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

func findWorker(snapshot core.Snapshot, workerID string) (core.Worker, bool) {
	for _, worker := range snapshot.Workers {
		if worker.ID == workerID {
			return worker, true
		}
	}
	return core.Worker{}, false
}

func executionNodeForWorker(snapshot core.Snapshot, workerID string) core.ExecutionNode {
	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID == workerID {
			return node
		}
	}
	return core.ExecutionNode{}
}

func taskSteering(snapshot core.Snapshot, taskID string) []string {
	var out []string
	seen := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskSteered {
			continue
		}
		var payload struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			message := strings.TrimSpace(payload.Message)
			if message != "" && !seen[message] {
				seen[message] = true
				out = append(out, message)
			}
		}
	}
	return out
}

func (s *Service) taskSteering(ctx context.Context, taskID string) []string {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	return taskSteering(snapshot, taskID)
}

func dedupeTrimmedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func latestTaskSteering(snapshot core.Snapshot, taskID string) string {
	steering := taskSteering(snapshot, taskID)
	if len(steering) == 0 {
		return ""
	}
	return steering[len(steering)-1]
}

func pendingWorkerSteering(snapshot core.Snapshot, taskID string) []WorkerSteeringItem {
	latestPlanned := int64(0)
	workers := map[string]core.Worker{}
	nodes := map[string]core.ExecutionNode{}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID {
			workers[worker.ID] = worker
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.WorkerID != "" {
			nodes[node.WorkerID] = node
		}
	}
	for _, event := range snapshot.Events {
		if event.TaskID == taskID && event.Type == core.EventTaskPlanned && event.ID > latestPlanned {
			latestPlanned = event.ID
		}
	}
	var items []WorkerSteeringItem
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventWorkerSteered || event.ID <= latestPlanned {
			continue
		}
		var payload struct {
			WorkerID   string `json:"workerId"`
			NodeID     string `json:"nodeId"`
			WorkerKind string `json:"workerKind"`
			Role       string `json:"role"`
			SpawnID    string `json:"spawnId"`
			Status     string `json:"status"`
			Reason     string `json:"reason"`
			Message    string `json:"message"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		payload.WorkerID = nonEmpty(payload.WorkerID, event.WorkerID)
		payload.Message = strings.TrimSpace(payload.Message)
		if payload.WorkerID == "" || payload.Message == "" {
			continue
		}
		if worker, ok := workers[payload.WorkerID]; ok {
			payload.WorkerKind = nonEmpty(payload.WorkerKind, worker.Kind)
		}
		if node, ok := nodes[payload.WorkerID]; ok {
			payload.NodeID = nonEmpty(payload.NodeID, node.ID)
			payload.WorkerKind = nonEmpty(payload.WorkerKind, node.WorkerKind)
			payload.Role = nonEmpty(payload.Role, node.Role)
			payload.SpawnID = nonEmpty(payload.SpawnID, node.SpawnID)
		}
		items = append(items, WorkerSteeringItem{
			EventID:    event.ID,
			WorkerID:   payload.WorkerID,
			NodeID:     payload.NodeID,
			WorkerKind: payload.WorkerKind,
			Role:       payload.Role,
			SpawnID:    payload.SpawnID,
			Status:     payload.Status,
			Reason:     payload.Reason,
			Message:    payload.Message,
		})
	}
	return items
}

func (s *Service) pendingWorkerSteering(ctx context.Context, taskID string) []WorkerSteeringItem {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	return pendingWorkerSteering(snapshot, taskID)
}

func firstPendingWorkerSteering(snapshot core.Snapshot, taskID string) (WorkerSteeringItem, bool) {
	items := pendingWorkerSteering(snapshot, taskID)
	if len(items) == 0 {
		return WorkerSteeringItem{}, false
	}
	return items[0], true
}

func (s *Service) firstPendingWorkerSteering(ctx context.Context, taskID string) (WorkerSteeringItem, bool) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return WorkerSteeringItem{}, false
	}
	return firstPendingWorkerSteering(snapshot, taskID)
}

func workerSteeringAlreadyPending(snapshot core.Snapshot, taskID string, workerID string, message string) bool {
	message = strings.TrimSpace(message)
	for _, item := range pendingWorkerSteering(snapshot, taskID) {
		if item.WorkerID == workerID && strings.TrimSpace(item.Message) == message {
			return true
		}
	}
	return false
}

func retryPlanForTask(snapshot core.Snapshot, taskID string) (Plan, error) {
	var plans []Plan
	var workerIDs []string
	terminalWorkerID := ""
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskPlanned:
			var plan Plan
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				return Plan{}, fmt.Errorf("decode task plan: %w", err)
			}
			plans = append(plans, plan)
		case core.EventExecutionPlanned:
			var payload struct {
				WorkerID string `json:"workerId"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, fmt.Errorf("decode execution plan: %w", err)
			}
			workerIDs = append(workerIDs, nonEmpty(payload.WorkerID, event.WorkerID))
		case core.EventWorkerCompleted:
			var payload struct {
				Status core.WorkerStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, fmt.Errorf("decode worker completion: %w", err)
			}
			if payload.Status == core.WorkerFailed || payload.Status == core.WorkerCanceled {
				terminalWorkerID = event.WorkerID
			}
		}
	}
	if len(plans) == 0 {
		return Plan{}, errors.New("task has no persisted plan to retry")
	}
	if terminalWorkerID != "" {
		for i := len(workerIDs) - 1; i >= 0; i-- {
			if workerIDs[i] == terminalWorkerID && i < len(plans) {
				return retryPlanWithResume(snapshot, plans[i], taskID, terminalWorkerID), nil
			}
		}
	}
	return retryPlanWithResume(snapshot, plans[len(plans)-1], taskID, terminalWorkerID), nil
}

func taskFailedDuringDynamicReplan(snapshot core.Snapshot, taskID string) bool {
	return latestTaskFailureMatches(snapshot, taskID, func(errorText string) bool {
		return strings.Contains(errorText, "dynamic replan")
	})
}

func retryPullRequestFollowUpPlan(snapshot core.Snapshot, taskID string) (Plan, bool, error) {
	latestFollowUpID, ok := latestOpenPullRequestFollowUpEvent(snapshot, taskID)
	if !ok {
		return Plan{}, false, nil
	}
	var plan Plan
	havePlan := false
	terminalWorkerID := ""
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.ID <= latestFollowUpID {
			continue
		}
		switch event.Type {
		case core.EventTaskPlanned:
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				return Plan{}, false, fmt.Errorf("decode pull request follow-up plan: %w", err)
			}
			havePlan = true
		case core.EventWorkerCompleted:
			var payload struct {
				Status core.WorkerStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, false, fmt.Errorf("decode pull request follow-up worker completion: %w", err)
			}
			if payload.Status == core.WorkerFailed || payload.Status == core.WorkerCanceled {
				terminalWorkerID = event.WorkerID
			}
		}
	}
	if !havePlan {
		return Plan{}, false, nil
	}
	return retryPlanWithResume(snapshot, plan, taskID, terminalWorkerID), true, nil
}

func latestOpenPullRequestFollowUpEvent(snapshot core.Snapshot, taskID string) (int64, bool) {
	latestFollowUp := int64(0)
	latestPullRequestID := ""
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventPRFollowUp {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
			continue
		}
		if event.ID >= latestFollowUp {
			latestFollowUp = event.ID
			latestPullRequestID = strings.TrimSpace(payload.ID)
		}
	}
	if latestPullRequestID == "" {
		return 0, false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == latestPullRequestID && !isTerminalPullRequestState(pr.State) {
			return latestFollowUp, true
		}
	}
	return 0, false
}

func taskFailureRecoverableFromGraph(snapshot core.Snapshot, taskID string, results []WorkerTurnResult) bool {
	if len(candidateResults(results)) == 0 {
		return false
	}
	if latestTaskFailureMatches(snapshot, taskID, func(errorText string) bool {
		return errorText == "" ||
			strings.Contains(errorText, "dynamic replan") ||
			strings.Contains(errorText, "final candidate") ||
			strings.Contains(errorText, "selected final candidate") ||
			strings.Contains(errorText, "multiple competing final candidates") ||
			strings.Contains(errorText, "depends on unknown spawn") ||
			strings.Contains(errorText, "depends on unknown worker") ||
			strings.Contains(errorText, "worker command failed")
	}) {
		return true
	}
	latest := latestWorkerResult(results)
	return latest.Status == core.WorkerFailed || latest.Status == core.WorkerCanceled
}

func latestTaskFailureMatches(snapshot core.Snapshot, taskID string, match func(string) bool) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.Type != core.EventTaskStatus {
			continue
		}
		var payload struct {
			Status core.TaskStatus `json:"status"`
			Error  string          `json:"error"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false
		}
		return payload.Status == core.TaskFailed && match(strings.ToLower(strings.TrimSpace(payload.Error)))
	}
	return false
}

func latestWorkerResult(results []WorkerTurnResult) WorkerTurnResult {
	if len(results) == 0 {
		return WorkerTurnResult{}
	}
	return results[len(results)-1]
}

func firstWorkerResultWithStatus(results []WorkerTurnResult, status core.WorkerStatus) WorkerTurnResult {
	for _, result := range results {
		if result.Status == status {
			return result
		}
	}
	return WorkerTurnResult{}
}

func retryGraphStateForTask(snapshot core.Snapshot, taskID string) (Plan, []WorkerTurnResult, error) {
	var initial Plan
	haveInitial := false
	workerMetadata := map[string]map[string]any{}
	results := []WorkerTurnResult{}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskPlanned:
			var plan Plan
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				return Plan{}, nil, fmt.Errorf("decode task plan: %w", err)
			}
			if !haveInitial {
				initial = plan
				haveInitial = true
			}
		case core.EventWorkerCreated:
			var payload struct {
				Kind     string         `json:"kind"`
				Metadata map[string]any `json:"metadata"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, nil, fmt.Errorf("decode worker created: %w", err)
			}
			metadata := map[string]any{}
			for key, value := range payload.Metadata {
				metadata[key] = value
			}
			if payload.Kind != "" {
				metadata["workerKind"] = payload.Kind
			}
			workerMetadata[event.WorkerID] = metadata
		case core.EventWorkerCompleted:
			var payload struct {
				Status           core.WorkerStatus `json:"status"`
				Summary          string            `json:"summary"`
				Error            string            `json:"error"`
				WorkspaceChanges WorkspaceChanges  `json:"workspaceChanges"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, nil, fmt.Errorf("decode worker completion: %w", err)
			}
			metadata := workerMetadata[event.WorkerID]
			result := WorkerTurnResult{
				WorkerID: event.WorkerID,
				Status:   payload.Status,
				Kind:     stringMetadata(metadata, "workerKind"),
				Summary:  payload.Summary,
				Error:    payload.Error,
				Changes:  payload.WorkspaceChanges,
			}
			result.NodeID = stringMetadata(metadata, "nodeID")
			result.Role = stringMetadata(metadata, "spawnRole")
			result.SpawnID = stringMetadata(metadata, "spawnID")
			result.BaseWorkerID = stringMetadata(metadata, "baseWorkerID")
			results = append(results, result)
		}
	}
	if !haveInitial {
		return Plan{}, nil, errors.New("task has no persisted initial plan to retry graph")
	}
	if len(results) == 0 {
		return Plan{}, nil, errors.New("task has no completed worker results to retry graph")
	}
	return initial, results, nil
}

func retryPlanWithResume(snapshot core.Snapshot, plan Plan, taskID string, workerID string) Plan {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return plan
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["retryFromWorkerID"] = workerID
	if steering := taskSteering(snapshot, taskID); len(steering) > 0 {
		plan.Metadata["retrySteering"] = steering
	}
	if execution := workerExecutionInfo(snapshot, workerID); len(execution) > 0 {
		if targetID := stringMetadataValue(execution["targetId"]); targetID != "" {
			plan.Metadata["retryTargetID"] = targetID
		}
		if targetKind := stringMetadataValue(execution["targetKind"]); targetKind != "" {
			plan.Metadata["retryTargetKind"] = targetKind
		}
		if remoteSession := stringMetadataValue(execution["remoteSession"]); remoteSession != "" {
			plan.Metadata["retryRemoteSession"] = remoteSession
		}
		if remoteRunDir := stringMetadataValue(execution["remoteRunDir"]); remoteRunDir != "" {
			plan.Metadata["retryRemoteRunDir"] = remoteRunDir
		}
		if remoteWorkDir := stringMetadataValue(execution["remoteWorkDir"]); remoteWorkDir != "" {
			plan.Metadata["retryRemoteWorkDir"] = remoteWorkDir
		}
	}
	if sessionID := workerProviderSessionID(snapshot, workerID, plan.WorkerKind); sessionID != "" {
		plan.Metadata["retryResumeSessionID"] = sessionID
	}
	return plan
}

func workerProviderSessionID(snapshot core.Snapshot, workerID string, kind string) string {
	for _, event := range snapshot.Events {
		if event.WorkerID != workerID || event.Type != core.EventWorkerOutput {
			continue
		}
		var payload struct {
			Raw json.RawMessage `json:"raw"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || len(payload.Raw) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(payload.Raw, &raw); err != nil {
			continue
		}
		switch kind {
		case "codex":
			if raw["type"] == "thread.started" {
				if sessionID := stringMetadataValue(raw["thread_id"]); sessionID != "" {
					return sessionID
				}
			}
		case "claude":
			if raw["type"] == "system" && raw["subtype"] == "init" {
				if sessionID := stringMetadataValue(raw["session_id"]); sessionID != "" {
					return sessionID
				}
			}
		}
	}
	return ""
}

func workerExecutionInfo(snapshot core.Snapshot, workerID string) map[string]any {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.WorkerID != workerID || event.Type != core.EventExecutionPlanned {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			return payload
		}
	}
	return nil
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

func (s *Service) describeWorkspaceChangesForCompletion(ctx context.Context, workspace PreparedWorkspace) WorkspaceChanges {
	changes := s.describeWorkspaceChanges(ctx, workspace)
	if changes.Error != "" {
		return changes
	}
	diff, err := s.describeWorkspaceDiff(ctx, workspace)
	if err != nil {
		changes.Error = err.Error()
		return changes
	}
	changes.Diff = strings.TrimSpace(diff)
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

func (s *Service) retryWorkspace(ctx context.Context, taskID string, newWorkerID string, previousWorkerID string) (PreparedWorkspace, bool, error) {
	previousWorkerID = strings.TrimSpace(previousWorkerID)
	if previousWorkerID == "" {
		return PreparedWorkspace{}, false, nil
	}
	workspace, err := s.workspaceForWorker(ctx, previousWorkerID)
	if err != nil {
		return PreparedWorkspace{}, false, fmt.Errorf("load retry workspace for %s: %w", previousWorkerID, err)
	}
	cwd := strings.TrimSpace(workspace.CWD)
	if cwd == "" {
		return PreparedWorkspace{}, false, fmt.Errorf("retry workspace for %s has no cwd", previousWorkerID)
	}
	info, err := os.Stat(cwd)
	if err != nil {
		return PreparedWorkspace{}, false, fmt.Errorf("retry workspace %s is not available: %w", cwd, err)
	}
	if !info.IsDir() {
		return PreparedWorkspace{}, false, fmt.Errorf("retry workspace %s is not a directory", cwd)
	}
	workspace.TaskID = taskID
	workspace.WorkerID = newWorkerID
	return workspace, true, nil
}

func (s *Service) baseWorkspaceSpec(ctx context.Context, spec WorkspaceSpec, baseWorkerID string) (WorkspaceSpec, error) {
	base, err := s.workspaceForWorker(ctx, baseWorkerID)
	if err != nil {
		return spec, fmt.Errorf("load base workspace for %s: %w", baseWorkerID, err)
	}
	if strings.TrimSpace(base.CWD) == "" {
		return spec, fmt.Errorf("base workspace for %s has no cwd", baseWorkerID)
	}
	if base.VCSType == "ssh" {
		return spec, nil
	}
	spec.BaseWorkDir = base.CWD
	if summary, err := s.workerCompletionSummary(ctx, baseWorkerID); err == nil {
		spec.WorkerSummary = summary
	}
	switch base.VCSType {
	case "jj":
		if strings.TrimSpace(base.WorkspaceName) != "" {
			spec.BaseRevision = base.WorkspaceName + "@"
		}
	case "git":
		if strings.TrimSpace(base.BaseChange) != "" {
			spec.BaseRevision = base.BaseChange
		}
	}
	return spec, nil
}

func (s *Service) workerCompletionSummary(ctx context.Context, workerID string) (string, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerCompleted || event.WorkerID != workerID {
			continue
		}
		var payload struct {
			Summary string `json:"summary"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return "", fmt.Errorf("decode worker completion: %w", err)
		}
		if strings.TrimSpace(payload.Summary) == "" {
			return "", eventstore.ErrNotFound
		}
		return payload.Summary, nil
	}
	return "", eventstore.ErrNotFound
}

func (s *Service) workerRanOnTarget(ctx context.Context, workerID string, targetID string) (bool, error) {
	workerID = strings.TrimSpace(workerID)
	targetID = strings.TrimSpace(targetID)
	if workerID == "" || targetID == "" {
		return false, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventExecutionPlanned || event.WorkerID != workerID {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false, err
		}
		return stringMetadataValue(payload["targetId"]) == targetID || stringMetadataValue(payload["targetID"]) == targetID, nil
	}
	return false, nil
}

func (s *Service) workerHandoffPatch(ctx context.Context, workerID string) (string, WorkspaceChanges, error) {
	changes, err := s.completedWorkspaceChanges(ctx, workerID)
	if err != nil {
		return "", WorkspaceChanges{}, err
	}
	if strings.TrimSpace(changes.Diff) != "" {
		return changes.Diff, changes, nil
	}
	if len(changes.ChangedFiles) == 0 && !changes.Dirty {
		return "", changes, nil
	}
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return "", changes, fmt.Errorf("base worker %s has changes but no persisted patch: %w", workerID, err)
	}
	if workspace.VCSType == "ssh" {
		return "", changes, fmt.Errorf("base worker %s has remote changes but no persisted diff.patch", workerID)
	}
	diff, err := s.describeWorkspaceDiff(ctx, workspace)
	if err != nil {
		return "", changes, fmt.Errorf("read base worker patch for %s: %w", workerID, err)
	}
	changes.Diff = strings.TrimSpace(diff)
	if changes.Diff == "" {
		return "", changes, fmt.Errorf("base worker %s has changes but generated an empty patch", workerID)
	}
	return changes.Diff, changes, nil
}

func (s *Service) remoteRetryWorkDir(ctx context.Context, target TargetConfig, previousWorkerID string) (string, error) {
	workspace, err := s.workspaceForWorker(ctx, previousWorkerID)
	if err != nil {
		return "", fmt.Errorf("load retry workspace for %s: %w", previousWorkerID, err)
	}
	cwd := strings.TrimSpace(workspace.CWD)
	if cwd == "" {
		return "", fmt.Errorf("retry workspace for %s has no cwd", previousWorkerID)
	}
	ok, err := s.sshRunner.DirectoryExists(ctx, target, cwd)
	if err != nil {
		return "", fmt.Errorf("check retry workspace %s: %w", cwd, err)
	}
	if !ok {
		return "", fmt.Errorf("retry workspace %s is not available", cwd)
	}
	return cwd, nil
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
		if strings.TrimSpace(workspace.TaskID) == "" {
			workspace.TaskID = event.TaskID
		}
		if strings.TrimSpace(workspace.WorkerID) == "" {
			workspace.WorkerID = event.WorkerID
		}
		return workspace, nil
	}
	return PreparedWorkspace{}, eventstore.ErrNotFound
}

func (s *Service) completedWorkspaceChanges(ctx context.Context, workerID string) (WorkspaceChanges, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return WorkspaceChanges{}, err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerCompleted || event.WorkerID != workerID {
			continue
		}
		var payload struct {
			WorkspaceChanges WorkspaceChanges `json:"workspaceChanges"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return WorkspaceChanges{}, fmt.Errorf("decode worker completion: %w", err)
		}
		if payload.WorkspaceChanges.Root == "" && payload.WorkspaceChanges.CWD == "" {
			return WorkspaceChanges{}, eventstore.ErrNotFound
		}
		return payload.WorkspaceChanges, nil
	}
	return WorkspaceChanges{}, eventstore.ErrNotFound
}

func (s *Service) projectForTaskID(ctx context.Context, taskID string) (core.Project, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Project{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.Project{}, eventstore.ErrNotFound
	}
	return s.projectForTask(task)
}

func applyRemotePatch(ctx context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	result := baseWorkerApplyResult(workspace, "remote_patch_apply")
	result.SourceRoot = project.LocalPath
	result.AppliedFiles = changes.ChangedFiles
	if strings.TrimSpace(changes.Diff) == "" {
		if len(changes.ChangedFiles) == 0 && !changes.Dirty {
			return result, nil
		}
		return result, errors.New("remote worker changes did not include diff.patch")
	}
	patchFile := filepath.Join(os.TempDir(), "aged-remote-"+shortID(workspace.WorkerID)+".patch")
	if err := os.WriteFile(patchFile, []byte(changes.Diff), 0o600); err != nil {
		return result, err
	}
	defer os.Remove(patchFile)
	if _, err := runCommand(ctx, project.LocalPath, "git", "apply", "--check", "--whitespace=nowarn", patchFile); err != nil {
		if cleanErr := ensureGitTrackedClean(ctx, project.LocalPath, "remote patch 3-way apply"); cleanErr != nil {
			return result, fmt.Errorf("remote patch needs 3-way apply but source checkout is not safe to mutate; check failed: %w; source status: %v", err, cleanErr)
		}
		if threeWayErr := probeGitApplyThreeWay(ctx, project.LocalPath, patchFile); threeWayErr != nil {
			return result, fmt.Errorf("remote patch has conflicts or no longer applies cleanly; check failed: %w; 3-way apply failed: %v", err, threeWayErr)
		}
		if _, threeWayErr := runCommand(ctx, project.LocalPath, "git", "apply", "--3way", "--whitespace=nowarn", patchFile); threeWayErr == nil {
			result.Method = "remote_patch_apply_3way"
			return result, nil
		} else {
			if cleanupErr := resetCleanGitApplyAttempt(ctx, project.LocalPath); cleanupErr != nil {
				return result, fmt.Errorf("remote patch has conflicts or no longer applies cleanly; check failed: %w; 3-way apply failed: %v; restore source checkout: %v", err, threeWayErr, cleanupErr)
			}
			return result, fmt.Errorf("remote patch has conflicts or no longer applies cleanly; check failed: %w; 3-way apply failed: %v", err, threeWayErr)
		}
	}
	if _, err := runCommand(ctx, project.LocalPath, "git", "apply", "--whitespace=nowarn", patchFile); err != nil {
		return result, fmt.Errorf("apply checked remote patch: %w", err)
	}
	return result, nil
}

func sourceCheckoutCommit(ctx context.Context, sourceRoot string) string {
	sourceRoot = strings.TrimSpace(sourceRoot)
	if sourceRoot == "" {
		return ""
	}
	out, err := runCommand(ctx, sourceRoot, "git", "rev-parse", "--verify", "HEAD")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func probeGitApplyThreeWay(ctx context.Context, sourceRoot string, patchFile string) error {
	tempRoot, err := os.MkdirTemp("", "aged-git-apply-probe-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempRoot)
	probeDir := filepath.Join(tempRoot, "worktree")
	if _, err := runCommand(ctx, sourceRoot, "git", "worktree", "add", "--detach", probeDir, "HEAD"); err != nil {
		return fmt.Errorf("create apply probe worktree: %w", err)
	}
	defer func() {
		_, _ = runCommand(context.Background(), sourceRoot, "git", "worktree", "remove", "--force", probeDir)
	}()
	if _, err := runCommand(ctx, probeDir, "git", "apply", "--3way", "--whitespace=nowarn", patchFile); err != nil {
		return err
	}
	return nil
}

func resetCleanGitApplyAttempt(ctx context.Context, dir string) error {
	if _, err := runCommand(ctx, dir, "git", "reset", "--hard", "HEAD"); err != nil {
		return err
	}
	return nil
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

func (s *Service) updateTaskObjective(ctx context.Context, taskID string, status core.ObjectiveStatus, phase string, summary string) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskObjective,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status":  status,
			"phase":   phase,
			"summary": summary,
		}),
	})
	return err
}

func (s *Service) updateTaskWorkPlan(ctx context.Context, taskID string, workPlan core.WorkPlan) error {
	_, err := s.append(ctx, core.Event{
		Type:    core.EventTaskWorkPlan,
		TaskID:  taskID,
		Payload: core.MustJSON(workPlan),
	})
	return err
}

func (s *Service) recordTaskMilestone(ctx context.Context, taskID string, name string, phase string, summary string, metadata map[string]any) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskMilestone,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"name":     name,
			"phase":    phase,
			"summary":  summary,
			"metadata": metadata,
		}),
	})
	return err
}

func (s *Service) recordTaskArtifact(ctx context.Context, taskID string, id string, kind string, name string, url string, ref string, metadata map[string]any) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskArtifact,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":       id,
			"kind":     kind,
			"name":     name,
			"url":      url,
			"ref":      ref,
			"metadata": metadata,
		}),
	})
	return err
}

func (s *Service) recordWorkerArtifacts(ctx context.Context, taskID string, workerID string, workerKind string, state *workerRunState, changes WorkspaceChanges) error {
	for _, artifact := range changes.Artifacts {
		metadata := artifact.Metadata
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata["workerId"] = workerID
		if artifact.Path != "" {
			metadata["path"] = artifact.Path
		}
		if artifact.Content != "" {
			metadata["content"] = artifact.Content
		}
		if err := s.recordTaskArtifact(ctx, taskID, nonEmpty(artifact.ID, workerID+"-"+artifact.Kind), artifact.Kind, artifact.Name, "", artifact.Path, metadata); err != nil {
			return err
		}
	}
	summary := state.summaryText()
	if strings.TrimSpace(summary) == "" {
		return nil
	}
	for _, artifact := range resultArtifacts(workerID, workerKind, summary) {
		if err := s.recordTaskArtifact(ctx, taskID, artifact.ID, artifact.Kind, artifact.Name, "", "", artifact.Metadata); err != nil {
			return err
		}
	}
	return nil
}

type resultArtifact struct {
	ID       string
	Kind     string
	Name     string
	Metadata map[string]any
}

func resultArtifacts(workerID string, workerKind string, summary string) []resultArtifact {
	lower := strings.ToLower(summary)
	artifacts := []resultArtifact{}
	if workerKind == "benchmark_compare" || strings.Contains(lower, "## benchmark results") {
		metadata := parseMarkdownKeyValues(summary)
		metadata["workerId"] = workerID
		metadata["content"] = truncateArtifactContent(summary)
		artifacts = append(artifacts, resultArtifact{
			ID:       workerID + "-benchmark",
			Kind:     "benchmark_report",
			Name:     "Benchmark report",
			Metadata: metadata,
		})
	}
	if strings.Contains(lower, "flamegraph") || strings.Contains(lower, "profiler") || strings.Contains(lower, "profile report") {
		artifacts = append(artifacts, resultArtifact{
			ID:   workerID + "-profile",
			Kind: "profiler_report",
			Name: "Profiler report",
			Metadata: map[string]any{
				"workerId": workerID,
				"content":  truncateArtifactContent(summary),
			},
		})
	}
	for _, spec := range []struct {
		marker string
		kind   string
		name   string
	}{
		{"## test report", "test_report", "Test report"},
		{"## ci", "ci_run", "CI run"},
		{"## review comments", "review_comments", "Review comments"},
		{"## deployment", "deployment", "Deployment"},
		{"## package", "package", "Package"},
	} {
		if strings.Contains(lower, spec.marker) {
			artifacts = append(artifacts, resultArtifact{
				ID:   workerID + "-" + spec.kind,
				Kind: spec.kind,
				Name: spec.name,
				Metadata: map[string]any{
					"workerId": workerID,
					"content":  truncateArtifactContent(summary),
				},
			})
		}
	}
	return artifacts
}

func parseMarkdownKeyValues(text string) map[string]any {
	values := map[string]any{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		values[camelArtifactKey(key)] = value
	}
	return values
}

func camelArtifactKey(key string) string {
	parts := strings.FieldsFunc(strings.ToLower(key), func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	if len(parts) == 0 {
		return key
	}
	out := parts[0]
	for _, part := range parts[1:] {
		if part == "" {
			continue
		}
		out += strings.ToUpper(part[:1]) + part[1:]
	}
	return out
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
		"baseWorkerID",
		"baseWorkspaceCWD",
		"baseWorkspaceReuseError",
		"baseRevision",
		"baseHandoff",
		"basePatchApplied",
		"baseChangedFiles",
		"workspaceBaseRef",
		"workspaceBaseRefKind",
		"workspaceBaseRevision",
		"pullRequestID",
		"pullRequestRepo",
		"pullRequestNumber",
		"pullRequestBranch",
		"pullRequestBase",
		"pullRequestURL",
		"nodeID",
		"parentNodeID",
		"planID",
		"targetID",
		"targetKind",
		"targetLabels",
		"ignoredTargetLabels",
		"requiredMemoryMB",
		"requiredStorageMB",
		"ignoredRequiredMemoryMB",
		"ignoredRequiredStorageMB",
		"requiredTargetID",
		"ignoredRequiredTargetID",
		"targetSelectionPolicy",
		"targetSelectionSource",
		"targetRequirementsSource",
		"fallbackFromTargetID",
		"fallbackFromTargetKind",
		"remoteSession",
		"remoteRunDir",
		"remoteWorkDir",
		"initialWorker",
		"scheduledWorkerID",
		"spawnID",
		"spawnReason",
		"spawnRole",
		"turn",
		"dynamicReplanTurn",
		"executionMode",
		"loopIteration",
		"loopRole",
		"loopWorkerKind",
		"reasoningEffort",
		"retryFromWorkerID",
		"retryTargetID",
		"retryTargetKind",
		"retryRemoteSession",
		"retryRemoteRunDir",
		"retryRemoteWorkDir",
		"retryResumeSessionID",
		"retrySteering",
		"retryWorkspaceReused",
		"retryWorkspaceCWD",
		"retryWorkspaceError",
		"usageAwareScheduling",
		"usageOriginalWorkerKind",
		"usageSelectedWorkerKind",
		"usageSelectionReason",
		"usageCurrentPressure",
		"usageAlternatePressure",
	} {
		if value, ok := plan.Metadata[key]; ok && value != nil && value != "" {
			metadata[key] = value
		}
	}
	if plan.Rationale != "" {
		metadata["rationale"] = plan.Rationale
	}
	if plan.WorkPlan != nil {
		metadata["workPlan"] = plan.WorkPlan
	}
	if len(plan.Steps) > 0 {
		metadata["steps"] = plan.Steps
	}
	if len(plan.RequiredApprovals) > 0 {
		metadata["requiredApprovals"] = plan.RequiredApprovals
	}
	if len(plan.Workers) > 0 {
		metadata["workers"] = plan.Workers
	}
	if len(plan.Spawns) > 0 {
		metadata["spawns"] = plan.Spawns
	}
	return metadata
}

func copyPlanMetadata(source map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range source {
		out[key] = value
	}
	return out
}

func normalizeReasoningEffort(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "default", "":
		return ""
	case "low", "medium", "high", "xhigh", "max":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func normalizePlanReasoning(plan *Plan) {
	if plan == nil {
		return
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.ReasoningEffort = normalizeReasoningEffort(nonEmpty(plan.ReasoningEffort, stringMetadata(plan.Metadata, "reasoningEffort"), stringMetadata(plan.Metadata, "thinkingLevel"), stringMetadata(plan.Metadata, "effort")))
	if plan.ReasoningEffort != "" {
		plan.Metadata["reasoningEffort"] = plan.ReasoningEffort
	}
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

func boolMetadata(metadata map[string]any, key string) bool {
	if metadata == nil {
		return false
	}
	value, _ := metadata[key].(bool)
	return value
}

func candidateBaseWorkerID(metadata map[string]any) string {
	baseWorkerID := stringMetadata(metadata, "baseWorkerID")
	if strings.EqualFold(baseWorkerID, "source") {
		return ""
	}
	return baseWorkerID
}

func shouldInheritLatestCandidate(metadata map[string]any) bool {
	return candidateBaseWorkerID(metadata) == "" && !strings.EqualFold(stringMetadata(metadata, "baseWorkerID"), "source")
}

func intMetadata(metadata map[string]any, key string) int {
	if metadata == nil {
		return 0
	}
	switch value := metadata[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		number, _ := value.Int64()
		return int(number)
	default:
		return 0
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

func anySliceMetadata(metadata map[string]any, key string) []any {
	if metadata == nil {
		return nil
	}
	switch value := metadata[key].(type) {
	case []any:
		return value
	default:
		return nil
	}
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

func (s *workerRunState) summaryText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.summary
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
		if spawnID, ok := plan.Metadata["spawnID"].(string); ok {
			result.SpawnID = spawnID
		}
		if baseWorkerID, ok := plan.Metadata["baseWorkerID"].(string); ok {
			result.BaseWorkerID = baseWorkerID
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
