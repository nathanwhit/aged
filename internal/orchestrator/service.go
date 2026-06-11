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
	"strconv"
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
	deferredNextWorkPattern   = regexp.MustCompile(`\b(?:i am|i'm|i will|i'll|will|going to|about to)\s+(?:run|running|rerun|execute|start|try|check|validate|test|rebuild|build)\b.*\bnext\b`)
	pullRequestRepoNumberRE   = regexp.MustCompile(`(?i)\b([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#([0-9]+)\b`)
	pullRequestBareNumberRE   = regexp.MustCompile(`(?i)\b(?:PR|pull\s+request)\s*#?\s*([0-9]+)\b`)
	errWorkerCallbackDeferred = errors.New("worker callback deferred")
)

const (
	taskCancelReasonStartupRecovery = "startup_worker_recovery"
	taskCancelReasonSteeringRestart = "steering_restart"
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

	mu                  sync.Mutex
	prCommentMu         sync.Mutex
	cancels             map[string]context.CancelFunc
	taskCancels         map[string]context.CancelFunc
	taskRuns            map[string]string
	tasks               map[string]string
	steering            map[string]chan string
	remoteRuns          map[string]remoteRun
	workerCaps          map[string]worker.Capabilities
	workerCancelReasons map[string]string

	steeringRestarts map[string]struct{}
	activeWorkItems  map[string]struct{}

	retainedArtifactCleanup        RetainedWorkspaceArtifactCleanupOptions
	retainedArtifactCleanupEnabled bool
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

func workerExecutionPrompt(prompt string, workspace PreparedWorkspace, allowCreateTaskCallbacks bool) string {
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
	if allowCreateTaskCallbacks {
		if helper := strings.TrimSpace(workspaceCreateTaskHelperPath(workspace)); helper != "" {
			b.WriteString("# Aged Task Creation\n\n")
			b.WriteString("If this worker needs to delegate, fan out, or spawn follow-up aged tasks, use this helper:\n")
			b.WriteString(helper)
			b.WriteString("\n\n")
			b.WriteString("It reads the new task prompt from stdin. Example: `printf '%s\\n' \"Concrete task prompt\" | ")
			b.WriteString(helper)
			b.WriteString(" --title \"Follow-up\"`. When the worker task explicitly asks you to spawn or create aged tasks, queue those tasks with this helper instead of doing the delegated implementation yourself.\n\n")
		}
	}
	if helper := strings.TrimSpace(workspacePublishPRHelperPath(workspace)); helper != "" {
		b.WriteString("# Aged Pull Request Publication\n\n")
		b.WriteString("If this worker needs to publish an intermediate pull request for its own changes, use this helper instead of `gh pr create`:\n")
		b.WriteString(helper)
		b.WriteString("\n\n")
		b.WriteString("It reads the pull request body from stdin and asks the original aged orchestrator to publish the current worker result. Example: `printf '%s\\n' \"Summary and validation\" | ")
		b.WriteString(helper)
		b.WriteString(" --title \"refactor(cron): remove saffron dependency\"`. The `--title` value must be a short reviewable code-change title suitable for a PR title and commit subject; never use status narration like tests passed, pushing changes, final status, or opening a PR. Durable loops should use this helper so aged records and babysits the PR while the loop continues.\n\n")
	}
	if helper := strings.TrimSpace(workspaceUpdatePRHelperPath(workspace)); helper != "" {
		b.WriteString("# Aged Pull Request Metadata Updates\n\n")
		b.WriteString("If this worker needs to update the title or description of an existing tracked pull request, use this helper instead of `gh pr edit`:\n")
		b.WriteString(helper)
		b.WriteString("\n\n")
		b.WriteString("It reads the replacement pull request body from stdin and asks the original aged orchestrator to perform a metadata-only update. Example: `printf '%s\\n' \"Updated PR description\" | ")
		b.WriteString(helper)
		b.WriteString(" --number 123 --title \"refactor(cron): remove saffron dependency\" --comment \"Updated the PR title and description.\"`. If you pass `--title`, make it the desired stable PR title, not a progress/status message. Pass `--comment` only when aged should post that exact public comment after the update succeeds.\n\n")
	}
	if sharedRoot := strings.TrimSpace(workspace.SharedRoot); sharedRoot != "" {
		b.WriteString("# Shared Artifact Workspace\n\n")
		b.WriteString("This task has a shared artifact workspace for non-repo assets such as baseline binaries, benchmark harnesses, profiling captures, generated data, and logs that should survive across worker turns without becoming pull request changes.\n\n")
		b.WriteString("Shared root: ")
		b.WriteString(sharedRoot)
		b.WriteString("\n")
		if dir := strings.TrimSpace(workspace.SharedArtifactsDir); dir != "" {
			b.WriteString("Published artifacts directory: ")
			b.WriteString(dir)
			b.WriteString("\n")
		}
		if dir := strings.TrimSpace(workspace.SharedWorkerDir); dir != "" {
			b.WriteString("This worker's scratch directory: ")
			b.WriteString(dir)
			b.WriteString("\n")
		}
		b.WriteString("\nThe environment exports `AGED_SHARED_DIR`, `AGED_SHARED_ARTIFACTS_DIR`, and `AGED_WORKER_SCRATCH_DIR` when these paths are available. Keep repo changes in the execution workspace; keep task-local scratch assets in the shared artifact workspace. Treat files under the published artifacts directory as durable/versioned outputs and avoid overwriting another worker's artifact in place.\n\n")
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

func workspaceUpdatePRHelperPath(workspace PreparedWorkspace) string {
	base := workspaceAgedWorkerDir(workspace)
	if base == "" {
		return ""
	}
	return filepath.Join(base, "bin", "aged-update-pr")
}

func workspaceCallbackDir(workspace PreparedWorkspace) string {
	base := workspaceAgedWorkerDir(workspace)
	if base == "" {
		return ""
	}
	return filepath.Join(base, "callbacks")
}

func workspaceSharedEnv(workspace PreparedWorkspace) map[string]string {
	env := map[string]string{}
	if dir := strings.TrimSpace(workspace.SharedRoot); dir != "" {
		env["AGED_SHARED_DIR"] = dir
	}
	if dir := strings.TrimSpace(workspace.SharedArtifactsDir); dir != "" {
		env["AGED_SHARED_ARTIFACTS_DIR"] = dir
	}
	if dir := strings.TrimSpace(workspace.SharedWorkerDir); dir != "" {
		env["AGED_WORKER_SCRATCH_DIR"] = dir
	}
	if len(env) == 0 {
		return nil
	}
	return env
}

func applySharedWorkspace(workspace PreparedWorkspace, shared SharedWorkspace) PreparedWorkspace {
	workspace.SharedRoot = shared.Root
	workspace.SharedArtifactsDir = shared.ArtifactsDir
	workspace.SharedWorkerDir = shared.WorkerDir
	return workspace
}

func installLocalCreateTaskHelper(workspace PreparedWorkspace, allowCreateTaskCallbacks bool) (string, string, error) {
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
	if allowCreateTaskCallbacks {
		if err := os.WriteFile(helperPath, []byte(localCreateTaskHelperScript(callbackDir, workspace.TaskID, workspace.WorkerID)), 0o700); err != nil {
			return "", "", err
		}
	} else {
		helperPath = ""
	}
	publishHelperPath := workspacePublishPRHelperPath(workspace)
	if publishHelperPath != "" {
		if err := os.WriteFile(publishHelperPath, []byte(localPublishPRHelperScript(callbackDir, workspace.TaskID, workspace.WorkerID)), 0o700); err != nil {
			return "", "", err
		}
	}
	updateHelperPath := workspaceUpdatePRHelperPath(workspace)
	if updateHelperPath != "" {
		if err := os.WriteFile(updateHelperPath, []byte(localUpdatePRHelperScript(callbackDir, workspace.TaskID, workspace.WorkerID)), 0o700); err != nil {
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

func localUpdatePRHelperScript(callbackDir string, taskID string, workerID string) string {
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
	b.WriteString(remoteUpdatePRHelperScript())
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

func remoteWorkerExecutionPrompt(prompt string, workspace PreparedWorkspace, allowCreateTaskCallbacks bool) string {
	prompt = workerExecutionPrompt(prompt, workspace, allowCreateTaskCallbacks)
	var b strings.Builder
	b.WriteString("# Original Orchestrator\n\n")
	b.WriteString("This worker is running on a remote execution target under an existing aged orchestrator. Do not start a new aged daemon or orchestrator from this worker.\n\n")
	if allowCreateTaskCallbacks {
		b.WriteString("To create follow-up work, use the `aged-create-task` helper on PATH. It reads the new task prompt from stdin and queues it for the original orchestrator over the existing SSH control channel. ")
		b.WriteString("When creating follow-up work, do not ask the follow-up task to open a draft pull request unless the user explicitly requested a draft PR; project configuration controls draft-by-default behavior. ")
	}
	b.WriteString("To publish this worker result as an intermediate pull request, use the `aged-publish-pr` helper on PATH instead of `gh pr create`; it reads the pull request body from stdin and the orchestrator records the PR. The `aged-publish-pr --title` value must be a short reviewable code-change title suitable for a PR title and commit subject, for example `refactor(cron): remove saffron dependency`; never use status narration like tests passed, pushing changes, final status, or opening a PR. ")
	b.WriteString("To update the title or description of an existing tracked pull request, use the `aged-update-pr` helper on PATH instead of `gh pr edit`; it reads the replacement PR body from stdin and the orchestrator records a metadata-only PR update. If you provide `--title`, make it the desired stable PR title, not a progress/status message. Pass `--comment` only when you want aged to post that exact public comment after the update succeeds. ")
	b.WriteString("The remote environment also exports `AGED_PARENT_TASK_ID`, `AGED_PARENT_WORKER_ID`, `AGED_WORKER_CALLBACK_DIR`, and the shared artifact workspace variables when available.\n\n")
	b.WriteString(prompt)
	return b.String()
}

func planAllowsCreateTaskCallbacks(plan Plan) bool {
	return !boolMetadata(plan.Metadata, "backgroundPullRequestFollowUp") && !boolMetadata(plan.Metadata, "disableCreateTaskCallbacks")
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
		store:               store,
		broker:              NewBroker(),
		brain:               brain,
		runners:             runners,
		baseRunners:         maps.Clone(runners),
		pluginRunners:       map[string]struct{}{},
		workDir:             workDir,
		projects:            projects,
		plugins:             NewPluginRegistry(builtinPlugins()),
		promptSets:          NewPromptSetRegistry(nil, ""),
		workspaces:          workspaces,
		targets:             targets,
		sshRunner:           sshRunner,
		prPublisher:         NewLocalPullRequestPublisher(),
		remoteApply:         applyRemotePatch,
		cancels:             map[string]context.CancelFunc{},
		taskCancels:         map[string]context.CancelFunc{},
		taskRuns:            map[string]string{},
		tasks:               map[string]string{},
		steering:            map[string]chan string{},
		remoteRuns:          map[string]remoteRun{},
		workerCaps:          map[string]worker.Capabilities{},
		workerCancelReasons: map[string]string{},
		steeringRestarts:    map[string]struct{}{},
		activeWorkItems:     map[string]struct{}{},
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
	if err := s.recoverQueuedSpawnWorkItems(ctx, snapshot); err != nil {
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

func (s *Service) recoverQueuedSpawnWorkItems(ctx context.Context, snapshot core.Snapshot) error {
	for _, task := range snapshot.Tasks {
		if task.Status != core.TaskRunning && task.Status != core.TaskPlanning {
			continue
		}
		hasQueuedSpawnWork := false
		for _, item := range snapshot.WorkItems {
			if item.TaskID == task.ID && item.Status == core.WorkItemQueued && isRunnableSpawnWorkItem(item, task.ID) {
				hasQueuedSpawnWork = true
				break
			}
		}
		if !hasQueuedSpawnWork {
			continue
		}
		started, err := s.startRunnableSpawnWorkItems(ctx, task.ID)
		if err != nil {
			return err
		}
		if started == 0 {
			continue
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":             "startup_spawn_work_recovery",
			"status":           "resumed",
			"reason":           "daemon restarted with queued spawned objective work; resuming runnable work items",
			"startedWorkCount": started,
		}); err != nil {
			return err
		}
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
		if err := s.completeInterruptedObjectiveRoutines(ctx, snapshot, task.ID, "superseded by startup planning recovery"); err != nil {
			return err
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
				if err := s.startObjectiveRoutine(ctx, task, "pr.followup", "Resume queued pull request feedback after daemon restart.", func(taskCtx context.Context) {
					s.resumePullRequestFeedbackQueue(taskCtx, task.ID)
				}); err != nil {
					return err
				}
			} else {
				if err := s.startObjectiveRoutine(ctx, task, "pr.followup", "Resume interrupted pull request follow-up planning after daemon restart.", func(taskCtx context.Context) {
					s.resumeLegacyPullRequestFollowUpPlanning(taskCtx, task.ID)
				}); err != nil {
					return err
				}
			}
			continue
		}
		if initial, results, err := objectiveReplanStateForTask(snapshot, task.ID); err == nil {
			_, err := s.append(ctx, core.Event{
				Type:   core.EventTaskAction,
				TaskID: task.ID,
				Payload: core.MustJSON(map[string]any{
					"kind":   "startup_planning_recovery",
					"status": "resumed",
					"reason": "daemon restarted while objective replanning was in progress; resuming from persisted worker results",
				}),
			})
			if err != nil {
				return err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "recovering"
			if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Resume objective replanning from persisted worker results after daemon restart.", func(taskCtx context.Context) {
				s.resumeObjectiveReplan(taskCtx, task, initial, results)
			}); err != nil {
				return err
			}
			continue
		}
		if plan, err := retryPlanForTask(snapshot, task.ID); err == nil {
			_, err := s.append(ctx, core.Event{
				Type:   core.EventTaskAction,
				TaskID: task.ID,
				Payload: core.MustJSON(map[string]any{
					"kind":   "startup_planning_recovery",
					"status": "resumed",
					"reason": "daemon restarted while persisted plan execution was in progress; retrying the latest plan",
				}),
			})
			if err != nil {
				return err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "recovering"
			if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Retry the persisted plan after daemon restart.", func(taskCtx context.Context) {
				s.retryTask(taskCtx, task, plan)
			}); err != nil {
				return err
			}
			continue
		}
		_, err := s.append(ctx, core.Event{
			Type:   core.EventTaskAction,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"kind":   "startup_planning_recovery",
				"status": "resumed",
				"reason": "daemon restarted during initial planning before a plan or worker was recorded; restarting planning",
			}),
		})
		if err != nil {
			return err
		}
		if err := s.startObjectiveRoutine(ctx, task, "objective.plan", "Restart initial planning after daemon restart.", func(taskCtx context.Context) {
			s.runTask(taskCtx, task)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) completeInterruptedObjectiveRoutines(ctx context.Context, snapshot core.Snapshot, taskID string, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "superseded by startup recovery"
	}
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || isTerminalWorkItemStatus(item.Status) {
			continue
		}
		if strings.TrimSpace(item.TargetKind) != "objective" || strings.TrimSpace(item.TargetID) != taskID {
			continue
		}
		if strings.TrimSpace(item.WorkerID) != "" {
			continue
		}
		if err := s.recordWorkItemCompleted(ctx, taskID, item.ID, core.WorkItemFailed, "", reason); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) recoverOrphanedRunningGraphTasks(ctx context.Context, snapshot core.Snapshot) error {
	for _, task := range snapshot.Tasks {
		if task.Status != core.TaskRunning || taskHasActiveObjectiveWorkers(snapshot, task.ID) {
			continue
		}
		if !taskLatestStatusIs(snapshot, task.ID, core.TaskRunning) {
			continue
		}
		hasActiveWorkers := taskHasActiveWorkers(snapshot, task.ID)
		if plan, ok, planErr := retryPullRequestFollowUpPlan(snapshot, task.ID); !hasActiveWorkers && planErr != nil {
			return planErr
		} else if !hasActiveWorkers && ok {
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
			if err := s.startObjectiveRoutine(ctx, task, "pr.followup", "Retry interrupted pull request follow-up after daemon restart.", func(taskCtx context.Context) {
				s.retryPullRequestFollowUpTask(taskCtx, task, plan)
			}); err != nil {
				return err
			}
			continue
		}
		initial, results, err := objectiveReplanStateForTask(snapshot, task.ID)
		if err != nil || len(candidateResults(results)) == 0 {
			if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   "startup_running_recovery",
				"status": "waiting",
				"reason": "daemon restarted while task was running, but no active worker or recoverable objective state was found",
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
			"reason": "daemon restarted while task was running with no active workers; resuming from persisted worker results",
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
		if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Resume objective replanning from persisted worker results after daemon restart.", func(taskCtx context.Context) {
			s.resumeObjectiveReplan(taskCtx, task, initial, results)
		}); err != nil {
			return err
		}
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
		if err := s.setTaskStatusWithReason(ctx, worker.TaskID, core.TaskCanceled, taskCancelReasonStartupRecovery); err != nil {
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
		delete(s.workerCancelReasons, node.WorkerID)
		s.mu.Unlock()
	}()

	runState := &workerRunState{}
	sink := eventSink{service: s, taskID: node.TaskID, workerID: node.WorkerID, state: runState}
	plan := recoveredExecutionNodePlan(node)
	workItemID, workItemErr := s.recordRecoveredPlanWorkItemStarted(ctx, node.TaskID, node.WorkerID, plan)
	if workItemErr != nil {
		_ = s.recordTaskAction(ctx, node.TaskID, map[string]any{
			"kind":     "startup_remote_worker_recovery",
			"status":   "work_item_record_failed",
			"workerID": node.WorkerID,
			"reason":   workItemErr.Error(),
		})
	}
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
	workerStatus, statusErr = runState.normalizeCompletionStatus(plan, workerStatus, statusErr, changes)
	_ = s.appendWorkerCompleted(ctx, node.TaskID, node.WorkerID, runState.completionPayload(workerStatus, statusErr, changes))
	_ = s.recordWorkerArtifacts(ctx, node.TaskID, node.WorkerID, node.WorkerKind, runState, changes)
	if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, node.TaskID, workItemID, node.WorkerID, workerStatus, statusErr); completeErr != nil {
		_ = s.recordTaskAction(ctx, node.TaskID, map[string]any{
			"kind":     "startup_remote_worker_recovery",
			"status":   "work_item_complete_failed",
			"workerID": node.WorkerID,
			"reason":   completeErr.Error(),
		})
	}
	s.cleanupTerminalWorkspaceArtifacts(ctx, node.TaskID, node.WorkerID, PreparedWorkspace{
		Root:            run.RunDir,
		CWD:             run.WorkDir,
		WorkspaceName:   run.Session,
		Mode:            "remote",
		VCSType:         "ssh",
		CleanupPolicy:   string(WorkspaceCleanupRetain),
		WorkerID:        node.WorkerID,
		TaskID:          node.TaskID,
		TargetID:        run.Target.ID,
		TargetKind:      string(run.Target.Kind),
		SharedRoot:      run.SharedRoot,
		SharedWorkerDir: run.SharedWorkerDir,
	}, workspaceResultForWorkerStatus(workerStatus))
	if workerStatus == core.WorkerCanceled {
		if snapshot, err := s.store.Snapshot(ctx); err == nil && !taskHasActiveWorkers(snapshot, node.TaskID) {
			_ = s.setTaskStatus(ctx, node.TaskID, core.TaskCanceled)
		}
		return
	}
	go s.resumeRecoveredRemoteTask(context.Background(), node.TaskID)
}

func (s *Service) recordRecoveredPlanWorkItemStarted(ctx context.Context, taskID string, workerID string, plan Plan) (string, error) {
	if strings.TrimSpace(stringMetadata(plan.Metadata, "workItemKind")) == "" {
		return "", nil
	}
	itemID := planWorkerWorkItemID(taskID, plan)
	if itemID == "" {
		return "", nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	if item, ok := workItemByIDFromSnapshot(snapshot, taskID, itemID); ok {
		if item.Status == core.WorkItemRunning && strings.TrimSpace(item.WorkerID) == strings.TrimSpace(workerID) {
			return itemID, nil
		}
		if item.Status == core.WorkItemSucceeded || item.Status == core.WorkItemFailed || item.Status == core.WorkItemCanceled {
			return "", nil
		}
	}
	if strings.TrimSpace(stringMetadata(plan.Metadata, "sourceAction")) == "plan" {
		delete(plan.Metadata, "sourceAction")
	}
	return s.recordPlanWorkItemStarted(ctx, taskID, workerID, plan)
}

func recoveredExecutionNodePlan(node core.ExecutionNode) Plan {
	metadata := map[string]any{}
	if len(node.Metadata) > 0 {
		_ = json.Unmarshal(node.Metadata, &metadata)
	}
	putIfMissing := func(key string, value any) {
		if value == nil {
			return
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
			return
		}
		if _, ok := metadata[key]; !ok {
			metadata[key] = value
		}
	}
	putIfMissing("nodeID", node.ID)
	putIfMissing("planID", node.PlanID)
	putIfMissing("parentNodeID", node.ParentNodeID)
	putIfMissing("spawnID", node.SpawnID)
	putIfMissing("spawnRole", node.Role)
	putIfMissing("spawnReason", node.Reason)
	putIfMissing("targetID", node.TargetID)
	putIfMissing("targetKind", node.TargetKind)
	putIfMissing("remoteSession", node.RemoteSession)
	putIfMissing("remoteRunDir", node.RemoteRunDir)
	putIfMissing("remoteWorkDir", node.RemoteWorkDir)
	if len(node.DependsOn) > 0 {
		putIfMissing("dependsOn", node.DependsOn)
	}
	if strings.TrimSpace(stringMetadata(metadata, "workItemID")) == "" && strings.TrimSpace(node.PlanID) == "" && strings.TrimSpace(node.SpawnID) == "" {
		putIfMissing("workItemID", "recovered_worker_"+node.WorkerID)
	}
	if strings.TrimSpace(stringMetadata(metadata, "workItemKind")) == "" {
		role := strings.Join([]string{node.Role, node.SpawnID, stringMetadata(metadata, "scheduledWorkerID")}, " ")
		reason := strings.Join([]string{node.Reason, stringMetadata(metadata, "rationale"), stringMetadata(metadata, "parentRationale")}, " ")
		putIfMissing("workItemKind", objectiveWorkerWorkItemKind(role, reason))
	}
	if strings.TrimSpace(stringMetadata(metadata, "sourceAction")) == "" {
		putIfMissing("sourceAction", "recovered_worker")
	}
	return Plan{
		WorkerKind:      node.WorkerKind,
		Rationale:       node.Reason,
		ReasoningEffort: stringMetadata(metadata, "reasoningEffort"),
		Metadata:        metadata,
	}
}

func (s *Service) Events(ctx context.Context, afterID int64, limit int) ([]core.Event, error) {
	return s.store.ListEvents(ctx, afterID, limit)
}

func (s *Service) TaskEvents(ctx context.Context, taskID string, limit int) ([]core.Event, error) {
	return s.store.ListTaskEvents(ctx, taskID, limit)
}

func (s *Service) SessionTail(ctx context.Context, sessionID string, afterID int64, limit int, kinds ...core.EventType) (core.SessionTail, error) {
	session, err := s.sessionByID(ctx, sessionID)
	if err != nil {
		return core.SessionTail{}, err
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.SessionTail{}, err
	}
	if len(kinds) == 0 {
		kinds = []core.EventType{
			core.EventWorkerOutput,
			core.EventWorkerStarted,
			core.EventWorkerCompleted,
			core.EventWorkerSteered,
		}
	}
	var events []core.Event
	if afterID > 0 {
		events, err = s.store.ListWorkerEvents(ctx, session.WorkerID, afterID, limit, kinds...)
	} else {
		events, err = s.store.ListLatestWorkerEvents(ctx, session.WorkerID, limit, kinds...)
	}
	if err != nil {
		return core.SessionTail{}, err
	}
	lastEventID := afterID
	if lastEventID < 0 {
		lastEventID = 0
	}
	for _, event := range events {
		if event.ID > lastEventID {
			lastEventID = event.ID
		}
	}
	var currentAction *core.SessionCurrentAction
	if strings.TrimSpace(session.CurrentAction) != "" || strings.TrimSpace(session.CurrentActionLabel) != "" || session.CurrentActionAt != nil || session.CurrentActionEvent != 0 {
		currentAction = &core.SessionCurrentAction{
			Label:   session.CurrentActionLabel,
			Text:    session.CurrentAction,
			At:      session.CurrentActionAt,
			EventID: session.CurrentActionEvent,
		}
	}
	worker := sessionTailWorker(snapshot.Workers, session.WorkerID)
	node := sessionTailNode(snapshot.ExecutionNodes, session)
	pullRequests := sessionTailPullRequests(snapshot.PullRequests, session.TaskID, session.WorkerID)
	completion := s.sessionTailCompletion(ctx, session.WorkerID)
	changedFiles := []core.SessionChangedFile(nil)
	if completion != nil {
		changedFiles = completion.ChangedFiles
	}
	return core.SessionTail{
		SessionID:     session.ID,
		WorkerID:      session.WorkerID,
		TaskID:        session.TaskID,
		Status:        session.Status,
		LastEventID:   lastEventID,
		Events:        events,
		CurrentAction: currentAction,
		Session:       &session,
		Worker:        worker,
		Node:          node,
		PullRequests:  pullRequests,
		Completion:    completion,
		ChangedFiles:  changedFiles,
	}, nil
}

func sessionTailWorker(workers []core.Worker, workerID string) *core.Worker {
	for _, worker := range workers {
		if worker.ID == workerID {
			return &worker
		}
	}
	return nil
}

func sessionTailNode(nodes []core.ExecutionNode, session core.Session) *core.ExecutionNode {
	for _, node := range nodes {
		if session.NodeID != "" && node.ID == session.NodeID {
			return &node
		}
	}
	for _, node := range nodes {
		if node.WorkerID == session.WorkerID {
			return &node
		}
	}
	return nil
}

func sessionTailPullRequests(pullRequests []core.PullRequest, taskID string, workerID string) []core.PullRequest {
	taskPullRequests := []core.PullRequest{}
	workerPullRequests := []core.PullRequest{}
	for _, pr := range pullRequests {
		if pr.TaskID != taskID {
			continue
		}
		taskPullRequests = append(taskPullRequests, pr)
		if workerID != "" && pullRequestMetadataString(pr, "workerId") == workerID {
			workerPullRequests = append(workerPullRequests, pr)
		}
	}
	if len(workerPullRequests) > 0 {
		return workerPullRequests
	}
	return taskPullRequests
}

func (s *Service) sessionTailCompletion(ctx context.Context, workerID string) *core.SessionCompletion {
	events, err := s.store.ListWorkerEvents(ctx, workerID, 0, 1000, core.EventWorkerCompleted)
	if err != nil {
		return nil
	}
	var completion *core.SessionCompletion
	for _, event := range events {
		next := sessionCompletionFromEvent(event)
		if next != nil {
			completion = next
		}
	}
	return completion
}

func sessionCompletionFromEvent(event core.Event) *core.SessionCompletion {
	if event.Type != core.EventWorkerCompleted {
		return nil
	}
	var payload struct {
		Status           core.WorkerStatus         `json:"status,omitempty"`
		Summary          string                    `json:"summary,omitempty"`
		Error            string                    `json:"error,omitempty"`
		ChangedFiles     []core.SessionChangedFile `json:"changedFiles,omitempty"`
		WorkspaceChanges struct {
			ChangedFiles []core.SessionChangedFile `json:"changedFiles,omitempty"`
		} `json:"workspaceChanges,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil
	}
	changedFiles := payload.ChangedFiles
	if len(changedFiles) == 0 {
		changedFiles = payload.WorkspaceChanges.ChangedFiles
	}
	return &core.SessionCompletion{
		Status:       payload.Status,
		Summary:      payload.Summary,
		Error:        payload.Error,
		EventID:      event.ID,
		At:           event.At,
		ChangedFiles: changedFiles,
	}
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

func (s *Service) startObjectiveRoutine(ctx context.Context, task core.Task, kind string, reason string, fn func(context.Context)) error {
	itemID, err := s.queueObjectiveWorkItem(ctx, task, kind, reason)
	if err != nil {
		return err
	}
	s.startWorkItemRoutine(task.ID, itemID, "", fn)
	return nil
}

func (s *Service) startObjectiveRoutineByID(ctx context.Context, taskID string, kind string, reason string, fn func(context.Context)) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	return s.startObjectiveRoutine(ctx, task, kind, reason, fn)
}

func (s *Service) queueObjectiveWorkItem(ctx context.Context, task core.Task, kind string, reason string) (string, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "objective.plan"
	}
	itemID := uuid.NewString()
	metadata := map[string]any{
		"objectiveId": task.ID,
	}
	if phase := strings.TrimSpace(task.ObjectivePhase); phase != "" {
		metadata["objectivePhase"] = phase
	}
	if err := s.recordWorkItemQueued(ctx, task.ID, map[string]any{
		"id":         itemID,
		"kind":       kind,
		"targetKind": "objective",
		"targetId":   task.ID,
		"reason":     strings.TrimSpace(reason),
		"prompt":     task.Prompt,
		"metadata":   metadata,
	}); err != nil {
		return "", err
	}
	return itemID, nil
}

func (s *Service) startWorkItemRoutine(taskID string, itemID string, workerID string, fn func(context.Context)) {
	s.startTaskRoutine(taskID, func(taskCtx context.Context) {
		if err := s.recordWorkItemStarted(taskCtx, taskID, itemID, workerID); err != nil {
			_ = s.failTask(taskCtx, taskID, err)
			return
		}
		fn(taskCtx)
		_ = s.completeWorkItemFromTaskStatus(context.Background(), taskID, itemID, workerID)
	})
}

func (s *Service) completeWorkItemFromTaskStatus(ctx context.Context, taskID string, itemID string, workerID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return s.recordWorkItemCompleted(ctx, taskID, itemID, core.WorkItemFailed, workerID, err.Error())
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return s.recordWorkItemCompleted(ctx, taskID, itemID, core.WorkItemFailed, workerID, "objective not found")
	}
	status := core.WorkItemSucceeded
	errorText := ""
	switch task.Status {
	case core.TaskFailed:
		status = core.WorkItemFailed
		errorText = task.Error
	case core.TaskCanceled:
		status = core.WorkItemCanceled
		errorText = task.Error
	}
	return s.recordWorkItemCompleted(ctx, taskID, itemID, status, workerID, errorText)
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

	if err := s.startObjectiveRoutine(ctx, task, "objective.plan", "Initial objective planning and execution.", func(taskCtx context.Context) {
		s.runTask(taskCtx, task)
	}); err != nil {
		return core.Task{}, err
	}
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
	req.Message = strings.TrimSpace(req.Message)
	req.TargetKind = normalizeSteeringTargetKind(req.TargetKind)
	req.TargetID = strings.TrimSpace(req.TargetID)
	if req.Message == "" {
		return errors.New("message is required")
	}
	if req.TargetKind != "" {
		if req.TargetID == "" {
			return errors.New("targetId is required for targeted steering")
		}
		switch req.TargetKind {
		case "task":
			// Continue through the generic task steering flow.
		case "worker":
			return s.SteerWorker(ctx, req.TargetID, core.SteeringRequest{Message: req.Message})
		case "session":
			return s.SteerSession(ctx, req.TargetID, core.SteeringRequest{Message: req.Message})
		case "work_item":
			return s.SteerWorkItem(ctx, taskID, req.TargetID, core.SteeringRequest{Message: req.Message})
		case "pull_request":
			return s.SteerPullRequest(ctx, taskID, req.TargetID, core.SteeringRequest{Message: req.Message})
		default:
			return fmt.Errorf("unsupported steering target kind %q", req.TargetKind)
		}
	}
	status, ok, err := s.store.TaskStatus(ctx, taskID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	event, err := s.append(ctx, core.Event{
		Type:   core.EventTaskSteered,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"message":    req.Message,
			"targetKind": "task",
			"targetId":   taskID,
			"reason":     "user_task_steering",
		}),
	})
	if err != nil {
		return err
	}
	if err := s.recordObjectiveSteeringWorkItem(ctx, taskID, event.ID, req.Message); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "user_steering", "User steering was recorded for objective replanning."); err != nil {
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
	restartWorkerIDs := make([]string, 0)
	for _, active := range activeWorkers {
		if deliveredWorkerIDs[active.ID] {
			continue
		}
		restartWorkerIDs = append(restartWorkerIDs, active.ID)
	}
	if len(activeWorkers) > 0 {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":               "objective_steering",
			"status":             "queued",
			"reason":             "user steering recorded for the next objective replanning turn",
			"message":            req.Message,
			"deliveredWorkerIds": sortedTrueKeys(deliveredWorkerIDs),
			"restartWorkerIds":   restartWorkerIDs,
		})
	}
	if status == core.TaskWaiting {
		if err := s.startObjectiveRoutineByID(ctx, taskID, "user.steering", "Resume waiting objective with new user steering.", func(taskCtx context.Context) {
			s.resumeWaitingTask(taskCtx, taskID, req.Message)
		}); err != nil {
			return err
		}
	} else if len(restartWorkerIDs) > 0 {
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
		go s.restartRunningTaskWithSteering(context.Background(), taskID, req.Message, restartWorkerIDs)
	}
	return err
}

func (s *Service) recordObjectiveSteeringWorkItem(ctx context.Context, taskID string, eventID int64, message string) error {
	workItemID := "task_steering_work_" + strconv.FormatInt(eventID, 10)
	if err := s.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         workItemID,
		"kind":       "user.steering",
		"targetKind": "objective",
		"targetId":   taskID,
		"reason":     "User steering recorded for objective replanning.",
		"prompt":     message,
		"metadata": map[string]any{
			"steeringEventId": eventID,
			"reason":          "user_task_steering",
		},
	}); err != nil {
		return err
	}
	return s.recordWorkItemCompleted(ctx, taskID, workItemID, core.WorkItemSucceeded, "", "")
}

func (s *Service) AnswerQuestion(ctx context.Context, taskID string, questionID string, req core.AnswerQuestionRequest) error {
	answer := strings.TrimSpace(req.Answer)
	questionID = strings.TrimSpace(questionID)
	if answer == "" {
		return errors.New("answer is required")
	}
	if questionID == "" {
		return errors.New("questionId is required")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) {
		return nil
	}
	var question core.Question
	for _, candidate := range snapshot.Questions {
		if candidate.ID == questionID && candidate.TaskID == taskID {
			question = candidate
			break
		}
	}
	if question.ID == "" || question.Decided {
		return eventstore.ErrNotFound
	}
	event, err := s.append(ctx, core.Event{
		Type:     core.EventApprovalDecided,
		TaskID:   taskID,
		WorkerID: question.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"approved":   true,
			"answer":     answer,
			"question":   question.Question,
			"questionId": question.ID,
			"reason":     "user_question_answered",
			"workerId":   question.WorkerID,
		}),
	})
	if err != nil {
		return err
	}
	if approvalEventID, ok := approvalEventIDFromQuestionID(question.ID); ok {
		_ = s.recordWorkItemCompleted(ctx, taskID, userQuestionWorkItemID(approvalEventID), core.WorkItemSucceeded, question.WorkerID, "")
	} else {
		s.recordLatestUserQuestionWorkItemCompleted(ctx, taskID, question.WorkerID)
	}
	workItemID := "user_question_answered_" + strconv.FormatInt(event.ID, 10)
	if err := s.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         workItemID,
		"kind":       "user.question_answered",
		"targetKind": "question",
		"targetId":   question.ID,
		"reason":     "User answered a specific orchestrator question.",
		"prompt":     answer,
		"workerId":   question.WorkerID,
		"metadata": map[string]any{
			"approvalEventId": event.ID,
			"questionId":      question.ID,
			"workerId":        question.WorkerID,
			"reason":          question.Reason,
		},
	}); err != nil {
		return err
	}
	_ = s.recordWorkItemCompleted(ctx, taskID, workItemID, core.WorkItemSucceeded, question.WorkerID, "")
	if task.Status == core.TaskWaiting {
		return s.startObjectiveRoutine(ctx, task, "user.question_answered", "Resume waiting objective with a specific user answer.", func(taskCtx context.Context) {
			s.resumeWaitingTaskWithAnswer(taskCtx, taskID, question.WorkerID, question.Question, answer)
		})
	}
	return nil
}

func normalizeSteeringTargetKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "":
		return ""
	case "task", "objective":
		return "task"
	case "worker":
		return "worker"
	case "session":
		return "session"
	case "work-item", "workitem", "work_item", "item":
		return "work_item"
	case "pull-request", "pullrequest", "pull_request", "pr":
		return "pull_request"
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func (s *Service) SteerSession(ctx context.Context, sessionID string, req core.SteeringRequest) error {
	session, err := s.sessionByID(ctx, sessionID)
	if err != nil {
		return err
	}
	return s.SteerWorker(ctx, session.WorkerID, req)
}

func (s *Service) CancelSession(ctx context.Context, sessionID string) error {
	session, err := s.sessionByID(ctx, sessionID)
	if err != nil {
		return err
	}
	return s.CancelWorker(ctx, session.WorkerID)
}

func (s *Service) sessionByID(ctx context.Context, sessionID string) (core.Session, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return core.Session{}, errors.New("sessionId is required")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Session{}, err
	}
	for _, session := range snapshot.Sessions {
		if session.ID == sessionID {
			if strings.TrimSpace(session.WorkerID) == "" {
				return core.Session{}, eventstore.ErrNotFound
			}
			return session, nil
		}
	}
	return core.Session{}, eventstore.ErrNotFound
}

func (s *Service) SteerWorkItem(ctx context.Context, taskID string, itemID string, req core.SteeringRequest) error {
	message := strings.TrimSpace(req.Message)
	itemID = strings.TrimSpace(itemID)
	if message == "" {
		return errors.New("message is required")
	}
	if itemID == "" {
		return errors.New("work item id is required")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	var item core.WorkItem
	for _, candidate := range snapshot.WorkItems {
		if candidate.TaskID == taskID && candidate.ID == itemID {
			item = candidate
			break
		}
	}
	if item.ID == "" {
		return eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) {
		return nil
	}
	event, err := s.append(ctx, core.Event{
		Type:   core.EventTaskSteered,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"message":    message,
			"targetKind": "work_item",
			"targetId":   itemID,
			"reason":     "user_work_item_steering",
			"metadata": map[string]any{
				"workItemKind":   item.Kind,
				"workItemStatus": item.Status,
				"workerId":       item.WorkerID,
			},
		}),
	})
	if err != nil {
		return err
	}
	workItemID := "targeted_steering_" + strconv.FormatInt(event.ID, 10)
	if err := s.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         workItemID,
		"kind":       "user.steering",
		"targetKind": "work_item",
		"targetId":   itemID,
		"reason":     "User steering recorded for a specific work item.",
		"prompt":     message,
		"metadata": map[string]any{
			"steeringEventId": event.ID,
			"workItemKind":    item.Kind,
			"workItemStatus":  item.Status,
			"workerId":        item.WorkerID,
		},
	}); err != nil {
		return err
	}
	_ = s.recordWorkItemCompleted(ctx, taskID, workItemID, core.WorkItemSucceeded, item.WorkerID, "")
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveActive, "user_steering", "Targeted user steering was recorded for replanning."); err != nil {
		return err
	}
	if task.Status == core.TaskWaiting {
		if err := s.startObjectiveRoutine(ctx, task, "user.steering", "Resume waiting objective with targeted user steering.", func(taskCtx context.Context) {
			s.resumeWaitingTask(taskCtx, taskID, message)
		}); err != nil {
			return err
		}
	}
	return nil
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
	event, err := s.append(ctx, core.Event{
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
	})
	if err != nil {
		return err
	}
	if err := s.recordWorkItemQueued(ctx, task.ID, map[string]any{
		"id":         workerSteeringWorkItemID(event.ID),
		"kind":       "user.worker_steering",
		"targetKind": "worker",
		"targetId":   workerID,
		"reason":     "Worker-specific steering queued for replanning.",
		"prompt":     message,
		"metadata": map[string]any{
			"steeringEventId": event.ID,
			"workerId":        workerID,
			"nodeId":          node.ID,
			"workerKind":      nonEmpty(workerState.Kind, node.WorkerKind),
			"role":            node.Role,
			"spawnId":         node.SpawnID,
		},
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
		if err := s.startObjectiveRoutine(ctx, task, "user.steering", "Resume queued worker-specific steering.", func(taskCtx context.Context) {
			s.resumeWorkerSteeringQueue(taskCtx, task.ID)
		}); err != nil {
			return err
		}
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
		if err := s.cancelWorkerWithReason(ctx, workerID, taskCancelReasonSteeringRestart); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
			_ = s.recordTaskAction(ctx, taskID, map[string]any{
				"kind":     "steering_restart",
				"status":   "warning",
				"reason":   "worker cancellation failed",
				"workerId": workerID,
				"error":    err.Error(),
			})
		}
	}
	snapshot, err := s.waitForTaskWorkersStopped(ctx, taskID, 2*time.Minute)
	if err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":   "steering_restart",
			"status": "waiting",
			"reason": "timed out waiting for workers to stop",
			"error":  err.Error(),
		})
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
			if !taskHasActiveWorkers(snapshot, taskID) && !s.taskHasActiveWorkerRoutine(taskID) {
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

func (s *Service) taskHasActiveWorkerRoutine(taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, workerTaskID := range s.tasks {
		if workerTaskID == taskID {
			return true
		}
	}
	return false
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
		if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Retry durable loop objective.", func(taskCtx context.Context) {
			s.runDurableLoopTask(taskCtx, task)
		}); err != nil {
			return core.Task{}, err
		}
		return task, nil
	}
	if task.Status == core.TaskFailed && latestTaskFailureMatches(snapshot, taskID, isGraphDependencyFailure) {
		initial, results, stateErr := objectiveReplanStateForTask(snapshot, taskID)
		if stateErr == nil && taskFailureRecoverableFromObjectiveResults(snapshot, taskID, results) {
			if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
				return core.Task{}, err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "retrying"
			if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Retry objective after dependency failure.", func(taskCtx context.Context) {
				s.resumeObjectiveReplan(taskCtx, task, initial, results)
			}); err != nil {
				return core.Task{}, err
			}
			return task, nil
		}
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
			if err := s.startObjectiveRoutine(ctx, task, "pr.followup", "Retry failed pull request follow-up work.", func(taskCtx context.Context) {
				s.retryPullRequestFollowUpTask(taskCtx, task, plan)
			}); err != nil {
				return core.Task{}, err
			}
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
	if task.Status == core.TaskFailed {
		initial, results, stateErr := objectiveReplanStateForTask(snapshot, taskID)
		if stateErr == nil && taskFailureRecoverableFromObjectiveResults(snapshot, taskID, results) {
			if err := s.markTaskRetryPlanning(ctx, taskID); err != nil {
				return core.Task{}, err
			}
			task.Status = core.TaskPlanning
			task.Error = ""
			task.ObjectiveStatus = core.ObjectiveActive
			task.ObjectivePhase = "retrying"
			if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Retry objective from persisted worker results.", func(taskCtx context.Context) {
				s.resumeObjectiveReplan(taskCtx, task, initial, results)
			}); err != nil {
				return core.Task{}, err
			}
			return task, nil
		}
	}
	if task.Status == core.TaskFailed && taskFailedDuringDynamicReplan(snapshot, taskID) {
		initial, results, err := objectiveReplanStateForTask(snapshot, taskID)
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
		if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Retry objective after dynamic replan failure.", func(taskCtx context.Context) {
			s.resumeObjectiveReplan(taskCtx, task, initial, results)
		}); err != nil {
			return core.Task{}, err
		}
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
	if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Retry latest objective plan.", func(taskCtx context.Context) {
		s.retryTask(taskCtx, task, plan)
	}); err != nil {
		return core.Task{}, err
	}
	return task, nil
}

func (s *Service) resumeRecoveredRemoteTask(ctx context.Context, taskID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || isTerminalTaskStatus(task.Status) || taskHasActiveObjectiveWorkers(snapshot, taskID) {
		return
	}
	if taskExecutionMode(task) == executionModeLoop {
		s.runDurableLoopTask(ctx, task)
		return
	}
	hasActiveWorkers := taskHasActiveWorkers(snapshot, taskID)
	if plan, ok, err := retryPullRequestFollowUpPlan(snapshot, taskID); !hasActiveWorkers && err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	} else if !hasActiveWorkers && ok {
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
		s.retryPullRequestFollowUpTask(ctx, task, plan)
		return
	}
	initial, results, err := objectiveReplanStateForTask(snapshot, taskID)
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
	s.resumeObjectiveReplan(ctx, task, initial, results)
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

func taskHasActiveObjectiveWorkers(snapshot core.Snapshot, taskID string) bool {
	backgroundFollowUpWorkers := map[string]bool{}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID != taskID || isTerminalWorkerStatus(node.Status) {
			continue
		}
		if executionNodeIsBackgroundPullRequestFollowUp(node) {
			backgroundFollowUpWorkers[node.WorkerID] = true
			continue
		}
		return true
	}
	for _, activeWorker := range snapshot.Workers {
		if activeWorker.TaskID != taskID || isTerminalWorkerStatus(activeWorker.Status) {
			continue
		}
		if backgroundFollowUpWorkers[activeWorker.ID] {
			continue
		}
		return true
	}
	return false
}

func taskHasRunningObjectiveWorkers(snapshot core.Snapshot, taskID string) bool {
	terminalWorkItemWorkers := map[string]bool{}
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || item.WorkerID == "" {
			continue
		}
		switch item.Status {
		case core.WorkItemSucceeded, core.WorkItemFailed, core.WorkItemCanceled:
			terminalWorkItemWorkers[item.WorkerID] = true
		}
	}
	backgroundFollowUpWorkers := map[string]bool{}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID != taskID || (node.Status != core.WorkerQueued && node.Status != core.WorkerRunning) {
			continue
		}
		if terminalWorkItemWorkers[node.WorkerID] {
			continue
		}
		if executionNodeIsBackgroundPullRequestFollowUp(node) {
			backgroundFollowUpWorkers[node.WorkerID] = true
			continue
		}
		return true
	}
	for _, activeWorker := range snapshot.Workers {
		if activeWorker.TaskID != taskID || (activeWorker.Status != core.WorkerQueued && activeWorker.Status != core.WorkerRunning) {
			continue
		}
		if terminalWorkItemWorkers[activeWorker.ID] {
			continue
		}
		if backgroundFollowUpWorkers[activeWorker.ID] {
			continue
		}
		return true
	}
	return false
}

func executionNodeIsBackgroundPullRequestFollowUp(node core.ExecutionNode) bool {
	if boolMetadataFromJSON(node.Metadata, "backgroundPullRequestFollowUp") {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(node.Role), "github_pr_followup")
}

func boolMetadataFromJSON(raw json.RawMessage, key string) bool {
	if len(raw) == 0 {
		return false
	}
	metadata := map[string]any{}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return false
	}
	return boolMetadata(metadata, key)
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

type activeTaskWorkerIDStore interface {
	ActiveTaskWorkerIDs(ctx context.Context, taskID string) ([]string, error)
}

func (s *Service) activeTaskWorkerIDs(ctx context.Context, taskID string) ([]string, error) {
	if store, ok := s.store.(activeTaskWorkerIDStore); ok {
		return store.ActiveTaskWorkerIDs(ctx, taskID)
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return activeTaskWorkerIDs(snapshot, taskID), nil
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
	if err := s.setTaskStatusAllowingTerminalOverride(ctx, taskID, core.TaskPlanning, "retrying"); err != nil {
		return err
	}
	if err := s.updateTaskObjectiveAllowingTerminalOverride(ctx, taskID, core.ObjectiveActive, "retrying", "Retrying task."); err != nil {
		return err
	}
	return nil
}

func (s *Service) CancelWorker(ctx context.Context, workerID string) error {
	return s.cancelWorkerWithReason(ctx, workerID, taskCancelReasonUser)
}

func (s *Service) cancelWorkerWithReason(ctx context.Context, workerID string, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = taskCancelReasonUser
	}
	s.mu.Lock()
	cancel := s.cancels[workerID]
	remote := s.remoteRuns[workerID]
	taskID := s.tasks[workerID]
	if cancel != nil {
		s.workerCancelReasons[workerID] = reason
	}
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
		_ = s.markLiveWorkerCanceled(ctx, taskID, workerID, remote, reason)
	}
	return nil
}

func (s *Service) workerCancelReason(workerID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.TrimSpace(s.workerCancelReasons[workerID])
}

func workerCancelError(reason string) error {
	if strings.TrimSpace(reason) == taskCancelReasonSteeringRestart {
		return errors.New("worker canceled for steering restart")
	}
	return context.Canceled
}

func addWorkerCancelReason(payload map[string]any, reason string) map[string]any {
	reason = strings.TrimSpace(reason)
	if reason != "" {
		payload["reason"] = reason
	}
	return payload
}

func (s *Service) markLiveWorkerCanceled(ctx context.Context, taskID string, workerID string, remote remoteRun, reason string) error {
	if s.workerCompleted(ctx, taskID, workerID) {
		return nil
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = taskCancelReasonUser
	}
	errorText := "worker canceled by user request"
	if reason == taskCancelReasonSteeringRestart {
		errorText = "worker canceled for steering restart"
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
			"error":            errorText,
			"reason":           reason,
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
	_, ok, err := s.store.TaskStatus(ctx, taskID)
	if err != nil {
		return err
	}
	if !ok {
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
	activeWorkerIDs, err := s.activeTaskWorkerIDs(ctx, taskID)
	if err != nil {
		return err
	}
	for _, workerID := range activeWorkerIDs {
		if canceledWorkers[workerID] {
			continue
		}
		_ = s.CancelWorker(ctx, workerID)
	}
	if err := s.cancelTaskWorkItems(ctx, taskID); err != nil {
		return err
	}

	return s.setTaskStatusWithReason(ctx, taskID, core.TaskCanceled, taskCancelReasonUser)
}

func (s *Service) CancelWorkItem(ctx context.Context, taskID string, itemID string) error {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" {
		return eventstore.ErrNotFound
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, item := range snapshot.WorkItems {
		if item.ID != itemID || item.TaskID != taskID {
			continue
		}
		if item.Status != core.WorkItemQueued && item.Status != core.WorkItemRunning {
			return nil
		}
		workerID := strings.TrimSpace(item.WorkerID)
		if workerID == "" && item.TargetKind == "worker" {
			workerID = strings.TrimSpace(item.TargetID)
		}
		if workerID != "" {
			if err := s.CancelWorker(ctx, workerID); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
				return err
			}
		}
		if err := s.recordUserQuestionCanceled(ctx, taskID, item, workerID, "work item canceled by user request"); err != nil {
			return err
		}
		return s.recordWorkItemCompleted(ctx, taskID, item.ID, core.WorkItemCanceled, workerID, "work item canceled by user request")
	}
	return eventstore.ErrNotFound
}

func (s *Service) cancelTaskWorkItems(ctx context.Context, taskID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || (item.Status != core.WorkItemQueued && item.Status != core.WorkItemRunning) {
			continue
		}
		if err := s.recordUserQuestionCanceled(ctx, taskID, item, item.WorkerID, "task canceled by user request"); err != nil {
			return err
		}
		if err := s.recordWorkItemCompleted(ctx, taskID, item.ID, core.WorkItemCanceled, item.WorkerID, "task canceled by user request"); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) recordUserQuestionCanceled(ctx context.Context, taskID string, item core.WorkItem, workerID string, answer string) error {
	if item.Kind != "user.question" {
		return nil
	}
	questionID, ok := questionIDFromUserQuestionWorkItemID(item.ID)
	if !ok {
		return nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	var question core.Question
	for _, candidate := range snapshot.Questions {
		if candidate.ID == questionID && candidate.TaskID == taskID {
			question = candidate
			break
		}
	}
	if question.ID == "" || question.Decided {
		return nil
	}
	if strings.TrimSpace(workerID) == "" {
		workerID = question.WorkerID
	}
	approved := false
	_, err = s.append(ctx, core.Event{
		Type:     core.EventApprovalDecided,
		TaskID:   taskID,
		WorkerID: strings.TrimSpace(workerID),
		Payload: core.MustJSON(map[string]any{
			"approved":   approved,
			"answer":     nonEmpty(answer, "user question canceled"),
			"question":   question.Question,
			"questionId": question.ID,
			"reason":     "user_question_canceled",
			"workerId":   strings.TrimSpace(workerID),
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

func (s *Service) taskIsTerminal(ctx context.Context, taskID string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	return taskIsTerminalFromSnapshot(snapshot, taskID), nil
}

func taskIsTerminalFromSnapshot(snapshot core.Snapshot, taskID string) bool {
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return isTerminalTaskStatus(task.Status)
		}
	}
	return false
}

func (s *Service) taskArtifacts(ctx context.Context, taskID string) []core.TaskArtifact {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	return taskArtifactsFromSnapshot(snapshot, taskID)
}

func taskArtifactsFromSnapshot(snapshot core.Snapshot, taskID string) []core.TaskArtifact {
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
	return taskPullRequestStatesFromSnapshot(snapshot, taskID)
}

func taskPullRequestStatesFromSnapshot(snapshot core.Snapshot, taskID string) []ReplanPullRequestState {
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
	return isTerminalTaskStatus(task.Status)
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

func (s *Service) RecommendApplyPolicy(ctx context.Context, taskID string) (ApplyPolicyRecommendation, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ApplyPolicyRecommendation{}, err
	}
	if _, ok := findTask(snapshot, taskID); !ok {
		return ApplyPolicyRecommendation{}, eventstore.ErrNotFound
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
	normalizePlanShape(&plan)
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

	if len(plan.WorkItems) == 0 {
		return
	}
	if _, err := s.queuePlanWorkItems(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	started, err := s.startRunnableSpawnWorkItems(ctx, task.ID)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":             "plan_work_items",
		"status":           "queued",
		"reason":           "Initial plan queued durable objective work items.",
		"queuedWorkItems":  len(plan.WorkItems),
		"startedWorkCount": started,
	})
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
	failure := latestWorkerResultWithStatus(results, core.WorkerFailed)
	if failure.Status != core.WorkerFailed {
		return false
	}
	if exhaustion, ok := classifyProviderUsageExhaustion(failure.Kind, failure.Error, failure.Summary); ok {
		_ = s.waitForProviderCapacity(ctx, task.ID, failure.WorkerID, exhaustion)
		return true
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
			ok, reason, results := s.replanLoop(ctx, task, initial, results)
			if !ok {
				return true
			}
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   "worker_failure_recovery",
				"when":   "after_worker_failure",
				"reason": nonEmpty(reason, "Orchestrator selected a recovery result."),
				"status": "completed",
			})
			_ = s.completeTask(ctx, task.ID, results, "", reason)
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
	ok, reason, results := s.replanLoop(ctx, task, initial, results)
	if !ok {
		return true
	}
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":   "worker_failure_recovery",
		"when":   "after_worker_failure",
		"reason": nonEmpty(reason, "Orchestrator selected a recovery result."),
		"status": "completed",
	})
	_ = s.completeTask(ctx, task.ID, results, "", reason)
	return true
}

func (s *Service) handleWorkerSetFailureWithReplan(ctx context.Context, task core.Task, plan Plan, failedResults []WorkerTurnResult, allResults []WorkerTurnResult) bool {
	failure := latestWorkerResultWithStatus(failedResults, core.WorkerFailed)
	if failure.Status != core.WorkerFailed {
		return false
	}
	if s.recoverWorkerFailureWithReplan(ctx, task, plan, allResults, nil) {
		return true
	}
	return !s.finishOrContinueTask(ctx, task.ID, failure)
}

func (s *Service) handleWorkerQuestion(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, waiting WorkerTurnResult) {
	question := nonEmpty(waiting.Summary, waiting.Error, "worker requested orchestrator input")
	_ = s.recordUserActionNeeded(ctx, task.ID, waiting.WorkerID, "worker_needs_input", question, map[string]any{
		"summary": waiting.Summary,
		"error":   waiting.Error,
	})
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		if !s.userQuestionPending(ctx, task.ID, waiting.WorkerID) {
			return
		}
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
		s.recordLatestUserQuestionWorkItemCompleted(ctx, task.ID, waiting.WorkerID)
		normalizePlanShape(decision.Plan)
		if err := decision.Validate(); err != nil {
			_ = s.failTask(ctx, task.ID, fmt.Errorf("invalid question replan decision: %w", err))
			return
		}
		if decision.Plan.Metadata == nil {
			decision.Plan.Metadata = map[string]any{}
		}
		decision.Plan.Metadata["parentNodeID"] = waiting.NodeID
		decision.Plan.Metadata["questionWorkerID"] = waiting.WorkerID
		if shouldInheritLatestCandidateForPlan(*decision.Plan) {
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
		if len(decision.Plan.WorkItems) == 0 {
			return
		}
		if _, err := s.queuePlanWorkItems(ctx, task, *decision.Plan); err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return
		}
		started, err := s.startRunnableSpawnWorkItems(ctx, task.ID)
		if err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return
		}
		_ = s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":            "question_replan_work_items",
			"status":          "queued",
			"reason":          "Autonomous question answer queued durable work items.",
			"queuedWorkItems": len(decision.Plan.WorkItems),
			"startedWork":     started,
		})
	case "wait":
		if err := decision.Validate(); err != nil {
			_ = s.failTask(ctx, task.ID, fmt.Errorf("invalid question replan decision: %w", err))
			return
		}
		_ = s.waitForUserAction(ctx, task.ID, waiting.WorkerID, "orchestrator_wait", nonEmpty(decision.Message, decision.Rationale, question), map[string]any{
			"rationale": decision.Rationale,
		})
	case "complete":
		_ = s.completeTask(ctx, task.ID, results, "", decision.Rationale)
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
	questionID := latestPendingQuestionID(snapshot, taskID, waitingWorkerID)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventApprovalDecided,
		TaskID:   taskID,
		WorkerID: waitingWorkerID,
		Payload: core.MustJSON(map[string]any{
			"approved":   true,
			"answer":     feedback,
			"question":   question,
			"questionId": questionID,
			"reason":     "user_feedback",
		}),
	})
	s.recordLatestUserQuestionWorkItemCompleted(ctx, taskID, waitingWorkerID)
	s.resumeWaitingTaskWithAnswer(ctx, taskID, waitingWorkerID, question, feedback)
}

func (s *Service) resumeWaitingTaskWithAnswer(ctx context.Context, taskID string, waitingWorkerID string, question string, feedback string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskWaiting {
		return
	}
	if s.retryWaitingPublishPullRequestAction(ctx, task, snapshot) {
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
	if resumingPullRequestFollowUp(snapshot, taskID) {
		if pr, ok := latestPullRequestFollowUp(snapshot, taskID); ok {
			plan = canonicalizePullRequestFollowUpPlan(plan, pr)
		}
	}
	normalizePlanShape(&plan)
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
	if len(plan.WorkItems) == 0 {
		return
	}
	if _, err := s.queuePlanWorkItems(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if _, err := s.startRunnableSpawnWorkItems(ctx, taskID); err != nil {
		_ = s.failTask(ctx, taskID, err)
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
	normalizePlanShape(&plan)
	if err := plan.Validate(); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if pr, ok := latestPullRequestFollowUp(snapshot, taskID); ok {
		plan = canonicalizePullRequestFollowUpPlan(plan, pr)
	}
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if len(plan.WorkItems) == 0 {
		return
	}
	if _, err := s.queuePlanWorkItems(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if _, err := s.startRunnableSpawnWorkItems(ctx, taskID); err != nil {
		_ = s.failTask(ctx, taskID, err)
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
	initial, results, err := objectiveReplanStateForTask(snapshot, taskID)
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
	replanOK, completionReason, results := s.replanLoop(ctx, task, initial, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, taskID, results, "", completionReason)
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
	pending := pendingWorkerSteering(snapshot, taskID)
	if len(pending) == 0 {
		return
	}
	if s.resumeCodeReviewGateSteering(ctx, task, snapshot) {
		return
	}
	initial, results, err := objectiveReplanStateForTask(snapshot, taskID)
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
	replanOK, completionReason, results := s.replanLoop(ctx, task, initial, results)
	s.recordWorkerSteeringWorkItemsCompleted(ctx, taskID, pending)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, taskID, results, "", completionReason)
}

func (s *Service) resumeCodeReviewGateSteering(ctx context.Context, task core.Task, snapshot core.Snapshot) bool {
	steering, ok := firstPendingWorkerSteering(snapshot, task.ID)
	if !ok || steering.SpawnID != "code-review-gate" || strings.TrimSpace(steering.CandidateWorkerID) == "" {
		return false
	}
	phase := nonEmpty(steering.ReviewPhase, "completion")
	if phase != "completion" && phase != "intermediate" {
		return false
	}
	initial, results, err := objectiveReplanStateForTask(snapshot, task.ID)
	if err != nil {
		_ = s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   "worker_steering_queue",
			"status": "waiting",
			"reason": "queued code review steering could not resume automatically",
			"error":  err.Error(),
		})
		return true
	}
	candidate, ok := workerResultByID(results, steering.CandidateWorkerID)
	if !ok {
		_ = s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":              "worker_steering_queue",
			"status":            "waiting",
			"reason":            "queued code review steering references a missing candidate",
			"candidateWorkerId": steering.CandidateWorkerID,
			"workerId":          steering.WorkerID,
		})
		return true
	}
	project, err := s.projectForTask(task)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return true
	}
	policy := normalizedReviewPolicy(project.ReviewPolicy)
	workerKind := s.preferredWorkerKindFromSteering(steering.Message)
	plan := s.codeReviewGatePlan(task, candidate, policy, phase, workerKind)
	plan = annotateWorkerSteeringPlan(plan, steering)
	plan.Rationale = "retrying failed code review gate with user steering"
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return true
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = "worker_steering"
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return true
	}
	_ = s.recordWorkItemCompleted(ctx, task.ID, workerSteeringWorkItemID(steering.EventID), core.WorkItemSucceeded, steering.WorkerID, "")
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		result = failedFollowUpResult(plan, err)
	}
	results = append(results, result)
	if result.Status != core.WorkerSucceeded {
		reason := nonEmpty(result.Error, result.Summary, "code review worker did not complete successfully")
		_ = s.recordCodeReviewGateResult(ctx, task.ID, candidate.WorkerID, phase, result, "failed", reason)
		_ = s.waitForUserAction(ctx, task.ID, result.WorkerID, "code_review_gate", "Code review worker failed before it could approve or reject publication.\n\n"+reason+"\n\nSteer the failed review worker to retry the review, choose a different review provider, or steer the task to take another path.", map[string]any{
			"candidateWorkerId": candidate.WorkerID,
			"reviewWorkerId":    result.WorkerID,
			"phase":             phase,
			"error":             reason,
		})
		return true
	}
	if codeReviewBlocksPublication(result, policy) {
		reason := nonEmpty(result.Summary, "code review requested changes")
		_ = s.recordCodeReviewGateResult(ctx, task.ID, candidate.WorkerID, phase, result, "blocked", reason)
		if handled, recoverErr := s.recoverCodeReviewBlockedCandidate(ctx, task.ID, results, candidate.WorkerID, reason); handled && recoverErr != nil {
			_ = s.failTask(ctx, task.ID, recoverErr)
		}
		return true
	}
	reason := nonEmpty(result.Summary, "code review approved publication")
	_ = s.recordCodeReviewGateResult(ctx, task.ID, candidate.WorkerID, phase, result, "passed", reason)
	if phase == "intermediate" {
		action, ok := latestPublishPullRequestPlanActionForCandidate(snapshot, task.ID, results, candidate.WorkerID)
		if !ok {
			_ = s.waitForUserAction(ctx, task.ID, result.WorkerID, "code_review_gate", "Code review approved publication, but aged could not find the interrupted intermediate pull request action to resume. Steer the task to publish the approved candidate or continue another path.", map[string]any{
				"candidateWorkerId": candidate.WorkerID,
				"reviewWorkerId":    result.WorkerID,
				"phase":             phase,
			})
			return true
		}
		ok, results, err := s.executePlanAction(ctx, task, action, results)
		if err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return true
		}
		if !ok {
			return true
		}
		replanOK, completionReason, results := s.replanLoop(ctx, task, initial, results)
		if !replanOK {
			return true
		}
		_ = s.completeTask(ctx, task.ID, results, "", completionReason)
		return true
	}
	_ = s.completeTask(ctx, task.ID, results, candidate.WorkerID, reason)
	return true
}

func latestPublishPullRequestPlanActionForCandidate(snapshot core.Snapshot, taskID string, results []WorkerTurnResult, candidateWorkerID string) (PlanAction, bool) {
	candidateWorkerID = strings.TrimSpace(candidateWorkerID)
	if candidateWorkerID == "" {
		return PlanAction{}, false
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventTaskPlanned || event.TaskID != taskID {
			continue
		}
		var plan Plan
		if err := json.Unmarshal(event.Payload, &plan); err != nil {
			continue
		}
		for j := len(plan.Actions) - 1; j >= 0; j-- {
			action := plan.Actions[j]
			if strings.TrimSpace(action.Kind) != "publish_pull_request" {
				continue
			}
			if planActionWorkerID(results, action.WorkerID) == candidateWorkerID {
				return action, true
			}
		}
	}
	return PlanAction{}, false
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
	normalizePlanShape(&plan)
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
	if len(plan.WorkItems) == 0 {
		return
	}
	if _, err := s.queuePlanWorkItems(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if _, err := s.startRunnableSpawnWorkItems(ctx, task.ID); err != nil {
		_ = s.failTask(ctx, task.ID, err)
	}
}

func (s *Service) retryPullRequestFollowUpTask(ctx context.Context, task core.Task, plan Plan) {
	plan = s.ensurePullRequestFollowUpRetryWorkItem(plan, task)
	normalizePlanShape(&plan)
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
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":      "pull_request_background_followup",
		"status":    "queued",
		"reason":    "interrupted pull request follow-up was requeued as durable work items",
		"workItems": len(plan.WorkItems),
	})
	if len(plan.WorkItems) == 0 {
		return
	}
	if _, err := s.queuePlanWorkItems(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if _, err := s.startRunnableSpawnWorkItems(ctx, task.ID); err != nil {
		_ = s.failTask(ctx, task.ID, err)
	}
}

func (s *Service) ensurePullRequestFollowUpRetryWorkItem(plan Plan, task core.Task) Plan {
	if len(plan.WorkItems) > 0 || !boolMetadata(plan.Metadata, "backgroundPullRequestFollowUp") {
		return plan
	}
	pullRequestID := nonEmpty(stringMetadata(plan.Metadata, "pullRequestID"), stringMetadata(plan.Metadata, "pullRequestId"))
	pullRequestURL := nonEmpty(stringMetadata(plan.Metadata, "pullRequestURL"), stringMetadata(plan.Metadata, "url"))
	metadata := copyPlanMetadata(plan.Metadata)
	metadata["sourceAction"] = "plan"
	metadata["backgroundPullRequestFollowUp"] = true
	metadata["executeActionsOnSuccess"] = true
	metadata["pullRequestID"] = pullRequestID
	metadata["url"] = pullRequestURL
	if _, ok := metadata["planActions"]; !ok && len(plan.Actions) > 0 {
		metadata["planActions"] = plan.Actions
	}
	itemID := "retry_pr_followup"
	if pullRequestID != "" {
		itemID = "retry_" + pullRequestID
	}
	plan.WorkItems = []WorkItemRequest{{
		ID:         itemID,
		Kind:       "pr.followup",
		TargetKind: "pull_request",
		TargetID:   nonEmpty(pullRequestID, task.ID),
		Reason:     "Retry failed pull request follow-up work.",
		Prompt:     nonEmpty(plan.Prompt, task.Prompt, "Retry pull request follow-up work."),
		WorkerKind: nonEmpty(plan.WorkerKind, stringMetadata(plan.Metadata, "workerKind"), s.pullRequestFollowUpWorkerKind()),
		Metadata:   metadata,
	}}
	plan.WorkerKind = ""
	plan.Prompt = ""
	plan.Actions = nil
	return plan
}

func (s *Service) resumeObjectiveReplan(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) {
	replanOK, completionReason, results := s.replanLoop(ctx, task, initial, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, task.ID, results, "", completionReason)
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
	plannedResumeSession := strings.TrimSpace(resumeSessionID) != ""
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
	sharedWorkspace, err := s.prepareSharedWorkspace(ctx, workspaceSpec)
	if err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, fmt.Errorf("prepare shared artifact workspace: %w", err)
	}
	workspace = applySharedWorkspace(workspace, sharedWorkspace)
	plan.Metadata["sharedWorkspace"] = sharedWorkspace.Root
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
	allowCreateTaskCallbacks := planAllowsCreateTaskCallbacks(plan)
	helperPath, callbackDir, err := installLocalCreateTaskHelper(workspace, allowCreateTaskCallbacks)
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
	if reusedWorkspace {
		if !capabilities.ResumeSession {
			resumeSessionID = ""
			delete(plan.Metadata, "retryResumeSessionID")
			if plannedResumeSession {
				s.restoreDurableLoopFullPromptForDegradedResume(ctx, task, &plan)
			}
		}
	} else {
		resumeSessionID = ""
		delete(plan.Metadata, "retryResumeSessionID")
		if retryFromWorkerID != "" && plannedResumeSession {
			s.restoreDurableLoopFullPromptForDegradedResume(ctx, task, &plan)
		}
	}
	prompt := workerExecutionPrompt(plan.Prompt, workspace, allowCreateTaskCallbacks)
	if reusedWorkspace {
		prompt = retryWorkerExecutionPrompt(prompt, retryFromWorkerID, resumeSessionID, retrySteering, stringMetadata(plan.Metadata, "retryContextKind"))
	} else if retryFromWorkerID != "" && len(retrySteering) > 0 {
		prompt = retryWorkerExecutionPrompt(prompt, retryFromWorkerID, "", retrySteering, stringMetadata(plan.Metadata, "retryContextKind"))
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
		Env:             workspaceSharedEnv(workspace),
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
	workItemID, err := s.recordPlanWorkItemStarted(ctx, task.ID, workerID, plan)
	if err != nil {
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
		delete(s.workerCancelReasons, workerID)
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
		cancelReason := ""
		if errors.Is(workerCtx.Err(), context.Canceled) {
			status = core.WorkerCanceled
			workspaceResult = WorkspaceResultCanceled
			cancelReason = s.workerCancelReason(workerID)
			err = workerCancelError(cancelReason)
		} else if exhaustion, ok := classifyProviderUsageExhaustion(plan.WorkerKind, runState.failureText(err)); ok {
			err = errors.New(nonEmpty(exhaustion.Detail, exhaustion.Summary))
		}
		changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
		if s.workerCompleted(context.Background(), task.ID, workerID) {
			status = core.WorkerCanceled
			cancelReason = s.workerCancelReason(workerID)
			err = workerCancelError(cancelReason)
		} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, addWorkerCancelReason(runState.completionPayload(status, err, changes), cancelReason)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		s.drainLocalWorkerCallbacks(ctx, task.ID, workerID, callbackDir)
		_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, workspaceResult)
		s.cleanupTerminalWorkspaceArtifacts(ctx, task.ID, workerID, workspace, workspaceResult)
		result := runState.turnResult(workerID, plan, status, err, changes)
		if status == core.WorkerFailed {
			if fallback, handled, fallbackErr := s.runProviderUsageFallback(ctx, task, plan, result); handled {
				return fallback, fallbackErr
			}
		}
		if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, task.ID, workItemID, workerID, status, err); completeErr != nil {
			return WorkerTurnResult{}, completeErr
		}
		return result, nil
	}

	if runState.isWaitingForInput() {
		changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
		status := core.WorkerWaiting
		var statusErr error
		if exhaustion, ok := classifyProviderUsageExhaustion(plan.WorkerKind, runState.failureText(nil)); ok {
			status = core.WorkerFailed
			statusErr = errors.New(exhaustion.Summary)
		}
		if s.workerCompleted(context.Background(), task.ID, workerID) {
			status = core.WorkerCanceled
			statusErr = context.Canceled
		} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(status, statusErr, changes)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		s.drainLocalWorkerCallbacks(ctx, task.ID, workerID, callbackDir)
		result := runState.turnResult(workerID, plan, status, statusErr, changes)
		if status == core.WorkerFailed {
			if fallback, handled, fallbackErr := s.runProviderUsageFallback(ctx, task, plan, result); handled {
				return fallback, fallbackErr
			}
		}
		if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, task.ID, workItemID, workerID, status, statusErr); completeErr != nil {
			return WorkerTurnResult{}, completeErr
		}
		return result, nil
	}

	changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
	status := core.WorkerSucceeded
	var statusErr error
	status, statusErr = runState.normalizeCompletionStatus(plan, status, statusErr, changes)
	if status == core.WorkerFailed {
		if exhaustion, ok := classifyProviderUsageExhaustion(plan.WorkerKind, runState.failureText(statusErr)); ok {
			statusErr = errors.New(nonEmpty(exhaustion.Detail, exhaustion.Summary))
		}
	}
	if s.workerCompleted(context.Background(), task.ID, workerID) {
		status = core.WorkerCanceled
		statusErr = context.Canceled
	} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(status, statusErr, changes)); completionErr != nil {
		return WorkerTurnResult{}, completionErr
	}
	_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
	s.drainLocalWorkerCallbacks(ctx, task.ID, workerID, callbackDir)
	workspaceResult := WorkspaceResultSucceeded
	if status == core.WorkerFailed {
		workspaceResult = WorkspaceResultFailed
	}
	_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, workspaceResult)
	s.cleanupTerminalWorkspaceArtifacts(ctx, task.ID, workerID, workspace, workspaceResult)
	result := runState.turnResult(workerID, plan, status, statusErr, changes)
	if status == core.WorkerFailed {
		if fallback, handled, fallbackErr := s.runProviderUsageFallback(ctx, task, plan, result); handled {
			return fallback, fallbackErr
		}
	}
	if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, task.ID, workItemID, workerID, status, statusErr); completeErr != nil {
		return WorkerTurnResult{}, completeErr
	}
	return result, nil
}

func (s *Service) rebalancePlanWorkerKind(ctx context.Context, plan Plan) Plan {
	if s == nil || s.usageSource == nil {
		return plan
	}
	if boolMetadata(plan.Metadata, "workerKindPinned") {
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
	shouldSwitch := false
	reason := ""
	switch {
	case currentOK && !currentUsage.Available:
		shouldSwitch = true
		reason = fmt.Sprintf("%s usage monitor reports unavailable", kind)
	case currentKnown && currentPressure >= 100:
		shouldSwitch = true
		if alternateKnown {
			reason = fmt.Sprintf("%s usage pressure %d%%, %s usage pressure %d%%", kind, currentPressure, alternate, alternatePressure)
		} else {
			reason = fmt.Sprintf("%s usage pressure %d%%; %s usage pressure unknown", kind, currentPressure, alternate)
		}
	case currentOK && alternateOK && currentKnown && alternateKnown && alternatePressure+providerUsageSwitchMargin <= currentPressure:
		shouldSwitch = true
		reason = fmt.Sprintf("%s usage pressure %d%%, %s usage pressure %d%%", kind, currentPressure, alternate, alternatePressure)
	}
	if !shouldSwitch {
		return plan
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["usageAwareScheduling"] = true
	plan.Metadata["usageOriginalWorkerKind"] = kind
	plan.Metadata["usageSelectedWorkerKind"] = alternate
	plan.Metadata["usageSelectionReason"] = reason
	if currentKnown {
		plan.Metadata["usageCurrentPressure"] = currentPressure
	}
	if alternateKnown {
		plan.Metadata["usageAlternatePressure"] = alternatePressure
	}
	plan.WorkerKind = alternate
	return plan
}

func (s *Service) runProviderUsageFallback(ctx context.Context, task core.Task, plan Plan, result WorkerTurnResult) (WorkerTurnResult, bool, error) {
	exhaustion, ok := classifyProviderUsageExhaustion(result.Kind, result.Error, result.Summary)
	if !ok {
		return WorkerTurnResult{}, false, nil
	}
	alternate := alternateProviderKind(result.Kind)
	if alternate == "" || s.runners[alternate] == nil || boolMetadata(plan.Metadata, "usageFallbackAttempt") {
		return WorkerTurnResult{}, false, nil
	}
	fallbackPlan := plan
	fallbackPlan.WorkerKind = alternate
	fallbackPlan.Metadata = copyPlanMetadata(plan.Metadata)
	delete(fallbackPlan.Metadata, "nodeID")
	delete(fallbackPlan.Metadata, "planID")
	delete(fallbackPlan.Metadata, "targetID")
	delete(fallbackPlan.Metadata, "targetKind")
	delete(fallbackPlan.Metadata, "remoteSession")
	delete(fallbackPlan.Metadata, "remoteRunDir")
	delete(fallbackPlan.Metadata, "remoteWorkDir")
	fallbackPlan.Metadata["usageFallbackAttempt"] = true
	fallbackPlan.Metadata["usageFallbackFromProvider"] = result.Kind
	fallbackPlan.Metadata["usageFallbackToProvider"] = alternate
	fallbackPlan.Metadata["usageFallbackFromWorkerID"] = result.WorkerID
	fallbackPlan.Metadata["usageFallbackReason"] = exhaustion.Summary
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":     "provider_usage_fallback",
		"status":   "started",
		"reason":   "Worker provider usage was exhausted; retrying the same plan on the alternate provider.",
		"provider": nonEmpty(exhaustion.Provider, result.Kind),
		"fromKind": result.Kind,
		"toKind":   alternate,
		"workerId": result.WorkerID,
		"error":    nonEmpty(exhaustion.Detail, result.Error, result.Summary),
	})
	fallback, err := s.runPlannedWorker(ctx, task, fallbackPlan)
	status := "completed"
	if err != nil || fallback.Status != core.WorkerSucceeded {
		status = "failed"
	}
	_ = s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":       "provider_usage_fallback",
		"status":     status,
		"reason":     "Alternate provider usage fallback finished.",
		"provider":   nonEmpty(exhaustion.Provider, result.Kind),
		"fromKind":   result.Kind,
		"toKind":     alternate,
		"workerId":   result.WorkerID,
		"fallbackId": fallback.WorkerID,
		"error":      errorString(err),
	})
	return fallback, true, err
}

func alternateProviderKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "claude":
		return "codex"
	case "codex":
		return "claude"
	default:
		return ""
	}
}

func (s *Service) waitForProviderCapacity(ctx context.Context, taskID string, workerID string, exhaustion ProviderUsageExhaustion) error {
	summary := exhaustion.Summary
	if summary == "" {
		summary = "Model provider usage is exhausted."
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "provider_usage_exhausted", summary); err != nil {
		return err
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskWaiting); err != nil {
		return err
	}
	return s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":     "provider_usage_exhausted",
		"status":   "waiting_external",
		"reason":   summary,
		"provider": exhaustion.Provider,
		"workerId": workerID,
		"error":    exhaustion.Detail,
	})
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

func (s *Service) prepareSharedWorkspace(ctx context.Context, spec WorkspaceSpec) (SharedWorkspace, error) {
	if preparer, ok := s.workspaces.(sharedWorkspacePreparer); ok {
		return preparer.PrepareShared(ctx, spec)
	}
	return prepareSharedWorkspaceAt("", spec)
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
	plannedResumeSession := strings.TrimSpace(resumeSessionID) != ""
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
	sharedWorkspace, err := s.sshRunner.PrepareSharedWorkspace(ctx, target, task.ID, workerID)
	if err != nil {
		return WorkerTurnResult{}, fmt.Errorf("prepare remote shared artifact workspace: %w", err)
	}
	remoteRun.SharedRoot = sharedWorkspace.Root
	remoteRun.SharedArtifactsDir = sharedWorkspace.ArtifactsDir
	remoteRun.SharedWorkerDir = sharedWorkspace.WorkerDir
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
		Root:               remoteWorkDir,
		CWD:                remoteWorkDir,
		SourceRoot:         remoteSourceDir,
		Mode:               "remote",
		VCSType:            "ssh",
		WorkerID:           workerID,
		TaskID:             task.ID,
		TargetID:           target.ID,
		TargetKind:         string(target.Kind),
		SharedRoot:         sharedWorkspace.Root,
		SharedArtifactsDir: sharedWorkspace.ArtifactsDir,
		SharedWorkerDir:    sharedWorkspace.WorkerDir,
	}
	capabilities := worker.RunnerCapabilities(runner)
	capabilities.LiveSteering = false
	if reusedWorkspace {
		if !capabilities.ResumeSession {
			resumeSessionID = ""
			delete(plan.Metadata, "retryResumeSessionID")
			if plannedResumeSession {
				s.restoreDurableLoopFullPromptForDegradedResume(ctx, task, &plan)
			}
		}
	} else if retryFromWorkerID != "" && plannedResumeSession {
		s.restoreDurableLoopFullPromptForDegradedResume(ctx, task, &plan)
	}
	spec := worker.Spec{
		ID:              workerID,
		TaskID:          task.ID,
		Kind:            plan.WorkerKind,
		Prompt:          remoteWorkerExecutionPrompt(plan.Prompt, workspace, planAllowsCreateTaskCallbacks(plan)),
		WorkDir:         remoteWorkDir,
		ResumeSessionID: resumeSessionID,
		ReasoningEffort: plan.ReasoningEffort,
		TargetID:        target.ID,
		TargetKind:      string(target.Kind),
		Env:             workspaceSharedEnv(workspace),
	}
	if reusedWorkspace {
		spec.ResumeSessionID = resumeSessionID
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
	plan.Metadata["sharedWorkspace"] = sharedWorkspace.Root

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
	workItemID, err := s.recordPlanWorkItemStarted(ctx, task.ID, workerID, plan)
	if err != nil {
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
		delete(s.workerCancelReasons, workerID)
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
		s.cleanupTerminalWorkspaceArtifacts(ctx, task.ID, workerID, workspace, WorkspaceResultFailed)
		if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, task.ID, workItemID, workerID, core.WorkerFailed, err); completeErr != nil {
			return WorkerTurnResult{}, completeErr
		}
		return runState.turnResult(workerID, plan, core.WorkerFailed, err, changes), nil
	}
	if err := s.sshRunner.Start(workerCtx, remoteRun, command, stdin); err != nil {
		changes := remoteWorkerStartFailureChanges(workspace)
		if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, runState.completionPayload(core.WorkerFailed, err, changes)); completionErr != nil {
			return WorkerTurnResult{}, completionErr
		}
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		s.cleanupTerminalWorkspaceArtifacts(ctx, task.ID, workerID, workspace, WorkspaceResultFailed)
		if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, task.ID, workItemID, workerID, core.WorkerFailed, err); completeErr != nil {
			return WorkerTurnResult{}, completeErr
		}
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
	cancelReason := ""
	if errors.Is(workerCtx.Err(), context.Canceled) {
		workerStatus = core.WorkerCanceled
		cancelReason = s.workerCancelReason(workerID)
		statusErr = workerCancelError(cancelReason)
	}
	changes := s.sshRunner.DescribeChanges(ctx, remoteRun)
	workerStatus, statusErr = runState.normalizeCompletionStatus(plan, workerStatus, statusErr, changes)
	if workerStatus == core.WorkerFailed {
		if exhaustion, ok := classifyProviderUsageExhaustion(plan.WorkerKind, runState.failureText(statusErr)); ok {
			statusErr = errors.New(nonEmpty(exhaustion.Detail, exhaustion.Summary))
		}
	}
	if s.workerCompleted(context.Background(), task.ID, workerID) {
		workerStatus = core.WorkerCanceled
		cancelReason = s.workerCancelReason(workerID)
		statusErr = workerCancelError(cancelReason)
	} else if completionErr := s.appendWorkerCompleted(ctx, task.ID, workerID, addWorkerCancelReason(runState.completionPayload(workerStatus, statusErr, changes), cancelReason)); completionErr != nil {
		return WorkerTurnResult{}, completionErr
	}
	_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
	sshRunner.CallbackHandler = s.handleRemoteWorkerCallbacks
	if err := sshRunner.drainRemoteCallbacks(ctx, remoteRun, sink); err != nil {
		_ = sink.Event(ctx, worker.Event{Kind: worker.EventError, Stream: "stderr", Text: "failed to drain terminal remote worker callbacks: " + err.Error()})
	}
	s.cleanupTerminalWorkspaceArtifacts(ctx, task.ID, workerID, workspace, workspaceResultForWorkerStatus(workerStatus))
	result := runState.turnResult(workerID, plan, workerStatus, statusErr, changes)
	if workerStatus == core.WorkerFailed {
		if fallback, handled, fallbackErr := s.runProviderUsageFallback(ctx, task, plan, result); handled {
			return fallback, fallbackErr
		}
	}
	if completeErr := s.recordPlanWorkItemCompletedForWorker(ctx, task.ID, workItemID, workerID, workerStatus, statusErr); completeErr != nil {
		return WorkerTurnResult{}, completeErr
	}
	return result, nil
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
		case "update_pull_request":
			if err := s.handleWorkerUpdatePullRequestCallback(ctx, run.TaskID, run.WorkerID, callback, "remote"); err != nil {
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
	if pr, ok := s.createTaskCallbackTargetsParentPullRequest(ctx, run.TaskID, callback); ok {
		return s.recordIgnoredPullRequestCreateTaskCallback(ctx, run.TaskID, run.WorkerID, callback.ID, "remote", pr)
	}
	if !s.workerAllowsCreateTaskCallbacks(ctx, run.TaskID, run.WorkerID) {
		return s.recordIgnoredCreateTaskCallback(ctx, run.TaskID, run.WorkerID, callback.ID, "remote")
	}
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
		case "update_pull_request":
			if err := s.handleWorkerUpdatePullRequestCallback(ctx, taskID, workerID, callback, "local"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported local worker callback %q from %s", callback.Type, callback.ID)
		}
	}
	return nil
}

func (s *Service) handleLocalCreateTaskCallback(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback) error {
	if pr, ok := s.createTaskCallbackTargetsParentPullRequest(ctx, taskID, callback); ok {
		return s.recordIgnoredPullRequestCreateTaskCallback(ctx, taskID, workerID, callback.ID, "local", pr)
	}
	if !s.workerAllowsCreateTaskCallbacks(ctx, taskID, workerID) {
		return s.recordIgnoredCreateTaskCallback(ctx, taskID, workerID, callback.ID, "local")
	}
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

func (s *Service) createTaskCallbackTargetsParentPullRequest(ctx context.Context, taskID string, callback RemoteWorkerCallback) (core.PullRequest, bool) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false
	}
	text := strings.TrimSpace(callback.Title + "\n" + callback.Prompt)
	if text == "" {
		return core.PullRequest{}, false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID {
			continue
		}
		if pullRequestReferenceMatchesText(pr, text) {
			return pr, true
		}
	}
	return core.PullRequest{}, false
}

func pullRequestReferenceMatchesText(pr core.PullRequest, text string) bool {
	if pr.ID != "" && strings.Contains(text, pr.ID) {
		return true
	}
	if pr.URL != "" && strings.Contains(text, pr.URL) {
		return true
	}
	for _, value := range githubPullRequestURLRE.FindAllString(text, -1) {
		repo, number := parsePullRequestURL(value)
		if pullRequestRefMatches(pr, repo, number) {
			return true
		}
	}
	for _, match := range pullRequestRepoNumberRE.FindAllStringSubmatch(text, -1) {
		number, err := strconv.Atoi(match[2])
		if err == nil && pullRequestRefMatches(pr, match[1], number) {
			return true
		}
	}
	for _, match := range pullRequestBareNumberRE.FindAllStringSubmatch(text, -1) {
		number, err := strconv.Atoi(match[1])
		if err == nil && pr.Number > 0 && pr.Number == number {
			return true
		}
	}
	return false
}

func pullRequestRefMatches(pr core.PullRequest, repo string, number int) bool {
	return number > 0 && pr.Number == number && (repo == "" || strings.EqualFold(pr.Repo, repo))
}

func (s *Service) workerAllowsCreateTaskCallbacks(ctx context.Context, taskID string, workerID string) bool {
	metadata := s.recordedWorkerMetadata(ctx, taskID, workerID)
	if boolMetadata(metadata, "backgroundPullRequestFollowUp") || boolMetadata(metadata, "disableCreateTaskCallbacks") {
		return false
	}
	return true
}

func (s *Service) recordedWorkerMetadata(ctx context.Context, taskID string, workerID string) map[string]any {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.WorkerID != workerID {
			continue
		}
		if event.Type != core.EventWorkerCreated && event.Type != core.EventExecutionPlanned {
			continue
		}
		var payload struct {
			Metadata map[string]any `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || len(payload.Metadata) == 0 {
			continue
		}
		return payload.Metadata
	}
	return nil
}

func (s *Service) recordIgnoredCreateTaskCallback(ctx context.Context, taskID string, workerID string, callbackID string, source string) error {
	reason := "ignored " + source + " create-task callback from pull request follow-up worker; PR follow-up must stay on the parent task"
	_, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":       "log",
			"stream":     "stderr",
			"text":       reason,
			"callbackId": callbackID,
		}),
	})
	return err
}

func (s *Service) recordIgnoredPullRequestCreateTaskCallback(ctx context.Context, taskID string, workerID string, callbackID string, source string, pr core.PullRequest) error {
	target := pr.ID
	if pr.Repo != "" && pr.Number > 0 {
		target = fmt.Sprintf("%s#%d", pr.Repo, pr.Number)
	}
	reason := "ignored " + source + " create-task callback for tracked pull request " + target + "; PR follow-up must stay on the parent task"
	_, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":          "log",
			"stream":        "stderr",
			"text":          reason,
			"callbackId":    callbackID,
			"pullRequestId": pr.ID,
			"url":           pr.URL,
		}),
	})
	return err
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

func (s *Service) handleWorkerUpdatePullRequestCallback(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback, source string) error {
	title := strings.TrimSpace(callback.Title)
	body := strings.TrimSpace(callback.Body)
	if title == "" && body == "" {
		return fmt.Errorf("%s worker callback %s has empty pull request title and body", source, callback.ID)
	}
	pr, err := s.pullRequestForWorkerUpdateCallback(ctx, taskID, callback)
	if err != nil {
		return fmt.Errorf("resolve pull request for %s update callback %s: %w", source, callback.ID, err)
	}
	req := core.PublishPullRequestRequest{
		WorkerID:        workerID,
		Repo:            strings.TrimSpace(callback.Repo),
		Base:            strings.TrimSpace(callback.Base),
		Branch:          strings.TrimSpace(callback.Branch),
		Title:           title,
		Body:            body,
		FeedbackComment: strings.TrimSpace(callback.Comment),
		MetadataOnly:    true,
	}
	inputs := map[string]any{
		"id":           pr.ID,
		"repo":         nonEmpty(req.Repo, pr.Repo),
		"number":       firstNonZero(callback.Number, pr.Number),
		"url":          nonEmpty(callback.URL, pr.URL),
		"branch":       nonEmpty(req.Branch, pr.Branch),
		"base":         nonEmpty(req.Base, pr.Base),
		"title":        title,
		"body":         body,
		"comment":      req.FeedbackComment,
		"metadataOnly": true,
	}
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":          "update_pull_request",
		"when":          "worker_callback",
		"source":        source,
		"callbackId":    callback.ID,
		"workerId":      workerID,
		"pullRequestId": pr.ID,
		"inputs":        inputs,
		"status":        "started",
	}); err != nil {
		return err
	}
	updated, err := s.UpdateTaskPullRequest(ctx, taskID, pr, req)
	if err != nil {
		if errors.Is(err, errTerminalPullRequest) || errors.Is(err, errPullRequestTargetMismatch) {
			errorText := "pull request is already closed or merged; publish a fresh pull request for new candidate work"
			if errors.Is(err, errPullRequestTargetMismatch) {
				errorText = err.Error()
			}
			return s.recordTaskAction(ctx, taskID, map[string]any{
				"kind":          "update_pull_request",
				"when":          "worker_callback",
				"source":        source,
				"callbackId":    callback.ID,
				"workerId":      workerID,
				"pullRequestId": pr.ID,
				"inputs":        inputs,
				"status":        "skipped",
				"error":         errorText,
			})
		}
		return fmt.Errorf("update pull request from %s callback %s: %w", source, callback.ID, err)
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	if err := s.commentPullRequestFeedbackAddressed(ctx, task, pr, updated, req, workerID); err != nil {
		_ = s.recordTaskAction(ctx, taskID, map[string]any{
			"kind":          "pull_request_feedback_comment",
			"when":          "worker_callback",
			"source":        source,
			"callbackId":    callback.ID,
			"workerId":      workerID,
			"pullRequestId": updated.ID,
			"url":           updated.URL,
			"status":        "failed",
			"error":         err.Error(),
		})
	}
	if err := s.markPullRequestFeedbackTriggered(ctx, updated); err != nil {
		return err
	}
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":          "update_pull_request",
		"when":          "worker_callback",
		"source":        source,
		"callbackId":    callback.ID,
		"workerId":      workerID,
		"pullRequestId": updated.ID,
		"url":           updated.URL,
		"inputs":        inputs,
	}); err != nil {
		return err
	}
	_, err = s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":          "log",
			"stream":        "stdout",
			"text":          source + " worker updated pull request metadata: " + updated.URL,
			"callbackId":    callback.ID,
			"pullRequestId": updated.ID,
			"url":           updated.URL,
		}),
	})
	return err
}

func (s *Service) pullRequestForWorkerUpdateCallback(ctx context.Context, taskID string, callback RemoteWorkerCallback) (core.PullRequest, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	var fallback core.PullRequest
	fallbackCount := 0
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID || isTerminalPullRequestState(pr.State) {
			continue
		}
		if pullRequestMatchesUpdateTarget(pr, "", strings.TrimSpace(callback.Repo), callback.Number, strings.TrimSpace(callback.URL), strings.TrimSpace(callback.Branch)) {
			return pr, nil
		}
		fallback = pr
		fallbackCount++
	}
	if fallbackCount == 1 && strings.TrimSpace(callback.Repo) == "" && callback.Number == 0 && strings.TrimSpace(callback.URL) == "" && strings.TrimSpace(callback.Branch) == "" {
		return fallback, nil
	}
	return core.PullRequest{}, eventstore.ErrNotFound
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
	req := core.PublishPullRequestRequest{
		WorkerID:             publishWorkerID,
		Repo:                 strings.TrimSpace(callback.Repo),
		Base:                 strings.TrimSpace(callback.Base),
		Branch:               strings.TrimSpace(callback.Branch),
		Title:                strings.TrimSpace(callback.Title),
		Body:                 body,
		Draft:                callback.Draft,
		ContinueAfterPublish: callback.ContinueAfterPublish,
	}
	if ready, err := s.workerPublishCallbackRequestReady(ctx, taskID, publishWorkerID, callback, source, req); err != nil {
		return err
	} else if !ready {
		return nil
	}
	pr, err := s.PublishTaskPullRequest(ctx, taskID, req)
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

func (s *Service) workerPublishCallbackRequestReady(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback, source string, req core.PublishPullRequestRequest) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return false, eventstore.ErrNotFound
	}
	if err := validatePullRequestPublicationRequest(task, req); err == nil {
		return true, nil
	} else {
		reason := err.Error()
		if recordErr := s.recordSkippedWorkerPublishPullRequestCallback(ctx, taskID, workerID, callback, source, reason); recordErr != nil {
			return false, recordErr
		}
		return false, nil
	}
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
	if err := s.recordSkippedWorkerPublishPullRequestCallback(ctx, taskID, workerID, callback, source, reason); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Service) recordSkippedWorkerPublishPullRequestCallback(ctx context.Context, taskID string, workerID string, callback RemoteWorkerCallback, source string, reason string) error {
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
		return err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":       "log",
			"stream":     "stdout",
			"text":       source + " worker skipped pull request publication: " + reason,
			"callbackId": callback.ID,
			"reason":     reason,
		}),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) finishOrContinueTask(ctx context.Context, taskID string, result WorkerTurnResult) bool {
	switch result.Status {
	case core.WorkerSucceeded:
		return true
	case core.WorkerWaiting:
		_ = s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	case core.WorkerCanceled:
		if s.workerCanceledForSteeringRestart(ctx, taskID, result.WorkerID) {
			_ = s.recordTaskAction(ctx, taskID, map[string]any{
				"kind":     "worker_canceled",
				"status":   "superseded",
				"reason":   "worker was canceled for steering restart",
				"workerId": result.WorkerID,
			})
			return false
		}
		_ = s.setTaskStatus(ctx, taskID, core.TaskCanceled)
	default:
		if exhaustion, ok := classifyProviderUsageExhaustion(result.Kind, result.Error, result.Summary); ok {
			_ = s.waitForProviderCapacity(ctx, taskID, result.WorkerID, exhaustion)
			return false
		}
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
	_, _, _ = results, selectedWorkerID, reason
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveSatisfied, "satisfied", "Task objective is complete."); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, taskID, core.TaskSucceeded)
}

type codeReviewGateResult struct {
	Ready          bool
	Results        []WorkerTurnResult
	Reason         string
	ReviewWorkerID string
	Status         string
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
	plan := s.codeReviewGatePlan(task, candidate, policy, phase, "")
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
		out.Status = "failed"
		out.Reason = nonEmpty(result.Error, result.Summary, "code review worker did not complete successfully")
		_ = s.recordCodeReviewGateResult(ctx, taskID, candidateWorkerID, phase, result, "failed", out.Reason)
		return out, nil
	}
	if codeReviewBlocksPublication(result, policy) {
		out.Ready = false
		out.Status = "blocked"
		out.Reason = nonEmpty(result.Summary, "code review requested changes")
		_ = s.recordCodeReviewGateResult(ctx, taskID, candidateWorkerID, phase, result, "blocked", out.Reason)
		return out, nil
	}
	out.Ready = true
	out.Status = "passed"
	out.Reason = nonEmpty(result.Summary, "code review approved publication")
	_ = s.recordCodeReviewGateResult(ctx, taskID, candidateWorkerID, phase, result, "passed", out.Reason)
	return out, nil
}

func (s *Service) codeReviewGatePlan(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string, workerKindOverride string) Plan {
	workerKind := strings.TrimSpace(workerKindOverride)
	if workerKind == "" {
		workerKind = s.codeReviewWorkerKind(policy, task, candidate)
	}
	return Plan{
		WorkerKind:      workerKind,
		Prompt:          s.codeReviewGatePrompt(task, candidate, policy, phase),
		ReasoningEffort: "high",
		Rationale:       "project review policy requires code review before pull request publication",
		Metadata: map[string]any{
			"baseWorkerID":      candidate.WorkerID,
			"codeReviewGate":    true,
			"reviewPhase":       phase,
			"spawnID":           "code-review-gate",
			"spawnRole":         "review",
			"spawnReason":       "Project review policy requires an independent code review before publishing this candidate.",
			"candidateWorkerID": candidate.WorkerID,
		},
	}
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

func (s *Service) preferredWorkerKindFromSteering(message string) string {
	words := steeringWords(message)
	if len(words) == 0 {
		return ""
	}
	kinds := make([]string, 0, len(s.runners))
	for kind := range s.runners {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		if words[strings.ToLower(strings.TrimSpace(kind))] {
			return kind
		}
	}
	return ""
}

func steeringWords(message string) map[string]bool {
	words := map[string]bool{}
	for _, field := range strings.FieldsFunc(strings.ToLower(message), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) {
		field = strings.TrimSpace(field)
		if field != "" {
			words[field] = true
		}
	}
	return words
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
	if err := s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":     "code_review_recovery",
		"when":     "before_completion_pr",
		"reason":   "Code review blocked publication; waiting for explicit steering.",
		"workerId": candidateWorkerID,
		"status":   "waiting",
		"error":    reason,
	}); err != nil {
		return true, err
	}
	_ = results
	_ = s.waitForUserAction(ctx, taskID, candidateWorkerID, "code_review_gate", "Code review blocked publication.\n\n"+reason+"\n\nSteer the task to fix the findings or publish/update a specific PR artifact explicitly.", map[string]any{
		"error": reason,
	})
	return true, nil
}

func annotateWorkerFailure(results []WorkerTurnResult, workerID string, label string, failureErr error) []WorkerTurnResult {
	out := make([]WorkerTurnResult, len(results))
	copy(out, results)
	for index := range out {
		if out[index].WorkerID != workerID {
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

func (s *Service) sanitizePublishPullRequestTarget(ctx context.Context, taskID string, action PlanAction) (PlanAction, bool, error) {
	if strings.TrimSpace(action.Kind) != "publish_pull_request" || len(action.Inputs) == 0 {
		return action, false, nil
	}
	id := strings.TrimSpace(nonEmpty(stringMetadata(action.Inputs, "id"), stringMetadata(action.Inputs, "pullRequestId")))
	repo := strings.ToLower(strings.TrimSpace(stringMetadata(action.Inputs, "repo")))
	number := intMetadata(action.Inputs, "number")
	url := strings.TrimSpace(stringMetadata(action.Inputs, "url"))
	branch := strings.TrimSpace(nonEmpty(stringMetadata(action.Inputs, "branch"), stringMetadata(action.Inputs, "headBranch")))
	if id == "" && url == "" && number == 0 && branch == "" {
		return action, false, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return action, false, err
	}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID || !pullRequestMatchesUpdateTarget(pr, id, repo, number, url, branch) {
			continue
		}
		sanitized := action
		sanitized.Inputs = maps.Clone(action.Inputs)
		for _, key := range []string{"id", "pullRequestId", "number", "url", "branch", "headBranch", "existingPullRequest"} {
			delete(sanitized.Inputs, key)
		}
		return sanitized, true, nil
	}
	return action, false, nil
}

func isRecoverablePublishConflict(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "remote patch has conflicts") ||
		strings.Contains(lower, "patch does not apply") ||
		strings.Contains(lower, "3-way apply failed") ||
		(strings.Contains(lower, "applied patch") && strings.Contains(lower, "conflicts")) ||
		strings.Contains(lower, "non-fast-forward") ||
		strings.Contains(lower, "failed to push some refs") ||
		(strings.Contains(lower, "[rejected]") && strings.Contains(lower, "push"))
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
		if strings.HasSuffix(results[i].SpawnID, ":"+workerRef) {
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

func singleUpdateCandidateWorkerID(results []WorkerTurnResult) string {
	if len(results) != 1 {
		return ""
	}
	result := results[0]
	if result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
		return result.WorkerID
	}
	return ""
}

func pullRequestUpdateActionHandlesCurrentFeedback(pr core.PullRequest, action PlanAction) bool {
	if !pullRequestHasUntriggeredFeedback(pr) {
		return false
	}
	if pullRequestFeedbackBodyRequiresMetadataUpdate(pullRequestLatestFeedbackBody(pr.Metadata)) {
		return updatePullRequestActionHasMetadata(action)
	}
	return true
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

func splitPreFollowUpActions(actions []PlanAction, results []WorkerTurnResult) ([]PlanAction, []PlanAction) {
	early := []PlanAction{}
	remaining := []PlanAction{}
	for _, action := range actions {
		if strings.TrimSpace(action.WorkerID) != "" {
			if _, ok := workerResultByReference(results, action.WorkerID); ok {
				if strings.TrimSpace(action.When) == "immediate" && planActionConsumesWorkerResult(action) {
					action.When = "after_success"
				}
				if strings.TrimSpace(action.When) != "immediate" {
					early = append(early, action)
					continue
				}
			}
		}
		if strings.TrimSpace(action.When) == "immediate" {
			continue
		}
		remaining = append(remaining, action)
	}
	return early, remaining
}

func planActionConsumesWorkerResult(action PlanAction) bool {
	switch strings.TrimSpace(action.Kind) {
	case "publish_pull_request", "update_pull_request":
		return true
	default:
		return false
	}
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
		var sanitized bool
		var err error
		action, sanitized, err = s.sanitizePublishPullRequestTarget(ctx, task.ID, action)
		if err != nil {
			return false, results, err
		}
		if sanitized {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":     "publish_pull_request_target_sanitized",
				"when":     nonEmpty(action.When, "after_success"),
				"reason":   "publish_pull_request referenced an existing task pull request; removed stale PR identity so a fresh intermediate PR gets its own branch.",
				"inputs":   action.Inputs,
				"workerId": workerID,
				"status":   "applied",
			}); err != nil {
				return false, results, err
			}
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
			if review.Status == "failed" {
				_ = s.waitForUserAction(ctx, task.ID, review.ReviewWorkerID, "code_review_gate", "Code review worker failed before it could approve or reject publication.\n\n"+review.Reason+"\n\nSteer the failed review worker to retry the review, choose a different review provider, or steer the task to take another path.", map[string]any{
					"candidateWorkerId": workerID,
					"reviewWorkerId":    review.ReviewWorkerID,
					"phase":             "intermediate",
					"error":             review.Reason,
				})
				return false, results, nil
			}
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
			if boolMetadata(action.Inputs, "continueAfterPublish") && isRecoverablePublishConflict(err) {
				results = annotateWorkerFailure(results, workerID, "intermediate publish failed", err)
				if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":     action.Kind,
					"when":     nonEmpty(action.When, "after_success"),
					"reason":   "Intermediate pull request publication failed with a recoverable patch conflict; continuing dynamic replanning with the failed candidate blocked.",
					"inputs":   action.Inputs,
					"workerId": workerID,
					"status":   "continued",
					"error":    err.Error(),
				}); actionErr != nil {
					return false, results, actionErr
				}
				if objectiveErr := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "intermediate_pr_publish_failed", "Intermediate pull request publication failed; objective continues with replanning."); objectiveErr != nil {
					return false, results, objectiveErr
				}
				return true, results, nil
			}
			if blocker, ok := classifyUserRecoverableBlocker(err.Error()); ok {
				if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":     action.Kind,
					"when":     nonEmpty(action.When, "after_success"),
					"reason":   action.Reason,
					"inputs":   action.Inputs,
					"workerId": workerID,
					"status":   "waiting",
					"error":    err.Error(),
				}); actionErr != nil {
					return false, results, actionErr
				}
				_ = s.waitForUserAction(ctx, task.ID, workerID, blocker.Reason, blocker.Question, map[string]any{
					"summary":    blocker.Summary,
					"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
					"error":      err.Error(),
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
		metadataOnly := updatePullRequestActionMetadataOnly(action)
		workerID := ""
		if strings.TrimSpace(action.WorkerID) != "" {
			workerID = planActionWorkerID(results, action.WorkerID)
		} else if !metadataOnly {
			workerID = singleUpdateCandidateWorkerID(results)
		}
		if workerID == "" && !metadataOnly {
			err := errors.New("update_pull_request requires an explicit workerId when pushing worker changes for multi-result tasks")
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   action.Kind,
				"when":   nonEmpty(action.When, "after_success"),
				"reason": action.Reason,
				"inputs": action.Inputs,
				"status": "skipped",
				"error":  err.Error(),
			}); err != nil {
				return false, results, err
			}
			if nonEmpty(action.When, "after_success") == "after_success" {
				return false, results, err
			}
			return true, results, nil
		}
		if !metadataOnly {
			if result, ok := workerResultByID(results, workerID); ok && !resultHasCandidateChanges(result) {
				if updatePullRequestActionHasMetadata(action) {
					metadataOnly = true
				} else {
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
			}
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
			if errors.Is(err, eventstore.ErrNotFound) {
				if err := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":   action.Kind,
					"when":   nonEmpty(action.When, "after_success"),
					"reason": action.Reason,
					"inputs": action.Inputs,
					"status": "skipped",
					"error":  "pull request target is not tracked by this task",
				}); err != nil {
					return false, results, err
				}
				return true, results, nil
			}
			return false, results, err
		}
		req := updatePullRequestRequestFromAction(action)
		req.MetadataOnly = metadataOnly
		req.WorkerID = workerID
		if !metadataOnly {
			if ready, err := s.reviewPlanPullRequestUpdateReadiness(ctx, task, action, results, workerID, pr); err != nil {
				return false, results, err
			} else if !ready {
				return true, results, nil
			}
		}
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
			if errors.Is(err, errTerminalPullRequest) || errors.Is(err, errPullRequestTargetMismatch) {
				errorText := "pull request is already closed or merged; publish a fresh pull request for new candidate work"
				if errors.Is(err, errPullRequestTargetMismatch) {
					errorText = err.Error()
				}
				if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":          action.Kind,
					"when":          nonEmpty(action.When, "after_success"),
					"reason":        action.Reason,
					"inputs":        action.Inputs,
					"workerId":      workerID,
					"pullRequestId": pr.ID,
					"status":        "skipped",
					"error":         errorText,
				}); actionErr != nil {
					return false, results, actionErr
				}
				return true, results, nil
			}
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
			if pullRequestContinuesTask(pr) {
				results = annotateWorkerFailure(results, workerID, "intermediate pull request update failed", err)
				if actionErr := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":          action.Kind,
					"when":          nonEmpty(action.When, "after_success"),
					"reason":        "Intermediate pull request update failed; continuing objective ownership instead of failing the task.",
					"inputs":        action.Inputs,
					"workerId":      workerID,
					"pullRequestId": pr.ID,
					"status":        "continued",
					"error":         err.Error(),
				}); actionErr != nil {
					return false, results, actionErr
				}
				if objectiveErr := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "intermediate_pr_update_failed", "Intermediate pull request update failed; objective continues with replanning and PR monitoring."); objectiveErr != nil {
					return false, results, objectiveErr
				}
				return true, results, nil
			}
			return false, results, err
		}
		if pullRequestUpdateActionHandlesCurrentFeedback(updated, action) {
			if err := s.commentPullRequestFeedbackAddressed(ctx, task, pr, updated, req, workerID); err != nil {
				_ = s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":          "pull_request_feedback_comment",
					"when":          nonEmpty(action.When, "after_success"),
					"reason":        "Pull request was updated, but aged could not post the follow-up status comment.",
					"workerId":      workerID,
					"pullRequestId": updated.ID,
					"url":           updated.URL,
					"status":        "failed",
					"error":         err.Error(),
				})
			}
			if err := s.markPullRequestFeedbackTriggered(ctx, updated); err != nil {
				return false, results, err
			}
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
			if errors.Is(err, errNoPullRequestsToWatch) {
				if err := s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":   action.Kind,
					"when":   nonEmpty(action.When, "after_success"),
					"reason": action.Reason,
					"inputs": action.Inputs,
					"status": "skipped",
					"error":  err.Error(),
				}); err != nil {
					return false, results, err
				}
				return true, results, nil
			}
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
		return !pullRequestWatchBlocksTask(task, prs), results, nil
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
	case "spawn_work":
		items, err := s.queueWorkItemsFromAction(ctx, task, action)
		if err != nil {
			return false, results, err
		}
		started, err := s.startRunnableSpawnWorkItems(ctx, task.ID)
		if err != nil {
			return false, results, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":              action.Kind,
			"when":              nonEmpty(action.When, "after_success"),
			"reason":            action.Reason,
			"inputs":            action.Inputs,
			"queuedWorkItemIds": workItemIDs(items),
			"queuedWorkCount":   len(items),
			"startedWorkCount":  started,
		}); err != nil {
			return false, results, err
		}
		return false, results, nil
	case "finish_objective":
		if reason := s.unpublishedCandidateCompletionBlockReason(ctx, task.ID, results); reason != "" {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":   action.Kind,
				"when":   nonEmpty(action.When, "after_success"),
				"reason": reason,
				"inputs": action.Inputs,
				"status": "rejected",
			}); err != nil {
				return false, results, err
			}
			return true, results, nil
		}
		summary := stringMetadata(action.Inputs, "summary")
		if summary == "" {
			summary = action.Reason
		}
		if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveSatisfied, "satisfied", summary); err != nil {
			return false, results, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
			"status": "completed",
		}); err != nil {
			return false, results, err
		}
		return false, results, s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
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
	Title        string   `json:"title"`
	Prompt       string   `json:"prompt"`
	WorkstreamID string   `json:"workstreamId,omitempty"`
	DependsOn    []string `json:"dependsOn,omitempty"`
}

type spawnedWorkItemSpec struct {
	ID              string         `json:"id,omitempty"`
	Kind            string         `json:"kind"`
	Reason          string         `json:"reason,omitempty"`
	Prompt          string         `json:"prompt,omitempty"`
	TargetKind      string         `json:"targetKind,omitempty"`
	TargetID        string         `json:"targetId,omitempty"`
	WorkerKind      string         `json:"workerKind,omitempty"`
	ReasoningEffort string         `json:"reasoningEffort,omitempty"`
	DependsOn       []string       `json:"dependsOn,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

func (s *Service) queueWorkItemsFromAction(ctx context.Context, task core.Task, action PlanAction) ([]core.WorkItem, error) {
	rawItems := anySliceMetadata(action.Inputs, "items")
	if len(rawItems) == 0 {
		return nil, errors.New("spawn_work requires at least one item")
	}
	queued := make([]core.WorkItem, 0, len(rawItems))
	for index, raw := range rawItems {
		spec, err := spawnedWorkItemSpecFromInput(raw)
		if err != nil {
			return nil, fmt.Errorf("spawn_work inputs.items[%d]: %w", index, err)
		}
		itemID := strings.TrimSpace(spec.ID)
		if itemID == "" {
			itemID = uuid.NewString()
		}
		kind := nonEmpty(spec.Kind, "objective.implement")
		targetKind := nonEmpty(spec.TargetKind, "objective")
		targetID := nonEmpty(spec.TargetID, task.ID)
		metadata := maps.Clone(spec.Metadata)
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata["sourceAction"] = action.Kind
		normalizedTargetKind, normalizedTargetID, terminalPR, terminal, err := s.normalizePullRequestFollowUpWorkItem(ctx, task.ID, kind, targetKind, targetID, metadata)
		if err != nil {
			return nil, err
		}
		targetKind = normalizedTargetKind
		targetID = normalizedTargetID
		if terminal {
			if err := s.recordTerminalPullRequestFollowUpSkipped(ctx, task.ID, itemID, terminalPR, "spawn_work item targets a terminal pull request"); err != nil {
				return nil, err
			}
			continue
		}
		if spec.WorkerKind != "" {
			metadata["workerKind"] = spec.WorkerKind
		}
		if spec.ReasoningEffort != "" {
			metadata["reasoningEffort"] = spec.ReasoningEffort
		}
		if len(spec.DependsOn) > 0 {
			metadata["dependsOn"] = spec.DependsOn
		}
		if err := s.recordWorkItemQueued(ctx, task.ID, map[string]any{
			"id":         itemID,
			"kind":       kind,
			"targetKind": targetKind,
			"targetId":   targetID,
			"reason":     spec.Reason,
			"prompt":     spec.Prompt,
			"metadata":   metadata,
		}); err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		queued = append(queued, core.WorkItem{
			ID:         itemID,
			TaskID:     task.ID,
			Kind:       kind,
			Status:     core.WorkItemQueued,
			TargetKind: targetKind,
			TargetID:   targetID,
			Reason:     spec.Reason,
			Prompt:     spec.Prompt,
			CreatedAt:  now,
			UpdatedAt:  now,
			Metadata:   core.MustJSON(metadata),
		})
	}
	return queued, nil
}

func spawnedWorkItemSpecFromInput(raw any) (spawnedWorkItemSpec, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return spawnedWorkItemSpec{}, err
	}
	var spec spawnedWorkItemSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return spawnedWorkItemSpec{}, err
	}
	spec.Kind = strings.TrimSpace(spec.Kind)
	if spec.Kind == "" {
		spec.Kind = "objective.implement"
	}
	spec.ID = strings.TrimSpace(spec.ID)
	spec.Reason = strings.TrimSpace(spec.Reason)
	spec.Prompt = strings.TrimSpace(spec.Prompt)
	spec.TargetKind = strings.TrimSpace(spec.TargetKind)
	spec.TargetID = strings.TrimSpace(spec.TargetID)
	spec.WorkerKind = strings.TrimSpace(spec.WorkerKind)
	spec.ReasoningEffort = strings.TrimSpace(spec.ReasoningEffort)
	spec.DependsOn = dedupeTrimmedStrings(spec.DependsOn)
	return spec, nil
}

func workItemIDs(items []core.WorkItem) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.ID)
	}
	return out
}

func (s *Service) queuePlanWorkItems(ctx context.Context, task core.Task, plan Plan) ([]core.WorkItem, error) {
	queued := make([]core.WorkItem, 0, len(plan.WorkItems))
	planInstanceID := stringMetadata(plan.Metadata, "planID")
	if planInstanceID == "" {
		planInstanceID = uuid.NewString()
	}
	itemIDs := map[string]string{}
	for index, request := range plan.WorkItems {
		baseID := workItemRequestID(request, index)
		itemIDs[baseID] = planQueuedWorkItemID(planInstanceID, baseID)
	}
	for index, request := range plan.WorkItems {
		baseItemID := workItemRequestID(request, index)
		itemID := itemIDs[baseItemID]
		kind := nonEmpty(strings.TrimSpace(request.Kind), "objective.implement")
		targetKind := nonEmpty(strings.TrimSpace(request.TargetKind), "objective")
		targetID := nonEmpty(strings.TrimSpace(request.TargetID), task.ID)
		metadata := maps.Clone(request.Metadata)
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata["sourceAction"] = "plan"
		metadata["sourceWorkItemID"] = baseItemID
		metadata["planID"] = planInstanceID
		metadata["workerKind"] = strings.TrimSpace(request.WorkerKind)
		metadata["reasoningEffort"] = strings.TrimSpace(request.ReasoningEffort)
		metadata["dependsOn"] = queuedDependencyIDs(request.DependsOn, itemIDs)
		metadata["role"] = workItemRole(kind, request.Reason)
		metadata["planRationale"] = plan.Rationale
		normalizedTargetKind, normalizedTargetID, terminalPR, terminal, err := s.normalizePullRequestFollowUpWorkItem(ctx, task.ID, kind, targetKind, targetID, metadata)
		if err != nil {
			return nil, err
		}
		targetKind = normalizedTargetKind
		targetID = normalizedTargetID
		if terminal {
			if err := s.recordTerminalPullRequestFollowUpSkipped(ctx, task.ID, itemID, terminalPR, "plan work item targets a terminal pull request"); err != nil {
				return nil, err
			}
			continue
		}
		if actions := deferredPlanActionsForWorkItem(plan.Actions, baseItemID); len(actions) > 0 {
			metadata["planActions"] = actions
			if deferredPlanActionsTargetWorkItem(actions, baseItemID) {
				metadata["executeActionsOnSuccess"] = true
			}
		}
		if plan.Metadata != nil {
			metadata["planMetadata"] = plan.Metadata
		}
		if existing, ok, err := s.existingPullRequestFollowUpWorkItem(ctx, task.ID, kind, targetKind, targetID, metadata); err != nil {
			return nil, err
		} else if ok {
			if err := s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":               "duplicate_pull_request_followup_skipped",
				"status":             "skipped",
				"reason":             "A queued or running pull request follow-up already exists for this pull request.",
				"workItemId":         itemID,
				"existingWorkItemId": existing.ID,
				"targetId":           targetID,
			}); err != nil {
				return nil, err
			}
			continue
		}
		if err := s.recordWorkItemQueued(ctx, task.ID, map[string]any{
			"id":         itemID,
			"kind":       kind,
			"targetKind": targetKind,
			"targetId":   targetID,
			"reason":     request.Reason,
			"prompt":     request.Prompt,
			"metadata":   metadata,
		}); err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		queued = append(queued, core.WorkItem{
			ID:         itemID,
			TaskID:     task.ID,
			Kind:       kind,
			Status:     core.WorkItemQueued,
			TargetKind: targetKind,
			TargetID:   targetID,
			Reason:     request.Reason,
			Prompt:     request.Prompt,
			CreatedAt:  now,
			UpdatedAt:  now,
			Metadata:   core.MustJSON(metadata),
		})
	}
	return queued, nil
}

func (s *Service) existingPullRequestFollowUpWorkItem(ctx context.Context, taskID string, kind string, targetKind string, targetID string, metadata map[string]any) (core.WorkItem, bool, error) {
	if kind != "pr.followup" || targetKind != "pull_request" {
		return core.WorkItem{}, false, nil
	}
	prID := strings.TrimSpace(targetID)
	if prID == "" {
		prID = strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestID"), stringMetadata(metadata, "pullRequestId")))
	}
	if prID == "" {
		return core.WorkItem{}, false, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.WorkItem{}, false, err
	}
	signature := strings.TrimSpace(stringMetadata(metadata, "feedbackSignature"))
	if existing, ok := pullRequestFollowUpWorkItem(snapshot, taskID, prID, signature); ok {
		return existing, true, nil
	}
	return core.WorkItem{}, false, nil
}

func (s *Service) normalizePullRequestFollowUpWorkItem(ctx context.Context, taskID string, kind string, targetKind string, targetID string, metadata map[string]any) (string, string, core.PullRequest, bool, error) {
	if kind != "pr.followup" {
		return targetKind, targetID, core.PullRequest{}, false, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return targetKind, targetID, core.PullRequest{}, false, err
	}
	id := ""
	if targetKind == "pull_request" {
		id = strings.TrimSpace(targetID)
	}
	id = nonEmpty(id, stringMetadata(metadata, "pullRequestID"), stringMetadata(metadata, "pullRequestId"), stringMetadata(metadata, "id"))
	repo := strings.ToLower(strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestRepo"), stringMetadata(metadata, "repo"))))
	number := firstNonZero(intMetadata(metadata, "pullRequestNumber"), intMetadata(metadata, "number"))
	url := strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestURL"), stringMetadata(metadata, "url")))
	branch := strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestBranch"), stringMetadata(metadata, "branch"), stringMetadata(metadata, "headBranch")))
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID || !pullRequestMatchesUpdateTarget(pr, id, repo, number, url, branch) {
			continue
		}
		metadata["pullRequestID"] = pr.ID
		metadata["pullRequestId"] = pr.ID
		metadata["pullRequestRepo"] = pr.Repo
		metadata["pullRequestNumber"] = pr.Number
		metadata["pullRequestURL"] = pr.URL
		metadata["pullRequestBranch"] = pr.Branch
		metadata["repo"] = pr.Repo
		metadata["number"] = pr.Number
		metadata["url"] = pr.URL
		metadata["branch"] = pr.Branch
		return "pull_request", pr.ID, pr, isTerminalPullRequestState(pr.State), nil
	}
	if id != "" {
		return "pull_request", id, core.PullRequest{}, false, nil
	}
	return targetKind, targetID, core.PullRequest{}, false, nil
}

func (s *Service) recordTerminalPullRequestFollowUpSkipped(ctx context.Context, taskID string, workItemID string, pr core.PullRequest, reason string) error {
	return s.recordTaskAction(ctx, taskID, map[string]any{
		"kind":          "terminal_pull_request_followup_skipped",
		"status":        "skipped",
		"reason":        reason,
		"workItemId":    workItemID,
		"pullRequestId": pr.ID,
		"repo":          pr.Repo,
		"number":        pr.Number,
		"url":           pr.URL,
		"state":         pr.State,
	})
}

func planQueuedWorkItemID(planID string, itemID string) string {
	planID = strings.TrimSpace(planID)
	itemID = strings.TrimSpace(itemID)
	if planID == "" {
		return itemID
	}
	return planID + ":" + itemID
}

func queuedDependencyIDs(dependsOn []string, itemIDs map[string]string) []string {
	out := make([]string, 0, len(dependsOn))
	for _, dep := range dedupeTrimmedStrings(dependsOn) {
		if queuedID := itemIDs[dep]; queuedID != "" {
			out = append(out, queuedID)
			continue
		}
		out = append(out, dep)
	}
	return out
}

func deferredPlanActions(actions []PlanAction) []PlanAction {
	out := make([]PlanAction, 0, len(actions))
	for _, action := range actions {
		if strings.TrimSpace(action.When) == "immediate" {
			continue
		}
		out = append(out, action)
	}
	return out
}

func workItemRole(kind string, reason string) string {
	if trimmed := strings.TrimSpace(strings.TrimPrefix(kind, "objective.")); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(reason); trimmed != "" {
		return trimmed
	}
	return "worker"
}

func (s *Service) startRunnableSpawnWorkItems(ctx context.Context, taskID string) (int, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return 0, eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) {
		return 0, nil
	}
	if err := s.failBlockedSpawnWorkItems(ctx, snapshot, taskID); err != nil {
		return 0, err
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	items := runnableSpawnWorkItems(snapshot, taskID)
	started := 0
	for _, item := range items {
		if !s.claimActiveWorkItem(item.ID) {
			continue
		}
		started++
		go s.runSpawnedWorkItem(context.Background(), taskID, item.ID)
	}
	return started, nil
}

func (s *Service) runSpawnedWorkItem(ctx context.Context, taskID string, itemID string) {
	defer s.releaseActiveWorkItem(itemID)
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, "", err.Error())
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || isTerminalTaskStatus(task.Status) {
		return
	}
	item, ok := workItemByIDFromSnapshot(snapshot, taskID, itemID)
	if !ok || item.Status != core.WorkItemQueued {
		return
	}
	if !spawnWorkItemDependenciesSatisfied(snapshot, taskID, item) {
		return
	}
	if pr, ok := terminalPullRequestForFollowUpWorkItem(snapshot, taskID, item); ok {
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemSucceeded, "", "pull request is already terminal")
		_ = s.recordTerminalPullRequestFollowUpSkipped(context.Background(), taskID, itemID, pr, "queued pull request follow-up target is already terminal")
		s.resumeObjectiveAfterSpawnWorkDrained(context.Background(), taskID)
		return
	}
	plan, err := s.planForSpawnedWorkItem(snapshot, task, item)
	if err != nil {
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, "", err.Error())
		_ = s.failTask(context.Background(), taskID, err)
		return
	}
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, "", err.Error())
		s.resumeObjectiveAfterSpawnWorkDrained(context.Background(), taskID)
		return
	}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		_ = s.recordWorkItemStarted(context.Background(), taskID, itemID, result.WorkerID)
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, result.WorkerID, err.Error())
		s.resumeObjectiveAfterSpawnWorkDrained(context.Background(), taskID)
		return
	}
	if result.Status == core.WorkerWaiting {
		_ = s.recordWorkItemStarted(context.Background(), taskID, itemID, result.WorkerID)
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemSucceeded, result.WorkerID, "")
		s.handleWorkerQuestion(context.Background(), task, plan, completedWorkerResultsForTask(snapshot, taskID), result)
		return
	}
	_ = s.recordWorkItemStarted(context.Background(), taskID, itemID, result.WorkerID)
	if result.Status == core.WorkerCanceled {
		if s.workerCanceledForSteeringRestart(context.Background(), taskID, result.WorkerID) {
			_ = s.recordTaskAction(context.Background(), taskID, map[string]any{
				"kind":       "spawn_work_item",
				"status":     "superseded",
				"reason":     "work item worker was canceled for task steering restart",
				"workerId":   result.WorkerID,
				"workItemId": itemID,
			})
			return
		}
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemCanceled, result.WorkerID, result.Error)
		_ = s.setTaskStatus(context.Background(), taskID, core.TaskCanceled)
		return
	}
	if result.Status == core.WorkerFailed {
		if exhaustion, ok := classifyProviderUsageExhaustion(result.Kind, result.Error, result.Summary); ok {
			_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, result.WorkerID, result.Error)
			s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "waiting_external", "worker provider usage is exhausted; waiting for provider capacity", result.WorkerID, nonEmpty(result.Error, result.Summary))
			_ = s.waitForProviderCapacity(context.Background(), taskID, result.WorkerID, exhaustion)
			return
		}
		if blocker, ok := classifyUserRecoverableBlocker(nonEmpty(result.Error, result.Summary)); ok {
			_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, result.WorkerID, result.Error)
			s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "continued", "background pull request follow-up worker did not complete successfully; queued feedback remains for the next objective replan", result.WorkerID, nonEmpty(result.Error, result.Summary))
			_ = s.waitForUserAction(context.Background(), taskID, result.WorkerID, blocker.Reason, blocker.Question, map[string]any{
				"summary":    blocker.Summary,
				"workerKind": result.Kind,
				"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
				"error":      result.Error,
			})
			return
		}
		if _, ok := s.brain.(ReplanProvider); !ok {
			_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, result.WorkerID, result.Error)
			s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "continued", "background pull request follow-up worker did not complete successfully; queued feedback remains for the next objective replan", result.WorkerID, nonEmpty(result.Error, result.Summary))
			s.finishOrContinueTask(context.Background(), taskID, result)
			return
		}
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, result.WorkerID, result.Error)
		s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "continued", "background pull request follow-up worker did not complete successfully; queued feedback remains for the next objective replan", result.WorkerID, nonEmpty(result.Error, result.Summary))
	}
	if result.Status == core.WorkerSucceeded {
		if boolMetadata(workItemMetadata(item), "executeActionsOnSuccess") && len(plan.Actions) > 0 {
			actionResults := append(completedWorkerResultsForTask(snapshot, taskID), result)
			actionPlan, skippedWatchActions := immediateWorkItemActionPlan(plan)
			keepGoing := true
			var err error
			if len(actionPlan.Actions) > 0 {
				keepGoing, _, err = s.runPlanActions(context.Background(), task, actionPlan, actionResults)
			}
			if err != nil {
				_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemFailed, result.WorkerID, err.Error())
				s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "continued", "background pull request follow-up action failed; queued feedback remains for the next objective replan", result.WorkerID, err.Error())
				s.resumeObjectiveAfterSpawnWorkDrained(context.Background(), taskID)
				return
			}
			for _, action := range skippedWatchActions {
				_ = s.recordTaskAction(context.Background(), task.ID, map[string]any{
					"kind":             action.Kind,
					"when":             nonEmpty(action.When, "after_success"),
					"reason":           action.Reason,
					"inputs":           action.Inputs,
					"pullRequestCount": 1,
					"status":           "background",
				})
			}
			if !keepGoing {
				_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemSucceeded, result.WorkerID, "")
				s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "completed", "background pull request follow-up completed while objective work continued", result.WorkerID, "")
				return
			}
		}
		_ = s.recordWorkItemCompleted(context.Background(), taskID, itemID, core.WorkItemSucceeded, result.WorkerID, "")
		s.recordWorkItemFollowUpStatus(context.Background(), taskID, item, "completed", "background pull request follow-up completed while objective work continued", result.WorkerID, "")
	}
	if _, err := s.startRunnableSpawnWorkItems(context.Background(), taskID); err != nil {
		_ = s.recordTaskAction(context.Background(), taskID, map[string]any{
			"kind":   "spawn_work_scheduler",
			"status": "failed",
			"reason": "failed to start newly unblocked spawned work items",
			"error":  err.Error(),
		})
	}
	s.resumeObjectiveAfterSpawnWorkDrained(context.Background(), taskID)
}

func immediateWorkItemActionPlan(plan Plan) (Plan, []PlanAction) {
	actionPlan := plan
	actionPlan.Actions = nil
	skippedWatchActions := []PlanAction{}
	skipWatchActions := boolMetadata(plan.Metadata, "backgroundPullRequestFollowUp")
	for _, action := range plan.Actions {
		if skipWatchActions && strings.TrimSpace(action.Kind) == "watch_pull_requests" {
			skippedWatchActions = append(skippedWatchActions, action)
			continue
		}
		actionPlan.Actions = append(actionPlan.Actions, action)
	}
	return actionPlan, skippedWatchActions
}

func deferredPlanActionsForWorkItem(actions []PlanAction, workItemID string) []PlanAction {
	out := []PlanAction{}
	for _, action := range deferredPlanActions(actions) {
		if planActionHasWorkerRef(action) && !planActionTargetsWorkItem(action, workItemID) {
			continue
		}
		out = append(out, action)
	}
	return out
}

func deferredPlanActionsTargetWorkItem(actions []PlanAction, workItemID string) bool {
	for _, action := range actions {
		if planActionTargetsWorkItem(action, workItemID) {
			return true
		}
	}
	return false
}

func planActionHasWorkerRef(action PlanAction) bool {
	return strings.TrimSpace(action.WorkerID) != ""
}

func planActionTargetsWorkItem(action PlanAction, workItemID string) bool {
	ref := strings.TrimSpace(action.WorkerID)
	workItemID = strings.TrimSpace(workItemID)
	if ref == "" || workItemID == "" {
		return false
	}
	return ref == workItemID || strings.HasSuffix(workItemID, ":"+ref) || strings.HasSuffix(ref, ":"+workItemID)
}

func (s *Service) recordWorkItemFollowUpStatus(ctx context.Context, taskID string, item core.WorkItem, status string, reason string, workerID string, errorText string) {
	metadata := workItemMetadata(item)
	if !boolMetadata(metadata, "backgroundPullRequestFollowUp") {
		return
	}
	payload := map[string]any{
		"kind":          "pull_request_background_followup",
		"status":        status,
		"reason":        reason,
		"pullRequestId": nonEmpty(stringMetadata(metadata, "pullRequestID"), stringMetadata(metadata, "pullRequestId"), item.TargetID),
		"url":           stringMetadata(metadata, "url"),
		"workerId":      workerID,
	}
	if errorText != "" {
		payload["error"] = errorText
	}
	_ = s.recordTaskAction(ctx, taskID, payload)
}

func (s *Service) planForSpawnedWorkItem(snapshot core.Snapshot, task core.Task, item core.WorkItem) (Plan, error) {
	metadata := workItemMetadata(item)
	dependsOn := stringSliceMetadata(metadata, "dependsOn")
	results := completedWorkerResultsForTask(snapshot, task.ID)
	role := nonEmpty(stringMetadata(metadata, "role"), strings.TrimPrefix(item.Kind, "objective."), item.Kind)
	reason := nonEmpty(item.Reason, stringMetadata(metadata, "reason"), "Run spawned objective work item.")
	prompt := strings.TrimSpace(item.Prompt)
	if prompt == "" {
		prompt = fmt.Sprintf("Run this objective work item.\n\nKind: %s\nReason: %s", item.Kind, reason)
	}
	prompt = buildInitialWorkerPrompt(prompt, results, dependsOn)
	requestedWorkerKind := stringMetadata(metadata, "workerKind")
	workerKind := s.workerKindForWorkItem(role, reason, requestedWorkerKind, "")
	if _, ok := s.runners[workerKind]; !ok {
		return Plan{}, fmt.Errorf("unknown worker kind %q for spawned work item %s", workerKind, item.ID)
	}
	reasoningEffort := normalizeReasoningEffort(stringMetadata(metadata, "reasoningEffort"))
	planMetadata := anyMapMetadata(metadata, "planMetadata")
	for key, value := range metadata {
		if key == "planMetadata" {
			continue
		}
		planMetadata[key] = value
	}
	if planMetadata == nil {
		planMetadata = map[string]any{}
	}
	if strings.TrimSpace(requestedWorkerKind) != "" {
		planMetadata["workerKindPinned"] = true
	}
	planMetadata["brain"] = nonEmpty(stringMetadata(planMetadata, "brain"), "work-item-scheduler")
	planMetadata["scheduler"] = nonEmpty(stringMetadata(planMetadata, "scheduler"), "work-item")
	planMetadata["workItemID"] = item.ID
	planMetadata["workItemKind"] = item.Kind
	planMetadata["spawnID"] = item.ID
	planMetadata["spawnRole"] = role
	planMetadata["spawnReason"] = reason
	planMetadata["dependsOn"] = dependsOn
	planMetadata["nodeID"] = nonEmpty(stringMetadata(planMetadata, "nodeID"), uuid.NewString())
	planMetadata["planID"] = nonEmpty(stringMetadata(planMetadata, "planID"), uuid.NewString())
	if baseWorkerID := latestCandidateWorkerIDForDependencies(results, dependsOn); baseWorkerID != "" {
		planMetadata["baseWorkerID"] = baseWorkerID
	}
	if parentNodeID := latestNodeIDForDependencies(results, dependsOn); parentNodeID != "" && stringMetadata(planMetadata, "parentNodeID") == "" {
		planMetadata["parentNodeID"] = parentNodeID
	}
	if reasoningEffort != "" {
		planMetadata["reasoningEffort"] = reasoningEffort
	}
	return Plan{
		WorkerKind:      workerKind,
		Prompt:          prompt,
		ReasoningEffort: reasoningEffort,
		Rationale:       "spawned work item: " + reason,
		Steps: []PlanStep{{
			Title:       "Run " + role,
			Description: reason,
		}},
		Actions:  planActionsFromMetadata(planMetadata),
		Metadata: planMetadata,
	}, nil
}

func terminalPullRequestForFollowUpWorkItem(snapshot core.Snapshot, taskID string, item core.WorkItem) (core.PullRequest, bool) {
	if item.Kind != "pr.followup" {
		return core.PullRequest{}, false
	}
	metadata := workItemMetadata(item)
	id := item.TargetID
	if item.TargetKind != "pull_request" {
		id = ""
	}
	id = nonEmpty(id, stringMetadata(metadata, "pullRequestID"), stringMetadata(metadata, "pullRequestId"), stringMetadata(metadata, "id"))
	repo := strings.ToLower(strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestRepo"), stringMetadata(metadata, "repo"))))
	number := firstNonZero(intMetadata(metadata, "pullRequestNumber"), intMetadata(metadata, "number"))
	url := strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestURL"), stringMetadata(metadata, "url")))
	branch := strings.TrimSpace(nonEmpty(stringMetadata(metadata, "pullRequestBranch"), stringMetadata(metadata, "branch"), stringMetadata(metadata, "headBranch")))
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID && pullRequestMatchesUpdateTarget(pr, id, repo, number, url, branch) && isTerminalPullRequestState(pr.State) {
			return pr, true
		}
	}
	return core.PullRequest{}, false
}

func anyMapMetadata(metadata map[string]any, key string) map[string]any {
	out := map[string]any{}
	if metadata == nil {
		return out
	}
	if _, ok := metadata[key]; !ok || metadata[key] == nil {
		return out
	}
	switch value := metadata[key].(type) {
	case map[string]any:
		for k, v := range value {
			out[k] = v
		}
	case map[string]string:
		for k, v := range value {
			out[k] = v
		}
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return out
		}
		_ = json.Unmarshal(data, &out)
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func planActionsFromMetadata(metadata map[string]any) []PlanAction {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata["planActions"]
	if !ok || raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var actions []PlanAction
	if err := json.Unmarshal(data, &actions); err != nil {
		return nil
	}
	return actions
}

func runnableSpawnWorkItems(snapshot core.Snapshot, taskID string) []core.WorkItem {
	items := []core.WorkItem{}
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || item.Status != core.WorkItemQueued || !isRunnableSpawnWorkItem(item, taskID) {
			continue
		}
		if !spawnWorkItemDependenciesSatisfied(snapshot, taskID, item) {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items
}

func isRunnableSpawnWorkItem(item core.WorkItem, taskID string) bool {
	if !isSpawnWorkItem(item) {
		return false
	}
	if item.Status != core.WorkItemQueued {
		return false
	}
	if item.TargetKind == "worker" {
		return false
	}
	return true
}

func isSpawnWorkItem(item core.WorkItem) bool {
	metadata := workItemMetadata(item)
	source := stringMetadata(metadata, "sourceAction")
	if source != "spawn_work" && source != "plan" {
		return false
	}
	return true
}

func (s *Service) failBlockedSpawnWorkItems(ctx context.Context, snapshot core.Snapshot, taskID string) error {
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || item.Status != core.WorkItemQueued || !isRunnableSpawnWorkItem(item, taskID) {
			continue
		}
		dep, ok := failedSpawnWorkDependency(snapshot, taskID, item)
		if !ok {
			continue
		}
		if err := s.recordWorkItemCompleted(ctx, taskID, item.ID, core.WorkItemFailed, "", "dependency "+dep+" did not complete successfully"); err != nil {
			return err
		}
	}
	return nil
}

func spawnWorkItemDependenciesSatisfied(snapshot core.Snapshot, taskID string, item core.WorkItem) bool {
	for _, dep := range stringSliceMetadata(workItemMetadata(item), "dependsOn") {
		if !spawnWorkDependencySatisfied(snapshot, taskID, dep) {
			return false
		}
	}
	return true
}

func spawnWorkDependencySatisfied(snapshot core.Snapshot, taskID string, dep string) bool {
	dep = strings.TrimSpace(dep)
	if dep == "" {
		return true
	}
	for _, item := range snapshot.WorkItems {
		if item.TaskID == taskID && item.ID == dep {
			return item.Status == core.WorkItemSucceeded
		}
	}
	for _, result := range completedWorkerResultsForTask(snapshot, taskID) {
		if result.SpawnID == dep || result.WorkerID == dep {
			return result.Status == core.WorkerSucceeded
		}
	}
	return false
}

func failedSpawnWorkDependency(snapshot core.Snapshot, taskID string, item core.WorkItem) (string, bool) {
	for _, dep := range stringSliceMetadata(workItemMetadata(item), "dependsOn") {
		dep = strings.TrimSpace(dep)
		if dep == "" {
			continue
		}
		for _, candidate := range snapshot.WorkItems {
			if candidate.TaskID != taskID || candidate.ID != dep {
				continue
			}
			if candidate.Status == core.WorkItemFailed || candidate.Status == core.WorkItemCanceled {
				return dep, true
			}
		}
		for _, result := range completedWorkerResultsForTask(snapshot, taskID) {
			if result.SpawnID != dep && result.WorkerID != dep {
				continue
			}
			if result.Status == core.WorkerFailed || result.Status == core.WorkerCanceled {
				return dep, true
			}
		}
	}
	return "", false
}

func (s *Service) resumeObjectiveAfterSpawnWorkDrained(ctx context.Context, taskID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskRunning {
		return
	}
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || !isSpawnWorkItem(item) {
			continue
		}
		if item.Status == core.WorkItemQueued || item.Status == core.WorkItemRunning {
			return
		}
	}
	drainWorkItemID := "spawn_work_drain:" + taskID
	if !s.claimActiveWorkItem(drainWorkItemID) {
		return
	}
	defer s.releaseActiveWorkItem(drainWorkItemID)
	if taskHasRunningObjectiveWorkers(snapshot, taskID) {
		return
	}
	initial, results, err := objectiveReplanStateForTask(snapshot, taskID)
	if err != nil {
		return
	}
	terminalStatus, terminalReason, hasTerminalWorkItem := terminalDrainedWorkItemStatus(snapshot, taskID)
	_, hasReplanner := s.brain.(ReplanProvider)
	if hasTerminalWorkItem && !hasReplanner {
		switch terminalStatus {
		case core.WorkItemFailed:
			_ = s.failTask(ctx, taskID, errors.New(nonEmpty(terminalReason, "queued work item failed")))
		case core.WorkItemCanceled:
			_ = s.setTaskStatus(ctx, taskID, core.TaskCanceled)
		}
		return
	}
	if !hasTerminalWorkItem {
		actionPlan := drainedPlanActionPlan(snapshot, taskID)
		if len(actionPlan.Actions) > 0 {
			keepGoing, nextResults, err := s.runPlanActions(ctx, task, actionPlan, results)
			if err != nil {
				if s.waitForRecoverableError(ctx, taskID, "", err) {
					return
				}
				_ = s.failTask(ctx, taskID, err)
				return
			}
			results = nextResults
			if !keepGoing {
				return
			}
		}
	}
	if !hasReplanner {
		_ = s.completeTask(ctx, taskID, results, "", "All queued work items completed.")
		return
	}
	replanWorkItemID := "spawn_work_replan:" + taskID
	if !s.claimActiveWorkItem(replanWorkItemID) {
		return
	}
	if err := s.startObjectiveRoutine(ctx, task, "session.recover", "Resume objective replanning after spawned work items completed.", func(taskCtx context.Context) {
		defer s.releaseActiveWorkItem(replanWorkItemID)
		s.resumeObjectiveReplan(taskCtx, task, initial, results)
	}); err != nil {
		s.releaseActiveWorkItem(replanWorkItemID)
	}
}

func drainedPlanActionPlan(snapshot core.Snapshot, taskID string) Plan {
	latestPlanID := ""
	var latestTime time.Time
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || !isSpawnWorkItem(item) {
			continue
		}
		planID := stringMetadata(workItemMetadata(item), "planID")
		if planID == "" {
			continue
		}
		if latestPlanID == "" || item.CreatedAt.After(latestTime) {
			latestPlanID = planID
			latestTime = item.CreatedAt
		}
	}
	for i := len(snapshot.WorkItems) - 1; i >= 0; i-- {
		item := snapshot.WorkItems[i]
		if item.TaskID != taskID || !isSpawnWorkItem(item) {
			continue
		}
		metadata := workItemMetadata(item)
		if boolMetadata(metadata, "executeActionsOnSuccess") {
			continue
		}
		if latestPlanID != "" && stringMetadata(metadata, "planID") != latestPlanID {
			continue
		}
		actions := planActionsFromMetadata(metadata)
		if len(actions) == 0 {
			continue
		}
		return Plan{Actions: actions}
	}
	return Plan{}
}

func terminalDrainedWorkItemStatus(snapshot core.Snapshot, taskID string) (core.WorkItemStatus, string, bool) {
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || !isSpawnWorkItem(item) {
			continue
		}
		switch item.Status {
		case core.WorkItemFailed, core.WorkItemCanceled:
			if item.Status == core.WorkItemCanceled && (workItemCanceledForSteeringRestart(item) || workerCanceledForSteeringRestartInSnapshot(snapshot, taskID, item.WorkerID)) {
				continue
			}
			return item.Status, item.Error, true
		}
	}
	return "", "", false
}

func workItemCanceledForSteeringRestart(item core.WorkItem) bool {
	text := strings.ToLower(strings.TrimSpace(item.Error))
	return strings.Contains(text, "steering restart") || strings.Contains(text, taskCancelReasonSteeringRestart)
}

func (s *Service) workerCanceledForSteeringRestart(ctx context.Context, taskID string, workerID string) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	return workerCanceledForSteeringRestartInSnapshot(snapshot, taskID, workerID)
}

func workerCanceledForSteeringRestartInSnapshot(snapshot core.Snapshot, taskID string, workerID string) bool {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return false
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.WorkerID != workerID || event.Type != core.EventWorkerCompleted {
			continue
		}
		var payload struct {
			Status core.WorkerStatus `json:"status"`
			Reason string            `json:"reason,omitempty"`
			Error  string            `json:"error,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false
		}
		if payload.Status != core.WorkerCanceled {
			return false
		}
		return payload.Reason == taskCancelReasonSteeringRestart || strings.Contains(strings.ToLower(payload.Error), "steering restart")
	}
	return false
}

func (s *Service) claimActiveWorkItem(itemID string) bool {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.activeWorkItems[itemID]; ok {
		return false
	}
	s.activeWorkItems[itemID] = struct{}{}
	return true
}

func (s *Service) releaseActiveWorkItem(itemID string) {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeWorkItems, itemID)
}

func workItemByIDFromSnapshot(snapshot core.Snapshot, taskID string, itemID string) (core.WorkItem, bool) {
	for _, item := range snapshot.WorkItems {
		if item.TaskID == taskID && item.ID == itemID {
			return item, true
		}
	}
	return core.WorkItem{}, false
}

func workItemMetadata(item core.WorkItem) map[string]any {
	metadata := map[string]any{}
	if len(item.Metadata) == 0 {
		return metadata
	}
	if err := json.Unmarshal(item.Metadata, &metadata); err != nil {
		return map[string]any{}
	}
	if metadata == nil {
		return map[string]any{}
	}
	return metadata
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

func (s *Service) reviewPlanPublicationReadiness(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult, workerID string) (bool, error) {
	ready, _, err := s.reviewTaskPublicationReadiness(ctx, task, action, results, workerID)
	return ready, err
}

func (s *Service) reviewPlanPullRequestUpdateReadiness(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult, workerID string, pr core.PullRequest) (bool, error) {
	reviewer, ok := s.brain.(PublicationReviewProvider)
	if !ok {
		return true, nil
	}
	candidate, ok := workerResultByID(results, workerID)
	if !ok {
		return false, fmt.Errorf("update_pull_request action selected unknown worker %s", workerID)
	}
	reviewAction := action
	reviewAction.Inputs = maps.Clone(action.Inputs)
	if reviewAction.Inputs == nil {
		reviewAction.Inputs = map[string]any{}
	}
	reviewAction.Inputs["existingPullRequest"] = map[string]any{
		"id":     pr.ID,
		"repo":   pr.Repo,
		"number": pr.Number,
		"url":    pr.URL,
		"title":  pr.Title,
		"base":   pr.Base,
		"branch": pr.Branch,
		"state":  pr.State,
	}
	review, err := reviewer.ReviewPublication(ctx, task, candidate, reviewAction)
	if err != nil {
		if recordErr := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":          "update_pull_request_readiness_review",
			"when":          nonEmpty(action.When, "after_success"),
			"reason":        "Pull request update readiness review failed; continuing with the planned update.",
			"workerId":      workerID,
			"pullRequestId": pr.ID,
			"status":        "ignored",
			"error":         err.Error(),
		}); recordErr != nil {
			return false, recordErr
		}
		return true, nil
	}
	if review.Ready {
		return true, nil
	}
	reason := strings.TrimSpace(review.Reason)
	if reason == "" {
		reason = "candidate is not a coherent update for the existing pull request"
	}
	return false, s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":            "update_pull_request_readiness_rejected",
		"when":            nonEmpty(action.When, "after_success"),
		"reason":          reason,
		"actionReason":    action.Reason,
		"workerId":        workerID,
		"pullRequestId":   pr.ID,
		"status":          "rejected",
		"candidateStatus": candidate.Status,
	})
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
		"kind":            "replan_completion_rejected",
		"status":          "rejected",
		"turn":            turn,
		"reason":          reason,
		"replanAction":    decision.Action,
		"replanRationale": decision.Rationale,
	})
}

func (s *Service) waitForUserAction(ctx context.Context, taskID string, workerID string, reason string, question string, metadata map[string]any) error {
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingUser, "approval_needed", question); err != nil {
		return err
	}
	if err := s.recordUserActionNeeded(ctx, taskID, workerID, reason, question, metadata); err != nil {
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
	event, err := s.append(ctx, core.Event{
		Type:     core.EventApprovalNeeded,
		TaskID:   taskID,
		WorkerID: strings.TrimSpace(workerID),
		Payload:  core.MustJSON(payload),
	})
	if err != nil {
		return err
	}
	targetKind := "task"
	targetID := taskID
	if strings.TrimSpace(workerID) != "" {
		targetKind = "worker"
		targetID = strings.TrimSpace(workerID)
	}
	return s.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         userQuestionWorkItemID(event.ID),
		"kind":       "user.question",
		"targetKind": targetKind,
		"targetId":   targetID,
		"reason":     payload["reason"],
		"prompt":     payload["question"],
		"metadata": map[string]any{
			"approvalEventId": event.ID,
			"workerId":        strings.TrimSpace(workerID),
			"reason":          payload["reason"],
		},
	})
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
			reason:  "target_storage_full",
			summary: "The execution target is out of disk space or quota.",
			any:     []string{"no space left on device", "disk quota exceeded"},
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
		reason = "completion candidate does not satisfy the task objective yet"
	}
	return reason, true
}

func (s *Service) taskIsBroadObjective(ctx context.Context, taskID string) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return false
	}
	return taskIsBroadObjective(task)
}

func taskIsBroadObjective(task core.Task) bool {
	var metadata map[string]any
	if len(task.Metadata) > 0 {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	return strings.EqualFold(strings.TrimSpace(stringMetadataValue(metadata["objectiveMode"])), "broad")
}

type replanLoopOptions struct {
	RecoveryHint string
}

func (s *Service) replanLoop(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) (bool, string, []WorkerTurnResult) {
	return s.replanLoopWithOptions(ctx, task, initial, results, replanLoopOptions{})
}

func (s *Service) replanLoopWithOptions(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, options replanLoopOptions) (bool, string, []WorkerTurnResult) {
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		return true, "", results
	}
	recoveryHint := options.RecoveryHint
	stalledTurns := 0
	limitUnproductiveTurns := !taskIsBroadObjective(task)
	currentWorkPlan := initial.WorkPlan
	for {
		stateSnapshot, err := s.store.Snapshot(ctx)
		if err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return false, "", results
		}
		turn := nextReplanTurn(stateSnapshot, task.ID)
		if taskIsTerminalFromSnapshot(stateSnapshot, task.ID) {
			return false, "", results
		}
		if limitUnproductiveTurns && stalledTurns >= maxConsecutiveUnproductiveReplanTurns {
			recoveryOptions := options
			recoveryOptions.RecoveryHint = recoveryHint
			return s.recoverReplanLimit(ctx, task, turn, results, recoveryOptions)
		}
		decision, err := replanner.Replan(ctx, task, OrchestrationState{
			InitialPlan:                initial,
			WorkPlan:                   currentWorkPlan,
			Results:                    results,
			ContextLedger:              s.taskContextLedger(ctx, task.ID),
			Artifacts:                  taskArtifactsFromSnapshot(stateSnapshot, task.ID),
			PullRequests:               taskPullRequestStatesFromSnapshot(stateSnapshot, task.ID),
			TaskSteering:               taskSteering(stateSnapshot, task.ID),
			PendingPullRequestFeedback: pendingPullRequestFeedbackFromSnapshot(stateSnapshot, task.ID),
			PendingWorkerSteering:      pendingWorkerSteering(stateSnapshot, task.ID),
			Turn:                       turn,
			RecoveryHint:               recoveryHint,
		})
		if err != nil {
			if ctx.Err() != nil {
				return false, "", results
			}
			recoveryOptions := options
			recoveryOptions.RecoveryHint = recoveryHint
			return s.recoverReplanError(ctx, task, turn, results, fmt.Errorf("dynamic replan failed: %w", err), recoveryOptions)
		}
		if decision.Plan != nil {
			normalizePlanShape(decision.Plan)
		}
		if err := decision.Validate(); err != nil {
			recoveryOptions := options
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
			return false, "", results
		}
		if decision.WorkPlan != nil {
			if err := s.updateTaskWorkPlan(ctx, task.ID, *decision.WorkPlan); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			}
			currentWorkPlan = decision.WorkPlan
		}
		switch decision.Action {
		case "complete":
			if block := s.pendingReplanQueueBlock(ctx, task.ID); block.UserReason != "" || block.InternalReason != "" {
				reason := nonEmpty(block.UserReason, block.InternalReason)
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				if block.UserReason != "" {
					stalledTurns++
					continue
				}
				if err := s.waitForReplanQueueDrain(ctx, task, turn, errors.New(reason), reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				return false, "", results
			}
			if reason := unpublishedCandidateCompletionBlockReasonFromSnapshot(stateSnapshot, task.ID, results); reason != "" {
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				recoveryHint = reason
				stalledTurns++
				continue
			}
			if reason := broadObjectiveWorkPlanCompletionBlockReason(task, currentWorkPlan); reason != "" {
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				recoveryHint = reason
				stalledTurns++
				continue
			}
			return true, decision.Rationale, results
		case "finish_objective":
			if reason := unpublishedCandidateCompletionBlockReasonFromSnapshot(stateSnapshot, task.ID, results); reason != "" {
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				recoveryHint = reason
				stalledTurns++
				continue
			}
			if reason := broadObjectiveWorkPlanCompletionBlockReason(task, currentWorkPlan); reason != "" {
				if err := s.recordRejectedReplanCompletion(ctx, task.ID, turn, decision, reason); err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				recoveryHint = reason
				stalledTurns++
				continue
			}
			if err := s.finishObjectiveFromReplan(ctx, task, decision); err != nil {
				_ = s.failTask(ctx, task.ID, err)
			}
			return false, "", results
		case "wait":
			_ = s.waitForUserAction(ctx, task.ID, "", "orchestrator_wait", nonEmpty(decision.Message, decision.Rationale, "The orchestrator needs user input before continuing."), map[string]any{
				"turn":      turn,
				"rationale": decision.Rationale,
			})
			return false, "", results
		case "fail":
			_ = s.failTask(ctx, task.ID, errors.New(nonEmpty(decision.Message, decision.Rationale, "dynamic replan failed task")))
			return false, "", results
		case "continue":
			if terminal, err := s.taskIsTerminal(ctx, task.ID); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			} else if terminal {
				return false, "", results
			}
			beforeResults := results
			next := *decision.Plan
			normalizePlanShape(&next)
			if err := next.Validate(); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			}
			if pr, mismatchReason, ok := s.pullRequestFollowUpForPlan(ctx, task.ID, next); ok {
				if mismatchReason != "" {
					if err := s.recordTaskAction(ctx, task.ID, map[string]any{
						"kind":   "pull_request_followup_plan_rejected",
						"status": "rejected",
						"reason": mismatchReason,
					}); err != nil {
						_ = s.failTask(ctx, task.ID, err)
						return false, "", results
					}
					stalledTurns++
					continue
				}
				next = canonicalizePullRequestFollowUpPlan(next, pr)
			}
			if steering, ok := s.firstPendingWorkerSteering(ctx, task.ID); ok {
				next = annotateWorkerSteeringPlan(next, steering)
			}
			if next.Metadata == nil {
				next.Metadata = map[string]any{}
			}
			next.Metadata["dynamicReplanTurn"] = turn
			if shouldInheritLatestCandidateForPlan(next) {
				if baseWorkerID := latestCandidateWorkerID(results); baseWorkerID != "" {
					next.Metadata["baseWorkerID"] = baseWorkerID
				}
			}
			normalizePlanReasoning(&next)
			if _, err := s.append(ctx, core.Event{
				Type:    core.EventTaskPlanned,
				TaskID:  task.ID,
				Payload: core.MustJSON(next),
			}); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			}
			earlyActions, remainingActions := splitPreFollowUpActions(next.Actions, results)
			if len(earlyActions) > 0 {
				beforePRs, err := s.taskPullRequestCount(ctx, task.ID)
				if err != nil {
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				var keepGoing bool
				keepGoing, results, err = s.runPlanActions(ctx, task, planWithActions(next, earlyActions), results)
				if err != nil {
					if ctx.Err() != nil {
						return false, "", results
					}
					if s.waitForRecoverableError(ctx, task.ID, "", err) {
						return false, "", results
					}
					_ = s.failTask(ctx, task.ID, err)
					return false, "", results
				}
				if !keepGoing {
					return false, "", results
				}
				if containsPublishPullRequestAction(earlyActions) {
					afterPRs, err := s.taskPullRequestCount(ctx, task.ID)
					if err != nil {
						_ = s.failTask(ctx, task.ID, err)
						return false, "", results
					}
					if afterPRs <= beforePRs {
						stalledTurns++
						continue
					}
				}
				next.Actions = remainingActions
			}
			if len(next.WorkItems) == 0 {
				stalledTurns++
				continue
			}
			if _, err := s.queuePlanWorkItems(ctx, task, next); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			}
			started, err := s.startRunnableSpawnWorkItems(ctx, task.ID)
			if err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			}
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":             "replan_work_items",
				"status":           "queued",
				"reason":           "Dynamic replan queued durable objective work items.",
				"queuedWorkItems":  len(next.WorkItems),
				"startedWorkCount": started,
				"turn":             turn,
			})
			if terminal, err := s.taskIsTerminal(ctx, task.ID); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", results
			} else if terminal {
				return false, "", results
			}
			_ = beforeResults
			return false, "", results
		}
	}
}

func nextReplanTurn(snapshot core.Snapshot, taskID string) int {
	turn := 1
	for _, event := range snapshot.Events {
		if event.Type != core.EventTaskReplanned || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Turn int `json:"turn"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.Turn >= turn {
			turn = payload.Turn + 1
			continue
		}
		turn++
	}
	return turn
}

func (s *Service) finishObjectiveFromReplan(ctx context.Context, task core.Task, decision ReplanDecision) error {
	summary := nonEmpty(decision.Message, decision.Rationale, "Objective finished.")
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveSatisfied, "satisfied", summary); err != nil {
		return err
	}
	if err := s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":      "finish_objective",
		"when":      "replan",
		"reason":    decision.Rationale,
		"summary":   summary,
		"status":    "completed",
		"replanned": true,
	}); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, task.ID, core.TaskSucceeded)
}

func unpublishedCandidateCompletionBlockReasonFromSnapshot(snapshot core.Snapshot, taskID string, results []WorkerTurnResult) string {
	task, ok := findTask(snapshot, taskID)
	if !ok || !taskIsBroadObjective(task) {
		return ""
	}
	published := map[string]bool{}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID {
			continue
		}
		metadata := map[string]any{}
		if len(pr.Metadata) > 0 {
			_ = json.Unmarshal(pr.Metadata, &metadata)
		}
		if workerID := strings.TrimSpace(stringMetadata(metadata, "workerId")); workerID != "" {
			published[workerID] = true
		}
	}
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		if result.Status != core.WorkerSucceeded || !resultHasCandidateChanges(result) {
			continue
		}
		workerID := strings.TrimSpace(result.WorkerID)
		if workerID != "" && published[workerID] {
			continue
		}
		files := workspaceChangedFilePaths(result.Changes.ChangedFiles)
		if len(files) == 0 {
			files = []string{"candidate diff"}
		}
		return fmt.Sprintf("successful worker %s has unpublished candidate changes (%s); publish, update, or explicitly continue with another work item before finishing the objective", nonEmpty(workerID, "unknown"), strings.Join(files, ", "))
	}
	return ""
}

func (s *Service) unpublishedCandidateCompletionBlockReason(ctx context.Context, taskID string, results []WorkerTurnResult) string {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ""
	}
	return unpublishedCandidateCompletionBlockReasonFromSnapshot(snapshot, taskID, results)
}

func broadObjectiveWorkPlanCompletionBlockReason(task core.Task, workPlan *core.WorkPlan) string {
	if !taskIsBroadObjective(task) || workPlan == nil {
		return ""
	}
	open := incompleteWorkPlanItems(workPlan.Workstreams)
	open = append(open, incompleteWorkPlanItems(workPlan.Validation)...)
	if len(open) == 0 {
		return ""
	}
	if len(open) > 4 {
		open = append(open[:4], fmt.Sprintf("%d more", len(open)-4))
	}
	return fmt.Sprintf("broad objective work plan still has incomplete items: %s", strings.Join(open, ", "))
}

func incompleteWorkPlanItems(items []core.WorkPlanItem) []string {
	open := []string{}
	for _, item := range items {
		status := strings.ToLower(strings.TrimSpace(item.Status))
		if status == "done" || status == "dropped" {
			continue
		}
		label := strings.TrimSpace(item.ID)
		if label == "" {
			label = strings.TrimSpace(item.Goal)
		}
		if label == "" {
			label = "unnamed"
		}
		if status != "" {
			label += " (" + status + ")"
		} else {
			label += " (no status)"
		}
		open = append(open, label)
	}
	return open
}

func (s *Service) recoverReplanError(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, replanErr error, options replanLoopOptions) (bool, string, []WorkerTurnResult) {
	return s.recoverReplanFallback(ctx, task, turn, results, replanErr, options, replanFallbackConfig{
		CompleteReasonPrefix: "fallback completion after replanner error",
		CompleteMessage:      "The replanner returned an invalid decision, so aged paused for explicit steering.",
		WaitRationale:        "replanner returned an invalid decision",
		WaitQuestion:         "Dynamic replanning failed. Provide steering so aged can continue with explicit work items/actions.",
		WaitReason:           "dynamic_replan_error",
		WaitObjective:        "Dynamic replanning needs user steering before continuing.",
	})
}

func (s *Service) recoverReplanLimit(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, options replanLoopOptions) (bool, string, []WorkerTurnResult) {
	replanErr := fmt.Errorf("dynamic replanning reached %d consecutive turns without productive progress", maxConsecutiveUnproductiveReplanTurns)
	return s.recoverReplanFallback(ctx, task, turn, results, replanErr, options, replanFallbackConfig{
		CompleteReasonPrefix: "fallback completion after dynamic replanning stalled",
		CompleteMessage:      "Dynamic replanning stopped making productive progress, so aged paused for explicit steering.",
		WaitRationale:        "dynamic replanning stopped making productive progress",
		WaitQuestion:         "Dynamic replanning stopped making productive progress. Provide steering so aged can continue with explicit work items/actions.",
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

type replanQueueBlock struct {
	UserReason     string
	InternalReason string
}

func (s *Service) recoverReplanFallback(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, replanErr error, options replanLoopOptions, config replanFallbackConfig) (bool, string, []WorkerTurnResult) {
	if block := s.pendingReplanQueueBlock(ctx, task.ID); block.UserReason != "" || block.InternalReason != "" {
		if block.UserReason != "" {
			s.waitForReplanFallback(ctx, task, turn, replanErr, config, block.UserReason)
			return false, "", results
		}
		if err := s.waitForReplanQueueDrain(ctx, task, turn, replanErr, block.InternalReason); err != nil {
			_ = s.failTask(ctx, task.ID, err)
		}
		return false, "", results
	}
	if isReplanContextWindowError(replanErr) {
		s.waitForReplanContextOverflow(ctx, task, turn, replanErr, results)
		return false, "", results
	}
	_ = options
	s.waitForReplanFallback(ctx, task, turn, replanErr, config, "the replanner must emit explicit work items/actions or finish_objective")
	return false, "", results
}

func (s *Service) pendingReplanQueueBlock(ctx context.Context, taskID string) replanQueueBlock {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return replanQueueBlock{}
	}
	var userReasons []string
	var internalReasons []string
	if pending := pendingPullRequestFeedback(snapshot, taskID); len(pending) > 0 {
		uncovered := 0
		covered := 0
		for _, item := range pending {
			if pullRequestFeedbackCoveredByFollowUp(snapshot, taskID, item) {
				covered++
				continue
			}
			uncovered++
		}
		if uncovered > 0 {
			userReasons = append(userReasons, "queued pull request feedback must be handled before completion")
		}
		if covered > 0 {
			internalReasons = append(internalReasons, "queued pull request feedback is already being handled by pull request follow-up work")
		}
	}
	if pending := pendingWorkerSteering(snapshot, taskID); len(pending) > 0 {
		userReasons = append(userReasons, "queued worker steering must be handled before completion")
	}
	return replanQueueBlock{
		UserReason:     strings.Join(userReasons, "; "),
		InternalReason: strings.Join(internalReasons, "; "),
	}
}

func pullRequestFeedbackCoveredByFollowUp(snapshot core.Snapshot, taskID string, item PullRequestFeedbackItem) bool {
	if _, ok := pullRequestFollowUpWorkItem(snapshot, taskID, item.PullRequestID, item.FeedbackSignature); ok {
		return true
	}
	return activePullRequestFollowUpWorker(snapshot, taskID, item.PullRequestID)
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

func (s *Service) waitForReplanQueueDrain(ctx context.Context, task core.Task, turn int, replanErr error, reason string) error {
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventTaskReplanned,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"turn": turn,
			"decision": ReplanDecision{
				Action:    "wait",
				Rationale: "waiting for queued follow-up work",
				Message:   nonEmpty(reason, "queued follow-up work is already running"),
			},
			"fallback":          true,
			"internalQueueWait": true,
			"error":             replanErr.Error(),
			"candidateError":    reason,
		}),
	}); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "waiting_followup", nonEmpty(reason, "Waiting for queued follow-up work to finish.")); err != nil {
		return err
	}
	if _, err := s.startRunnableSpawnWorkItems(ctx, task.ID); err != nil {
		return err
	}
	if terminal, err := s.taskIsTerminal(ctx, task.ID); err != nil {
		return err
	} else if !terminal {
		if err := s.setTaskStatus(ctx, task.ID, core.TaskRunning); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) waitForReplanContextOverflow(ctx context.Context, task core.Task, turn int, replanErr error, results []WorkerTurnResult) {
	digest := latestCompactReplanDigest(results)
	message := "Replan context too large. aged could not fit the compact orchestration state into the model context window, so it paused for explicit steering."
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

func latestNodeIDForDependencies(results []WorkerTurnResult, dependsOn []string) string {
	if len(dependsOn) == 0 {
		return ""
	}
	deps := map[string]bool{}
	for _, dep := range dependsOn {
		deps[strings.TrimSpace(dep)] = true
	}
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		if deps[result.SpawnID] && strings.TrimSpace(result.NodeID) != "" {
			return result.NodeID
		}
	}
	return ""
}

func completedWorkerResultsForTask(snapshot core.Snapshot, taskID string) []WorkerTurnResult {
	workerMetadata := map[string]map[string]any{}
	results := []WorkerTurnResult{}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventWorkerCreated:
			var payload struct {
				Kind     string         `json:"kind"`
				Metadata map[string]any `json:"metadata"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				continue
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
				continue
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
	return results
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

func (s *Service) workerKindForWorkItem(role string, reason string, requested string, fallback string) string {
	if strings.TrimSpace(requested) != "" {
		if _, ok := s.runners[requested]; ok {
			return requested
		}
	}
	text := strings.ToLower(role + " " + reason)
	if strings.Contains(text, "review") || strings.Contains(text, "feedback") || strings.Contains(text, "critique") {
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
	if strings.HasPrefix(ref, "refs/pull/") {
		var lastErr error
		remoteSuffix := strings.TrimPrefix(ref, "refs/")
		for _, remote := range projectFetchRemotes(project) {
			remoteRef := "refs/remotes/" + remote + "/" + remoteSuffix
			refspec := "+" + ref + ":" + remoteRef
			if _, err := runCommand(ctx, project.LocalPath, "git", "fetch", "--prune", remote, refspec); err != nil {
				lastErr = err
				continue
			}
			if gitCommitRefExists(ctx, project.LocalPath, remoteRef) {
				return remoteRef, nil
			}
		}
		if lastErr != nil {
			return "", fmt.Errorf("sync git ref %q: %w", ref, lastErr)
		}
		return "", fmt.Errorf("sync git ref %q: no configured remote contains ref", ref)
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

func taskMetadataExecutionMode(metadata map[string]any) string {
	switch strings.ToLower(strings.TrimSpace(stringMetadataValue(metadata["executionMode"]))) {
	case "loop", "durable_loop", "agent_loop":
		return executionModeLoop
	default:
		return "orchestrated"
	}
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

func sortedTrueKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for key, ok := range values {
		if ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
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
		candidateWorkerID := ""
		reviewPhase := ""
		if node, ok := nodes[payload.WorkerID]; ok {
			payload.NodeID = nonEmpty(payload.NodeID, node.ID)
			payload.WorkerKind = nonEmpty(payload.WorkerKind, node.WorkerKind)
			payload.Role = nonEmpty(payload.Role, node.Role)
			payload.SpawnID = nonEmpty(payload.SpawnID, node.SpawnID)
			var metadata map[string]any
			if len(node.Metadata) > 0 && json.Unmarshal(node.Metadata, &metadata) == nil {
				candidateWorkerID = stringMetadata(metadata, "candidateWorkerID")
				reviewPhase = stringMetadata(metadata, "reviewPhase")
				payload.WorkerKind = nonEmpty(payload.WorkerKind, stringMetadata(metadata, "workerKind"))
			}
		}
		items = append(items, WorkerSteeringItem{
			EventID:           event.ID,
			WorkerID:          payload.WorkerID,
			NodeID:            payload.NodeID,
			WorkerKind:        payload.WorkerKind,
			Role:              payload.Role,
			SpawnID:           payload.SpawnID,
			CandidateWorkerID: candidateWorkerID,
			ReviewPhase:       reviewPhase,
			Status:            payload.Status,
			Reason:            payload.Reason,
			Message:           payload.Message,
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

func workerSteeringWorkItemID(eventID int64) string {
	return "worker_steering_" + strconv.FormatInt(eventID, 10)
}

func planWorkerWorkItemID(taskID string, plan Plan) string {
	if id := strings.TrimSpace(stringMetadata(plan.Metadata, "workItemID")); id != "" {
		return id
	}
	nodeID := strings.TrimSpace(stringMetadata(plan.Metadata, "nodeID"))
	if nodeID != "" {
		return "objective_worker_" + nodeID
	}
	planID := strings.TrimSpace(stringMetadata(plan.Metadata, "planID"))
	if planID != "" {
		return "objective_worker_" + planID
	}
	spawnID := strings.TrimSpace(stringMetadata(plan.Metadata, "spawnID"))
	if spawnID != "" {
		return "objective_worker_" + strings.TrimSpace(taskID) + "_" + spawnID
	}
	return ""
}

func (s *Service) recordPlanWorkItemStarted(ctx context.Context, taskID string, workerID string, plan Plan) (string, error) {
	kind := strings.TrimSpace(stringMetadata(plan.Metadata, "workItemKind"))
	if kind == "" {
		return "", nil
	}
	itemID := planWorkerWorkItemID(taskID, plan)
	if itemID == "" {
		return "", nil
	}
	if plan.Metadata != nil {
		plan.Metadata["workItemID"] = itemID
	}
	if strings.TrimSpace(stringMetadata(plan.Metadata, "sourceAction")) == "plan" {
		if err := s.recordWorkItemStarted(ctx, taskID, itemID, workerID); err != nil {
			return "", err
		}
		return itemID, nil
	}
	role := stringMetadata(plan.Metadata, "spawnRole")
	reason := stringMetadata(plan.Metadata, "spawnReason")
	if reason == "" {
		reason = plan.Rationale
	}
	if err := s.recordWorkItemQueued(ctx, taskID, map[string]any{
		"id":         itemID,
		"kind":       kind,
		"targetKind": "worker",
		"targetId":   workerID,
		"reason":     reason,
		"prompt":     plan.Prompt,
		"metadata": map[string]any{
			"workerId":        workerID,
			"workerKind":      plan.WorkerKind,
			"nodeId":          stringMetadata(plan.Metadata, "nodeID"),
			"planId":          stringMetadata(plan.Metadata, "planID"),
			"spawnId":         stringMetadata(plan.Metadata, "spawnID"),
			"role":            role,
			"reason":          reason,
			"dependsOn":       stringSliceMetadata(plan.Metadata, "dependsOn"),
			"parentNodeId":    stringMetadata(plan.Metadata, "parentNodeID"),
			"baseWorkerId":    stringMetadata(plan.Metadata, "baseWorkerID"),
			"reasoningEffort": stringMetadata(plan.Metadata, "reasoningEffort"),
			"sourceAction":    stringMetadata(plan.Metadata, "sourceAction"),
		},
	}); err != nil {
		return "", err
	}
	if err := s.recordWorkItemStarted(ctx, taskID, itemID, workerID); err != nil {
		return "", err
	}
	return itemID, nil
}

func (s *Service) recordPlanWorkItemCompletedForWorker(ctx context.Context, taskID string, itemID string, workerID string, status core.WorkerStatus, statusErr error) error {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" || status == core.WorkerWaiting {
		return nil
	}
	workStatus := workItemStatusForWorkerStatus(status)
	errText := ""
	if statusErr != nil {
		errText = statusErr.Error()
	}
	return s.recordWorkItemCompleted(ctx, taskID, itemID, workStatus, workerID, errText)
}

func workItemStatusForWorkerStatus(status core.WorkerStatus) core.WorkItemStatus {
	switch status {
	case core.WorkerSucceeded:
		return core.WorkItemSucceeded
	case core.WorkerCanceled:
		return core.WorkItemCanceled
	default:
		return core.WorkItemFailed
	}
}

func objectiveWorkerWorkItemKind(role string, reason string) string {
	text := strings.ToLower(role + " " + reason)
	switch {
	case strings.Contains(text, "compose") || strings.Contains(text, "reconcile") || strings.Contains(text, "integrate"):
		return "objective.compose"
	case strings.Contains(text, "slice") || strings.Contains(text, "shard") || strings.Contains(text, "file set") || strings.Contains(text, "subsystem") || strings.Contains(text, "port "):
		return "objective.slice"
	case strings.Contains(text, "validate") || strings.Contains(text, "verify") || strings.Contains(text, "test") || strings.Contains(text, "review") || strings.Contains(text, "benchmark"):
		return "objective.validate"
	default:
		return "objective.implement"
	}
}

func userQuestionWorkItemID(eventID int64) string {
	return "user_question_" + strconv.FormatInt(eventID, 10)
}

func questionIDFromUserQuestionWorkItemID(workItemID string) (string, bool) {
	raw := strings.TrimPrefix(strings.TrimSpace(workItemID), "user_question_")
	if raw == strings.TrimSpace(workItemID) || raw == "" {
		return "", false
	}
	if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
		return "", false
	}
	return "approval_" + raw, true
}

func approvalEventIDFromQuestionID(questionID string) (int64, bool) {
	raw := strings.TrimPrefix(strings.TrimSpace(questionID), "approval_")
	if raw == strings.TrimSpace(questionID) || raw == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	return id, err == nil
}

func (s *Service) recordWorkerSteeringWorkItemsCompleted(ctx context.Context, taskID string, items []WorkerSteeringItem) {
	for _, item := range items {
		_ = s.recordWorkItemCompleted(ctx, taskID, workerSteeringWorkItemID(item.EventID), core.WorkItemSucceeded, item.WorkerID, "")
	}
}

func (s *Service) recordLatestUserQuestionWorkItemCompleted(ctx context.Context, taskID string, workerID string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	workerID = strings.TrimSpace(workerID)
	var latest core.WorkItem
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || item.Kind != "user.question" || item.Status != core.WorkItemQueued {
			continue
		}
		if workerID != "" && item.TargetKind == "worker" && item.TargetID != workerID {
			continue
		}
		if workerID == "" && item.TargetKind == "worker" {
			continue
		}
		if latest.ID == "" || item.CreatedAt.After(latest.CreatedAt) {
			latest = item
		}
	}
	if latest.ID == "" {
		return
	}
	_ = s.recordWorkItemCompleted(ctx, taskID, latest.ID, core.WorkItemSucceeded, workerID, "")
}

func (s *Service) userQuestionPending(ctx context.Context, taskID string, workerID string) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	workerID = strings.TrimSpace(workerID)
	for _, item := range snapshot.WorkItems {
		if item.TaskID != taskID || item.Kind != "user.question" || item.Status != core.WorkItemQueued {
			continue
		}
		if workerID != "" && item.TargetKind == "worker" && item.TargetID != workerID {
			continue
		}
		if workerID == "" && item.TargetKind == "worker" {
			continue
		}
		return true
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

func normalizedWorkerDependencies(dependsOn []string) []string {
	deps := make([]string, 0, len(dependsOn))
	for _, dep := range dependsOn {
		dep = strings.TrimSpace(dep)
		if dep != "" {
			deps = append(deps, dep)
		}
	}
	return deps
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
	return latestFollowUp, true
}

func taskFailureRecoverableFromObjectiveResults(snapshot core.Snapshot, taskID string, results []WorkerTurnResult) bool {
	if len(results) == 0 {
		return false
	}
	if latestTaskFailureMatches(snapshot, taskID, isGraphDependencyFailure) {
		return true
	}
	if len(candidateResults(results)) == 0 {
		return false
	}
	if latestTaskFailureMatches(snapshot, taskID, func(errorText string) bool {
		return errorText == "" ||
			strings.Contains(errorText, "dynamic replan") ||
			strings.Contains(errorText, "candidate selection") ||
			strings.Contains(errorText, "multiple competing candidates") ||
			strings.Contains(errorText, "worker command failed")
	}) {
		return true
	}
	latest := latestWorkerResult(results)
	return latest.Status == core.WorkerFailed || latest.Status == core.WorkerCanceled
}

func isGraphDependencyFailure(errorText string) bool {
	return strings.Contains(errorText, "depends on unknown spawn") ||
		strings.Contains(errorText, "depends on unknown worker")
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

func latestWorkerResultWithStatus(results []WorkerTurnResult, status core.WorkerStatus) WorkerTurnResult {
	for i := len(results) - 1; i >= 0; i-- {
		if results[i].Status == status {
			return results[i]
		}
	}
	return WorkerTurnResult{}
}

func firstWorkerResultWithStatus(results []WorkerTurnResult, status core.WorkerStatus) WorkerTurnResult {
	for _, result := range results {
		if result.Status == status {
			return result
		}
	}
	return WorkerTurnResult{}
}

func objectiveReplanStateForTask(snapshot core.Snapshot, taskID string) (Plan, []WorkerTurnResult, error) {
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
		return Plan{}, nil, errors.New("task has no persisted plan to resume objective replanning")
	}
	if len(results) == 0 {
		return Plan{}, nil, errors.New("task has no completed worker results to resume objective replanning")
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

func latestPendingQuestionID(snapshot core.Snapshot, taskID string, workerID string) string {
	workerID = strings.TrimSpace(workerID)
	var latest core.Question
	for _, question := range snapshot.Questions {
		if question.TaskID != taskID || question.Decided {
			continue
		}
		if workerID != "" && question.WorkerID != "" && question.WorkerID != workerID {
			continue
		}
		if latest.ID == "" || question.CreatedAt.After(latest.CreatedAt) {
			latest = question
		}
	}
	return latest.ID
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
	return s.setTaskStatusChecked(ctx, taskID, status, "", "", false)
}

func (s *Service) setTaskStatusWithReason(ctx context.Context, taskID string, status core.TaskStatus, reason string) error {
	return s.setTaskStatusChecked(ctx, taskID, status, reason, "", false)
}

func (s *Service) setTaskStatusAllowingTerminalOverride(ctx context.Context, taskID string, status core.TaskStatus, reason string) error {
	return s.setTaskStatusChecked(ctx, taskID, status, reason, "", true)
}

func (s *Service) failTask(ctx context.Context, taskID string, err error) error {
	if err == nil {
		err = errors.New("task failed")
	}
	return s.setTaskStatusChecked(ctx, taskID, core.TaskFailed, "", err.Error(), false)
}

func (s *Service) setTaskStatusChecked(ctx context.Context, taskID string, status core.TaskStatus, reason string, statusError string, allowTerminalOverride bool) error {
	if status == "" {
		return errors.New("task status is required")
	}
	current, found, err := s.store.TaskStatus(ctx, taskID)
	if err != nil {
		return err
	}
	if !found {
		return eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(current) && current != status && !allowTerminalOverride {
		return nil
	}
	payload := map[string]any{"status": status}
	if strings.TrimSpace(reason) != "" {
		payload["reason"] = strings.TrimSpace(reason)
	}
	if strings.TrimSpace(statusError) != "" {
		payload["error"] = strings.TrimSpace(statusError)
	}
	_, err = s.append(ctx, core.Event{
		Type:    core.EventTaskStatus,
		TaskID:  taskID,
		Payload: core.MustJSON(payload),
	})
	return err
}

func (s *Service) updateTaskObjective(ctx context.Context, taskID string, status core.ObjectiveStatus, phase string, summary string) error {
	return s.updateTaskObjectiveChecked(ctx, taskID, status, phase, summary, false)
}

func (s *Service) updateTaskObjectiveAllowingTerminalOverride(ctx context.Context, taskID string, status core.ObjectiveStatus, phase string, summary string) error {
	return s.updateTaskObjectiveChecked(ctx, taskID, status, phase, summary, true)
}

func (s *Service) updateTaskObjectiveChecked(ctx context.Context, taskID string, status core.ObjectiveStatus, phase string, summary string, allowTerminalOverride bool) error {
	current, found, err := s.store.TaskStatus(ctx, taskID)
	if err != nil {
		return err
	}
	if !found {
		return eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(current) && !allowTerminalOverride {
		switch status {
		case core.ObjectiveSatisfied, core.ObjectiveAbandoned:
		default:
			return nil
		}
	}
	_, err = s.append(ctx, core.Event{
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
		"workItemID",
		"workItemKind",
		"sourceAction",
		"executeActionsOnSuccess",
		"planActions",
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
		"usageFallbackAttempt",
		"usageFallbackFromProvider",
		"usageFallbackToProvider",
		"usageFallbackFromWorkerID",
		"usageFallbackReason",
		"backgroundPullRequestFollowUp",
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

func explicitBoolMetadata(metadata map[string]any, key string) (bool, bool) {
	if metadata == nil {
		return false, false
	}
	value, ok := metadata[key].(bool)
	return value, ok
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

func shouldInheritLatestCandidateForPlan(plan Plan) bool {
	if len(plan.WorkItems) > 0 {
		return false
	}
	return shouldInheritLatestCandidate(plan.Metadata)
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

func (s *workerRunState) failureText(runErr error) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := []string{s.lastError, s.summary}
	if runErr != nil {
		parts = append(parts, runErr.Error())
	}
	return strings.Join(parts, "\n")
}

func (s *workerRunState) normalizeCompletionStatus(plan Plan, status core.WorkerStatus, runErr error, changes WorkspaceChanges) (core.WorkerStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if status != core.WorkerSucceeded || runErr != nil {
		return status, runErr
	}
	if reason := s.incompleteSuccessReasonLocked(changes); reason != "" {
		return core.WorkerFailed, errors.New(reason)
	}
	if reason := s.emptyRetrySuccessReasonLocked(plan, changes); reason != "" {
		return core.WorkerFailed, errors.New(reason)
	}
	return status, runErr
}

func (s *workerRunState) incompleteSuccessReasonLocked(changes WorkspaceChanges) string {
	if strings.TrimSpace(s.lastError) == "" || !workerSummaryDefersCompletion(s.summary) {
		return ""
	}
	reason := "worker reported success after an unresolved tool or runtime error while deferring completion"
	if strings.TrimSpace(s.summary) != "" {
		reason += ": " + strings.TrimSpace(s.summary)
	}
	return reason
}

func (s *workerRunState) emptyRetrySuccessReasonLocked(plan Plan, changes WorkspaceChanges) string {
	if strings.TrimSpace(s.summary) != "" || s.logCount == 0 || stringMetadata(plan.Metadata, "retryFromWorkerID") == "" {
		return ""
	}
	if changes.Dirty || len(changes.ChangedFiles) > 0 {
		return ""
	}
	return "worker reported success without a final summary or new workspace changes while running in a retained retry workspace"
}

func workerSummaryDefersCompletion(summary string) bool {
	normalized := strings.ToLower(strings.TrimSpace(summary))
	if normalized == "" {
		return false
	}
	normalized = strings.NewReplacer(
		"\u2018", "'",
		"\u2019", "'",
	).Replace(normalized)
	normalized = strings.Join(strings.Fields(normalized), " ")
	for _, marker := range []string{
		"waiting for",
		"will re-invoke",
		"re-invoke when",
		"wakeup",
		"wake up",
		"still active",
		"still running",
		"still progressing",
		"still no errors",
		"continues without errors",
		"continuing until",
		"continuing to poll",
		"waiting for final",
		"wait for final",
		"continue later",
		"continue when",
		"not complete",
		"not finished",
		"inconclusive",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return deferredNextWorkPattern.MatchString(normalized)
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
	if resultErr := s.finalErrorLocked(status, runErr); resultErr != "" {
		payload["error"] = resultErr
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
	if resultErr := s.finalErrorLocked(status, runErr); resultErr != "" {
		result.Error = resultErr
	}
	return result
}

func (s *workerRunState) finalErrorLocked(status core.WorkerStatus, runErr error) string {
	if runErr != nil {
		return runErr.Error()
	}
	if status != core.WorkerSucceeded {
		return s.lastError
	}
	return ""
}

type jsonRawMessage []byte

func (m jsonRawMessage) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return []byte("null"), nil
	}
	return m, nil
}
