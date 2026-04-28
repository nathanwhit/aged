package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

type WorkspaceManager interface {
	Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error)
	DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error)
	ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error)
	Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error)
}

type WorkspaceSpec struct {
	TaskID   string
	WorkerID string
	WorkDir  string
}

type PreparedWorkspace struct {
	Root          string `json:"root"`
	CWD           string `json:"cwd"`
	SourceRoot    string `json:"sourceRoot"`
	WorkspaceName string `json:"workspaceName"`
	Change        string `json:"change"`
	BaseChange    string `json:"baseChange"`
	Status        string `json:"status"`
	SourceStatus  string `json:"sourceStatus"`
	Mode          string `json:"mode"`
	Dirty         bool   `json:"dirty"`
	SourceDirty   bool   `json:"sourceDirty"`
	VCSType       string `json:"vcsType"`
	CleanupPolicy string `json:"cleanupPolicy"`
	WorkerID      string `json:"workerId"`
	TaskID        string `json:"taskId"`
}

type WorkspaceChangedFile struct {
	Path   string `json:"path"`
	Status string `json:"status"`
}

type WorkspaceChanges struct {
	Root          string                 `json:"root"`
	CWD           string                 `json:"cwd"`
	WorkspaceName string                 `json:"workspaceName"`
	Mode          string                 `json:"mode"`
	VCSType       string                 `json:"vcsType"`
	Status        string                 `json:"status"`
	DiffStat      string                 `json:"diffStat"`
	ChangedFiles  []WorkspaceChangedFile `json:"changedFiles"`
	Dirty         bool                   `json:"dirty"`
	Error         string                 `json:"error,omitempty"`
}

type WorkspaceResult string

const (
	WorkspaceResultSucceeded WorkspaceResult = "succeeded"
	WorkspaceResultFailed    WorkspaceResult = "failed"
	WorkspaceResultCanceled  WorkspaceResult = "canceled"
)

type WorkspaceCleanupPolicy string

const (
	WorkspaceCleanupRetain           WorkspaceCleanupPolicy = "retain"
	WorkspaceCleanupDeleteOnSuccess  WorkspaceCleanupPolicy = "delete_on_success"
	WorkspaceCleanupDeleteOnTerminal WorkspaceCleanupPolicy = "delete_on_terminal"
)

type WorkspaceCleanup struct {
	Root          string          `json:"root"`
	CWD           string          `json:"cwd"`
	WorkspaceName string          `json:"workspaceName"`
	Mode          string          `json:"mode"`
	VCSType       string          `json:"vcsType"`
	Policy        string          `json:"policy"`
	Result        WorkspaceResult `json:"result"`
	Cleaned       bool            `json:"cleaned"`
	Reason        string          `json:"reason,omitempty"`
	Error         string          `json:"error,omitempty"`
}

type WorkspaceMode string
type WorkspaceVCS string

const (
	WorkspaceModeIsolated WorkspaceMode = "isolated"
	WorkspaceModeShared   WorkspaceMode = "shared"

	WorkspaceVCSAuto WorkspaceVCS = "auto"
	WorkspaceVCSJJ   WorkspaceVCS = "jj"
	WorkspaceVCSGit  WorkspaceVCS = "git"
)

func NewWorkspaceManager(vcs WorkspaceVCS, mode WorkspaceMode, workspaceRoot string, cleanupPolicy WorkspaceCleanupPolicy) WorkspaceManager {
	if vcs == "" {
		vcs = WorkspaceVCSAuto
	}
	switch vcs {
	case WorkspaceVCSJJ:
		return NewJJWorkspaceManager(mode, workspaceRoot, cleanupPolicy)
	case WorkspaceVCSGit:
		return NewGitWorkspaceManager(mode, workspaceRoot, cleanupPolicy)
	default:
		return AutoWorkspaceManager{Mode: mode, WorkspaceRoot: workspaceRoot, CleanupPolicy: cleanupPolicy}
	}
}

type AutoWorkspaceManager struct {
	Mode          WorkspaceMode
	WorkspaceRoot string
	CleanupPolicy WorkspaceCleanupPolicy
}

func (m AutoWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	absWorkDir, err := filepath.Abs(spec.WorkDir)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	if _, err := runJJ(ctx, absWorkDir, "root"); err == nil {
		manager := NewJJWorkspaceManager(m.Mode, m.WorkspaceRoot, m.cleanupPolicy())
		manager.CleanupPolicy = m.cleanupPolicy()
		return manager.Prepare(ctx, spec)
	}
	if _, err := runGit(ctx, absWorkDir, "rev-parse", "--show-toplevel"); err == nil {
		manager := NewGitWorkspaceManager(m.Mode, m.WorkspaceRoot, m.cleanupPolicy())
		manager.CleanupPolicy = m.cleanupPolicy()
		return manager.Prepare(ctx, spec)
	}
	return PreparedWorkspace{}, fmt.Errorf("workdir is not inside a supported VCS workspace: %s", absWorkDir)
}

func (m AutoWorkspaceManager) Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		manager.CleanupPolicy = WorkspaceCleanupPolicy(workspace.CleanupPolicy)
		return manager.Cleanup(ctx, workspace, result)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		manager.CleanupPolicy = WorkspaceCleanupPolicy(workspace.CleanupPolicy)
		return manager.Cleanup(ctx, workspace, result)
	default:
		cleanup := cleanupSkipped(workspace, result, "unsupported VCS type")
		return cleanup, nil
	}
}

func (m AutoWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.DescribeChanges(ctx, workspace)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.DescribeChanges(ctx, workspace)
	default:
		changes := baseWorkspaceChanges(workspace)
		changes.Error = "unsupported VCS type"
		return changes, nil
	}
}

func (m AutoWorkspaceManager) ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.ApplyChanges(ctx, workspace, changes)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.ApplyChanges(ctx, workspace, changes)
	default:
		return baseWorkerApplyResult(workspace, "unsupported"), fmt.Errorf("unsupported VCS type %q", workspace.VCSType)
	}
}

func (m AutoWorkspaceManager) cleanupPolicy() WorkspaceCleanupPolicy {
	if m.CleanupPolicy != "" {
		return m.CleanupPolicy
	}
	return WorkspaceCleanupRetain
}

type JJWorkspaceManager struct {
	Mode          WorkspaceMode
	WorkspaceRoot string
	CleanupPolicy WorkspaceCleanupPolicy
}

func NewJJWorkspaceManager(mode WorkspaceMode, workspaceRoot string, cleanupPolicy WorkspaceCleanupPolicy) JJWorkspaceManager {
	if mode == "" {
		mode = WorkspaceModeIsolated
	}
	if workspaceRoot == "" {
		workspaceRoot = ".aged/workspaces"
	}
	if cleanupPolicy == "" {
		cleanupPolicy = WorkspaceCleanupRetain
	}
	return JJWorkspaceManager{
		Mode:          mode,
		WorkspaceRoot: workspaceRoot,
		CleanupPolicy: cleanupPolicy,
	}
}

func (m JJWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	if strings.TrimSpace(spec.WorkDir) == "" {
		return PreparedWorkspace{}, errors.New("workdir is required")
	}
	absWorkDir, err := filepath.Abs(spec.WorkDir)
	if err != nil {
		return PreparedWorkspace{}, err
	}

	root, err := runJJ(ctx, absWorkDir, "root")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("resolve jj root: %w", err)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return PreparedWorkspace{}, errors.New("jj root returned empty path")
	}

	baseChange, err := runJJ(ctx, root, "log", "-r", "@", "--no-graph")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read jj working copy change: %w", err)
	}
	baseChange = strings.TrimSpace(baseChange)

	sourceStatus, err := runJJ(ctx, root, "status")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read jj status: %w", err)
	}
	sourceStatus = strings.TrimSpace(sourceStatus)

	if m.Mode == WorkspaceModeShared {
		return PreparedWorkspace{
			Root:          root,
			CWD:           root,
			SourceRoot:    root,
			WorkspaceName: "shared",
			Change:        baseChange,
			BaseChange:    baseChange,
			Status:        sourceStatus,
			SourceStatus:  sourceStatus,
			Dirty:         isDirty(sourceStatus),
			SourceDirty:   isDirty(sourceStatus),
			Mode:          string(WorkspaceModeShared),
			VCSType:       "jj",
			CleanupPolicy: string(WorkspaceCleanupRetain),
			WorkerID:      spec.WorkerID,
			TaskID:        spec.TaskID,
		}, nil
	}

	destination, workspaceName, err := m.workspaceDestination(root, spec)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	if _, err := os.Stat(destination); err == nil {
		return PreparedWorkspace{}, fmt.Errorf("workspace destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return PreparedWorkspace{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return PreparedWorkspace{}, err
	}

	if _, err := runJJ(ctx, root, "workspace", "add", "--name", workspaceName, "--revision", "@", "--sparse-patterns", "copy", destination); err != nil {
		return PreparedWorkspace{}, fmt.Errorf("create isolated jj workspace: %w", err)
	}

	change, err := runJJ(ctx, destination, "log", "-r", "@", "--no-graph")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated jj working copy change: %w", err)
	}
	change = strings.TrimSpace(change)

	status, err := runJJ(ctx, destination, "status")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated jj status: %w", err)
	}
	status = strings.TrimSpace(status)

	return PreparedWorkspace{
		Root:          destination,
		CWD:           destination,
		SourceRoot:    root,
		WorkspaceName: workspaceName,
		Change:        change,
		BaseChange:    baseChange,
		Status:        status,
		SourceStatus:  sourceStatus,
		Dirty:         isDirty(status),
		SourceDirty:   isDirty(sourceStatus),
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       "jj",
		CleanupPolicy: m.cleanupPolicy(),
		WorkerID:      spec.WorkerID,
		TaskID:        spec.TaskID,
	}, nil
}

func (m JJWorkspaceManager) workspaceDestination(root string, spec WorkspaceSpec) (string, string, error) {
	name := "aged-" + shortID(spec.WorkerID)
	workspaceRoot := m.WorkspaceRoot
	if workspaceRoot == "" {
		workspaceRoot = ".aged/workspaces"
	}
	if !filepath.IsAbs(workspaceRoot) {
		workspaceRoot = filepath.Join(root, workspaceRoot)
	}
	return filepath.Join(workspaceRoot, name), name, nil
}

func (m JJWorkspaceManager) cleanupPolicy() string {
	if m.CleanupPolicy != "" {
		return string(m.CleanupPolicy)
	}
	return string(WorkspaceCleanupRetain)
}

func (m JJWorkspaceManager) Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	cleanup, shouldClean := cleanupDecision(workspace, result)
	if !shouldClean {
		return cleanup, nil
	}
	if workspace.Mode != string(WorkspaceModeIsolated) {
		cleanup.Reason = "shared workspace is not removed"
		return cleanup, nil
	}
	if workspace.WorkspaceName == "" || workspace.Root == "" || workspace.SourceRoot == "" {
		cleanup.Error = "workspace cleanup requires workspace name, root, and source root"
		return cleanup, errors.New(cleanup.Error)
	}
	if _, err := runJJ(ctx, workspace.SourceRoot, "workspace", "forget", workspace.WorkspaceName); err != nil {
		cleanup.Error = err.Error()
		return cleanup, fmt.Errorf("forget jj workspace: %w", err)
	}
	if err := os.RemoveAll(workspace.Root); err != nil {
		cleanup.Error = err.Error()
		return cleanup, fmt.Errorf("remove jj workspace directory: %w", err)
	}
	cleanup.Cleaned = true
	return cleanup, nil
}

func (m JJWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	changes := baseWorkspaceChanges(workspace)
	if workspace.CWD == "" {
		changes.Error = "workspace cwd is required"
		return changes, errors.New(changes.Error)
	}

	status, err := runJJRefreshingStale(ctx, workspace.CWD, "status")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read jj workspace status: %w", err)
	}
	changes.Status = strings.TrimSpace(status)
	changes.Dirty = isDirty(changes.Status)

	diffStat, err := runJJRefreshingStale(ctx, workspace.CWD, "diff", "--stat")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read jj workspace diff stat: %w", err)
	}
	changes.DiffStat = strings.TrimSpace(diffStat)

	summary, err := runJJRefreshingStale(ctx, workspace.CWD, "diff", "--summary")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read jj workspace diff summary: %w", err)
	}
	changes.ChangedFiles = parseJJDiffSummary(summary)
	return changes, nil
}

func (m JJWorkspaceManager) ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	result := baseWorkerApplyResult(workspace, "jj_new_merge")
	result.AppliedFiles = changes.ChangedFiles
	if workspace.Mode != string(WorkspaceModeIsolated) {
		return result, errors.New("applying jj worker changes requires an isolated workspace")
	}
	if workspace.SourceRoot == "" || workspace.WorkspaceName == "" {
		return result, errors.New("applying jj worker changes requires source root and workspace name")
	}
	if len(changes.ChangedFiles) == 0 && !changes.Dirty {
		return result, nil
	}
	workerRevision := workspace.WorkspaceName + "@"
	message := "Apply worker " + shortID(workspace.WorkerID)
	if _, err := runJJRefreshingStale(ctx, workspace.CWD, "status"); err != nil {
		return result, fmt.Errorf("refresh jj worker workspace: %w", err)
	}
	if _, err := runJJ(ctx, workspace.SourceRoot, "new", "--message", message, "@", workerRevision); err != nil {
		return result, fmt.Errorf("create jj merge revision: %w", err)
	}
	return result, nil
}

func isDirty(status string) bool {
	return !strings.Contains(status, "The working copy has no changes.")
}

var unsafeWorkspaceNameChars = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func shortID(id string) string {
	id = unsafeWorkspaceNameChars.ReplaceAllString(id, "-")
	id = strings.Trim(id, "-")
	if len(id) > 12 {
		return id[:12]
	}
	if id == "" {
		return "worker"
	}
	return id
}

func runJJ(ctx context.Context, dir string, args ...string) (string, error) {
	return runCommand(ctx, dir, "jj", args...)
}

func runJJRefreshingStale(ctx context.Context, dir string, args ...string) (string, error) {
	out, err := runJJ(ctx, dir, args...)
	if err == nil || !strings.Contains(err.Error(), "working copy is stale") {
		return out, err
	}
	if _, updateErr := runJJ(ctx, dir, "workspace", "update-stale"); updateErr != nil {
		return out, err
	}
	return runJJ(ctx, dir, args...)
}

type GitWorkspaceManager struct {
	Mode          WorkspaceMode
	WorkspaceRoot string
	CleanupPolicy WorkspaceCleanupPolicy
}

func NewGitWorkspaceManager(mode WorkspaceMode, workspaceRoot string, cleanupPolicy WorkspaceCleanupPolicy) GitWorkspaceManager {
	if mode == "" {
		mode = WorkspaceModeIsolated
	}
	if workspaceRoot == "" {
		workspaceRoot = ".aged/workspaces"
	}
	if cleanupPolicy == "" {
		cleanupPolicy = WorkspaceCleanupRetain
	}
	return GitWorkspaceManager{
		Mode:          mode,
		WorkspaceRoot: workspaceRoot,
		CleanupPolicy: cleanupPolicy,
	}
}

func (m GitWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	if strings.TrimSpace(spec.WorkDir) == "" {
		return PreparedWorkspace{}, errors.New("workdir is required")
	}
	absWorkDir, err := filepath.Abs(spec.WorkDir)
	if err != nil {
		return PreparedWorkspace{}, err
	}

	root, err := runGit(ctx, absWorkDir, "rev-parse", "--show-toplevel")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("resolve git root: %w", err)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return PreparedWorkspace{}, errors.New("git root returned empty path")
	}

	baseChange, err := runGit(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read git HEAD: %w", err)
	}
	baseChange = strings.TrimSpace(baseChange)

	sourceStatus, err := runGit(ctx, root, "status", "--short", "--branch")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read git status: %w", err)
	}
	sourceStatus = strings.TrimSpace(sourceStatus)

	if m.Mode == WorkspaceModeShared {
		return PreparedWorkspace{
			Root:          root,
			CWD:           root,
			SourceRoot:    root,
			WorkspaceName: "shared",
			Change:        baseChange,
			BaseChange:    baseChange,
			Status:        sourceStatus,
			SourceStatus:  sourceStatus,
			Dirty:         gitStatusDirty(sourceStatus),
			SourceDirty:   gitStatusDirty(sourceStatus),
			Mode:          string(WorkspaceModeShared),
			VCSType:       "git",
			CleanupPolicy: string(WorkspaceCleanupRetain),
			WorkerID:      spec.WorkerID,
			TaskID:        spec.TaskID,
		}, nil
	}
	if gitStatusDirty(sourceStatus) {
		return PreparedWorkspace{}, errors.New("isolated git workspaces require a clean source working tree")
	}

	destination, workspaceName := gitWorkspaceDestination(root, m.WorkspaceRoot, spec)
	if _, err := os.Stat(destination); err == nil {
		return PreparedWorkspace{}, fmt.Errorf("workspace destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return PreparedWorkspace{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return PreparedWorkspace{}, err
	}
	if _, err := runGit(ctx, root, "worktree", "add", "--detach", destination, "HEAD"); err != nil {
		return PreparedWorkspace{}, fmt.Errorf("create isolated git worktree: %w", err)
	}

	change, err := runGit(ctx, destination, "rev-parse", "HEAD")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated git HEAD: %w", err)
	}
	change = strings.TrimSpace(change)

	status, err := runGit(ctx, destination, "status", "--short", "--branch")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated git status: %w", err)
	}
	status = strings.TrimSpace(status)

	return PreparedWorkspace{
		Root:          destination,
		CWD:           destination,
		SourceRoot:    root,
		WorkspaceName: workspaceName,
		Change:        change,
		BaseChange:    baseChange,
		Status:        status,
		SourceStatus:  sourceStatus,
		Dirty:         gitStatusDirty(status),
		SourceDirty:   gitStatusDirty(sourceStatus),
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       "git",
		CleanupPolicy: m.cleanupPolicy(),
		WorkerID:      spec.WorkerID,
		TaskID:        spec.TaskID,
	}, nil
}

func (m GitWorkspaceManager) cleanupPolicy() string {
	if m.CleanupPolicy != "" {
		return string(m.CleanupPolicy)
	}
	return string(WorkspaceCleanupRetain)
}

func (m GitWorkspaceManager) Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	cleanup, shouldClean := cleanupDecision(workspace, result)
	if !shouldClean {
		return cleanup, nil
	}
	if workspace.Mode != string(WorkspaceModeIsolated) {
		cleanup.Reason = "shared workspace is not removed"
		return cleanup, nil
	}
	if workspace.Root == "" || workspace.SourceRoot == "" {
		cleanup.Error = "workspace cleanup requires root and source root"
		return cleanup, errors.New(cleanup.Error)
	}
	if _, err := runGit(ctx, workspace.SourceRoot, "worktree", "remove", "--force", workspace.Root); err != nil {
		cleanup.Error = err.Error()
		return cleanup, fmt.Errorf("remove git worktree: %w", err)
	}
	cleanup.Cleaned = true
	return cleanup, nil
}

func (m GitWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	changes := baseWorkspaceChanges(workspace)
	if workspace.CWD == "" {
		changes.Error = "workspace cwd is required"
		return changes, errors.New(changes.Error)
	}

	status, err := runGit(ctx, workspace.CWD, "status", "--short", "--branch")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace status: %w", err)
	}
	changes.Status = strings.TrimSpace(status)
	changes.Dirty = gitStatusDirty(changes.Status)

	diffStat, err := runGit(ctx, workspace.CWD, "diff", "--stat", "HEAD", "--")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace diff stat: %w", err)
	}
	changes.DiffStat = strings.TrimSpace(diffStat)

	porcelain, err := runGit(ctx, workspace.CWD, "status", "--porcelain=v1")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace changed files: %w", err)
	}
	changes.ChangedFiles = parseGitPorcelain(porcelain)
	return changes, nil
}

func (m GitWorkspaceManager) ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	result := baseWorkerApplyResult(workspace, "git_commit_merge")
	result.AppliedFiles = changes.ChangedFiles
	if workspace.Mode != string(WorkspaceModeIsolated) {
		return result, errors.New("applying git worker changes requires an isolated workspace")
	}
	if workspace.SourceRoot == "" || workspace.Root == "" {
		return result, errors.New("applying git worker changes requires source and workspace roots")
	}
	if len(changes.ChangedFiles) == 0 && !changes.Dirty {
		return result, nil
	}
	if _, err := runGit(ctx, workspace.Root, "add", "-A"); err != nil {
		return result, fmt.Errorf("stage git worker changes: %w", err)
	}
	if _, err := runGit(ctx, workspace.Root, "commit", "-m", "Apply worker "+shortID(workspace.WorkerID)); err != nil {
		return result, fmt.Errorf("commit git worker changes: %w", err)
	}
	commit, err := runGit(ctx, workspace.Root, "rev-parse", "HEAD")
	if err != nil {
		return result, fmt.Errorf("read git worker commit: %w", err)
	}
	if _, err := runGit(ctx, workspace.SourceRoot, "merge", "--no-ff", strings.TrimSpace(commit)); err != nil {
		return result, fmt.Errorf("merge git worker commit: %w", err)
	}
	return result, nil
}

func cleanupDecision(workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, bool) {
	policy := WorkspaceCleanupPolicy(workspace.CleanupPolicy)
	if policy == "" {
		policy = WorkspaceCleanupRetain
	}
	cleanup := WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        string(policy),
		Result:        result,
	}
	switch policy {
	case WorkspaceCleanupDeleteOnSuccess:
		if result == WorkspaceResultSucceeded {
			return cleanup, true
		}
		cleanup.Reason = "policy only deletes successful workspaces"
		return cleanup, false
	case WorkspaceCleanupDeleteOnTerminal:
		return cleanup, true
	default:
		cleanup.Reason = "policy retains workspaces"
		return cleanup, false
	}
}

func cleanupSkipped(workspace PreparedWorkspace, result WorkspaceResult, reason string) WorkspaceCleanup {
	return WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        workspace.CleanupPolicy,
		Result:        result,
		Reason:        reason,
	}
}

func baseWorkspaceChanges(workspace PreparedWorkspace) WorkspaceChanges {
	return WorkspaceChanges{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Status:        workspace.Status,
		Dirty:         workspace.Dirty,
	}
}

func baseWorkerApplyResult(workspace PreparedWorkspace, method string) WorkerApplyResult {
	return WorkerApplyResult{
		SourceRoot:    workspace.SourceRoot,
		WorkspaceRoot: workspace.Root,
		Method:        method,
	}
}

func parseJJDiffSummary(summary string) []WorkspaceChangedFile {
	var files []WorkspaceChangedFile
	for _, line := range strings.Split(summary, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		status, path, ok := strings.Cut(line, " ")
		if !ok {
			files = append(files, WorkspaceChangedFile{Path: line, Status: "changed"})
			continue
		}
		files = append(files, WorkspaceChangedFile{Path: strings.TrimSpace(path), Status: normalizeChangeStatus(status)})
	}
	return files
}

func parseGitPorcelain(status string) []WorkspaceChangedFile {
	var files []WorkspaceChangedFile
	for _, line := range strings.Split(status, "\n") {
		if strings.TrimSpace(line) == "" || len(line) < 4 {
			continue
		}
		code := strings.TrimSpace(line[:2])
		path := strings.TrimSpace(line[3:])
		if before, after, ok := strings.Cut(path, " -> "); ok {
			files = append(files, WorkspaceChangedFile{Path: before, Status: "renamed_from"})
			path = after
		}
		files = append(files, WorkspaceChangedFile{Path: path, Status: normalizeChangeStatus(code)})
	}
	return files
}

func normalizeChangeStatus(status string) string {
	switch {
	case strings.Contains(status, "?"):
		return "untracked"
	case strings.Contains(status, "A"):
		return "added"
	case strings.Contains(status, "D"):
		return "deleted"
	case strings.Contains(status, "R"):
		return "renamed"
	case strings.Contains(status, "C"):
		return "copied"
	case strings.Contains(status, "M"):
		return "modified"
	default:
		return strings.ToLower(status)
	}
}

func gitWorkspaceDestination(root string, workspaceRoot string, spec WorkspaceSpec) (string, string) {
	name := "aged-" + shortID(spec.WorkerID)
	if workspaceRoot == "" {
		workspaceRoot = ".aged/workspaces"
	}
	if !filepath.IsAbs(workspaceRoot) {
		workspaceRoot = filepath.Join(root, workspaceRoot)
	}
	return filepath.Join(workspaceRoot, name), name
}

func gitStatusDirty(status string) bool {
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "##") {
			return true
		}
	}
	return false
}

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	return runCommand(ctx, dir, "git", args...)
}

func runCommand(ctx context.Context, dir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return "", fmt.Errorf("%w: %s", err, detail)
		}
		return "", err
	}
	return stdout.String(), nil
}
