package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"aged/internal/core"
)

const defaultRetainedArtifactCleanupMinAge = time.Hour
const defaultRetainedArtifactCleanupInterval = 10 * time.Minute

var defaultRetainedArtifactDirNames = []string{"target"}

type RetainedWorkspaceArtifactCleanupOptions struct {
	MinAge           time.Duration
	DryRun           bool
	ArtifactDirNames []string
	Now              time.Time
	IgnoreMinAge     bool
}

type RetainedWorkspaceArtifactCleanupReport struct {
	DryRun           bool               `json:"dryRun"`
	MinAgeSeconds    int64              `json:"minAgeSeconds"`
	ArtifactDirNames []string           `json:"artifactDirNames"`
	Scanned          int                `json:"scanned"`
	Cleaned          int                `json:"cleaned"`
	Skipped          int                `json:"skipped"`
	BytesRemoved     int64              `json:"bytesRemoved"`
	Workspaces       []WorkspaceCleanup `json:"workspaces,omitempty"`
}

func (s *Service) StartRetainedWorkspaceArtifactCleanup(ctx context.Context, interval time.Duration, options RetainedWorkspaceArtifactCleanupOptions) {
	if s == nil {
		return
	}
	s.SetRetainedWorkspaceArtifactCleanup(options)
	if interval <= 0 {
		interval = defaultRetainedArtifactCleanupInterval
	}
	go func() {
		s.cleanupRetainedWorkspaceArtifactsLogged(ctx, options)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cleanupRetainedWorkspaceArtifactsLogged(ctx, options)
			}
		}
	}()
}

func (s *Service) SetRetainedWorkspaceArtifactCleanup(options RetainedWorkspaceArtifactCleanupOptions) {
	if s == nil {
		return
	}
	options = normalizeRetainedArtifactCleanupOptions(options)
	s.mu.Lock()
	s.retainedArtifactCleanup = options
	s.retainedArtifactCleanupEnabled = true
	s.mu.Unlock()
}

func (s *Service) retainedArtifactCleanupOptions() (RetainedWorkspaceArtifactCleanupOptions, bool) {
	if s == nil {
		return RetainedWorkspaceArtifactCleanupOptions{}, false
	}
	s.mu.Lock()
	options := s.retainedArtifactCleanup
	enabled := s.retainedArtifactCleanupEnabled
	s.mu.Unlock()
	return options, enabled
}

func (s *Service) cleanupRetainedWorkspaceArtifactsLogged(ctx context.Context, options RetainedWorkspaceArtifactCleanupOptions) {
	report, err := s.CleanupRetainedWorkspaceArtifacts(ctx, options)
	if err != nil {
		slog.Warn("cleanup retained workspace artifacts", "error", err)
	} else if report.Scanned > 0 {
		slog.Info("cleanup retained workspace artifacts", "scanned", report.Scanned, "cleaned", report.Cleaned, "skipped", report.Skipped, "bytesRemoved", report.BytesRemoved, "dryRun", report.DryRun)
	}
}

func (s *Service) CleanupRetainedWorkspaceArtifacts(ctx context.Context, options RetainedWorkspaceArtifactCleanupOptions) (RetainedWorkspaceArtifactCleanupReport, error) {
	options = normalizeRetainedArtifactCleanupOptions(options)
	report := RetainedWorkspaceArtifactCleanupReport{
		DryRun:           options.DryRun,
		MinAgeSeconds:    int64(options.MinAge.Seconds()),
		ArtifactDirNames: append([]string(nil), options.ArtifactDirNames...),
	}
	if s == nil || s.store == nil {
		return report, errors.New("service store is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return report, err
	}
	workers := map[string]core.Worker{}
	for _, worker := range snapshot.Workers {
		workers[worker.ID] = worker
	}
	activeNodes := activeExecutionNodesByWorker(snapshot.ExecutionNodes)
	workspaces := latestWorkspaceEvents(snapshot.Events)
	for workerID, event := range workspaces {
		workerState, ok := workers[workerID]
		if !ok {
			continue
		}
		var workspace PreparedWorkspace
		if err := json.Unmarshal(event.Payload, &workspace); err != nil {
			return report, fmt.Errorf("decode retained workspace for worker %s: %w", workerID, err)
		}
		report.Scanned++
		cleanup := s.cleanupRetainedWorkspaceArtifacts(ctx, workerState, workspace, options, activeNodes[workerID])
		report.Workspaces = append(report.Workspaces, cleanup)
		if cleanup.Cleaned {
			report.Cleaned++
			report.BytesRemoved += cleanup.BytesRemoved
			if _, err := s.append(ctx, core.Event{
				Type:     core.EventWorkerCleanup,
				TaskID:   workerState.TaskID,
				WorkerID: workerID,
				Payload:  core.MustJSON(cleanup),
			}); err != nil {
				return report, err
			}
		} else {
			report.Skipped++
		}
	}
	return report, nil
}

func latestWorkspaceEvents(events []core.Event) map[string]core.Event {
	workspaces := map[string]core.Event{}
	for _, event := range events {
		if event.Type != core.EventWorkerWorkspace || strings.TrimSpace(event.WorkerID) == "" {
			continue
		}
		workspaces[event.WorkerID] = event
	}
	return workspaces
}

func activeExecutionNodesByWorker(nodes []core.ExecutionNode) map[string][]core.ExecutionNode {
	active := map[string][]core.ExecutionNode{}
	for _, node := range nodes {
		if strings.TrimSpace(node.WorkerID) == "" || isTerminalWorkerStatus(node.Status) {
			continue
		}
		active[node.WorkerID] = append(active[node.WorkerID], node)
	}
	return active
}

func normalizeRetainedArtifactCleanupOptions(options RetainedWorkspaceArtifactCleanupOptions) RetainedWorkspaceArtifactCleanupOptions {
	if options.MinAge <= 0 {
		options.MinAge = defaultRetainedArtifactCleanupMinAge
	}
	if options.Now.IsZero() {
		options.Now = time.Now().UTC()
	}
	names := cleanArtifactDirNames(options.ArtifactDirNames)
	if len(names) == 0 {
		names = append([]string(nil), defaultRetainedArtifactDirNames...)
	}
	options.ArtifactDirNames = names
	return options
}

func cleanArtifactDirNames(names []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.ContainsRune(name, os.PathSeparator) {
			continue
		}
		if name == "." || name == ".." || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

func (s *Service) cleanupRetainedWorkspaceArtifacts(ctx context.Context, worker core.Worker, workspace PreparedWorkspace, options RetainedWorkspaceArtifactCleanupOptions, activeNodes []core.ExecutionNode) WorkspaceCleanup {
	if workspace.TargetKind == string(TargetKindSSH) || workspace.VCSType == "ssh" || workspace.Mode == "remote" {
		return s.cleanupRemoteRetainedWorkspaceArtifacts(ctx, worker, workspace, options, activeNodes)
	}
	return cleanupLocalRetainedWorkspaceArtifacts(ctx, worker, workspace, options, activeNodes)
}

func retainedWorkspaceArtifactCleanupBase(worker core.Worker, workspace PreparedWorkspace, options RetainedWorkspaceArtifactCleanupOptions, activeNodes []core.ExecutionNode) (WorkspaceCleanup, bool) {
	cleanup := WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        workspace.CleanupPolicy,
		Result:        workspaceResultForWorkerStatus(worker.Status),
		DryRun:        options.DryRun,
	}
	policy := WorkspaceCleanupPolicy(workspace.CleanupPolicy)
	if policy == "" {
		policy = WorkspaceCleanupRetain
		cleanup.Policy = string(policy)
	}
	if policy != WorkspaceCleanupRetain {
		cleanup.Reason = "workspace cleanup policy is not retain"
		return cleanup, false
	}
	if !isTerminalWorkerStatus(worker.Status) {
		cleanup.Reason = "worker is not terminal"
		return cleanup, false
	}
	if len(activeNodes) > 0 {
		cleanup.Reason = "worker has active execution node"
		return cleanup, false
	}
	if worker.UpdatedAt.IsZero() {
		cleanup.Reason = "worker terminal time is unavailable"
		return cleanup, false
	}
	if !options.IgnoreMinAge && worker.UpdatedAt.After(options.Now.Add(-options.MinAge)) {
		cleanup.Reason = "worker is newer than retained artifact cleanup age"
		return cleanup, false
	}
	return cleanup, true
}

func cleanupLocalRetainedWorkspaceArtifacts(ctx context.Context, worker core.Worker, workspace PreparedWorkspace, options RetainedWorkspaceArtifactCleanupOptions, activeNodes []core.ExecutionNode) WorkspaceCleanup {
	cleanup, ok := retainedWorkspaceArtifactCleanupBase(worker, workspace, options, activeNodes)
	if !ok {
		return cleanup
	}
	if workspace.Mode != string(WorkspaceModeIsolated) {
		cleanup.Reason = "workspace is not an isolated local workspace"
		return cleanup
	}
	if workspace.VCSType != "git" && workspace.VCSType != "jj" {
		cleanup.Reason = "retained artifact cleanup only supports local git and jj workspaces"
		return cleanup
	}
	root, err := retainedArtifactCleanupRoot(workspace)
	if err != nil {
		cleanup.Error = err.Error()
		return cleanup
	}
	for _, name := range options.ArtifactDirNames {
		item := cleanupArtifactDir(ctx, workspace, root, name, options.DryRun)
		cleanup.ArtifactDirs = append(cleanup.ArtifactDirs, item)
		if item.Removed {
			cleanup.Cleaned = true
			cleanup.BytesRemoved += item.Bytes
		}
	}
	item := cleanupLocalSharedWorkerDir(workspace, options.DryRun)
	if item.Path != "" {
		cleanup.ArtifactDirs = append(cleanup.ArtifactDirs, item)
		if item.Removed {
			cleanup.Cleaned = true
			cleanup.BytesRemoved += item.Bytes
		}
	}
	if !cleanup.Cleaned {
		cleanup.Reason = "no retained artifact directories removed"
	}
	return cleanup
}

func (s *Service) cleanupRemoteRetainedWorkspaceArtifacts(ctx context.Context, worker core.Worker, workspace PreparedWorkspace, options RetainedWorkspaceArtifactCleanupOptions, activeNodes []core.ExecutionNode) WorkspaceCleanup {
	cleanup, ok := retainedWorkspaceArtifactCleanupBase(worker, workspace, options, activeNodes)
	if !ok {
		return cleanup
	}
	if workspace.Mode != "remote" {
		cleanup.Reason = "workspace is not a remote workspace"
		return cleanup
	}
	if workspace.VCSType != "ssh" {
		cleanup.Reason = "retained remote artifact cleanup only supports ssh workspaces"
		return cleanup
	}
	if s == nil || s.targets == nil {
		cleanup.Reason = "remote target registry is not configured"
		return cleanup
	}
	targetID := strings.TrimSpace(workspace.TargetID)
	if targetID == "" {
		cleanup.Reason = "remote workspace target id is unavailable"
		return cleanup
	}
	target, found := s.targets.Get(targetID)
	if !found || target.Kind != TargetKindSSH {
		cleanup.Reason = "remote target is not configured"
		return cleanup
	}
	root := strings.TrimSpace(workspace.CWD)
	if root == "" {
		cleanup.Reason = "remote workspace cwd is required"
		return cleanup
	}
	if !path.IsAbs(root) {
		cleanup.Reason = "remote workspace cwd must be absolute"
		return cleanup
	}
	for _, name := range options.ArtifactDirNames {
		item := s.sshRunner.cleanupRemoteArtifactDir(ctx, target, root, name, options.DryRun)
		cleanup.ArtifactDirs = append(cleanup.ArtifactDirs, item)
		if item.Removed {
			cleanup.Cleaned = true
			cleanup.BytesRemoved += item.Bytes
		}
	}
	if dir := strings.TrimSpace(workspace.SharedWorkerDir); dir != "" {
		safeDir, err := retainedRemoteSharedWorkerDir(workspace)
		if err != nil {
			item := ArtifactDirCleanup{Name: "shared_worker_scratch", Path: dir, DryRun: options.DryRun, Error: err.Error()}
			cleanup.ArtifactDirs = append(cleanup.ArtifactDirs, item)
		} else {
			item := s.sshRunner.cleanupRemoteDirectory(ctx, target, safeDir, "shared_worker_scratch", options.DryRun)
			cleanup.ArtifactDirs = append(cleanup.ArtifactDirs, item)
			if item.Removed {
				cleanup.Cleaned = true
				cleanup.BytesRemoved += item.Bytes
			}
		}
	}
	if !cleanup.Cleaned {
		cleanup.Reason = "no retained artifact directories removed"
	}
	return cleanup
}

func (s *Service) cleanupTerminalWorkspaceArtifacts(ctx context.Context, taskID string, workerID string, workspace PreparedWorkspace, result WorkspaceResult) {
	options, enabled := s.retainedArtifactCleanupOptions()
	if !enabled {
		return
	}
	options.Now = time.Now().UTC()
	options.IgnoreMinAge = true
	workerState := core.Worker{
		ID:        workerID,
		TaskID:    taskID,
		Status:    workerStatusForWorkspaceResult(result),
		UpdatedAt: options.Now,
	}
	cleanup := s.cleanupRetainedWorkspaceArtifacts(ctx, workerState, workspace, options, nil)
	if !cleanup.Cleaned {
		return
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCleanup,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(cleanup),
	}); err != nil {
		slog.Warn("record terminal retained artifact cleanup", "taskID", taskID, "workerID", workerID, "error", err)
	}
}

func workerStatusForWorkspaceResult(result WorkspaceResult) core.WorkerStatus {
	switch result {
	case WorkspaceResultSucceeded:
		return core.WorkerSucceeded
	case WorkspaceResultCanceled:
		return core.WorkerCanceled
	default:
		return core.WorkerFailed
	}
}

func workspaceResultForWorkerStatus(status core.WorkerStatus) WorkspaceResult {
	switch status {
	case core.WorkerSucceeded:
		return WorkspaceResultSucceeded
	case core.WorkerCanceled:
		return WorkspaceResultCanceled
	default:
		return WorkspaceResultFailed
	}
}

func retainedArtifactCleanupRoot(workspace PreparedWorkspace) (string, error) {
	root := strings.TrimSpace(workspace.Root)
	if root == "" {
		root = strings.TrimSpace(workspace.CWD)
	}
	if root == "" {
		return "", errors.New("workspace root is required")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootInfo, err := os.Lstat(absRoot)
	if err != nil {
		return "", err
	}
	if rootInfo.Mode()&fs.ModeSymlink != 0 {
		return "", fmt.Errorf("workspace root is a symlink: %s", root)
	}
	info, err := os.Stat(absRoot)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workspace root is not a directory: %s", root)
	}
	return absRoot, nil
}

func cleanupArtifactDir(ctx context.Context, workspace PreparedWorkspace, root string, name string, dryRun bool) ArtifactDirCleanup {
	item := ArtifactDirCleanup{Name: name, DryRun: dryRun}
	path, err := retainedArtifactPath(root, name)
	if err != nil {
		item.Error = err.Error()
		return item
	}
	item.Path = path
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		item.Reason = "artifact directory does not exist"
		return item
	}
	if err != nil {
		item.Error = err.Error()
		return item
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		item.Reason = "artifact path is a symlink"
		return item
	}
	if !info.IsDir() {
		item.Reason = "artifact path is not a directory"
		return item
	}
	hasChanges, err := artifactDirHasSourceChanges(ctx, workspace, root, name)
	if err != nil {
		item.Error = err.Error()
		return item
	}
	if hasChanges {
		item.Reason = "artifact directory contains VCS-visible changes"
		return item
	}
	size, err := directorySize(path)
	if err != nil {
		item.Error = err.Error()
		return item
	}
	item.Bytes = size
	if dryRun {
		item.WouldRemove = true
		item.Reason = "dry run"
		return item
	}
	if err := os.RemoveAll(path); err != nil {
		item.Error = err.Error()
		return item
	}
	item.Removed = true
	return item
}

func cleanupLocalSharedWorkerDir(workspace PreparedWorkspace, dryRun bool) ArtifactDirCleanup {
	item := ArtifactDirCleanup{Name: "shared_worker_scratch", DryRun: dryRun}
	dir := strings.TrimSpace(workspace.SharedWorkerDir)
	if dir == "" {
		return item
	}
	item.Path = dir
	if err := sharedWorkerDirIsSafe(workspace); err != nil {
		item.Error = err.Error()
		return item
	}
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		item.Reason = "shared worker scratch directory does not exist"
		return item
	}
	if err != nil {
		item.Error = err.Error()
		return item
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		item.Reason = "shared worker scratch directory is a symlink"
		return item
	}
	if !info.IsDir() {
		item.Reason = "shared worker scratch path is not a directory"
		return item
	}
	size, err := directorySize(dir)
	if err != nil {
		item.Error = err.Error()
		return item
	}
	item.Bytes = size
	if dryRun {
		item.WouldRemove = true
		item.Reason = "dry run"
		return item
	}
	if err := os.RemoveAll(dir); err != nil {
		item.Error = err.Error()
		return item
	}
	item.Removed = true
	return item
}

func sharedWorkerDirIsSafe(workspace PreparedWorkspace) error {
	workerDir := strings.TrimSpace(workspace.SharedWorkerDir)
	if workerDir == "" {
		return errors.New("shared worker scratch directory is required")
	}
	absWorkerDir, err := filepath.Abs(workerDir)
	if err != nil {
		return err
	}
	sharedRoot := strings.TrimSpace(workspace.SharedRoot)
	if sharedRoot == "" {
		return errors.New("shared worker scratch root is required")
	}
	absSharedRoot, err := filepath.Abs(sharedRoot)
	if err != nil {
		return err
	}
	workersRoot := filepath.Join(absSharedRoot, "workers")
	cleanWorkerDir := filepath.Clean(absWorkerDir)
	cleanWorkersRoot := filepath.Clean(workersRoot)
	if cleanWorkerDir == cleanWorkersRoot || !strings.HasPrefix(cleanWorkerDir, cleanWorkersRoot+string(os.PathSeparator)) {
		return fmt.Errorf("shared worker scratch path escapes workers root: %s", workerDir)
	}
	return nil
}

func retainedRemoteSharedWorkerDir(workspace PreparedWorkspace) (string, error) {
	workerDir := path.Clean(strings.TrimSpace(workspace.SharedWorkerDir))
	if workerDir == "" || workerDir == "." {
		return "", errors.New("shared worker scratch directory is required")
	}
	if !path.IsAbs(workerDir) {
		return "", fmt.Errorf("shared worker scratch directory must be absolute: %s", workerDir)
	}
	sharedRoot := path.Clean(strings.TrimSpace(workspace.SharedRoot))
	if sharedRoot == "" || sharedRoot == "." {
		return "", errors.New("shared worker scratch root is required")
	}
	if !path.IsAbs(sharedRoot) {
		return "", fmt.Errorf("shared worker scratch root must be absolute: %s", sharedRoot)
	}
	workersRoot := path.Join(sharedRoot, "workers")
	if workerDir == workersRoot || !strings.HasPrefix(workerDir, workersRoot+"/") {
		return "", fmt.Errorf("shared worker scratch path escapes workers root: %s", workspace.SharedWorkerDir)
	}
	return workerDir, nil
}

func retainedArtifactPath(root string, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.ContainsRune(name, os.PathSeparator) {
		return "", fmt.Errorf("unsafe artifact directory name %q", name)
	}
	path := filepath.Join(root, name)
	cleanRoot := filepath.Clean(root)
	cleanPath := filepath.Clean(path)
	if cleanPath == cleanRoot || !strings.HasPrefix(cleanPath, cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("artifact path escapes workspace root: %s", name)
	}
	return cleanPath, nil
}

func artifactDirHasSourceChanges(ctx context.Context, workspace PreparedWorkspace, root string, name string) (bool, error) {
	switch workspace.VCSType {
	case "git":
		out, err := runGit(ctx, root, "status", "--porcelain=v1", "--", name)
		if err != nil {
			return false, fmt.Errorf("check git artifact changes: %w", err)
		}
		return strings.TrimSpace(out) != "", nil
	case "jj":
		out, err := runJJRefreshingStale(ctx, root, "diff", "--summary", "--", name)
		if err != nil {
			return false, fmt.Errorf("check jj artifact changes: %w", err)
		}
		return strings.TrimSpace(out) != "", nil
	default:
		return true, nil
	}
}

func directorySize(root string) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
