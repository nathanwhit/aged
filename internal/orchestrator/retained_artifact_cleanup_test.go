package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestCleanupRetainedWorkspaceArtifactsRemovesIgnoredTarget(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-clean", "worker-clean", core.WorkerSucceeded, true)
	writeRetainedTargetFile(t, fixture.repo, "debug/artifact.bin", "build output\n")
	writeRetainedFile(t, fixture.repo, "file.txt", "source edit\n")

	report, err := fixture.cleanup(RetainedWorkspaceArtifactCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 1 || report.BytesRemoved == 0 {
		t.Fatalf("report = %+v, want one cleaned workspace with removed bytes", report)
	}
	if _, err := os.Stat(filepath.Join(fixture.repo, "target")); !os.IsNotExist(err) {
		t.Fatalf("target stat err = %v, want not exist", err)
	}
	contents, err := os.ReadFile(filepath.Join(fixture.repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "source edit\n" {
		t.Fatalf("source contents = %q", contents)
	}
	cleanup := lastWorkspaceCleanupEvent(t, fixture.ctx, fixture.store, "worker-clean")
	if !cleanup.Cleaned || len(cleanup.ArtifactDirs) != 1 || !cleanup.ArtifactDirs[0].Removed {
		t.Fatalf("cleanup event = %+v, want removed target", cleanup)
	}
}

func TestCleanupRetainedWorkspaceArtifactsRemovesRemoteTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	workspace := PreparedWorkspace{
		Root:            "/runs/worker-remote-clean",
		CWD:             "/runs/worker-remote-clean/repo",
		SourceRoot:      "/repo",
		WorkspaceName:   "aged-worker-remote-clean",
		Mode:            "remote",
		VCSType:         "ssh",
		CleanupPolicy:   string(WorkspaceCleanupRetain),
		TaskID:          "task-remote-clean",
		WorkerID:        "worker-remote-clean",
		TargetID:        "vm-clean",
		TargetKind:      string(TargetKindSSH),
		SharedRoot:      "/runs/shared/task-remote-clean",
		SharedWorkerDir: "/runs/shared/task-remote-clean/workers/worker-remote-clean",
	}
	appendRetainedCleanupWorker(t, ctx, store, workspace, core.WorkerSucceeded, now.Add(-2*time.Hour), true)
	executor := &retainedArtifactRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-clean",
		Kind:     TargetKindSSH,
		Host:     "vm-clean",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor})

	report, err := service.CleanupRetainedWorkspaceArtifacts(ctx, RetainedWorkspaceArtifactCleanupOptions{
		MinAge: time.Hour,
		Now:    now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 1 || report.BytesRemoved != 12288 {
		t.Fatalf("report = %+v, want remote target and shared worker scratch cleaned", report)
	}
	cleanup := lastWorkspaceCleanupEvent(t, ctx, store, "worker-remote-clean")
	if !cleanup.Cleaned || len(cleanup.ArtifactDirs) != 2 || !cleanup.ArtifactDirs[0].Removed || !cleanup.ArtifactDirs[1].Removed {
		t.Fatalf("cleanup event = %+v, want removed remote target and shared worker scratch", cleanup)
	}
	if cleanup.ArtifactDirs[0].Path != "/runs/worker-remote-clean/repo/target" {
		t.Fatalf("remote artifact path = %q", cleanup.ArtifactDirs[0].Path)
	}
	if cleanup.ArtifactDirs[1].Path != "/runs/shared/task-remote-clean/workers/worker-remote-clean" {
		t.Fatalf("remote shared worker path = %q", cleanup.ArtifactDirs[1].Path)
	}
	if len(executor.commands) != 2 ||
		!strings.Contains(strings.Join(executor.commands[0], " "), "/runs/worker-remote-clean/repo") ||
		!strings.Contains(strings.Join(executor.commands[1], " "), "/runs/shared/task-remote-clean/workers/worker-remote-clean") {
		t.Fatalf("remote cleanup commands = %+v, want repo target and shared worker cleanup", executor.commands)
	}
}

func TestTerminalWorkspaceArtifactCleanupRemovesNewTargetWhenEnabled(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	repo := initGitTestRepo(t)
	ignoreGitTarget(t, repo)
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	workspace := retainedCleanupWorkspace("task-terminal-clean", "worker-terminal-clean", repo, "git")
	workspace.SharedRoot = filepath.Join(t.TempDir(), "shared", workspace.TaskID)
	workspace.SharedWorkerDir = filepath.Join(workspace.SharedRoot, "workers", workspace.WorkerID)
	appendRetainedCleanupWorker(t, ctx, store, workspace, core.WorkerSucceeded, now, true)
	writeRetainedTargetFile(t, repo, "artifact.bin", "build output\n")
	if err := os.MkdirAll(filepath.Join(workspace.SharedWorkerDir, "cargo-target", "release-lite"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.SharedWorkerDir, "cargo-target", "release-lite", "artifact.bin"), []byte("shared build output\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	service.SetRetainedWorkspaceArtifactCleanup(RetainedWorkspaceArtifactCleanupOptions{MinAge: 24 * time.Hour})

	service.cleanupTerminalWorkspaceArtifacts(ctx, workspace.TaskID, workspace.WorkerID, workspace, WorkspaceResultSucceeded)

	if _, err := os.Stat(filepath.Join(repo, "target")); !os.IsNotExist(err) {
		t.Fatalf("target stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.SharedWorkerDir, "cargo-target", "release-lite", "artifact.bin")); !os.IsNotExist(err) {
		t.Fatalf("shared worker scratch stat err = %v, want not exist", err)
	}
	cleanup := lastWorkspaceCleanupEvent(t, ctx, store, "worker-terminal-clean")
	if !cleanup.Cleaned || len(cleanup.ArtifactDirs) != 2 || !cleanup.ArtifactDirs[0].Removed || !cleanup.ArtifactDirs[1].Removed {
		t.Fatalf("cleanup event = %+v, want terminal artifact and shared scratch cleanup", cleanup)
	}
	if cleanup.ArtifactDirs[1].Name != "shared_worker_scratch" {
		t.Fatalf("second cleanup item = %+v, want shared worker scratch", cleanup.ArtifactDirs[1])
	}
}

func TestTerminalWorkspaceArtifactCleanupRemovesNewRemoteSharedWorkerScratchWhenEnabled(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	workspace := PreparedWorkspace{
		Root:            "/runs/worker-remote-clean",
		CWD:             "/runs/worker-remote-clean/repo",
		SourceRoot:      "/repo",
		WorkspaceName:   "aged-worker-remote-clean",
		Mode:            "remote",
		VCSType:         "ssh",
		CleanupPolicy:   string(WorkspaceCleanupRetain),
		TaskID:          "task-remote-clean",
		WorkerID:        "worker-remote-clean",
		TargetID:        "vm-clean",
		TargetKind:      string(TargetKindSSH),
		SharedRoot:      "/runs/shared/task-remote-clean",
		SharedWorkerDir: "/runs/shared/task-remote-clean/workers/worker-remote-clean",
	}
	executor := &retainedArtifactRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-clean",
		Kind:     TargetKindSSH,
		Host:     "vm-clean",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor})
	service.SetRetainedWorkspaceArtifactCleanup(RetainedWorkspaceArtifactCleanupOptions{MinAge: 24 * time.Hour})

	service.cleanupTerminalWorkspaceArtifacts(ctx, workspace.TaskID, workspace.WorkerID, workspace, WorkspaceResultSucceeded)

	cleanup := lastWorkspaceCleanupEvent(t, ctx, store, "worker-remote-clean")
	if !cleanup.Cleaned || len(cleanup.ArtifactDirs) != 2 || !cleanup.ArtifactDirs[0].Removed || !cleanup.ArtifactDirs[1].Removed {
		t.Fatalf("cleanup event = %+v, want terminal remote target and shared scratch cleanup", cleanup)
	}
	if cleanup.ArtifactDirs[1].Path != "/runs/shared/task-remote-clean/workers/worker-remote-clean" {
		t.Fatalf("remote shared worker path = %q", cleanup.ArtifactDirs[1].Path)
	}
}

func TestStartRetainedWorkspaceArtifactCleanupRemovesOldTargetOnInterval(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-interval-clean", "worker-interval-clean", core.WorkerSucceeded, true)
	writeRetainedTargetFile(t, fixture.repo, "debug/artifact.bin", "build output\n")
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()

	fixture.service.StartRetainedWorkspaceArtifactCleanup(ctx, 10*time.Millisecond, RetainedWorkspaceArtifactCleanupOptions{
		MinAge: time.Hour,
		Now:    fixture.now,
	})

	waitForSnapshot(t, fixture.store, func(snapshot core.Snapshot) bool {
		for _, event := range snapshot.Events {
			if event.Type == core.EventWorkerCleanup && event.WorkerID == "worker-interval-clean" {
				return true
			}
		}
		return false
	}, func(snapshot core.Snapshot) string {
		return "periodic retained artifact cleanup did not emit cleanup event"
	})
	if _, err := os.Stat(filepath.Join(fixture.repo, "target")); !os.IsNotExist(err) {
		t.Fatalf("target stat err = %v, want not exist", err)
	}
}

func TestCleanupRetainedWorkspaceArtifactsProtectsActiveWorkers(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-active", "worker-active", core.WorkerRunning, true)
	writeRetainedTargetFile(t, fixture.repo, "artifact.bin", "build output\n")

	report, err := fixture.cleanup(RetainedWorkspaceArtifactCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 0 || report.Skipped != 1 {
		t.Fatalf("report = %+v, want active worker skipped", report)
	}
	if _, err := os.Stat(filepath.Join(fixture.repo, "target", "artifact.bin")); err != nil {
		t.Fatalf("active worker artifact was removed: %v", err)
	}
	if cleanupEventCount(t, fixture.ctx, fixture.store, "worker-active") != 0 {
		t.Fatalf("active worker should not emit cleanup event")
	}
}

func TestCleanupRetainedWorkspaceArtifactsProtectsActiveExecutionNodes(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-node-active", "worker-node-active", core.WorkerSucceeded, true)
	writeRetainedTargetFile(t, fixture.repo, "artifact.bin", "build output\n")

	appendRetainedCleanupExecutionNode(t, fixture.ctx, fixture.store, fixture.workspace, "node-active", core.WorkerRunning, fixture.now.Add(-time.Hour))

	report, err := fixture.cleanup(RetainedWorkspaceArtifactCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 0 || report.Skipped != 1 || len(report.Workspaces) != 1 {
		t.Fatalf("report = %+v, want active execution node skipped", report)
	}
	if !strings.Contains(report.Workspaces[0].Reason, "active execution node") {
		t.Fatalf("cleanup = %+v, want active execution node reason", report.Workspaces[0])
	}
	if _, err := os.Stat(filepath.Join(fixture.repo, "target", "artifact.bin")); err != nil {
		t.Fatalf("active execution node artifact was removed: %v", err)
	}
	if cleanupEventCount(t, fixture.ctx, fixture.store, "worker-node-active") != 0 {
		t.Fatalf("active execution node should not emit cleanup event")
	}
}

func TestCleanupRetainedWorkspaceArtifactsDryRunPreservesTarget(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-dry", "worker-dry", core.WorkerSucceeded, true)
	writeRetainedTargetFile(t, fixture.repo, "artifact.bin", "build output\n")

	report, err := fixture.cleanup(RetainedWorkspaceArtifactCleanupOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 0 || len(report.Workspaces) != 1 || len(report.Workspaces[0].ArtifactDirs) != 1 {
		t.Fatalf("report = %+v, want dry-run artifact report", report)
	}
	item := report.Workspaces[0].ArtifactDirs[0]
	if !item.DryRun || !item.WouldRemove || item.Removed {
		t.Fatalf("dry-run item = %+v, want would-remove without removed", item)
	}
	if _, err := os.Stat(filepath.Join(fixture.repo, "target", "artifact.bin")); err != nil {
		t.Fatalf("dry run removed target artifact: %v", err)
	}
}

func TestCleanupRetainedWorkspaceArtifactsSkipsSymlinkTarget(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-symlink", "worker-symlink", core.WorkerSucceeded, false)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "artifact.bin"), []byte("outside\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(fixture.repo, "target")); err != nil {
		t.Fatal(err)
	}

	report, err := fixture.cleanup(RetainedWorkspaceArtifactCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 0 || len(report.Workspaces) != 1 || len(report.Workspaces[0].ArtifactDirs) != 1 {
		t.Fatalf("report = %+v, want symlink target skipped", report)
	}
	item := report.Workspaces[0].ArtifactDirs[0]
	if !strings.Contains(item.Reason, "symlink") {
		t.Fatalf("artifact item = %+v, want symlink skip reason", item)
	}
	if _, err := os.Lstat(filepath.Join(fixture.repo, "target")); err != nil {
		t.Fatalf("symlink target was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "artifact.bin")); err != nil {
		t.Fatalf("outside artifact was removed: %v", err)
	}
}

func TestCleanupRetainedWorkspaceArtifactsSkipsSymlinkRoot(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	repo := initGitTestRepo(t)
	ignoreGitTarget(t, repo)
	if err := os.MkdirAll(filepath.Join(repo, "target"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "target", "artifact.bin"), []byte("build output\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rootLink := filepath.Join(t.TempDir(), "workspace-link")
	if err := os.Symlink(repo, rootLink); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	workspace := retainedCleanupWorkspace("task-root-symlink", "worker-root-symlink", rootLink, "git")
	appendRetainedCleanupWorker(t, ctx, store, workspace, core.WorkerSucceeded, now.Add(-2*time.Hour), true)
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())

	report, err := service.CleanupRetainedWorkspaceArtifacts(ctx, RetainedWorkspaceArtifactCleanupOptions{
		MinAge: time.Hour,
		Now:    now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 0 || len(report.Workspaces) != 1 {
		t.Fatalf("report = %+v, want symlink root skipped", report)
	}
	if !strings.Contains(report.Workspaces[0].Error, "symlink") {
		t.Fatalf("cleanup = %+v, want symlink root error", report.Workspaces[0])
	}
	if _, err := os.Stat(filepath.Join(repo, "target", "artifact.bin")); err != nil {
		t.Fatalf("symlink root target artifact was removed: %v", err)
	}
}

func TestCleanupRetainedWorkspaceArtifactsPreservesVCSVisibleTargetChanges(t *testing.T) {
	fixture := newRetainedCleanupFixture(t, "task-source", "worker-source", core.WorkerSucceeded, false)
	writeRetainedTargetFile(t, fixture.repo, "source.txt", "not ignored\n")

	report, err := fixture.cleanup(RetainedWorkspaceArtifactCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Cleaned != 0 || len(report.Workspaces) != 1 {
		t.Fatalf("report = %+v, want VCS-visible target skipped", report)
	}
	item := report.Workspaces[0].ArtifactDirs[0]
	if !strings.Contains(item.Reason, "VCS-visible changes") {
		t.Fatalf("artifact item = %+v, want VCS-visible skip reason", item)
	}
	if _, err := os.Stat(filepath.Join(fixture.repo, "target", "source.txt")); err != nil {
		t.Fatalf("VCS-visible target file was removed: %v", err)
	}
}

func TestCleanArtifactDirNamesRejectsUnsafeNames(t *testing.T) {
	names := cleanArtifactDirNames([]string{"target", "", ".", "..", "target", "/tmp/target", "target/cache", "target/../cache"})
	if len(names) != 1 || names[0] != "target" {
		t.Fatalf("names = %#v, want only target", names)
	}
}

func ignoreGitTarget(t *testing.T, repo string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte("target/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", ".gitignore")
	runTestGit(t, repo, "commit", "-m", "ignore target")
}

type retainedCleanupFixture struct {
	ctx       context.Context
	store     *eventstore.SQLiteStore
	repo      string
	now       time.Time
	workspace PreparedWorkspace
	service   *Service
}

type retainedArtifactRemoteExecutor struct {
	commands [][]string
	output   string
	err      error
}

func (e *retainedArtifactRemoteExecutor) Run(_ context.Context, argv []string) (string, error) {
	e.commands = append(e.commands, append([]string(nil), argv...))
	if e.output != "" || e.err != nil {
		return e.output, e.err
	}
	joined := strings.Join(argv, " ")
	if strings.Contains(joined, "/runs/shared/task-remote-clean/workers/worker-remote-clean") {
		return "path=/runs/shared/task-remote-clean/workers/worker-remote-clean\nbytes=8192\nstatus=removed\n", nil
	}
	if strings.Contains(joined, "/runs/worker-remote-clean/repo") {
		return "path=/runs/worker-remote-clean/repo/target\nbytes=4096\nstatus=removed\n", nil
	}
	return "path=/repo/target\nbytes=1024\nstatus=removed\n", nil
}

func newRetainedCleanupFixture(t *testing.T, taskID string, workerID string, status core.WorkerStatus, ignoreTarget bool) retainedCleanupFixture {
	t.Helper()
	ctx := context.Background()
	store := openTestStore(t)
	t.Cleanup(func() { store.Close() })
	repo := initGitTestRepo(t)
	if ignoreTarget {
		ignoreGitTarget(t, repo)
	}
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	workspace := retainedCleanupWorkspace(taskID, workerID, repo, "git")
	appendRetainedCleanupWorker(t, ctx, store, workspace, status, now.Add(-2*time.Hour), true)
	return retainedCleanupFixture{
		ctx:       ctx,
		store:     store,
		repo:      repo,
		now:       now,
		workspace: workspace,
		service:   NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir()),
	}
}

func (f retainedCleanupFixture) cleanup(options RetainedWorkspaceArtifactCleanupOptions) (RetainedWorkspaceArtifactCleanupReport, error) {
	if options.MinAge == 0 {
		options.MinAge = time.Hour
	}
	if options.Now.IsZero() {
		options.Now = f.now
	}
	return f.service.CleanupRetainedWorkspaceArtifacts(f.ctx, options)
}

func writeRetainedTargetFile(t *testing.T, repo string, name string, contents string) {
	t.Helper()
	writeRetainedFile(t, repo, filepath.Join("target", name), contents)
}

func writeRetainedFile(t *testing.T, repo string, name string, contents string) {
	t.Helper()
	path := filepath.Join(repo, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func retainedCleanupWorkspace(taskID string, workerID string, root string, vcs string) PreparedWorkspace {
	return PreparedWorkspace{
		Root:          root,
		CWD:           root,
		SourceRoot:    root,
		WorkspaceName: "aged-" + shortID(workerID),
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       vcs,
		CleanupPolicy: string(WorkspaceCleanupRetain),
		TaskID:        taskID,
		WorkerID:      workerID,
	}
}

func appendRetainedCleanupWorker(t *testing.T, ctx context.Context, store eventAppender, workspace PreparedWorkspace, status core.WorkerStatus, at time.Time, includeWorkspace bool) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		At:     at,
		Type:   core.EventTaskCreated,
		TaskID: workspace.TaskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "cleanup",
			"prompt": "cleanup",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if includeWorkspace {
		if _, err := store.Append(ctx, core.Event{
			At:       at,
			Type:     core.EventWorkerWorkspace,
			TaskID:   workspace.TaskID,
			WorkerID: workspace.WorkerID,
			Payload:  core.MustJSON(workspace),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		At:       at,
		Type:     core.EventWorkerCreated,
		TaskID:   workspace.TaskID,
		WorkerID: workspace.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "mock",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if status == core.WorkerRunning {
		if _, err := store.Append(ctx, core.Event{
			At:       at,
			Type:     core.EventWorkerStarted,
			TaskID:   workspace.TaskID,
			WorkerID: workspace.WorkerID,
			Payload:  core.MustJSON(map[string]any{}),
		}); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := store.Append(ctx, core.Event{
		At:       at,
		Type:     core.EventWorkerCompleted,
		TaskID:   workspace.TaskID,
		WorkerID: workspace.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": status,
		}),
	}); err != nil {
		t.Fatal(err)
	}
}

func appendRetainedCleanupExecutionNode(t *testing.T, ctx context.Context, store eventAppender, workspace PreparedWorkspace, nodeID string, status core.WorkerStatus, at time.Time) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		At:       at,
		Type:     core.EventExecutionPlanned,
		TaskID:   workspace.TaskID,
		WorkerID: workspace.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":     nodeID,
			"workerId":   workspace.WorkerID,
			"workerKind": "mock",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		At:       at,
		Type:     core.EventExecutionStatus,
		TaskID:   workspace.TaskID,
		WorkerID: workspace.WorkerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId": nodeID,
			"status": status,
		}),
	}); err != nil {
		t.Fatal(err)
	}
}

type eventAppender interface {
	Append(context.Context, core.Event) (core.Event, error)
}

func lastWorkspaceCleanupEvent(t *testing.T, ctx context.Context, store interface {
	Snapshot(context.Context) (core.Snapshot, error)
}, workerID string) WorkspaceCleanup {
	t.Helper()
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var cleanup WorkspaceCleanup
	for _, event := range snapshot.Events {
		if event.Type != core.EventWorkerCleanup || event.WorkerID != workerID {
			continue
		}
		if err := json.Unmarshal(event.Payload, &cleanup); err != nil {
			t.Fatal(err)
		}
	}
	if cleanup.Root == "" {
		t.Fatalf("no cleanup event for worker %s", workerID)
	}
	return cleanup
}

func cleanupEventCount(t *testing.T, ctx context.Context, store interface {
	Snapshot(context.Context) (core.Snapshot, error)
}, workerID string) int {
	t.Helper()
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerCleanup && event.WorkerID == workerID {
			count++
		}
	}
	return count
}
