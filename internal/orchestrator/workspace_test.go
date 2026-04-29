package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestJJWorkspaceManagerPreparesCurrentRepo(t *testing.T) {
	workspace, err := NewJJWorkspaceManager(WorkspaceModeShared, "", WorkspaceCleanupRetain).Prepare(context.Background(), WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker",
		WorkDir:  ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	if workspace.Root == "" {
		t.Fatalf("Root is empty")
	}
	if workspace.CWD != workspace.Root {
		t.Fatalf("CWD = %q, want root %q", workspace.CWD, workspace.Root)
	}
	if workspace.VCSType != "jj" {
		t.Fatalf("VCSType = %q", workspace.VCSType)
	}
	if workspace.Mode != "shared" {
		t.Fatalf("Mode = %q", workspace.Mode)
	}
	if !strings.Contains(workspace.Change, "@") {
		t.Fatalf("Change does not describe @: %q", workspace.Change)
	}
	if !strings.Contains(workspace.Status, "Working copy") && !strings.Contains(workspace.Status, "The working copy") {
		t.Fatalf("Status does not look like jj status: %q", workspace.Status)
	}
}

func TestAutoWorkspaceManagerUsesCurrentRepoVCS(t *testing.T) {
	workspace, err := NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeShared, "", WorkspaceCleanupRetain).Prepare(context.Background(), WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker",
		WorkDir:  ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	if workspace.VCSType != "jj" {
		t.Fatalf("VCSType = %q", workspace.VCSType)
	}
}

func TestGitStatusDirty(t *testing.T) {
	if gitStatusDirty("## main") {
		t.Fatalf("branch-only status should be clean")
	}
	if !gitStatusDirty("## main\n M file.go") {
		t.Fatalf("modified file should be dirty")
	}
}

func TestParseJJDiffSummary(t *testing.T) {
	files := parseJJDiffSummary("M internal/orchestrator/service.go\nA web/src/types.ts\nD old.txt\n")
	if len(files) != 3 {
		t.Fatalf("files = %+v", files)
	}
	if files[0] != (WorkspaceChangedFile{Path: "internal/orchestrator/service.go", Status: "modified"}) {
		t.Fatalf("first file = %+v", files[0])
	}
	if files[1] != (WorkspaceChangedFile{Path: "web/src/types.ts", Status: "added"}) {
		t.Fatalf("second file = %+v", files[1])
	}
	if files[2] != (WorkspaceChangedFile{Path: "old.txt", Status: "deleted"}) {
		t.Fatalf("third file = %+v", files[2])
	}
}

func TestParseGitPorcelain(t *testing.T) {
	files := parseGitPorcelain(" M internal/orchestrator/service.go\n?? web/src/types.ts\nR  old.txt -> new.txt\n")
	if len(files) != 4 {
		t.Fatalf("files = %+v", files)
	}
	if files[0] != (WorkspaceChangedFile{Path: "internal/orchestrator/service.go", Status: "modified"}) {
		t.Fatalf("first file = %+v", files[0])
	}
	if files[1] != (WorkspaceChangedFile{Path: "web/src/types.ts", Status: "untracked"}) {
		t.Fatalf("second file = %+v", files[1])
	}
	if files[2] != (WorkspaceChangedFile{Path: "old.txt", Status: "renamed_from"}) {
		t.Fatalf("third file = %+v", files[2])
	}
	if files[3] != (WorkspaceChangedFile{Path: "new.txt", Status: "renamed"}) {
		t.Fatalf("fourth file = %+v", files[3])
	}
}

func TestJJWorkspaceManagerDescribesIsolatedWorkerCommit(t *testing.T) {
	repo := initJJTestRepo(t)
	manager := NewJJWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	workspace, err := manager.Prepare(context.Background(), WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker-123",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}

	description := strings.TrimSpace(runTestJJ(t, workspace.CWD, "log", "-r", "@", "--no-graph", "-T", "description"))
	if description != "Worker worker-123" {
		t.Fatalf("worker description = %q", description)
	}
}

func TestJJWorkspaceManagerApplyBackfillsWorkerDescription(t *testing.T) {
	ctx := context.Background()
	repo := initJJTestRepo(t)
	manager := NewJJWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)
	workspace, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker1",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.CWD, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestJJ(t, workspace.CWD, "describe", "--message", "")

	changes, err := manager.DescribeChanges(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !changes.Dirty {
		t.Fatal("worker changes should be dirty")
	}
	if _, err := manager.ApplyChanges(ctx, workspace, changes); err != nil {
		t.Fatal(err)
	}

	workerDescription := strings.TrimSpace(runTestJJ(t, workspace.CWD, "log", "-r", "@", "--no-graph", "-T", "description"))
	if workerDescription != "Worker worker1" {
		t.Fatalf("worker description = %q", workerDescription)
	}
	sourceDescription := strings.TrimSpace(runTestJJ(t, repo, "log", "-r", "@", "--no-graph", "-T", "description"))
	if sourceDescription != "Apply worker worker1" {
		t.Fatalf("source description = %q", sourceDescription)
	}
}

func TestNativeWorkspaceApplyResultsPreserveMethodAndFiles(t *testing.T) {
	changedFiles := []WorkspaceChangedFile{
		{Path: "internal/orchestrator/workspace.go", Status: "modified"},
		{Path: "web/src/main.tsx", Status: "added"},
	}
	changes := WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: changedFiles,
	}

	tests := []struct {
		name      string
		workspace PreparedWorkspace
		apply     func(context.Context, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error)
		method    string
	}{
		{
			name: "jj",
			workspace: PreparedWorkspace{
				Root:          "/tmp/worker",
				SourceRoot:    "/tmp/source",
				WorkspaceName: "aged-worker",
				Mode:          string(WorkspaceModeShared),
			},
			apply:  NewJJWorkspaceManager(WorkspaceModeIsolated, "", WorkspaceCleanupRetain).ApplyChanges,
			method: "jj_new_merge",
		},
		{
			name: "git",
			workspace: PreparedWorkspace{
				Root:       "/tmp/worker",
				SourceRoot: "/tmp/source",
				Mode:       string(WorkspaceModeShared),
			},
			apply:  NewGitWorkspaceManager(WorkspaceModeIsolated, "", WorkspaceCleanupRetain).ApplyChanges,
			method: "git_commit_merge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.apply(context.Background(), tt.workspace, changes)
			if err == nil {
				t.Fatalf("expected validation error")
			}
			if result.Method != tt.method {
				t.Fatalf("method = %q, want %q", result.Method, tt.method)
			}
			if len(result.AppliedFiles) != len(changedFiles) {
				t.Fatalf("applied files = %+v", result.AppliedFiles)
			}
			for i := range changedFiles {
				if result.AppliedFiles[i] != changedFiles[i] {
					t.Fatalf("applied files = %+v", result.AppliedFiles)
				}
			}
		})
	}
}

func initJJTestRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj is not installed")
	}
	repo := t.TempDir()
	runTestJJ(t, repo, "git", "init", "--colocate")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestJJ(t, repo, "describe", "--message", "base")
	return repo
}

func runTestJJ(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("jj", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("jj %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestCleanupDecisionPolicies(t *testing.T) {
	workspace := PreparedWorkspace{
		Root:          "/tmp/workspace",
		CWD:           "/tmp/workspace",
		WorkspaceName: "aged-worker",
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       "git",
	}

	workspace.CleanupPolicy = string(WorkspaceCleanupRetain)
	cleanup, shouldClean := cleanupDecision(workspace, WorkspaceResultSucceeded)
	if shouldClean || cleanup.Cleaned {
		t.Fatalf("retain policy should not clean")
	}

	workspace.CleanupPolicy = string(WorkspaceCleanupDeleteOnSuccess)
	_, shouldClean = cleanupDecision(workspace, WorkspaceResultFailed)
	if shouldClean {
		t.Fatalf("delete_on_success should not clean failed workspaces")
	}
	_, shouldClean = cleanupDecision(workspace, WorkspaceResultSucceeded)
	if !shouldClean {
		t.Fatalf("delete_on_success should clean successful workspaces")
	}

	workspace.CleanupPolicy = string(WorkspaceCleanupDeleteOnTerminal)
	_, shouldClean = cleanupDecision(workspace, WorkspaceResultCanceled)
	if !shouldClean {
		t.Fatalf("delete_on_terminal should clean canceled workspaces")
	}
}
