package orchestrator

import (
	"context"
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
