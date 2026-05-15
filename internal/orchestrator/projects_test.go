package orchestrator

import (
	"os"
	"strings"
	"testing"

	"aged/internal/core"
)

func TestNewProjectRegistryRejectsMissingLocalPath(t *testing.T) {
	_, err := NewProjectRegistry([]core.Project{{
		ID:        "missing",
		LocalPath: t.TempDir() + "/does-not-exist",
	}}, "missing")
	if err == nil || !strings.Contains(err.Error(), "localPath") {
		t.Fatalf("err = %v, want localPath validation error", err)
	}
}

func TestNewProjectRegistryRejectsFileLocalPath(t *testing.T) {
	file := t.TempDir() + "/project-file"
	if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewProjectRegistry([]core.Project{{
		ID:        "file",
		LocalPath: file,
	}}, "file")
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("err = %v, want not-a-directory validation error", err)
	}
}

func TestNewProjectRegistryKeepsAutoVCSForPlainDirectory(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewProjectRegistry([]core.Project{{
		ID:        "plain",
		LocalPath: dir,
		VCS:       "auto",
	}}, "plain")
	if err != nil {
		t.Fatal(err)
	}
	project := registry.Default()
	if project.VCS != "auto" {
		t.Fatalf("VCS = %q, want auto", project.VCS)
	}
}

func TestNewProjectRegistryNormalizesPullRequestMergeMethod(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewProjectRegistry([]core.Project{{
		ID:        "plain",
		LocalPath: dir,
		PullRequestPolicy: core.PullRequestPolicy{
			MergeMethod: " ReBase ",
		},
	}}, "plain")
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.Default().PullRequestPolicy.MergeMethod; got != "rebase" {
		t.Fatalf("merge method = %q, want rebase", got)
	}
}

func TestNewProjectRegistryRejectsNegativeRequirements(t *testing.T) {
	_, err := NewProjectRegistry([]core.Project{{
		ID:        "plain",
		LocalPath: t.TempDir(),
		Requirements: core.ProjectRequirements{
			MemoryMB: -1,
		},
	}}, "plain")
	if err == nil || !strings.Contains(err.Error(), "requirements.memoryMb") {
		t.Fatalf("err = %v, want memory requirement validation error", err)
	}

	_, err = NewProjectRegistry([]core.Project{{
		ID:        "plain",
		LocalPath: t.TempDir(),
		Requirements: core.ProjectRequirements{
			StorageMB: -1,
		},
	}}, "plain")
	if err == nil || !strings.Contains(err.Error(), "requirements.storageMb") {
		t.Fatalf("err = %v, want storage requirement validation error", err)
	}
}

func TestNewProjectRegistryNormalizesReviewPolicy(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewProjectRegistry([]core.Project{{
		ID:        "plain",
		LocalPath: dir,
		ReviewPolicy: core.ReviewPolicy{
			Enabled:            true,
			BlockingSeverities: []string{" p1 ", "P1", "p0"},
			ReviewerKinds:      []string{" claude ", "codex", "claude"},
			PromptSetID:        " project-review ",
			Instructions:       " Check lifecycle edges. ",
		},
	}}, "plain")
	if err != nil {
		t.Fatal(err)
	}
	policy := registry.Default().ReviewPolicy
	if !policy.BeforeCompletionPR || !policy.BeforeIntermediatePR {
		t.Fatalf("review phases = completion %v intermediate %v, want defaults enabled", policy.BeforeCompletionPR, policy.BeforeIntermediatePR)
	}
	if strings.Join(policy.BlockingSeverities, ",") != "P1,P0" {
		t.Fatalf("blocking severities = %+v", policy.BlockingSeverities)
	}
	if strings.Join(policy.ReviewerKinds, ",") != "claude,codex" {
		t.Fatalf("reviewer kinds = %+v", policy.ReviewerKinds)
	}
	if policy.PromptSetID != "project-review" || policy.Instructions != "Check lifecycle edges." || policy.MaxAttempts != 2 {
		t.Fatalf("policy = %+v", policy)
	}
}

func TestNewProjectRegistryRejectsInvalidPullRequestMergeMethod(t *testing.T) {
	_, err := NewProjectRegistry([]core.Project{{
		ID:        "plain",
		LocalPath: t.TempDir(),
		PullRequestPolicy: core.PullRequestPolicy{
			MergeMethod: "octopus",
		},
	}}, "plain")
	if err == nil || !strings.Contains(err.Error(), "mergeMethod") {
		t.Fatalf("err = %v, want mergeMethod validation error", err)
	}
}
