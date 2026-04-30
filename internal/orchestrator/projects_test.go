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
