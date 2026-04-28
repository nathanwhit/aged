package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

type WorkspaceManager interface {
	Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error)
}

type WorkspaceSpec struct {
	TaskID   string
	WorkerID string
	WorkDir  string
}

type PreparedWorkspace struct {
	Root     string `json:"root"`
	CWD      string `json:"cwd"`
	Change   string `json:"change"`
	Status   string `json:"status"`
	Mode     string `json:"mode"`
	Dirty    bool   `json:"dirty"`
	VCSType  string `json:"vcsType"`
	WorkerID string `json:"workerId"`
	TaskID   string `json:"taskId"`
}

type JJWorkspaceManager struct{}

func (JJWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
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

	change, err := runJJ(ctx, root, "log", "-r", "@", "--no-graph")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read jj working copy change: %w", err)
	}
	change = strings.TrimSpace(change)

	status, err := runJJ(ctx, root, "status")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read jj status: %w", err)
	}
	status = strings.TrimSpace(status)

	return PreparedWorkspace{
		Root:     root,
		CWD:      root,
		Change:   change,
		Status:   status,
		Dirty:    !strings.Contains(status, "The working copy has no changes."),
		Mode:     "shared",
		VCSType:  "jj",
		WorkerID: spec.WorkerID,
		TaskID:   spec.TaskID,
	}, nil
}

func runJJ(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "jj", args...)
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
