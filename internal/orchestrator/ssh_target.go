package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

type SSHRunner struct {
	Executor     RemoteExecutor
	PollInterval time.Duration
}

type remoteRun struct {
	Target    TargetConfig `json:"target"`
	Session   string       `json:"session"`
	RunDir    string       `json:"runDir"`
	WorkDir   string       `json:"workDir"`
	Status    string       `json:"status"`
	StartedAt time.Time    `json:"startedAt"`
}

type remoteStatus struct {
	Status string `json:"status"`
	Exit   int    `json:"exit,omitempty"`
	Error  string `json:"error,omitempty"`
}

type execRemoteExecutor struct{}

func (execRemoteExecutor) Run(ctx context.Context, argv []string) (string, error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func NewSSHRunner() SSHRunner {
	return SSHRunner{
		Executor:     execRemoteExecutor{},
		PollInterval: 2 * time.Second,
	}
}

func NewRemoteRun(target TargetConfig, spec worker.Spec) remoteRun {
	return remoteRun{
		Target:    target,
		Session:   "aged-" + shortWorkerID(spec.ID),
		RunDir:    path.Join(nonEmpty(target.WorkRoot, "/tmp/aged-workers"), spec.ID),
		WorkDir:   nonEmpty(nonEmpty(target.WorkDir, spec.WorkDir), "."),
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}
}

func (r SSHRunner) Start(ctx context.Context, run remoteRun, argv []string) error {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	script := remoteStartScript(run, argv)
	_, err := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", script))
	return err
}

func (r SSHRunner) Poll(ctx context.Context, run remoteRun, parser worker.Parser, sink worker.Sink) (remoteStatus, error) {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	stdoutOffset := 0
	stderrOffset := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status, err := r.PollOnce(ctx, run, parser, sink, &stdoutOffset, &stderrOffset)
		if err != nil {
			return status, err
		}
		if status.Status == "succeeded" || status.Status == "failed" || status.Status == "canceled" {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return remoteStatus{Status: "canceled"}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r SSHRunner) PollOnce(ctx context.Context, run remoteRun, parser worker.Parser, sink worker.Sink, stdoutOffset *int, stderrOffset *int) (remoteStatus, error) {
	stdout, _ := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "cat "+shellQuote(path.Join(run.RunDir, "stdout.log"))+" 2>/dev/null || true"))
	emitNewRemoteLines(ctx, parser, sink, "stdout", stdout, stdoutOffset)
	stderr, _ := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "cat "+shellQuote(path.Join(run.RunDir, "stderr.log"))+" 2>/dev/null || true"))
	emitNewRemoteLines(ctx, parser, sink, "stderr", stderr, stderrOffset)
	rawStatus, err := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "cat "+shellQuote(path.Join(run.RunDir, "status.json"))+" 2>/dev/null || printf '{\"status\":\"running\"}'"))
	if err != nil {
		return remoteStatus{Status: "unreachable", Error: strings.TrimSpace(rawStatus)}, err
	}
	var status remoteStatus
	if err := json.Unmarshal([]byte(strings.TrimSpace(rawStatus)), &status); err != nil {
		return remoteStatus{}, err
	}
	if status.Status == "" {
		status.Status = "running"
	}
	return status, nil
}

func (r SSHRunner) Cancel(ctx context.Context, run remoteRun) error {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	_, err := r.Executor.Run(ctx, sshArgs(run.Target, "tmux", "kill-session", "-t", run.Session))
	return err
}

func (r SSHRunner) DescribeChanges(ctx context.Context, run remoteRun) WorkspaceChanges {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	read := func(name string) string {
		out, _ := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "cat "+shellQuote(path.Join(run.RunDir, name))+" 2>/dev/null || true"))
		return strings.TrimSpace(out)
	}
	readRaw := func(name string) string {
		out, _ := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "cat "+shellQuote(path.Join(run.RunDir, name))+" 2>/dev/null || true"))
		return strings.TrimRight(out, "\r\n")
	}
	vcs := read("vcs.txt")
	root := nonEmpty(read("root.txt"), run.WorkDir)
	status := readRaw("changes.txt")
	diffStat := readRaw("diffstat.txt")
	diff := readRaw("diff.patch")
	changes := WorkspaceChanges{
		Root:          root,
		CWD:           run.WorkDir,
		WorkspaceName: run.Session,
		Mode:          "remote",
		VCSType:       nonEmpty(vcs, "ssh"),
		Status:        status,
		DiffStat:      nonEmpty(diffStat, status),
		Diff:          diff,
		Dirty:         strings.TrimSpace(status) != "",
	}
	switch vcs {
	case "jj":
		changes.ChangedFiles = parseJJDiffSummary(status)
	case "git":
		changes.ChangedFiles = parseGitPorcelain(status)
	}
	return changes
}

func remoteStartScript(run remoteRun, argv []string) string {
	command := shellJoin(argv)
	statusRunning := shellQuote(`{"status":"running"}`)
	return fmt.Sprintf(
		`mkdir -p %[1]s && printf %%s %[2]s > %[1]s/status.json && tmux new-session -d -s %[3]s %s`,
		shellQuote(run.RunDir),
		statusRunning,
		shellQuote(run.Session),
		shellQuote(fmt.Sprintf(`cd %s && (%s) > %s/stdout.log 2> %s/stderr.log; code=$?; %s; if [ "$code" -eq 0 ]; then printf '{"status":"succeeded","exit":0}' > %s/status.json; else printf '{"status":"failed","exit":%%s}' "$code" > %s/status.json; fi`,
			shellQuote(run.WorkDir),
			command,
			shellQuote(run.RunDir),
			shellQuote(run.RunDir),
			remoteChangeScript(run),
			shellQuote(run.RunDir),
			shellQuote(run.RunDir),
		)),
	)
}

func remoteChangeScript(run remoteRun) string {
	runDir := shellQuote(run.RunDir)
	return fmt.Sprintf(`if jj root >/dev/null 2>&1; then printf jj > %[1]s/vcs.txt; jj root > %[1]s/root.txt 2>/dev/null || pwd > %[1]s/root.txt; jj diff --summary > %[1]s/changes.txt 2>&1 || true; cp %[1]s/changes.txt %[1]s/diffstat.txt 2>/dev/null || true; jj diff --git > %[1]s/diff.patch 2>&1 || true; elif git rev-parse --show-toplevel >/dev/null 2>&1; then printf git > %[1]s/vcs.txt; git rev-parse --show-toplevel > %[1]s/root.txt 2>/dev/null || pwd > %[1]s/root.txt; git status --porcelain > %[1]s/changes.txt 2>&1 || true; git diff --stat > %[1]s/diffstat.txt 2>&1 || true; git diff --binary > %[1]s/diff.patch 2>&1 || true; else printf unknown > %[1]s/vcs.txt; pwd > %[1]s/root.txt; : > %[1]s/changes.txt; : > %[1]s/diffstat.txt; : > %[1]s/diff.patch; fi`, runDir)
}

func sshArgs(target TargetConfig, remoteArgv ...string) []string {
	args := []string{"ssh"}
	if target.Port > 0 {
		args = append(args, "-p", strconv.Itoa(target.Port))
	}
	if target.IdentityFile != "" {
		args = append(args, "-i", target.IdentityFile, "-o", "IdentitiesOnly=yes")
	}
	if target.InsecureIgnoreHostKey {
		args = append(args, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR")
	}
	host := target.Host
	if target.User != "" {
		host = target.User + "@" + host
	}
	args = append(args, host)
	if len(remoteArgv) > 0 {
		args = append(args, shellJoin(remoteArgv))
	}
	return args
}

func shellJoin(argv []string) string {
	quoted := make([]string, 0, len(argv))
	for _, arg := range argv {
		quoted = append(quoted, shellQuote(arg))
	}
	return strings.Join(quoted, " ")
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func shortWorkerID(id string) string {
	id = strings.ReplaceAll(id, "-", "")
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func emitNewRemoteLines(ctx context.Context, parser worker.Parser, sink worker.Sink, stream string, content string, offset *int) {
	if *offset > len(content) {
		*offset = 0
	}
	next := content[*offset:]
	*offset = len(content)
	for _, line := range strings.Split(next, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		_ = sink.Event(ctx, parser.ParseLine(stream, line))
	}
}

func remoteStatusToWorkerStatus(status remoteStatus) (core.WorkerStatus, error) {
	switch status.Status {
	case "succeeded":
		return core.WorkerSucceeded, nil
	case "failed":
		return core.WorkerFailed, errors.New(nonEmpty(status.Error, fmt.Sprintf("remote worker exited with status %d", status.Exit)))
	case "canceled":
		return core.WorkerCanceled, context.Canceled
	default:
		return core.WorkerFailed, fmt.Errorf("remote worker ended with unknown status %q", status.Status)
	}
}
