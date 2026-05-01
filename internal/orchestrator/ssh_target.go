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

type RemoteCheckoutSpec struct {
	RepoURL     string
	WorkDir     string
	DefaultBase string
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

func (execRemoteExecutor) RunInput(ctx context.Context, argv []string, input string) (string, error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = strings.NewReader(input)
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
		WorkDir:   nonEmpty(nonEmpty(spec.WorkDir, target.WorkDir), "."),
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}
}

func (r SSHRunner) Start(ctx context.Context, run remoteRun, argv []string, stdin string) error {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	if _, err := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "mkdir -p "+shellQuote(run.RunDir)+" && printf %s "+shellQuote(`{"status":"running"}`)+" > "+shellQuote(path.Join(run.RunDir, "status.json")))); err != nil {
		return err
	}
	if stdin != "" {
		inputExecutor, ok := r.Executor.(RemoteInputExecutor)
		if !ok {
			return errors.New("remote executor does not support stdin prompt upload")
		}
		if _, err := inputExecutor.RunInput(ctx, sshArgs(run.Target, "sh", "-lc", "cat > "+shellQuote(remotePromptPath(run))), stdin); err != nil {
			return err
		}
	}
	script := remoteStartScript(run, argv, stdin != "")
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

func (r SSHRunner) DirectoryExists(ctx context.Context, target TargetConfig, dir string) (bool, error) {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	_, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", "test -d "+shellQuote(dir)))
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (r SSHRunner) PrepareCheckout(ctx context.Context, target TargetConfig, spec RemoteCheckoutSpec) (string, error) {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	if strings.TrimSpace(spec.WorkDir) == "" {
		return "", errors.New("remote workDir is required")
	}
	out, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", remotePrepareCheckoutScript(spec)))
	if err != nil {
		return strings.TrimSpace(out), err
	}
	return strings.TrimSpace(out), nil
}

func (r SSHRunner) Probe(ctx context.Context, target TargetConfig) (core.TargetHealth, core.TargetResources) {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	health := core.TargetHealth{
		Status:    "unknown",
		CheckedAt: time.Now().UTC(),
	}
	workDir := nonEmpty(target.WorkDir, ".")
	out, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", remoteProbeScript(workDir)))
	if err != nil {
		health.Status = "error"
		health.Error = strings.TrimSpace(nonEmpty(out, err.Error()))
		return health, core.TargetResources{}
	}
	values := parseProbeValues(out)
	resources := core.TargetResources{
		Load1:             parseProbeFloat(values["load1"]),
		CPUCount:          int(parseProbeInt(values["cpuCount"])),
		MemoryTotalMB:     parseProbeInt(values["memoryTotalKB"]) / 1024,
		MemoryAvailableMB: parseProbeInt(values["memoryAvailableKB"]) / 1024,
		DiskAvailableMB:   parseProbeInt(values["diskAvailableKB"]) / 1024,
		DiskUsedPercent:   parseProbeFloat(strings.TrimSuffix(values["diskUsedPercent"], "%")),
	}
	health.Reachable = true
	health.Tmux = parseProbeBool(values["tmux"])
	health.RepoPresent = parseProbeBool(values["repoPresent"])
	if health.Tmux {
		health.Status = "ok"
	} else {
		health.Status = "unhealthy"
		if !health.Tmux {
			health.Error = "tmux is not available"
		}
	}
	if health.Tmux && !health.RepoPresent {
		health.Error = "repo path will be prepared before worker start"
	}
	return health, resources
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
	stdout := readRaw("stdout.log")
	stderr := readRaw("stderr.log")
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
		Artifacts:     remoteLogArtifacts(run, stdout, stderr),
	}
	switch vcs {
	case "jj":
		changes.ChangedFiles = parseJJDiffSummary(status)
	case "git":
		changes.ChangedFiles = parseGitPorcelain(status)
	}
	return changes
}

func remoteLogArtifacts(run remoteRun, stdout string, stderr string) []WorkspaceArtifact {
	artifacts := []WorkspaceArtifact{}
	if strings.TrimSpace(stdout) != "" {
		artifacts = append(artifacts, WorkspaceArtifact{
			ID:      run.Session + "-stdout",
			Kind:    "worker_log",
			Name:    "Remote stdout",
			Path:    path.Join(run.RunDir, "stdout.log"),
			Content: truncateArtifactContent(stdout),
			Metadata: map[string]any{
				"stream": "stdout",
				"bytes":  len(stdout),
			},
		})
	}
	if strings.TrimSpace(stderr) != "" {
		artifacts = append(artifacts, WorkspaceArtifact{
			ID:      run.Session + "-stderr",
			Kind:    "worker_log",
			Name:    "Remote stderr",
			Path:    path.Join(run.RunDir, "stderr.log"),
			Content: truncateArtifactContent(stderr),
			Metadata: map[string]any{
				"stream": "stderr",
				"bytes":  len(stderr),
			},
		})
	}
	return artifacts
}

func truncateArtifactContent(content string) string {
	const limit = 64 * 1024
	if len(content) <= limit {
		return content
	}
	return content[:limit] + "\n[truncated]"
}

func remoteStartScript(run remoteRun, argv []string, hasStdin bool) string {
	command := shellJoin(argv)
	stdinRedirect := ""
	if hasStdin {
		stdinRedirect = " < " + shellQuote(remotePromptPath(run))
	}
	return fmt.Sprintf(
		`tmux new-session -d -s %[1]s %s`,
		shellQuote(run.Session),
		shellQuote(fmt.Sprintf(`%s; cd %s && (%s)%s > %s/stdout.log 2> %s/stderr.log; code=$?; %s; if [ "$code" -eq 0 ]; then printf '{"status":"succeeded","exit":0}' > %s/status.json; else printf '{"status":"failed","exit":%%s}' "$code" > %s/status.json; fi`,
			remoteWorkerEnvScript(),
			shellQuote(run.WorkDir),
			command,
			stdinRedirect,
			shellQuote(run.RunDir),
			shellQuote(run.RunDir),
			remoteChangeScript(run),
			shellQuote(run.RunDir),
			shellQuote(run.RunDir),
		)),
	)
}

func remotePromptPath(run remoteRun) string {
	return path.Join(run.RunDir, "prompt.txt")
}

func remoteWorkerEnvScript() string {
	return `export PATH="$HOME/.local/share/mise/shims:$HOME/.local/bin:$HOME/.cargo/bin:$HOME/.deno/bin:/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin:/snap/bin:$PATH"`
}

func remoteChangeScript(run remoteRun) string {
	runDir := shellQuote(run.RunDir)
	return fmt.Sprintf(`if jj root >/dev/null 2>&1; then printf jj > %[1]s/vcs.txt; jj root > %[1]s/root.txt 2>/dev/null || pwd > %[1]s/root.txt; jj diff --summary > %[1]s/changes.txt 2>&1 || true; cp %[1]s/changes.txt %[1]s/diffstat.txt 2>/dev/null || true; jj diff --git > %[1]s/diff.patch 2>&1 || true; elif git rev-parse --show-toplevel >/dev/null 2>&1; then printf git > %[1]s/vcs.txt; git rev-parse --show-toplevel > %[1]s/root.txt 2>/dev/null || pwd > %[1]s/root.txt; git status --porcelain > %[1]s/changes.txt 2>&1 || true; git diff --stat > %[1]s/diffstat.txt 2>&1 || true; git diff --binary > %[1]s/diff.patch 2>&1 || true; else printf unknown > %[1]s/vcs.txt; pwd > %[1]s/root.txt; : > %[1]s/changes.txt; : > %[1]s/diffstat.txt; : > %[1]s/diff.patch; fi`, runDir)
}

func remotePrepareCheckoutScript(spec RemoteCheckoutSpec) string {
	return fmt.Sprintf(`set -eu
work_dir=%[1]s
repo_url=%[2]s
base=%[3]s
if [ -d "$work_dir/.git" ]; then
  cd "$work_dir"
  if [ -n "$(git status --porcelain)" ]; then
    echo "remote checkout is dirty: $work_dir"
    exit 20
  fi
  if [ -n "$repo_url" ] && ! git remote get-url origin >/dev/null 2>&1; then
    git remote add origin "$repo_url"
  fi
  git fetch origin --prune
elif [ -d "$work_dir/.jj" ] && [ ! -d "$work_dir/.git" ]; then
  cd "$work_dir"
  if [ -n "$(jj diff --stat)" ]; then
    echo "remote jj checkout is dirty: $work_dir"
    exit 20
  fi
  jj git fetch || true
  echo "prepared jj checkout $work_dir"
  exit 0
else
  if [ -z "$repo_url" ]; then
    echo "remote checkout is missing and project repo is not configured: $work_dir"
    exit 21
  fi
  mkdir -p "$(dirname "$work_dir")"
  git clone "$repo_url" "$work_dir"
  cd "$work_dir"
  git fetch origin --prune
fi
if [ -n "$base" ]; then
  if git rev-parse --verify --quiet "origin/$base" >/dev/null; then
    git checkout --detach "origin/$base"
  else
    git checkout "$base"
    git pull --ff-only
  fi
fi
echo "prepared git checkout $work_dir"`, shellQuote(spec.WorkDir), shellQuote(spec.RepoURL), shellQuote(spec.DefaultBase))
}

func remoteProbeScript(workDir string) string {
	return fmt.Sprintf(`printf 'tmux=%%s\n' "$(command -v tmux >/dev/null 2>&1 && echo true || echo false)"
printf 'repoPresent=%%s\n' "$(test -d %s && echo true || echo false)"
df -Pk %s 2>/dev/null | awk 'NR==2 { print "diskAvailableKB="$4; print "diskUsedPercent="$5 }'
if [ -r /proc/meminfo ]; then awk '/MemTotal:/ { print "memoryTotalKB="$2 } /MemAvailable:/ { print "memoryAvailableKB="$2 }' /proc/meminfo; fi
if [ -r /proc/loadavg ]; then awk '{ print "load1="$1 }' /proc/loadavg; else uptime | sed -n 's/.*load averages*: *\([0-9.]*\).*/load1=\1/p'; fi
cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 0)"
printf 'cpuCount=%%s\n' "$cpu_count"`, shellQuote(workDir), shellQuote(workDir))
}

func parseProbeValues(out string) map[string]string {
	values := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return values
}

func parseProbeBool(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "true") || strings.TrimSpace(value) == "1"
}

func parseProbeFloat(value string) float64 {
	number, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return number
}

func parseProbeInt(value string) int64 {
	number, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return number
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
