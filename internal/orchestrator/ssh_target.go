package orchestrator

import (
	"context"
	"encoding/base64"
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
	Executor           RemoteExecutor
	PollInterval       time.Duration
	PollCommandTimeout time.Duration
	CallbackHandler    RemoteCallbackHandler
}

type RemoteCallbackHandler func(context.Context, remoteRun, []RemoteWorkerCallback) error

type RemoteWorkerCallback struct {
	ID                   string
	Type                 string
	Prompt               string
	Title                string
	ProjectID            string
	ParentTaskID         string
	ParentWorkerID       string
	Body                 string
	Repo                 string
	Base                 string
	Branch               string
	Draft                bool
	ContinueAfterPublish bool
}

type remoteRun struct {
	Target    TargetConfig `json:"target"`
	Session   string       `json:"session"`
	RunDir    string       `json:"runDir"`
	WorkDir   string       `json:"workDir"`
	TaskID    string       `json:"taskId,omitempty"`
	WorkerID  string       `json:"workerId,omitempty"`
	Status    string       `json:"status"`
	StartedAt time.Time    `json:"startedAt"`
}

type RemoteCheckoutSpec struct {
	RepoURL     string
	WorkDir     string
	DefaultBase string
	BaseRef     string
}

type remoteStatus struct {
	Status string `json:"status"`
	Exit   int    `json:"exit,omitempty"`
	Error  string `json:"error,omitempty"`
}

var errSSHPollCommandTimeout = errors.New("ssh poll command timed out")

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
		Executor:           execRemoteExecutor{},
		PollInterval:       2 * time.Second,
		PollCommandTimeout: 30 * time.Second,
	}
}

func NewRemoteRun(target TargetConfig, spec worker.Spec) remoteRun {
	return remoteRun{
		Target:    target,
		Session:   "aged-" + shortWorkerID(spec.ID),
		RunDir:    path.Join(nonEmpty(target.WorkRoot, "/tmp/aged-workers"), spec.ID),
		WorkDir:   nonEmpty(nonEmpty(spec.WorkDir, target.WorkDir), "."),
		TaskID:    spec.TaskID,
		WorkerID:  spec.ID,
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
	if err := r.installCallbackEnvironment(ctx, run); err != nil {
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

func (r SSHRunner) installCallbackEnvironment(ctx context.Context, run remoteRun) error {
	inputExecutor, ok := r.Executor.(RemoteInputExecutor)
	if !ok {
		return errors.New("remote executor does not support worker callback helper upload")
	}
	binDir := path.Join(run.RunDir, "bin")
	callbackDir := remoteCallbackDir(run)
	if _, err := r.Executor.Run(ctx, sshArgs(run.Target, "sh", "-lc", "mkdir -p "+shellQuote(binDir)+" "+shellQuote(callbackDir))); err != nil {
		return err
	}
	if _, err := inputExecutor.RunInput(ctx, sshArgs(run.Target, "sh", "-lc", "cat > "+shellQuote(remoteCallbackEnvPath(run))+" && chmod 600 "+shellQuote(remoteCallbackEnvPath(run))), remoteCallbackEnv(run)); err != nil {
		return err
	}
	if _, err := inputExecutor.RunInput(ctx, sshArgs(run.Target, "sh", "-lc", "cat > "+shellQuote(remoteCreateTaskHelperPath(run))+" && chmod 700 "+shellQuote(remoteCreateTaskHelperPath(run))), remoteCreateTaskHelperScript()); err != nil {
		return err
	}
	if _, err := inputExecutor.RunInput(ctx, sshArgs(run.Target, "sh", "-lc", "cat > "+shellQuote(remotePublishPRHelperPath(run))+" && chmod 700 "+shellQuote(remotePublishPRHelperPath(run))), remotePublishPRHelperScript()); err != nil {
		return err
	}
	return nil
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
	filter := worker.NewOutputFilter(parser)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	consecutivePollTimeouts := 0
	for {
		status, err := r.pollOnce(ctx, run, parser, filter, sink, &stdoutOffset, &stderrOffset)
		if err != nil {
			if errors.Is(err, errSSHPollCommandTimeout) && consecutivePollTimeouts < 1 {
				consecutivePollTimeouts++
				_ = filter.Flush(ctx, sink)
				select {
				case <-ctx.Done():
					_ = filter.Flush(ctx, sink)
					return remoteStatus{Status: "canceled"}, ctx.Err()
				case <-ticker.C:
				}
				continue
			}
			_ = filter.Flush(ctx, sink)
			return status, err
		}
		consecutivePollTimeouts = 0
		if status.Status == "succeeded" || status.Status == "failed" || status.Status == "canceled" {
			if err := filter.Flush(ctx, sink); err != nil {
				return status, err
			}
			return status, nil
		}
		select {
		case <-ctx.Done():
			_ = filter.Flush(ctx, sink)
			return remoteStatus{Status: "canceled"}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r SSHRunner) PollOnce(ctx context.Context, run remoteRun, parser worker.Parser, sink worker.Sink, stdoutOffset *int, stderrOffset *int) (remoteStatus, error) {
	filter := worker.NewOutputFilter(parser)
	status, err := r.pollOnce(ctx, run, parser, filter, sink, stdoutOffset, stderrOffset)
	if err != nil {
		_ = filter.Flush(ctx, sink)
		return status, err
	}
	if status.Status == "succeeded" || status.Status == "failed" || status.Status == "canceled" {
		if err := filter.Flush(ctx, sink); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (r SSHRunner) pollOnce(ctx context.Context, run remoteRun, parser worker.Parser, filter *worker.OutputFilter, sink worker.Sink, stdoutOffset *int, stderrOffset *int) (remoteStatus, error) {
	stdout, _ := r.runPollCommand(ctx, run.Target, "cat "+shellQuote(path.Join(run.RunDir, "stdout.log"))+" 2>/dev/null || true")
	emitNewRemoteLines(ctx, filter, sink, "stdout", stdout, stdoutOffset)
	stderr, _ := r.runPollCommand(ctx, run.Target, "cat "+shellQuote(path.Join(run.RunDir, "stderr.log"))+" 2>/dev/null || true")
	emitNewRemoteLines(ctx, filter, sink, "stderr", stderr, stderrOffset)
	rawStatus, err := r.runPollCommand(ctx, run.Target, "cat "+shellQuote(path.Join(run.RunDir, "status.json"))+" 2>/dev/null || printf '{\"status\":\"running\"}'")
	if err != nil {
		return remoteStatus{Status: "unreachable", Error: strings.TrimSpace(rawStatus)}, err
	}
	if err := r.drainRemoteCallbacks(ctx, run, sink); err != nil {
		if !errors.Is(err, errWorkerCallbackDeferred) {
			_ = sink.Event(ctx, worker.Event{Kind: worker.EventError, Stream: "stderr", Text: "failed to drain remote worker callbacks: " + err.Error()})
		}
	}
	var status remoteStatus
	if err := json.Unmarshal([]byte(strings.TrimSpace(rawStatus)), &status); err != nil {
		return remoteStatus{}, err
	}
	if status.Status == "" {
		status.Status = "running"
	}
	if status.Status == "running" {
		active, activeErr := r.remoteSessionActive(ctx, run)
		if activeErr == nil && !active {
			return inferTerminalRemoteStatus(parser, stdout, stderr), nil
		}
	}
	return status, nil
}

func (r SSHRunner) remoteSessionActive(ctx context.Context, run remoteRun) (bool, error) {
	_, err := r.runPollCommand(ctx, run.Target, "tmux has-session -t "+shellQuote(run.Session)+" 2>/dev/null")
	if err == nil {
		return true, nil
	}
	if commandExitCode(err) == 1 {
		return false, nil
	}
	return true, err
}

func inferTerminalRemoteStatus(parser worker.Parser, stdout string, stderr string) remoteStatus {
	status := remoteStatus{
		Status: "failed",
		Error:  "remote worker session ended before writing terminal status",
	}
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		event := parser.ParseLine("stdout", line)
		switch event.Kind {
		case worker.EventResult:
			status = remoteStatus{Status: "succeeded", Exit: 0}
		case worker.EventError:
			status = remoteStatus{Status: "failed", Error: nonEmpty(event.Text, status.Error)}
		}
	}
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		event := parser.ParseLine("stderr", line)
		if event.Kind == worker.EventError {
			status = remoteStatus{Status: "failed", Error: nonEmpty(event.Text, status.Error)}
		}
	}
	return status
}

func (r SSHRunner) drainRemoteCallbacks(ctx context.Context, run remoteRun, sink worker.Sink) error {
	if r.CallbackHandler == nil {
		return nil
	}
	raw, err := r.runPollCommand(ctx, run.Target, remoteCallbackReadScript(run))
	if err != nil {
		return err
	}
	callbacks, files, err := parseRemoteCallbackFiles(raw)
	if err != nil {
		return err
	}
	if len(callbacks) == 0 {
		return nil
	}
	if err := r.CallbackHandler(ctx, run, callbacks); err != nil {
		return err
	}
	if err := r.ackRemoteCallbacks(ctx, run, files); err != nil {
		_ = sink.Event(ctx, worker.Event{Kind: worker.EventError, Stream: "stderr", Text: "remote worker callbacks were handled but not acknowledged: " + err.Error()})
	}
	return nil
}

func (r SSHRunner) ackRemoteCallbacks(ctx context.Context, run remoteRun, files []string) error {
	if len(files) == 0 {
		return nil
	}
	parts := []string{"rm", "-f"}
	for _, file := range files {
		if base := path.Base(file); base == file && strings.HasSuffix(base, ".json") {
			parts = append(parts, shellQuote(path.Join(remoteCallbackDir(run), base)))
		}
	}
	if len(parts) == 2 {
		return nil
	}
	_, err := r.runPollCommand(ctx, run.Target, strings.Join(parts, " "))
	return err
}

func (r SSHRunner) runPollCommand(ctx context.Context, target TargetConfig, script string) (string, error) {
	timeout := r.PollCommandTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := r.Executor.Run(commandCtx, sshArgs(target, "sh", "-lc", script))
	if errors.Is(commandCtx.Err(), context.DeadlineExceeded) {
		return out, fmt.Errorf("%w after %s", errSSHPollCommandTimeout, timeout)
	}
	return out, err
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
	out, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", "test -d "+shellQuote(dir)))
	if err != nil {
		if commandExitCode(err) == 1 {
			return false, nil
		}
		detail := strings.TrimSpace(out)
		if detail != "" {
			return false, fmt.Errorf("%w: %s", err, detail)
		}
		return false, err
	}
	return true, nil
}

func commandExitCode(err error) int {
	var exitErr interface {
		ExitCode() int
	}
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
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

func (r SSHRunner) ApplyPatch(ctx context.Context, target TargetConfig, workDir string, runDir string, patchText string) error {
	patchText = normalizePatchText(patchText)
	if patchText == "" {
		return nil
	}
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	inputExecutor, ok := r.Executor.(RemoteInputExecutor)
	if !ok {
		return errors.New("remote executor does not support base patch upload")
	}
	patchPath := path.Join(runDir, "base.patch")
	if _, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", "mkdir -p "+shellQuote(runDir))); err != nil {
		return err
	}
	if _, err := inputExecutor.RunInput(ctx, sshArgs(target, "sh", "-lc", "cat > "+shellQuote(patchPath)), patchText); err != nil {
		return err
	}
	_, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", remoteApplyPatchScript(workDir, patchPath)))
	return err
}

func (r SSHRunner) Probe(ctx context.Context, target TargetConfig) (core.TargetHealth, core.TargetResources) {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	health := core.TargetHealth{
		Status:    "unknown",
		CheckedAt: time.Now().UTC(),
	}
	checkoutRoot := strings.TrimSpace(targetCheckoutRoot(target))
	if checkoutRoot == "" {
		health.Status = "unhealthy"
		health.Error = "remote checkoutRoot is required"
		return health, core.TargetResources{}
	}
	out, err := r.Executor.Run(ctx, sshArgs(target, "sh", "-lc", remoteProbeScript(checkoutRoot)))
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
	checkoutRootOK := parseProbeBool(values["checkoutRootOK"])
	health.Tools = map[string]bool{}
	for key, value := range values {
		name, ok := strings.CutPrefix(key, "tool.")
		if ok {
			health.Tools[name] = parseProbeBool(value)
		}
	}
	if health.Tmux && checkoutRootOK {
		health.Status = "ok"
	} else {
		health.Status = "unhealthy"
		if !checkoutRootOK {
			health.Error = nonEmpty(values["checkoutRootError"], "remote checkoutRoot is not writable")
		} else if !health.Tmux {
			health.Error = "tmux is not available"
		}
	}
	if health.Status == "ok" && !health.RepoPresent {
		health.Error = "project checkout path will be prepared before worker start"
	}
	return health, resources
}

func (r SSHRunner) DescribeChanges(ctx context.Context, run remoteRun) WorkspaceChanges {
	if r.Executor == nil {
		r.Executor = execRemoteExecutor{}
	}
	var firstReadErr error
	tryRead := func(name string) (string, bool) {
		out, err := r.runPollCommand(ctx, run.Target, "cat "+shellQuote(path.Join(run.RunDir, name))+" 2>/dev/null || true")
		if err != nil {
			if firstReadErr == nil {
				if detail := strings.TrimSpace(out); detail != "" {
					firstReadErr = fmt.Errorf("read remote %s: %w: %s", name, err, detail)
				} else {
					firstReadErr = fmt.Errorf("read remote %s: %w", name, err)
				}
			}
			return "", false
		}
		return out, true
	}
	read := func(name string) string {
		out, ok := tryRead(name)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}
	readRaw := func(name string) string {
		out, ok := tryRead(name)
		if !ok {
			return ""
		}
		return strings.TrimRight(out, "\r\n")
	}
	vcs := read("vcs.txt")
	root := nonEmpty(read("root.txt"), run.WorkDir)
	status := readRaw("changes.txt")
	diffStat := readRaw("diffstat.txt")
	diff := normalizePatchText(readRaw("diff.patch"))
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
	if firstReadErr != nil {
		changes.Error = firstReadErr.Error()
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
	inner := fmt.Sprintf(`AGED_REMOTE_CALLBACK_ENV=%s
AGED_REMOTE_HELPER_BIN=%s
export AGED_REMOTE_CALLBACK_ENV AGED_REMOTE_HELPER_BIN
%s
cd %s && (%s)%s > %s/stdout.log 2> %s/stderr.log
code=$?
%s
if [ "$code" -eq 0 ]; then printf '{"status":"succeeded","exit":0}' > %s/status.json; else printf '{"status":"failed","exit":%%s}' "$code" > %s/status.json; fi`,
		shellQuote(remoteCallbackEnvPath(run)),
		shellQuote(path.Join(run.RunDir, "bin")),
		remoteWorkerEnvScript(),
		shellQuote(run.WorkDir),
		command,
		stdinRedirect,
		shellQuote(run.RunDir),
		shellQuote(run.RunDir),
		remoteChangeScript(run),
		shellQuote(run.RunDir),
		shellQuote(run.RunDir),
	)
	tmuxCommand := remoteShellCommand(inner)
	return fmt.Sprintf(
		`tmux new-session -d -s %[1]s %s`,
		shellQuote(run.Session),
		shellQuote(tmuxCommand),
	)
}

func remotePromptPath(run remoteRun) string {
	return path.Join(run.RunDir, "prompt.txt")
}

func remoteCallbackEnvPath(run remoteRun) string {
	return path.Join(run.RunDir, "callback.env")
}

func remoteCallbackDir(run remoteRun) string {
	return path.Join(run.RunDir, "callbacks")
}

func remoteCreateTaskHelperPath(run remoteRun) string {
	return path.Join(run.RunDir, "bin", "aged-create-task")
}

func remotePublishPRHelperPath(run remoteRun) string {
	return path.Join(run.RunDir, "bin", "aged-publish-pr")
}

func remoteCallbackEnv(run remoteRun) string {
	lines := []string{
		"export AGED_PARENT_TASK_ID=" + shellQuote(run.TaskID),
		"export AGED_PARENT_WORKER_ID=" + shellQuote(run.WorkerID),
		"export AGED_WORKER_CALLBACK_DIR=" + shellQuote(remoteCallbackDir(run)),
	}
	return strings.Join(lines, "\n") + "\n"
}

func remoteCreateTaskHelperScript() string {
	return `#!/bin/sh
set -eu
case "${1:-}" in
  -h|--help)
    cat <<'EOF'
aged-create-task queues a follow-up task for the original aged orchestrator.

Usage:
  aged-create-task [--title TITLE] [--project-id PROJECT_ID] < prompt.txt
  printf '%s\n' "Prompt for the new task" | aged-create-task --title "Follow-up"

Input:
  Reads the full new task prompt from stdin. The orchestrator trims the prompt
  and rejects empty prompts when it drains worker callbacks.

Options:
  --title TITLE          Optional task title. If omitted, aged applies its
                         normal title/defaulting behavior.
  --project-id ID        Optional project ID for the new task.
  -h, --help             Show this help and exit.

Environment:
  AGED_WORKER_CALLBACK_DIR   Required for queueing callbacks. The helper writes
                             an atomic JSON callback file into this directory.
  AGED_PARENT_TASK_ID        Optional parent task metadata; exported by aged for
                             remote workers.
  AGED_PARENT_WORKER_ID      Optional parent worker metadata; exported by aged
                             for remote workers.

Behavior:
  The command does not contact the orchestrator directly. It writes a callback
  file that the existing SSH worker session drains over the original control
  channel. On success it prints "queued <path>".

Failure cases:
  Exits 2 if AGED_WORKER_CALLBACK_DIR is missing, an unknown argument is passed,
  or no base64 encoder is available (base64, openssl, or python3).
EOF
    exit 0
    ;;
esac
if [ -z "${AGED_WORKER_CALLBACK_DIR:-}" ]; then
  echo "AGED_WORKER_CALLBACK_DIR is required" >&2
  exit 2
fi
title=""
project_id=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --title) title="$2"; shift 2 ;;
    --project-id) project_id="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
b64() {
  if command -v base64 >/dev/null 2>&1; then base64 | tr -d '\n'
  elif command -v openssl >/dev/null 2>&1; then openssl base64 -A
  elif command -v python3 >/dev/null 2>&1; then python3 -c 'import base64,sys; print(base64.b64encode(sys.stdin.buffer.read()).decode())'
  else echo "base64, openssl, or python3 is required" >&2; exit 2
  fi
}
mkdir -p "$AGED_WORKER_CALLBACK_DIR"
stamp=$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || date +%s)
tmp="$AGED_WORKER_CALLBACK_DIR/create-task.$stamp.$$.${RANDOM:-0}.tmp"
out="${tmp%.tmp}.json"
prompt_b64=$(cat | b64)
title_b64=$(printf '%s' "$title" | b64)
project_b64=$(printf '%s' "$project_id" | b64)
parent_task_b64=$(printf '%s' "${AGED_PARENT_TASK_ID:-}" | b64)
parent_worker_b64=$(printf '%s' "${AGED_PARENT_WORKER_ID:-}" | b64)
printf '{"type":"create_task","promptBase64":"%s","titleBase64":"%s","projectIdBase64":"%s","parentTaskIdBase64":"%s","parentWorkerIdBase64":"%s"}\n' "$prompt_b64" "$title_b64" "$project_b64" "$parent_task_b64" "$parent_worker_b64" > "$tmp"
mv "$tmp" "$out"
printf 'queued %s\n' "$out"
`
}

func remotePublishPRHelperScript() string {
	return `#!/bin/sh
set -eu
case "${1:-}" in
  -h|--help)
    cat <<'EOF'
aged-publish-pr asks the original aged orchestrator to publish this worker result.

Usage:
  aged-publish-pr [--title TITLE] [--repo OWNER/REPO] [--base BRANCH] [--branch BRANCH] [--draft] [--wait-after-publish] < body.md
  printf '%s\n' "Summary and validation" | aged-publish-pr --title "Improve loop handling"

Input:
  Reads the pull request body from stdin. The orchestrator trims the body and
  rejects empty bodies.

Options:
  --title TITLE          Optional pull request title.
  --repo OWNER/REPO      Optional target repository.
  --base BRANCH          Optional base branch.
  --branch BRANCH        Optional head branch name.
  --draft                Open as a draft pull request.
  --wait-after-publish   Leave the parent task waiting on the PR instead of
                         continuing. By default this helper is for intermediate
                         PRs and the parent task continues after publication.
  -h, --help             Show this help and exit.
EOF
    exit 0
    ;;
esac
if [ -z "${AGED_WORKER_CALLBACK_DIR:-}" ]; then
  echo "AGED_WORKER_CALLBACK_DIR is required" >&2
  exit 2
fi
title=""
repo=""
base=""
branch=""
draft=false
continue_after_publish=true
while [ "$#" -gt 0 ]; do
  case "$1" in
    --title) title="$2"; shift 2 ;;
    --repo) repo="$2"; shift 2 ;;
    --base) base="$2"; shift 2 ;;
    --branch) branch="$2"; shift 2 ;;
    --draft) draft=true; shift ;;
    --wait-after-publish) continue_after_publish=false; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
b64() {
  if command -v base64 >/dev/null 2>&1; then base64 | tr -d '\n'
  elif command -v openssl >/dev/null 2>&1; then openssl base64 -A
  elif command -v python3 >/dev/null 2>&1; then python3 -c 'import base64,sys; print(base64.b64encode(sys.stdin.buffer.read()).decode())'
  else echo "base64, openssl, or python3 is required" >&2; exit 2
  fi
}
mkdir -p "$AGED_WORKER_CALLBACK_DIR"
stamp=$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || date +%s)
tmp="$AGED_WORKER_CALLBACK_DIR/publish-pr.$stamp.$$.${RANDOM:-0}.tmp"
out="${tmp%.tmp}.json"
body_b64=$(cat | b64)
title_b64=$(printf '%s' "$title" | b64)
repo_b64=$(printf '%s' "$repo" | b64)
base_b64=$(printf '%s' "$base" | b64)
branch_b64=$(printf '%s' "$branch" | b64)
parent_task_b64=$(printf '%s' "${AGED_PARENT_TASK_ID:-}" | b64)
parent_worker_b64=$(printf '%s' "${AGED_PARENT_WORKER_ID:-}" | b64)
printf '{"type":"publish_pull_request","bodyBase64":"%s","titleBase64":"%s","repoBase64":"%s","baseBase64":"%s","branchBase64":"%s","parentTaskIdBase64":"%s","parentWorkerIdBase64":"%s","draft":%s,"continueAfterPublish":%s}\n' "$body_b64" "$title_b64" "$repo_b64" "$base_b64" "$branch_b64" "$parent_task_b64" "$parent_worker_b64" "$draft" "$continue_after_publish" > "$tmp"
mv "$tmp" "$out"
printf 'queued %s\n' "$out"
`
}

func remoteCallbackReadScript(run remoteRun) string {
	return fmt.Sprintf(`dir=%s
[ -d "$dir" ] || exit 0
for file in "$dir"/*.json; do
  [ -f "$file" ] || continue
  base=$(basename "$file")
  printf 'AGED-CALLBACK-FILE:%%s\n' "$base"
  cat "$file"
  printf '\nAGED-CALLBACK-END\n'
done`, shellQuote(remoteCallbackDir(run)))
}

type remoteCallbackPayload struct {
	Type                 string `json:"type"`
	PromptBase64         string `json:"promptBase64"`
	BodyBase64           string `json:"bodyBase64,omitempty"`
	TitleBase64          string `json:"titleBase64,omitempty"`
	ProjectIDBase64      string `json:"projectIdBase64,omitempty"`
	RepoBase64           string `json:"repoBase64,omitempty"`
	BaseBase64           string `json:"baseBase64,omitempty"`
	BranchBase64         string `json:"branchBase64,omitempty"`
	ParentTaskIDBase64   string `json:"parentTaskIdBase64,omitempty"`
	ParentWorkerIDBase64 string `json:"parentWorkerIdBase64,omitempty"`
	Draft                bool   `json:"draft,omitempty"`
	ContinueAfterPublish bool   `json:"continueAfterPublish,omitempty"`
}

func parseRemoteCallbackFiles(raw string) ([]RemoteWorkerCallback, []string, error) {
	var callbacks []RemoteWorkerCallback
	var files []string
	lines := strings.Split(raw, "\n")
	for i := 0; i < len(lines); i++ {
		fileName, ok := strings.CutPrefix(lines[i], "AGED-CALLBACK-FILE:")
		if !ok {
			continue
		}
		fileName = strings.TrimSpace(fileName)
		var body strings.Builder
		for i++; i < len(lines) && lines[i] != "AGED-CALLBACK-END"; i++ {
			if body.Len() > 0 {
				body.WriteByte('\n')
			}
			body.WriteString(lines[i])
		}
		callback, err := decodeRemoteCallback(fileName, body.String())
		if err != nil {
			return nil, nil, err
		}
		callbacks = append(callbacks, callback)
		files = append(files, fileName)
	}
	return callbacks, files, nil
}

func decodeRemoteCallback(fileName string, body string) (RemoteWorkerCallback, error) {
	var payload remoteCallbackPayload
	if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &payload); err != nil {
		return RemoteWorkerCallback{}, fmt.Errorf("decode remote callback %s: %w", fileName, err)
	}
	decode := func(value string) (string, error) {
		if value == "" {
			return "", nil
		}
		bytes, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			bytes, err = base64.RawStdEncoding.DecodeString(value)
		}
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	}
	var prompt, title, prBody, projectID, repo, base, branch, parentTaskID, parentWorkerID string
	for _, field := range []struct {
		name  string
		value string
		out   *string
	}{
		{"prompt", payload.PromptBase64, &prompt},
		{"title", payload.TitleBase64, &title},
		{"body", payload.BodyBase64, &prBody},
		{"project", payload.ProjectIDBase64, &projectID},
		{"repo", payload.RepoBase64, &repo},
		{"base", payload.BaseBase64, &base},
		{"branch", payload.BranchBase64, &branch},
		{"parent task", payload.ParentTaskIDBase64, &parentTaskID},
		{"parent worker", payload.ParentWorkerIDBase64, &parentWorkerID},
	} {
		decoded, err := decode(field.value)
		if err != nil {
			return RemoteWorkerCallback{}, fmt.Errorf("decode remote callback %s %s: %w", field.name, fileName, err)
		}
		*field.out = decoded
	}
	return RemoteWorkerCallback{
		ID:                   strings.TrimSuffix(path.Base(fileName), ".json"),
		Type:                 nonEmpty(payload.Type, "create_task"),
		Prompt:               prompt,
		Title:                title,
		ProjectID:            projectID,
		ParentTaskID:         parentTaskID,
		ParentWorkerID:       parentWorkerID,
		Body:                 prBody,
		Repo:                 repo,
		Base:                 base,
		Branch:               branch,
		Draft:                payload.Draft,
		ContinueAfterPublish: payload.ContinueAfterPublish,
	}, nil
}

func remoteWorkerEnvScript() string {
	return `if [ -f "$AGED_REMOTE_CALLBACK_ENV" ]; then . "$AGED_REMOTE_CALLBACK_ENV"; fi
if [ -n "${AGED_REMOTE_HELPER_BIN:-}" ] && [ -d "$AGED_REMOTE_HELPER_BIN" ]; then PATH="$AGED_REMOTE_HELPER_BIN:$PATH"; fi
for dir in "$HOME"/.local/share/fnm/node-versions/*/installation/bin "$HOME"/.local/share/mise/installs/node/*/bin "$HOME"/.asdf/installs/nodejs/*/bin "$HOME"/.local/share/mise/shims "$HOME"/.npm-global/bin "$HOME"/.bun/bin "$HOME"/.local/bin "$HOME"/.cargo/bin "$HOME"/.deno/bin /bin /usr/bin /sbin /usr/sbin /usr/local/bin /snap/bin /exe.dev/bin; do
  if [ -d "$dir" ]; then PATH="$dir:$PATH"; fi
done
export PATH`
}

func remoteShellCommand(script string) string {
	if strings.TrimSpace(script) == "" {
		return ""
	}
	return "if command -v bash >/dev/null 2>&1; then exec bash -l -c " + shellQuote(script) + "; else " + script + "; fi"
}

func remoteChangeScript(run remoteRun) string {
	runDir := shellQuote(run.RunDir)
	return fmt.Sprintf(`if jj root >/dev/null 2>&1; then printf jj > %[1]s/vcs.txt; jj root > %[1]s/root.txt 2>/dev/null || pwd > %[1]s/root.txt; jj diff --summary > %[1]s/changes.txt 2>&1 || true; cp %[1]s/changes.txt %[1]s/diffstat.txt 2>/dev/null || true; jj diff --git > %[1]s/diff.patch 2>&1 || true; printf '\n' >> %[1]s/diff.patch; elif git rev-parse --show-toplevel >/dev/null 2>&1; then printf git > %[1]s/vcs.txt; git rev-parse --show-toplevel > %[1]s/root.txt 2>/dev/null || pwd > %[1]s/root.txt; git status --porcelain > %[1]s/changes.txt 2>&1 || true; git diff --stat > %[1]s/diffstat.txt 2>&1 || true; git diff --binary > %[1]s/diff.patch 2>&1 || true; printf '\n' >> %[1]s/diff.patch; git ls-files --others --exclude-standard | while IFS= read -r path; do git diff --no-index --binary -- /dev/null "$path" >> %[1]s/diff.patch 2>/dev/null || true; done; else printf unknown > %[1]s/vcs.txt; pwd > %[1]s/root.txt; : > %[1]s/changes.txt; : > %[1]s/diffstat.txt; : > %[1]s/diff.patch; fi`, runDir)
}

func remotePrepareCheckoutScript(spec RemoteCheckoutSpec) string {
	return fmt.Sprintf(`set -eu
work_dir=%[1]s
repo_url=%[2]s
base=%[3]s
base_ref=%[4]s
if [ -d "$work_dir/.git" ]; then
  cd "$work_dir"
  if [ -n "$(git status --porcelain)" ]; then
    stash_message="aged remote checkout backup $(date -u +%%Y%%m%%dT%%H%%M%%SZ)"
    git stash push --include-untracked -m "$stash_message"
    stash_ref=$(git rev-parse --short stash@{0} 2>/dev/null || true)
    echo "stashed dirty remote checkout ${stash_ref:-stash@{0}}: $stash_message"
  fi
  if [ -n "$repo_url" ] && ! git remote get-url origin >/dev/null 2>&1; then
    git remote add origin "$repo_url"
  fi
  git fetch origin --prune
elif [ -d "$work_dir/.jj" ] && [ ! -d "$work_dir/.git" ]; then
  cd "$work_dir"
  if [ -n "$(jj diff --stat)" ]; then
    echo "remote jj checkout is dirty; preserving current checkout: $work_dir"
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
  elif [ -n "$base_ref" ] && git cat-file -e "$base_ref^{commit}" 2>/dev/null; then
    git checkout --detach "$base_ref"
  else
    git checkout "$base"
    git pull --ff-only
  fi
elif [ -n "$base_ref" ] && git cat-file -e "$base_ref^{commit}" 2>/dev/null; then
  git checkout --detach "$base_ref"
fi
echo "prepared git checkout $work_dir"`, shellQuote(spec.WorkDir), shellQuote(spec.RepoURL), shellQuote(spec.DefaultBase), shellQuote(spec.BaseRef))
}

func remoteApplyPatchScript(workDir string, patchPath string) string {
	return fmt.Sprintf(`set -eu
cd %[1]s
patch_path=%[2]s
if git apply --check --whitespace=nowarn "$patch_path"; then
  git apply --whitespace=nowarn "$patch_path"
else
  probe_root=$(mktemp -d "${TMPDIR:-/tmp}/aged-apply-probe.XXXXXX")
  probe_dir="$probe_root/worktree"
  cleanup_probe() {
    git worktree remove --force "$probe_dir" >/dev/null 2>&1 || true
    rm -rf "$probe_root"
  }
  trap cleanup_probe EXIT
  git worktree add --detach "$probe_dir" HEAD >/dev/null
  if ! git -C "$probe_dir" apply --3way --whitespace=nowarn "$patch_path"; then
    exit 1
  fi
  cleanup_probe
  trap - EXIT
  if ! git apply --3way --whitespace=nowarn "$patch_path"; then
    git reset --hard HEAD >/dev/null 2>&1 || true
    exit 1
  fi
fi`, shellQuote(workDir), shellQuote(patchPath))
}

func remoteProbeScript(checkoutRoot string) string {
	return fmt.Sprintf(`%s
checkout_root=%s
checkout_root_error=""
if [ -d "$checkout_root" ]; then
  checkout_root_ok=true
elif checkout_root_error=$(mkdir -p "$checkout_root" 2>&1); then
  if [ -d "$checkout_root" ] && [ -w "$checkout_root" ]; then
    checkout_root_ok=true
  else
    checkout_root_ok=false
    checkout_root_error="remote checkoutRoot is not writable: $checkout_root"
  fi
else
  checkout_root_ok=false
fi
printf 'checkoutRootOK=%%s\n' "$checkout_root_ok"
if [ "$checkout_root_ok" != true ]; then printf 'checkoutRootError=%%s\n' "$checkout_root_error"; fi
printf 'tmux=%%s\n' "$(command -v tmux >/dev/null 2>&1 && echo true || echo false)"
printf 'tool.codex=%%s\n' "$(command -v codex >/dev/null 2>&1 && echo true || echo false)"
printf 'tool.claude=%%s\n' "$(command -v claude >/dev/null 2>&1 && echo true || echo false)"
printf 'tool.git=%%s\n' "$(command -v git >/dev/null 2>&1 && echo true || echo false)"
printf 'tool.jj=%%s\n' "$(command -v jj >/dev/null 2>&1 && echo true || echo false)"
printf 'repoPresent=%%s\n' "$(test -d "$checkout_root" && echo true || echo false)"
df -Pk "$checkout_root" 2>/dev/null | awk 'NR==2 { print "diskAvailableKB="$4; print "diskUsedPercent="$5 }'
if [ -r /proc/meminfo ]; then awk '/MemTotal:/ { print "memoryTotalKB="$2 } /MemAvailable:/ { print "memoryAvailableKB="$2 }' /proc/meminfo; fi
if [ -r /proc/loadavg ]; then awk '{ print "load1="$1 }' /proc/loadavg; else uptime | sed -n 's/.*load averages*: *\([0-9.]*\).*/load1=\1/p'; fi
cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 0)"
printf 'cpuCount=%%s\n' "$cpu_count"`, remoteWorkerEnvScript(), shellQuote(checkoutRoot))
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

func emitNewRemoteLines(ctx context.Context, filter *worker.OutputFilter, sink worker.Sink, stream string, content string, offset *int) {
	if *offset > len(content) {
		*offset = 0
	}
	next := content[*offset:]
	*offset = len(content)
	_ = worker.StreamReaderLines(ctx, stream, strings.NewReader(next), func(line string) error {
		if strings.TrimSpace(line) == "" {
			return nil
		}
		return filter.EmitLine(ctx, sink, stream, line)
	}, func(stream string, discarded int) error {
		return sink.Event(ctx, worker.Event{
			Kind:   worker.EventError,
			Stream: stream,
			Text:   fmt.Sprintf("discarded oversized JSON event line from remote %s log after %d bytes", stream, discarded),
		})
	})
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
