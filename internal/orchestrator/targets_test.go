package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestTargetRegistrySelectsMatchingLeastLoadedTarget(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "small", Kind: TargetKindSSH, Host: "small", WorkDir: "/repo-small", Labels: map[string]string{"role": "general"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 1}},
		{ID: "perf", Kind: TargetKindSSH, Host: "perf", WorkDir: "/repo-perf", Labels: map[string]string{"role": "benchmark"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 8, MemoryGB: 64}},
	})
	plan := Plan{
		Prompt: "run benchmark",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "benchmark"},
			"workerSize":   "large",
		},
	}
	target, err := registry.Select(plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "perf" {
		t.Fatalf("target = %q", target.ID)
	}
}

func TestTargetRegistryDeleteMissingWrapsNotFound(t *testing.T) {
	registry := NewLocalTargetRegistry()

	err := registry.Delete("missing")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("delete missing err = %v, want ErrNotFound", err)
	}
	if err.Error() != "target not found" {
		t.Fatalf("delete missing message = %q", err.Error())
	}
}

func TestTargetRegistryAvoidsUnhealthySSHTargets(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "bad", Kind: TargetKindSSH, Host: "bad", WorkDir: "/repo-bad", Labels: map[string]string{"role": "benchmark"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 20, MemoryGB: 128}},
		{ID: "good", Kind: TargetKindSSH, Host: "good", WorkDir: "/repo-good", Labels: map[string]string{"role": "benchmark"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1, MemoryGB: 16}},
	})
	registry.UpdateHealth("bad", core.TargetHealth{Status: "unhealthy", Reachable: true, Tmux: false, RepoPresent: true}, core.TargetResources{})
	registry.UpdateHealth("good", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.2, MemoryAvailableMB: 8192})

	target, err := registry.Select(Plan{
		Prompt:   "run benchmark",
		Metadata: map[string]any{"targetLabels": map[string]any{"role": "benchmark"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "good" {
		t.Fatalf("target = %q, want good", target.ID)
	}
	snapshot := registry.Snapshot()
	for _, state := range snapshot {
		if state.ID == "bad" && state.Available {
			t.Fatalf("bad target should not be available: %+v", state)
		}
	}
}

func TestTargetRegistrySkipsSSHWorkerWhenToolProbeIsMissing(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})
	registry.UpdateHealth("vm", core.TargetHealth{
		Status: "ok",
		Tools:  map[string]bool{"codex": false},
	}, core.TargetResources{})

	target, err := registry.Select(Plan{WorkerKind: "codex", Prompt: "run codex"})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %s, want local", target.ID)
	}
}

func TestTargetRegistrySelectIDRejectsSSHWorkerWhenToolProbeIsMissing(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	registry.UpdateHealth("vm", core.TargetHealth{
		Status:    "ok",
		Reachable: true,
		Tmux:      true,
		Tools:     map[string]bool{"codex": false},
	}, core.TargetResources{})

	_, err := registry.SelectID("vm", "codex")
	if err == nil || !strings.Contains(err.Error(), `execution target "vm" does not support worker kind "codex"`) {
		t.Fatalf("SelectID error = %v, want unsupported worker kind", err)
	}
}

func TestTargetRegistrySelectHonorsRequiredTargetID(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Labels: map[string]string{"role": "general"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "pinned", Kind: TargetKindLocal, Labels: map[string]string{"role": "pinned"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "run on pinned host",
		Metadata:   map[string]any{"requiredTargetID": "pinned"},
	}
	target, err := registry.Select(plan)
	if err != nil {
		t.Fatalf("Select err = %v, want nil", err)
	}
	if target.ID != "pinned" {
		t.Fatalf("target = %q, want pinned (required hard constraint should win over higher-scoring local)", target.ID)
	}
}

func TestTargetRegistrySelectFailsWhenRequiredTargetMissing(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	_, err := registry.Select(Plan{
		WorkerKind: "mock",
		Prompt:     "run on missing host",
		Metadata:   map[string]any{"requiredTargetID": "absent"},
	})
	if err == nil {
		t.Fatal("Select with unknown requiredTargetID succeeded, want error")
	}
	if !strings.Contains(err.Error(), `required execution target "absent" is not configured`) {
		t.Fatalf("Select error = %v, want hard error mentioning absent target", err)
	}
}

func TestTargetRegistrySelectFailsWhenRequiredTargetUnavailable(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "pinned", Kind: TargetKindSSH, Host: "pinned", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	registry.UpdateHealth("pinned", core.TargetHealth{Status: "unhealthy", Reachable: false}, core.TargetResources{})
	_, err := registry.Select(Plan{
		WorkerKind: "mock",
		Prompt:     "run on unhealthy host",
		Metadata:   map[string]any{"requiredTargetID": "pinned"},
	})
	if err == nil {
		t.Fatal("Select with unavailable requiredTargetID succeeded, want error")
	}
	if !strings.Contains(err.Error(), `required execution target "pinned" is not available`) {
		t.Fatalf("Select error = %v, want unavailable hard error", err)
	}
}

func TestTargetRegistrySkipsSSHWithoutRemoteCheckoutRoot(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm", Kind: TargetKindSSH, Host: "vm", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})

	target, err := registry.Select(Plan{WorkerKind: "mock", Prompt: "run remotely"})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %s, want local", target.ID)
	}
	snapshot := registry.Snapshot()
	for _, state := range snapshot {
		if state.ID == "vm" && state.Available {
			t.Fatalf("ssh target without checkoutRoot should not be available: %+v", state)
		}
	}
}

func TestTargetRegistryRegisterRejectsUnknownKind(t *testing.T) {
	registry := NewLocalTargetRegistry()
	for _, kind := range []TargetKind{"remote", "ssHh"} {
		_, err := registry.Register(TargetConfig{ID: "bad-" + string(kind), Kind: kind})
		if err == nil {
			t.Fatalf("Register kind %q succeeded, want error", kind)
		}
		if !strings.Contains(err.Error(), "target kind") || !strings.Contains(err.Error(), "local") || !strings.Contains(err.Error(), "ssh") {
			t.Fatalf("Register kind %q error = %q, want useful kind validation", kind, err.Error())
		}
	}
}

func testSSHTarget() TargetConfig {
	return TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo", WorkRoot: "/runs"}
}
func testSSHSpec() worker.Spec { return worker.Spec{ID: "worker-123", WorkDir: "/repo"} }
func testSSHRun() remoteRun    { return NewRemoteRun(testSSHTarget(), testSSHSpec()) }

func TestTargetRegistryRegisterAllowsKnownAndEmptyKinds(t *testing.T) {
	registry := NewLocalTargetRegistry()
	for _, tc := range []struct {
		name string
		in   TargetConfig
		want TargetKind
	}{
		{name: "empty", in: TargetConfig{ID: "empty"}, want: TargetKindLocal},
		{name: "local", in: TargetConfig{ID: "local-2", Kind: TargetKindLocal}, want: TargetKindLocal},
		{name: "ssh", in: TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, want: TargetKindSSH},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := registry.Register(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != tc.want {
				t.Fatalf("kind = %q, want %q", got.Kind, tc.want)
			}
		})
	}
}

func TestLoadTargetRegistryFailsForSSHTargetMissingHost(t *testing.T) {
	path := t.TempDir() + "/targets.json"
	if err := os.WriteFile(path, []byte(`{"targets":[{"id":"vm","kind":"ssh","workDir":"/repo"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadTargetRegistry(path)
	if err == nil || !strings.Contains(err.Error(), "ssh target host is required") {
		t.Fatalf("LoadTargetRegistry error = %v, want ssh target host is required", err)
	}
}

func TestSSHRunnerProbeReportsToolAvailability(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"checkoutRootOK=true",
		"tmux=true",
		"repoPresent=true",
		"tool.codex=false",
		"tool.claude=true",
		"cpuCount=4",
	}, "\n")}
	runner := SSHRunner{Executor: executor}

	health, _ := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo"})
	if health.Tools["codex"] {
		t.Fatalf("codex should be unavailable: %+v", health.Tools)
	}
	if !health.Tools["claude"] {
		t.Fatalf("claude should be available: %+v", health.Tools)
	}
}

func TestSSHRunnerProbeRejectsMissingCheckoutRoot(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	health, _ := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"})
	if health.Status != "unhealthy" || !strings.Contains(health.Error, "checkoutRoot") {
		t.Fatalf("health = %+v", health)
	}
	if len(executor.commands) != 0 {
		t.Fatalf("probe should fail before ssh command, got %+v", executor.commands)
	}
}

func TestSSHRunnerProbeRejectsUnpreparableCheckoutRoot(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"checkoutRootOK=false",
		"checkoutRootError=mkdir: cannot create directory '/Users': Permission denied",
		"tmux=true",
		"repoPresent=false",
		"cpuCount=4",
	}, "\n")}
	runner := SSHRunner{Executor: executor}
	health, _ := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", CheckoutRoot: "/Users/nathan/project"})
	if health.Status != "unhealthy" || !strings.Contains(health.Error, "Permission denied") {
		t.Fatalf("health = %+v", health)
	}
}

func TestSSHRunnerStartsTmuxAndPollsStatus(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	run := testSSHRun()
	if err := runner.Start(context.Background(), run, []string{"sh", "-lc", "echo ok"}, ""); err != nil {
		t.Fatal(err)
	}
	sawTmux := false
	for _, command := range executor.commands {
		if strings.Contains(strings.Join(command, " "), "tmux new-session") {
			sawTmux = true
			break
		}
	}
	if !sawTmux {
		t.Fatalf("start command = %+v", executor.commands)
	}
	sink := &recordingWorkerSink{}
	stdoutOffset := 0
	stderrOffset := 0
	status, err := runner.PollOnce(context.Background(), run, worker.ParserForKind("mock"), sink, &stdoutOffset, &stderrOffset)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if !sink.has(worker.EventLog, "stdout", "remote output") {
		t.Fatalf("missing remote output: %+v", sink.events)
	}
	changes := runner.DescribeChanges(context.Background(), run)
	if changes.VCSType != "git" || !changes.Dirty || len(changes.ChangedFiles) != 1 || changes.ChangedFiles[0].Path != "main.go" || !strings.Contains(changes.Diff, "diff --git") {
		t.Fatalf("changes = %+v", changes)
	}
	if !strings.HasSuffix(changes.Diff, "\n") {
		t.Fatalf("diff should be normalized with trailing newline: %q", changes.Diff)
	}
	if len(changes.Artifacts) != 1 || changes.Artifacts[0].Kind != "worker_log" || !strings.Contains(changes.Artifacts[0].Content, "remote output") {
		t.Fatalf("artifacts = %+v", changes.Artifacts)
	}
}

func TestSSHRunnerPollsLargeRemoteLogLine(t *testing.T) {
	largeLine := strings.Repeat("r", 2*1024*1024)
	executor := &scriptedPollExecutor{
		stdout: []string{largeLine + "\n"},
		status: []string{`{"status":"succeeded","exit":0}`},
	}
	runner := SSHRunner{Executor: executor}
	run := testSSHRun()
	sink := &recordingWorkerSink{}
	stdoutOffset := 0
	stderrOffset := 0

	status, err := runner.PollOnce(context.Background(), run, worker.ParserForKind("mock"), sink, &stdoutOffset, &stderrOffset)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if !sink.has(worker.EventLog, "stdout", largeLine) {
		t.Fatalf("large remote output was not preserved: events=%d", len(sink.events))
	}
}

func TestSSHRunnerPollDedupesRemoteCodexInfrastructureWarningsAcrossPolls(t *testing.T) {
	warning := "2026-04-30T02:06:16.268038Z ERROR codex_core::session: failed to record rollout items: thread 019ddc1f-f8f0-7da0-a932-a956e7f51071 not found"
	executor := &scriptedPollExecutor{
		stdout: []string{
			"",
			`{"type":"item.completed","item":{"type":"agent_message","text":"done"}}` + "\n",
		},
		stderr: []string{
			warning + "\n",
			warning + "\n" + warning + "\nactual codex failure\n",
		},
		status: []string{
			`{"status":"running"}`,
			`{"status":"succeeded","exit":0}`,
		},
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("codex"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if got := sink.count(worker.EventLog, "stderr", warning); got != 1 {
		t.Fatalf("remote infrastructure warning count = %d, want 1; events = %+v", got, sink.events)
	}
	if !sink.hasText(worker.EventLog, "stderr", "suppressed 1 repeated Codex infrastructure warnings") {
		t.Fatalf("missing suppression summary: %+v", sink.events)
	}
	if !sink.has(worker.EventError, "stderr", "actual codex failure") {
		t.Fatalf("real stderr failure was swallowed: %+v", sink.events)
	}
	if !sink.has(worker.EventResult, "stdout", "done") {
		t.Fatalf("result was swallowed: %+v", sink.events)
	}
}

func TestSSHRunnerInfersTerminalStatusWhenRemoteSessionDisappears(t *testing.T) {
	executor := &scriptedPollExecutor{
		stdout: []string{
			`{"type":"result","subtype":"success","result":"done"}` + "\n",
		},
		status:         []string{`{"status":"running"}`},
		sessionMissing: true,
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("claude"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v, want succeeded", status)
	}
	if !sink.has(worker.EventResult, "stdout", "done") {
		t.Fatalf("result was not emitted: %+v", sink.events)
	}
}

func TestSSHRunnerRefreshesTerminalStatusWhenRemoteSessionDisappearsAfterRunningRead(t *testing.T) {
	result := `{"type":"result","subtype":"success","result":"done"}`
	executor := &scriptedPollExecutor{
		stdout: []string{
			"working\n",
			"working\n" + result + "\n",
		},
		status: []string{
			`{"status":"running"}`,
			`{"status":"succeeded","exit":0}`,
		},
		sessionMissing: true,
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("claude"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" || status.InferredFromOutput {
		t.Fatalf("status = %+v, want non-inferred succeeded", status)
	}
	if !sink.has(worker.EventResult, "stdout", "done") {
		t.Fatalf("final result was not emitted after session exit refresh: %+v", sink.events)
	}
	if executor.commandContains("kill-session") {
		t.Fatalf("remote session was killed after status.json succeeded: %+v", executor.commands)
	}
}

func TestSSHRunnerCompletesWhenClaudeResultArrivesBeforeProcessExit(t *testing.T) {
	executor := &scriptedPollExecutor{
		stdout: []string{
			`{"type":"result","subtype":"success","result":"done"}` + "\n",
		},
		status: []string{`{"status":"running"}`},
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("claude"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v, want succeeded", status)
	}
	if !sink.has(worker.EventResult, "stdout", "done") {
		t.Fatalf("result was not emitted: %+v", sink.events)
	}
	if !executor.commandContains("kill-session") {
		t.Fatalf("remote session was not cleaned up: %+v", executor.commands)
	}
}

func TestSSHRunnerPollsRemoteLogsFromLastOffset(t *testing.T) {
	executor := &scriptedPollExecutor{
		stdout: []string{
			"one\n",
			"one\ntwo\n",
		},
		status: []string{
			`{"status":"running"}`,
			`{"status":"succeeded","exit":0}`,
		},
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("mock"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if got := sink.count(worker.EventLog, "stdout", "one"); got != 1 {
		t.Fatalf("one event count = %d, want 1; events = %+v", got, sink.events)
	}
	if got := sink.count(worker.EventLog, "stdout", "two"); got != 1 {
		t.Fatalf("two event count = %d, want 1; events = %+v", got, sink.events)
	}
	if got, want := executor.stdoutReads, []remoteLogReadRecord{{Offset: 0, Start: 0, Size: 4, Bytes: 4}, {Offset: 4, Start: 4, Size: 8, Bytes: 4}}; !equalRemoteLogReadRecords(got, want) {
		t.Fatalf("stdout reads = %+v, want %+v", got, want)
	}
}

func TestSSHRunnerPollResetsOffsetAfterRemoteLogTruncation(t *testing.T) {
	executor := &scriptedPollExecutor{
		stdout: []string{
			"old\nlong\n",
			"new\n",
		},
		status: []string{
			`{"status":"running"}`,
			`{"status":"succeeded","exit":0}`,
		},
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("mock"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	for _, text := range []string{"old", "long", "new"} {
		if !sink.has(worker.EventLog, "stdout", text) {
			t.Fatalf("missing %q after truncation: %+v", text, sink.events)
		}
	}
	if got, want := executor.stdoutReads, []remoteLogReadRecord{{Offset: 0, Start: 0, Size: 9, Bytes: 9}, {Offset: 9, Start: 0, Size: 4, Bytes: 4}}; !equalRemoteLogReadRecords(got, want) {
		t.Fatalf("stdout reads = %+v, want %+v", got, want)
	}
}

func TestSSHRunnerBuffersPartialRemoteLogLinesAcrossPolls(t *testing.T) {
	executor := &scriptedPollExecutor{
		stdout: []string{
			"hel",
			"hello\n",
		},
		status: []string{
			`{"status":"running"}`,
			`{"status":"succeeded","exit":0}`,
		},
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("mock"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if sink.has(worker.EventLog, "stdout", "hel") {
		t.Fatalf("partial line was emitted before completion: %+v", sink.events)
	}
	if got := sink.count(worker.EventLog, "stdout", "hello"); got != 1 {
		t.Fatalf("hello event count = %d, want 1; events = %+v", got, sink.events)
	}
}

func TestSSHRunnerInfersTerminalStatusFromBufferedRemoteResultWithoutTrailingNewline(t *testing.T) {
	result := `{"type":"result","subtype":"success","result":"done"}`
	executor := &scriptedPollExecutor{
		stdout: []string{
			result[:15],
			result,
		},
		status: []string{
			`{"status":"running"}`,
			`{"status":"running"}`,
		},
	}
	runner := SSHRunner{Executor: executor, PollInterval: time.Nanosecond}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("claude"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v, want succeeded", status)
	}
	if !sink.has(worker.EventResult, "stdout", "done") {
		t.Fatalf("buffered terminal result was not emitted: %+v", sink.events)
	}
	if !executor.commandContains("kill-session") {
		t.Fatalf("remote session was not cleaned up: %+v", executor.commands)
	}
}

func TestSSHRunnerPollRetriesHungStatusRead(t *testing.T) {
	executor := &timeoutThenStatusExecutor{}
	runner := SSHRunner{
		Executor:           executor,
		PollInterval:       time.Nanosecond,
		PollCommandTimeout: time.Millisecond,
	}
	run := testSSHRun()
	sink := &recordingWorkerSink{}

	status, err := runner.Poll(context.Background(), run, worker.ParserForKind("mock"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if executor.statusCalls != 2 {
		t.Fatalf("status calls = %d, want 2", executor.statusCalls)
	}
}

func TestSSHRunnerDescribeChangesTimesOutHungArtifactRead(t *testing.T) {
	executor := &timeoutDiffPatchExecutor{}
	runner := SSHRunner{Executor: executor, PollCommandTimeout: time.Millisecond}
	run := testSSHRun()

	start := time.Now()
	changes := runner.DescribeChanges(context.Background(), run)
	if time.Since(start) > time.Second {
		t.Fatalf("DescribeChanges did not bound hung artifact read")
	}
	if executor.diffPatchCalls != 1 {
		t.Fatalf("diff patch calls = %d, want 1", executor.diffPatchCalls)
	}
	if changes.VCSType != "git" || !changes.Dirty || len(changes.ChangedFiles) != 1 || changes.ChangedFiles[0].Path != "main.go" {
		t.Fatalf("changes = %+v", changes)
	}
	if changes.Diff != "" {
		t.Fatalf("diff = %q, want empty after timed-out read", changes.Diff)
	}
}

func TestSSHRunnerDescribeChangesReportsSSHTransportFailure(t *testing.T) {
	executor := &sshTransportFailureExecutor{
		errMessage: "exedev@uncle-storm.exe.xyz: Permission denied (publickey,keyboard-interactive).",
	}
	runner := SSHRunner{Executor: executor}
	run := testSSHRun()

	changes := runner.DescribeChanges(context.Background(), run)
	if changes.Error == "" {
		t.Fatalf("ssh transport failure should populate Error: %+v", changes)
	}
	if !strings.Contains(changes.Error, "Permission denied") {
		t.Fatalf("Error should preserve underlying ssh error, got %q", changes.Error)
	}
	if changes.Diff != "" {
		t.Fatalf("Diff should be empty when ssh transport fails, got %q", changes.Diff)
	}
	if len(changes.ChangedFiles) != 0 {
		t.Fatalf("ChangedFiles should be empty when ssh transport fails, got %+v", changes.ChangedFiles)
	}
	if changes.Dirty {
		t.Fatalf("Dirty should be false when ssh transport fails: %+v", changes)
	}
	if changes.Status != "" || changes.DiffStat != "" {
		t.Fatalf("Status/DiffStat should be empty when ssh transport fails: %+v", changes)
	}
}

func TestSSHRunnerApplyPatchNormalizesMissingTrailingNewline(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	target := TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}

	if err := runner.ApplyPatch(context.Background(), target, "/repo", "/tmp/run", "diff --git a/main.go b/main.go"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(executor.input, "\n") {
		t.Fatalf("uploaded patch should end with newline: %q", executor.input)
	}
}

func TestRemoteApplyPatchScriptConflictDoesNotDirtyCheckout(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := runTestGit(t, repo, "diff", "--binary", "HEAD", "--", "file.txt")
	runTestGit(t, repo, "checkout", "--", "file.txt")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "commit", "-m", "source")

	patchPath := filepath.Join(t.TempDir(), "base.patch")
	if err := os.WriteFile(patchPath, []byte(patch), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runCommand(ctx, "", "sh", "-lc", remoteApplyPatchScript(repo, patchPath)); err == nil {
		t.Fatal("remote apply script succeeded; want conflict")
	}
	if status := strings.TrimSpace(runTestGit(t, repo, "status", "--porcelain=v1")); status != "" {
		t.Fatalf("source status = %q, want clean after failed remote apply", status)
	}
	if unmerged := strings.TrimSpace(runTestGit(t, repo, "ls-files", "-u")); unmerged != "" {
		t.Fatalf("unmerged index entries = %q, want none", unmerged)
	}
	contents, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "source\n" {
		t.Fatalf("source file contents = %q, want committed source contents", contents)
	}
}

func TestRemoteChangeScriptIncludesUntrackedFilesInPatch(t *testing.T) {
	script := remoteChangeScript(NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker", WorkDir: "/repo"}))
	if !strings.Contains(script, "git ls-files --others --exclude-standard") || !strings.Contains(script, "git diff --no-index --binary") {
		t.Fatalf("remote change script does not append untracked files:\n%s", script)
	}
}

func TestRemoteChangeScriptFiltersPreExistingDirtyGitWorkspace(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	run := NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker", WorkDir: repo})
	run.RunDir = t.TempDir()

	writeTestFile(t, repo, "tracked-dirty.txt", "base\n")
	writeTestFile(t, repo, "tracked-worker.txt", "base\n")
	runTestGit(t, repo, "add", "tracked-dirty.txt", "tracked-worker.txt")
	runTestGit(t, repo, "commit", "-m", "tracked files")

	writeTestFile(t, repo, "tracked-dirty.txt", "base\npreexisting\n")
	writeTestFile(t, repo, "tracked-worker.txt", "base\npreexisting\n")
	writeTestFile(t, repo, "preexisting-untracked.txt", "preexisting\n")

	runRemoteTestScript(t, ctx, repo, remoteBaselineScript(run))

	writeTestFile(t, repo, "file.txt", "base\nworker clean file\n")
	writeTestFile(t, repo, "tracked-worker.txt", "base\npreexisting\nworker\n")
	writeTestFile(t, repo, "worker-added.txt", "worker added\n")

	runRemoteTestScript(t, ctx, repo, remoteChangeScript(run))

	changes := readTestRunFile(t, run.RunDir, "changes.txt")
	diff := readTestRunFile(t, run.RunDir, "diff.patch")
	nameStatus := readTestRunFile(t, run.RunDir, "name-status.z")
	files := parseGitNameStatus(nameStatus)
	assertChangedFiles(t, files, []WorkspaceChangedFile{
		{Path: "file.txt", Status: "modified"},
		{Path: "tracked-worker.txt", Status: "modified"},
		{Path: "worker-added.txt", Status: "added"},
	})

	for _, leaked := range []string{"tracked-dirty.txt", "preexisting-untracked.txt"} {
		if strings.Contains(changes, leaked) {
			t.Fatalf("changes leaked pre-existing dirty file %q:\n%s", leaked, changes)
		}
		if strings.Contains(diff, leaked) {
			t.Fatalf("diff leaked pre-existing dirty file %q:\n%s", leaked, diff)
		}
	}
	for _, captured := range []string{"file.txt", "tracked-worker.txt", "worker-added.txt"} {
		if !strings.Contains(diff, captured) {
			t.Fatalf("diff did not capture worker file %q:\n%s", captured, diff)
		}
	}
	if strings.Contains(diff, "\n+preexisting\n") {
		t.Fatalf("diff captured a pre-existing dirty hunk:\n%s", diff)
	}
	if !strings.Contains(diff, "\n+worker\n") || !strings.Contains(diff, "\n+worker added\n") {
		t.Fatalf("diff missing worker additions:\n%s", diff)
	}
}

func TestRemoteChangeScriptEmitsCumulativePublishDiffForReusedGitWorkspace(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	run := NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker", WorkDir: repo})
	run.RunDir = t.TempDir()

	writeTestFile(t, repo, "tracked.txt", "base\n")
	runTestGit(t, repo, "add", "tracked.txt")
	runTestGit(t, repo, "commit", "-m", "base")

	// Simulate state left behind by a previous worker in a reused workspace:
	// a tracked file is modified and a brand-new untracked file is added.
	// These represent the prior worker's uncommitted contribution.
	writeTestFile(t, repo, "tracked.txt", "base\nprior worker\n")
	writeTestFile(t, repo, "new-file.txt", "prior worker added\n")

	runRemoteTestScript(t, ctx, repo, remoteBaselineScript(run))

	// The follow-up worker modifies only the previously-untracked file.
	writeTestFile(t, repo, "new-file.txt", "prior worker added\nthis worker tweak\n")

	runRemoteTestScript(t, ctx, repo, remoteChangeScript(run))

	diff := readTestRunFile(t, run.RunDir, "diff.patch")
	publishDiff := readTestRunFile(t, run.RunDir, "publish-diff.patch")

	// Per-worker diff isolates only this worker's tweak.
	if !strings.Contains(diff, "new-file.txt") || !strings.Contains(diff, "+this worker tweak") {
		t.Fatalf("per-worker diff missing this worker's tweak:\n%s", diff)
	}
	if strings.Contains(diff, "tracked.txt") {
		t.Fatalf("per-worker diff leaked prior-worker change to tracked.txt:\n%s", diff)
	}

	// Publish diff is cumulative from HEAD: it must include both the prior
	// worker's contribution AND this worker's tweak. Otherwise applying it to
	// the project's base ref would fail with "does not exist in index" for
	// new-file.txt — the original bug behind task e794b639.
	if !strings.Contains(publishDiff, "tracked.txt") {
		t.Fatalf("publish diff missing prior-worker modification to tracked.txt:\n%s", publishDiff)
	}
	if !strings.Contains(publishDiff, "+prior worker") {
		t.Fatalf("publish diff missing prior-worker hunk:\n%s", publishDiff)
	}
	if !strings.Contains(publishDiff, "new-file.txt") {
		t.Fatalf("publish diff missing new-file.txt addition:\n%s", publishDiff)
	}
	if !strings.Contains(publishDiff, "+this worker tweak") {
		t.Fatalf("publish diff missing this worker's tweak:\n%s", publishDiff)
	}
	if strings.Contains(publishDiff, "diff --git a/new-file.txt b/new-file.txt\nindex ") {
		t.Fatalf("publish diff treats new-file.txt as a modification of an existing blob instead of an addition; it will not apply to HEAD:\n%s", publishDiff)
	}
}

func TestRemoteChangeScriptPreservesCleanGitWorkspaceCapture(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	run := NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker", WorkDir: repo})
	run.RunDir = t.TempDir()

	runRemoteTestScript(t, ctx, repo, remoteBaselineScript(run))
	writeTestFile(t, repo, "worker-added.txt", "worker added\n")
	runRemoteTestScript(t, ctx, repo, remoteChangeScript(run))

	changes := readTestRunFile(t, run.RunDir, "changes.txt")
	if !strings.Contains(changes, "?? worker-added.txt") {
		t.Fatalf("clean workspace capture should keep porcelain untracked status, got:\n%s", changes)
	}
	if _, err := os.Stat(filepath.Join(run.RunDir, "name-status.z")); err == nil {
		t.Fatal("clean workspace capture should not use filtered name-status artifact")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	diff := readTestRunFile(t, run.RunDir, "diff.patch")
	if !strings.Contains(diff, "diff --git a/worker-added.txt b/worker-added.txt") {
		t.Fatalf("clean workspace capture missed worker-added file:\n%s", diff)
	}
}

func TestSSHRunnerStartUploadsPromptForStdinCommand(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	run := NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker-stdin", WorkDir: "/repo"})

	if err := runner.Start(context.Background(), run, []string{"codex", "exec", "--json", "-"}, "large prompt"); err != nil {
		t.Fatal(err)
	}
	if executor.input != "large prompt" {
		t.Fatalf("input = %q", executor.input)
	}
	var sawPromptUpload bool
	var sawPromptRedirect bool
	var sawPathBootstrap bool
	var sawCallbackHelper bool
	for _, argv := range executor.commands {
		joined := strings.Join(argv, " ")
		if strings.Contains(joined, "cat >") && strings.Contains(joined, "prompt.txt") {
			sawPromptUpload = true
		}
		if strings.Contains(joined, "<") && strings.Contains(joined, "prompt.txt") {
			sawPromptRedirect = true
		}
		if strings.Contains(joined, ".local/share/mise/shims") {
			sawPathBootstrap = true
		}
		if strings.Contains(joined, "aged-create-task") || strings.Contains(joined, "callback.env") {
			sawCallbackHelper = true
		}
	}
	if !sawPromptUpload || !sawPromptRedirect || !sawPathBootstrap || !sawCallbackHelper {
		t.Fatalf("commands did not upload prompt, install callback helper, redirect stdin, and bootstrap PATH: %+v", executor.commands)
	}
}

func TestRemoteCreateTaskHelperHelp(t *testing.T) {
	helperPath := filepath.Join(t.TempDir(), "aged-create-task")
	if err := os.WriteFile(helperPath, []byte(remoteCreateTaskHelperScript()), 0o700); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(helperPath, "--help")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("aged-create-task --help failed: %v\n%s", err, output)
	}
	help := string(output)
	for _, want := range []string{
		"aged-create-task queues a follow-up task",
		"Usage:",
		"--title TITLE",
		"--project-id ID",
		"AGED_WORKER_CALLBACK_DIR",
		"Reads the full new task prompt from stdin",
		"queued <path>",
		"Exits 2",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("help output missing %q:\n%s", want, help)
		}
	}
}

func TestSSHRunnerProbeParsesTargetHealth(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"checkoutRootOK=true",
		"tmux=true",
		"repoPresent=true",
		"diskAvailableKB=10485760",
		"diskUsedPercent=42%",
		"memoryTotalKB=33554432",
		"memoryAvailableKB=16777216",
		"load1=1.25",
		"cpuCount=8",
	}, "\n")}
	runner := SSHRunner{Executor: executor}
	health, resources := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo"})
	if health.Status != "ok" || !health.Reachable || !health.Tmux || !health.RepoPresent {
		t.Fatalf("health = %+v", health)
	}
	if resources.CPUCount != 8 || resources.MemoryAvailableMB != 16384 || resources.DiskAvailableMB != 10240 || resources.DiskUsedPercent != 42 {
		t.Fatalf("resources = %+v", resources)
	}
}

func TestSSHRunnerProbeAllowsMissingRepoForPreparation(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"checkoutRootOK=true",
		"tmux=true",
		"repoPresent=false",
		"cpuCount=4",
		"load1=0.1",
	}, "\n")}
	runner := SSHRunner{Executor: executor}
	health, _ := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo"})
	if health.Status != "ok" || !strings.Contains(health.Error, "prepared") {
		t.Fatalf("health = %+v", health)
	}
}

func TestSSHRunnerDirectoryExistsReturnsFalseForMissingDirectory(t *testing.T) {
	executor := &fakeRemoteExecutor{directoryErr: exitCodeError{code: 1}}
	runner := SSHRunner{Executor: executor}

	ok, err := runner.DirectoryExists(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, "/missing")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if ok {
		t.Fatal("ok = true, want false")
	}
}

func TestSSHRunnerDirectoryExistsPreservesInfrastructureFailure(t *testing.T) {
	executor := &fakeRemoteExecutor{
		directoryOutput: "ssh: connect to host vm port 22: Connection refused\n",
		directoryErr:    exitCodeError{code: 255},
	}
	runner := SSHRunner{Executor: executor}

	ok, err := runner.DirectoryExists(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, "/repo")
	if err == nil {
		t.Fatal("err = nil, want infrastructure error")
	}
	if ok {
		t.Fatal("ok = true, want false")
	}
	if !strings.Contains(err.Error(), "exit status 255") || !strings.Contains(err.Error(), "Connection refused") {
		t.Fatalf("err = %v, want exit status and SSH detail", err)
	}
}

func TestSSHRunnerPrepareCheckoutClonesAndChecksOutBase(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	if _, err := runner.PrepareCheckout(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, RemoteCheckoutSpec{
		RepoURL:     "https://github.com/nathanwhit/aged.git",
		WorkDir:     "/srv/aged/repos/aged",
		DefaultBase: "main",
		BaseRef:     "abc123",
	}); err != nil {
		t.Fatal(err)
	}
	if len(executor.commands) == 0 {
		t.Fatal("missing prepare command")
	}
	joined := strings.Join(executor.commands[0], " ")
	for _, want := range []string{"git clone", "git fetch origin --prune", `git cat-file -e "$base_ref^{commit}"`, `git checkout --detach "$base_ref"`, "origin/$base"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("prepare command missing %q:\n%s", want, joined)
		}
	}
}

func TestSSHRunnerPrepareCheckoutStashesDirtyExistingGitCheckout(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	if _, err := runner.PrepareCheckout(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, RemoteCheckoutSpec{
		RepoURL:     "https://github.com/nathanwhit/aged.git",
		WorkDir:     "/srv/aged/repos/aged",
		DefaultBase: "main",
	}); err != nil {
		t.Fatal(err)
	}
	if len(executor.commands) == 0 {
		t.Fatal("missing prepare command")
	}
	joined := strings.Join(executor.commands[0], " ")
	for _, want := range []string{
		"git stash push --include-untracked",
		"stashed dirty remote checkout",
		`[ -n "$base" ]`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("prepare command missing %q:\n%s", want, joined)
		}
	}
	for _, blocked := range []string{"remote checkout is dirty", "exit 20", `[ -z "${dirty:-}" ]`} {
		if strings.Contains(joined, blocked) {
			t.Fatalf("prepare command still rejects dirty checkout with %q:\n%s", blocked, joined)
		}
	}
}

func TestNewRemoteRunUsesSpecWorkDirWhenTargetOmitsWorkDir(t *testing.T) {
	run := NewRemoteRun(TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm"}, testSSHSpec())
	if run.WorkDir != "/repo" {
		t.Fatalf("remote workDir = %q, want /repo", run.WorkDir)
	}
}

func TestServiceRunsWorkerOnRealSSHTarget(t *testing.T) {
	host := os.Getenv("AGED_SSH_SMOKE_HOST")
	if host == "" {
		t.Skip("set AGED_SSH_SMOKE_HOST to run real SSH target smoke")
	}
	port, _ := strconv.Atoi(os.Getenv("AGED_SSH_SMOKE_PORT"))
	user := os.Getenv("AGED_SSH_SMOKE_USER")
	identityFile := os.Getenv("AGED_SSH_SMOKE_IDENTITY_FILE")
	workDir := os.Getenv("AGED_SSH_SMOKE_WORKDIR")
	if workDir == "" {
		workDir = "/repo"
	}
	workRoot := os.Getenv("AGED_SSH_SMOKE_WORKROOT")
	if workRoot == "" {
		workRoot = "/runs"
	}

	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, t.TempDir()+"/aged.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:                    "ssh-smoke",
		Kind:                  TargetKindSSH,
		Host:                  host,
		Port:                  port,
		User:                  user,
		IdentityFile:          identityFile,
		InsecureIgnoreHostKey: true,
		WorkDir:               workDir,
		WorkRoot:              workRoot,
		Labels:                map[string]string{"role": "remote"},
		Capacity:              TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote",
		Prompt:     "run remote ssh smoke",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{
		"remote": buildOnlyRunner{kind: "remote", command: []string{"sh", "-lc", "printf 'remote smoke\\n'"}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{PollInterval: 100 * time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "SSH smoke", Prompt: "Run on container over SSH."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "ssh-smoke" {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "remote smoke") {
		t.Fatalf("missing remote smoke output")
	}
}

func TestServiceFallsBackToLocalWhenRemoteCheckoutIsDirty(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-dirty",
		Kind:     TargetKindSSH,
		Host:     "vm-dirty",
		WorkDir:  "/home/exedev/deno",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100},
	}})
	executor := &fakeRemoteExecutor{
		prepareOutput: "remote checkout is dirty: /home/exedev/deno",
		prepareErr:    errors.New("exit status 20"),
	}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run work",
		Metadata: map[string]any{
			"workerSize": "large",
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Fallback", Prompt: "Run with remote fallback."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	node := snapshot.ExecutionNodes[0]
	if node.TargetID != "local" || node.TargetKind != "local" {
		t.Fatalf("node = %+v, want local fallback", node)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "fallbackFromTargetID", "vm-dirty") {
		t.Fatalf("missing fallback metadata")
	}
}

func TestRecoverRemoteWorkersReservesTargetUntilCompletion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-recover-remote"
	workerID := "worker-recover-remote"
	remoteTarget := testSSHTarget()
	remoteTarget.Labels = map[string]string{"role": "remote"}
	remoteTarget.Capacity = TargetCapacity{MaxWorkers: 1, CPUWeight: 100}
	targets := NewTargetRegistry([]TargetConfig{
		remoteTarget,
		{
			ID:       "local",
			Kind:     TargetKindLocal,
			Labels:   map[string]string{"location": "local"},
			Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
		},
	})
	executor := &gatedRemoteStatusExecutor{
		release:       make(chan struct{}),
		statusStarted: make(chan struct{}, 1),
	}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	appendRecoverableRemoteWorker(t, ctx, store, taskID, workerID)
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.statusStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("recovered remote worker did not start polling")
	}
	if running := targetRunning(targets, "vm-1"); running != 1 {
		t.Fatalf("running count while recovered worker active = %d, want 1", running)
	}
	if err := service.DeleteTarget(ctx, "vm-1"); err == nil || !strings.Contains(err.Error(), "running workers") {
		t.Fatalf("DeleteTarget while recovered worker active error = %v, want running workers", err)
	}
	if _, err := targets.Select(Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
		Metadata:   map[string]any{"targetLabels": map[string]any{"role": "remote"}},
	}); err == nil {
		t.Fatal("Select chose a saturated recovered-worker target, want no available remote target")
	}

	close(executor.release)
	waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	waitForTargetRunning(t, targets, "vm-1", 0)
	if err := service.DeleteTarget(ctx, "vm-1"); err != nil {
		t.Fatalf("DeleteTarget after recovered worker completed = %v", err)
	}
}

func TestServiceRemoteWorkerFailsBeforePrepareWhenSSHTargetMissingCheckoutRoot(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run work",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, NewLocalTargetRegistry(), SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	_, err := service.runSSHPlannedWorker(ctx, core.Task{ID: "task", Title: "Task"}, Plan{
		WorkerKind: "mock",
		Prompt:     "run work",
		Metadata:   map[string]any{},
	}, eventRunner{kind: "mock"}, TargetConfig{ID: "vm-missing-workdir", Kind: TargetKindSSH, Host: "vm"})
	if err == nil || !strings.Contains(err.Error(), "remote checkoutRoot is required") {
		t.Fatalf("err = %v, want missing checkoutRoot", err)
	}
	for _, argv := range executor.commands {
		if strings.Contains(strings.Join(argv, " "), "git clone") {
			t.Fatalf("remote prepare should not run without a remote workDir: %+v", executor.commands)
		}
	}
}

type fakeRemoteExecutor struct {
	commands        [][]string
	probeOutput     string
	prepareOutput   string
	prepareErr      error
	directoryOutput string
	directoryErr    error
	callbackOutput  string
	input           string
}

type gatedRemoteStatusExecutor struct {
	release       chan struct{}
	statusStarted chan struct{}
}

func (e *fakeRemoteExecutor) Run(_ context.Context, argv []string) (string, error) {
	e.commands = append(e.commands, append([]string(nil), argv...))
	joined := strings.Join(argv, " ")
	switch {
	case e.prepareErr != nil && strings.Contains(joined, "git clone"):
		return e.prepareOutput, e.prepareErr
	case e.callbackOutput != "" && strings.Contains(joined, "AGED-CALLBACK-FILE"):
		return e.callbackOutput, nil
	case strings.Contains(joined, "repoPresent="):
		if e.probeOutput != "" {
			return e.probeOutput, nil
		}
		return "checkoutRootOK=true\ntmux=true\nrepoPresent=true\ncpuCount=4\nload1=0.1\n", nil
	case strings.Contains(joined, "test -d"):
		return e.directoryOutput, e.directoryErr
	case strings.Contains(joined, "stdout.log"):
		return "remote output\n", nil
	case strings.Contains(joined, "stderr.log"):
		return "", nil
	case strings.Contains(joined, "status.json"):
		return `{"status":"succeeded","exit":0}`, nil
	case strings.Contains(joined, "vcs.txt"):
		return "git\n", nil
	case strings.Contains(joined, "root.txt"):
		return "/repo\n", nil
	case strings.Contains(joined, "changes.txt"):
		return " M main.go\n", nil
	case strings.Contains(joined, "diffstat.txt"):
		return " main.go | 2 +-\n", nil
	case strings.Contains(joined, "diff.patch"):
		return "diff --git a/main.go b/main.go\n", nil
	default:
		return "", nil
	}
}

type exitCodeError struct {
	code int
}

func (e exitCodeError) Error() string {
	return "exit status " + strconv.Itoa(e.code)
}

func (e exitCodeError) ExitCode() int {
	return e.code
}

func (e *gatedRemoteStatusExecutor) Run(_ context.Context, argv []string) (string, error) {
	joined := strings.Join(argv, " ")
	switch {
	case strings.Contains(joined, "stdout.log"), strings.Contains(joined, "stderr.log"):
		return "", nil
	case strings.Contains(joined, "status.json"):
		select {
		case e.statusStarted <- struct{}{}:
		default:
		}
		select {
		case <-e.release:
			return `{"status":"succeeded","exit":0}`, nil
		default:
			return `{"status":"running"}`, nil
		}
	case strings.Contains(joined, "vcs.txt"):
		return "git\n", nil
	case strings.Contains(joined, "root.txt"):
		return "/repo\n", nil
	case strings.Contains(joined, "changes.txt"):
		return " M main.go\n", nil
	case strings.Contains(joined, "diffstat.txt"):
		return " main.go | 2 +-\n", nil
	case strings.Contains(joined, "diff.patch"):
		return "diff --git a/main.go b/main.go\n", nil
	default:
		return "", nil
	}
}

type scriptedPollExecutor struct {
	commands       [][]string
	stdout         []string
	stderr         []string
	status         []string
	poll           int
	sessionMissing bool
	stdoutReads    []remoteLogReadRecord
	stderrReads    []remoteLogReadRecord
}

type remoteLogReadRecord struct {
	Offset int
	Start  int
	Size   int
	Bytes  int
}

func (e *scriptedPollExecutor) Run(_ context.Context, argv []string) (string, error) {
	e.commands = append(e.commands, append([]string(nil), argv...))
	joined := strings.Join(argv, " ")
	index := e.poll
	if index >= len(e.status) {
		index = len(e.status) - 1
	}
	switch {
	case strings.Contains(joined, "stdout.log"):
		return e.remoteLogChunk("stdout", valueAt(e.stdout, index), joined), nil
	case strings.Contains(joined, "stderr.log"):
		return e.remoteLogChunk("stderr", valueAt(e.stderr, index), joined), nil
	case strings.Contains(joined, "status.json"):
		out := valueAt(e.status, index)
		if e.poll < len(e.status)-1 {
			e.poll++
		}
		return out, nil
	case strings.Contains(joined, "tmux has-session"):
		if e.sessionMissing {
			return "", exitCodeError{code: 1}
		}
		return "", nil
	default:
		return "", nil
	}
}

func (e *scriptedPollExecutor) remoteLogChunk(stream string, content string, command string) string {
	offset := scriptedLogOffset(command)
	start := offset
	if start > len(content) {
		start = 0
	}
	record := remoteLogReadRecord{
		Offset: offset,
		Start:  start,
		Size:   len(content),
		Bytes:  len(content) - start,
	}
	if stream == "stdout" {
		e.stdoutReads = append(e.stdoutReads, record)
	} else {
		e.stderrReads = append(e.stderrReads, record)
	}
	return fmt.Sprintf("%s %d %d\n%s", remoteLogChunkHeader, start, len(content), content[start:])
}

func scriptedLogOffset(command string) int {
	const prefix = "aged_log_offset="
	index := strings.Index(command, prefix)
	if index < 0 {
		return 0
	}
	value := command[index+len(prefix):]
	end := strings.IndexFunc(value, func(r rune) bool {
		return r < '0' || r > '9'
	})
	if end >= 0 {
		value = value[:end]
	}
	offset, _ := strconv.Atoi(value)
	return offset
}

func equalRemoteLogReadRecords(got []remoteLogReadRecord, want []remoteLogReadRecord) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func (e *scriptedPollExecutor) commandContains(pattern string) bool {
	for _, command := range e.commands {
		if strings.Contains(strings.Join(command, " "), pattern) {
			return true
		}
	}
	return false
}

func valueAt(values []string, index int) string {
	if len(values) == 0 {
		return ""
	}
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func writeTestFile(t *testing.T, repo string, name string, content string) {
	t.Helper()
	path := filepath.Join(repo, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runRemoteTestScript(t *testing.T, ctx context.Context, repo string, script string) {
	t.Helper()
	if _, err := runCommand(ctx, repo, "sh", "-lc", script); err != nil {
		t.Fatalf("remote script failed: %v\n%s", err, script)
	}
}

func readTestRunFile(t *testing.T, runDir string, name string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(runDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

type timeoutThenStatusExecutor struct {
	statusCalls int
}

func (e *timeoutThenStatusExecutor) Run(ctx context.Context, argv []string) (string, error) {
	joined := strings.Join(argv, " ")
	switch {
	case strings.Contains(joined, "stdout.log"), strings.Contains(joined, "stderr.log"):
		return "", nil
	case strings.Contains(joined, "status.json"):
		e.statusCalls++
		if e.statusCalls == 1 {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return `{"status":"succeeded","exit":0}`, nil
	default:
		return "", nil
	}
}

type timeoutDiffPatchExecutor struct {
	diffPatchCalls int
}

func (e *timeoutDiffPatchExecutor) Run(ctx context.Context, argv []string) (string, error) {
	joined := strings.Join(argv, " ")
	switch {
	case strings.Contains(joined, "vcs.txt"):
		return "git\n", nil
	case strings.Contains(joined, "root.txt"):
		return "/repo\n", nil
	case strings.Contains(joined, "changes.txt"):
		return " M main.go\n", nil
	case strings.Contains(joined, "diffstat.txt"):
		return " main.go | 2 +-\n", nil
	case strings.Contains(joined, "publish-diff.patch"):
		return "", nil
	case strings.Contains(joined, "diff.patch"):
		e.diffPatchCalls++
		<-ctx.Done()
		return "", ctx.Err()
	case strings.Contains(joined, "stdout.log"), strings.Contains(joined, "stderr.log"):
		return "", nil
	default:
		return "", nil
	}
}

func (e *fakeRemoteExecutor) RunInput(_ context.Context, argv []string, input string) (string, error) {
	e.commands = append(e.commands, append([]string(nil), argv...))
	joined := strings.Join(argv, " ")
	if strings.Contains(joined, "prompt.txt") || strings.Contains(joined, "base.patch") {
		e.input = input
	}
	return "", nil
}

type sshTransportFailureExecutor struct {
	errMessage string
}

func (e *sshTransportFailureExecutor) Run(_ context.Context, _ []string) (string, error) {
	return e.errMessage + "\n", errors.New("exit status 255")
}

func (e *sshTransportFailureExecutor) RunInput(_ context.Context, _ []string, _ string) (string, error) {
	return e.errMessage + "\n", errors.New("exit status 255")
}

func appendRecoverableRemoteWorker(t *testing.T, ctx context.Context, store eventstore.Store, taskID string, workerID string) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "run remotely"}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
			"metadata": map[string]any{
				"nodeID": "node-remote",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForTargetRunning(t *testing.T, targets *TargetRegistry, targetID string, running int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if targetRunning(targets, targetID) == running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("target %s running count = %d, want %d", targetID, targetRunning(targets, targetID), running)
}

func targetRunning(targets *TargetRegistry, targetID string) int {
	for _, target := range targets.Snapshot() {
		if target.ID == targetID {
			return target.Running
		}
	}
	return 0
}

type recordingWorkerSink struct {
	events []worker.Event
}

func (s *recordingWorkerSink) Event(_ context.Context, event worker.Event) error {
	s.events = append(s.events, event)
	return nil
}

func (s *recordingWorkerSink) has(kind worker.EventKind, stream string, text string) bool {
	for _, event := range s.events {
		if event.Kind == kind && event.Stream == stream && event.Text == text {
			return true
		}
	}
	return false
}

func (s *recordingWorkerSink) hasText(kind worker.EventKind, stream string, text string) bool {
	for _, event := range s.events {
		if event.Kind == kind && event.Stream == stream && strings.Contains(event.Text, text) {
			return true
		}
	}
	return false
}

func (s *recordingWorkerSink) count(kind worker.EventKind, stream string, text string) int {
	count := 0
	for _, event := range s.events {
		if event.Kind == kind && event.Stream == stream && event.Text == text {
			count++
		}
	}
	return count
}

func eventContains(events []core.Event, eventType core.EventType, text string) bool {
	for _, event := range events {
		if event.Type == eventType && strings.Contains(string(event.Payload), text) {
			return true
		}
	}
	return false
}
