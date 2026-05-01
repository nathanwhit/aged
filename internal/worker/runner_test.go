package worker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func shellQuoteTest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func TestCommandRunnerStreamsStdoutAndStderr(t *testing.T) {
	runner := NewCommandRunner("shell", func(spec Spec) []string {
		return spec.Command
	})
	sink := &recordingSink{}

	err := runner.Run(context.Background(), Spec{
		Command: []string{"/bin/sh", "-c", "printf 'out\\n'; printf 'err\\n' >&2"},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}

	if !sink.has(EventLog, "stdout", "out") {
		t.Fatalf("missing stdout log event: %+v", sink.events)
	}
	if !sink.has(EventError, "stderr", "err") {
		t.Fatalf("missing stderr error event: %+v", sink.events)
	}
}

func TestCommandRunnerReturnsNonZeroExit(t *testing.T) {
	runner := NewCommandRunner("shell", func(spec Spec) []string {
		return spec.Command
	})

	err := runner.Run(context.Background(), Spec{
		Command: []string{"/bin/sh", "-c", "exit 7"},
	}, &recordingSink{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "worker command failed") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandRunnerCancelsProcess(t *testing.T) {
	runner := NewCommandRunner("shell", func(spec Spec) []string {
		return spec.Command
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := runner.Run(ctx, Spec{
		Command: []string{"/bin/sh", "-c", "sleep 5"},
	}, &recordingSink{})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("cancellation took too long: %s", time.Since(start))
	}
}

func TestCommandRunnerHandlesLargeLines(t *testing.T) {
	runner := NewCommandRunner("shell", func(spec Spec) []string {
		return spec.Command
	})
	sink := &recordingSink{}

	err := runner.Run(context.Background(), Spec{
		Command: []string{"/bin/sh", "-c", "printf '%080000d\\n' 0"},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("events = %d", len(sink.events))
	}
	if got := len(sink.events[0].Text); got != 80000 {
		t.Fatalf("large line length = %d", got)
	}
}

func TestCommandRunnerReturnsCommandNotFound(t *testing.T) {
	runner := NewCommandRunner("shell", func(spec Spec) []string {
		return spec.Command
	})

	err := runner.Run(context.Background(), Spec{
		Command: []string{"definitely-aged-missing-command"},
	}, &recordingSink{})
	if err == nil {
		t.Fatal("expected error")
	}
	var pathErr *exec.Error
	if !errors.As(err, &pathErr) {
		t.Fatalf("error type = %T: %v", err, err)
	}
}

func TestCommandRunnerNormalizesCodexJSONLines(t *testing.T) {
	runner := NewCommandRunner("codex", func(spec Spec) []string {
		return spec.Command
	})
	sink := &recordingSink{}

	err := runner.Run(context.Background(), Spec{
		Command: []string{"/bin/sh", "-c", "printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"thread\"}' '{\"type\":\"item.completed\",\"item\":{\"id\":\"item_0\",\"type\":\"agent_message\",\"text\":\"{\\\"smoke\\\":\\\"codex\\\",\\\"status\\\":\\\"ok\\\"}\"}}' '{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}'"},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if !sink.has(EventResult, "stdout", "{\"smoke\":\"codex\",\"status\":\"ok\"}") {
		t.Fatalf("missing normalized codex result event: %+v", sink.events)
	}
	if len(sink.events[0].Raw) == 0 {
		t.Fatalf("missing raw codex payload")
	}
}

func TestCommandRunnerNormalizesClaudeJSONLines(t *testing.T) {
	runner := NewCommandRunner("claude", func(spec Spec) []string {
		return spec.Command
	})
	sink := &recordingSink{}

	err := runner.Run(context.Background(), Spec{
		Command: []string{"/bin/sh", "-c", "printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"session\"}' '{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"{\\\"smoke\\\":\\\"claude\\\",\\\"status\\\":\\\"ok\\\"}\"}]}}' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"{\\\"smoke\\\":\\\"claude\\\",\\\"status\\\":\\\"ok\\\"}\",\"total_cost_usd\":0.058204}'"},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if !sink.has(EventLog, "stdout", "{\"smoke\":\"claude\",\"status\":\"ok\"}") {
		t.Fatalf("missing normalized claude log event: %+v", sink.events)
	}
	if !sink.has(EventResult, "stdout", "{\"smoke\":\"claude\",\"status\":\"ok\"}") {
		t.Fatalf("missing normalized claude result event: %+v", sink.events)
	}
}

func TestSteerableCommandRunnerForwardsSteeringToStdin(t *testing.T) {
	runner := NewSteerableCommandRunner("codex", func(spec Spec) []string {
		return spec.Command
	}, func(message string) string {
		return "STEER:" + message
	})
	sink := &recordingSink{}
	steering := make(chan string, 1)
	steering <- "adjust course"
	close(steering)

	err := runner.Run(context.Background(), Spec{
		Command:  []string{"/bin/sh", "-c", "IFS= read -r line; printf '%s\\n' \"{\\\"type\\\":\\\"item.completed\\\",\\\"item\\\":{\\\"type\\\":\\\"agent_message\\\",\\\"text\\\":\\\"$line\\\"}}\""},
		Steering: steering,
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if !sink.has(EventResult, "stdout", "STEER:adjust course") {
		t.Fatalf("missing stdin steering result: %+v", sink.events)
	}
	if !sink.has(EventLog, "stdin", "delivered steering to worker stdin") {
		t.Fatalf("missing steering delivery log: %+v", sink.events)
	}
}

func TestDefaultCodexRunnerDoesNotAdvertiseStdinSteering(t *testing.T) {
	runner := DefaultRunners()["codex"]
	if steering, ok := runner.(SteeringSupport); ok && steering.SupportsSteering() {
		t.Fatal("default codex runner must not hold stdin open for steering")
	}
}

func TestDefaultCodexRunnerUsesYoloPermissions(t *testing.T) {
	runner := DefaultRunners()["codex"]
	got := runner.BuildCommand(Spec{WorkDir: "/tmp/aged-work", Prompt: "do the work", ReasoningEffort: "low"})
	want := []string{
		"codex",
		"exec",
		"--dangerously-bypass-approvals-and-sandbox",
		"--json",
		"--cd",
		"/tmp/aged-work",
		"-c",
		"model_reasoning_effort=\"low\"",
		"-",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestCommandRunnerWritesPromptToStdinForDashArgument(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "stdin.txt")
	runner := NewCommandRunner("codex", func(Spec) []string {
		return []string{"/bin/sh", "-c", "cat > " + shellQuoteTest(outPath), "-"}
	})
	err := runner.Run(context.Background(), Spec{Prompt: "large prompt body"}, &recordingSink{})
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "large prompt body" {
		t.Fatalf("stdin = %q", out)
	}
}

func TestDefaultCodexRunnerMapsMaxReasoningEffort(t *testing.T) {
	runner := DefaultRunners()["codex"]
	got := runner.BuildCommand(Spec{WorkDir: "/tmp/aged-work", Prompt: "do the work", ReasoningEffort: "max"})
	if !reflect.DeepEqual(got[len(got)-3:], []string{"-c", "model_reasoning_effort=\"xhigh\"", "-"}) {
		t.Fatalf("command = %#v", got)
	}
}

func TestDefaultCodexRunnerResumesSession(t *testing.T) {
	runner := DefaultRunners()["codex"]
	got := runner.BuildCommand(Spec{WorkDir: "/tmp/aged-work", Prompt: "continue", ResumeSessionID: "thread-1"})
	want := []string{"codex", "exec", "resume", "--dangerously-bypass-approvals-and-sandbox", "--json", "thread-1", "-"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestDefaultClaudeRunnerUsesEffortFlag(t *testing.T) {
	runner := DefaultRunners()["claude"]
	got := runner.BuildCommand(Spec{Prompt: "review this", ReasoningEffort: "xhigh"})
	want := []string{"claude", "--print", "--output-format", "stream-json", "--verbose", "--effort", "xhigh", "review this"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestDefaultClaudeRunnerResumesSession(t *testing.T) {
	runner := DefaultRunners()["claude"]
	got := runner.BuildCommand(Spec{Prompt: "continue", ResumeSessionID: "session-1"})
	want := []string{"claude", "--print", "--output-format", "stream-json", "--verbose", "--resume", "session-1", "continue"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestParserClassifiesNeedsInput(t *testing.T) {
	event := ParserForKind("codex").ParseLine("stdout", `{"type":"approval_request","message":"approve?"}`)
	if event.Kind != EventNeedsInput {
		t.Fatalf("kind = %q", event.Kind)
	}
	if event.Text != "approve?" {
		t.Fatalf("text = %q", event.Text)
	}
}

func TestParserDoesNotTreatCodexTurnCompletionAsResult(t *testing.T) {
	parser := ParserForKind("codex")

	agentMessage := parser.ParseLine("stdout", `{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`)
	if agentMessage.Kind != EventResult {
		t.Fatalf("agent message kind = %q", agentMessage.Kind)
	}
	if agentMessage.Text != "done" {
		t.Fatalf("agent message text = %q", agentMessage.Text)
	}

	turnCompleted := parser.ParseLine("stdout", `{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}`)
	if turnCompleted.Kind != EventLog {
		t.Fatalf("turn completed kind = %q", turnCompleted.Kind)
	}
}

func TestCodexParserDowngradesRolloutRecordError(t *testing.T) {
	parser := ParserForKind("codex")

	event := parser.ParseLine("stderr", "2026-04-30T02:06:16.268038Z ERROR codex_core::session: failed to record rollout items: thread 019ddc1f-f8f0-7da0-a932-a956e7f51071 not found")
	if event.Kind != EventLog {
		t.Fatalf("kind = %q, want %q", event.Kind, EventLog)
	}

	realError := parser.ParseLine("stderr", "actual codex failure")
	if realError.Kind != EventError {
		t.Fatalf("real error kind = %q, want %q", realError.Kind, EventError)
	}
}

func TestBenchmarkCompareRunnerReportsImprovement(t *testing.T) {
	sink := &recordingSink{}
	err := BenchmarkCompareRunner{}.Run(context.Background(), Spec{Prompt: `
command: go test -bench=Parser
baseline: 100
candidate: 112
threshold_percent: 5
higher_is_better: true
`}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 || sink.events[0].Kind != EventResult {
		t.Fatalf("events = %+v", sink.events)
	}
	if !strings.Contains(sink.events[0].Text, "verdict: improved") {
		t.Fatalf("report = %s", sink.events[0].Text)
	}
}

func TestBenchmarkCompareRunnerUsesRepeatedSamplesAndSameCommand(t *testing.T) {
	sink := &recordingSink{}
	err := BenchmarkCompareRunner{}.Run(context.Background(), Spec{Prompt: `
baseline_command: go test -bench=Parser
candidate_command: go test -bench=Parser
baseline_samples: 100, 101, 99
candidate_samples: 108, 109, 107
min_samples: 3
threshold_percent: 5
higher_is_better: true
`}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sink.events[0].Text, "sample_count: 3") || !strings.Contains(sink.events[0].Text, "verdict: improved") {
		t.Fatalf("report = %s", sink.events[0].Text)
	}
}

func TestBenchmarkCompareRunnerRejectsCommandMismatch(t *testing.T) {
	err := BenchmarkCompareRunner{}.Run(context.Background(), Spec{Prompt: `
baseline_command: go test -bench=Parser
candidate_command: go test -bench=Lexer
baseline_samples: 100, 101, 99
candidate_samples: 108, 109, 107
`}, &recordingSink{})
	if err == nil || !strings.Contains(err.Error(), "to match") {
		t.Fatalf("err = %v", err)
	}
}

type recordingSink struct {
	mu     sync.Mutex
	events []Event
}

func (s *recordingSink) Event(_ context.Context, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *recordingSink) has(kind EventKind, stream string, text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		if event.Kind == kind && event.Stream == stream && event.Text == text {
			return true
		}
	}
	return false
}
