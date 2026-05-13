package worker

import (
	"context"
	"encoding/json"
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
		Command: []string{"/bin/sh", "-c", "printf '%02000000d\\n' 0"},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("events = %d", len(sink.events))
	}
	if got := len(sink.events[0].Text); got != 2000000 {
		t.Fatalf("large line length = %d", got)
	}
}

func TestStreamLinesHandlesLargeJSONEvent(t *testing.T) {
	text := strings.Repeat("x", 2*1024*1024)
	line := `{"type":"item.completed","item":{"id":"msg","type":"agent_message","text":"` + text + `"}}`
	sink := &recordingSink{}
	errCh := make(chan error, 1)

	streamLines(context.Background(), sink, ParserForKind("codex"), "stdout", strings.NewReader(line+"\n"), errCh)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("events = %d, want 1", len(sink.events))
	}
	if event := sink.events[0]; event.Kind != EventResult || event.Stream != "stdout" || event.Text != text || len(event.Raw) != len(line) {
		t.Fatalf("event = kind %q stream %q text len %d raw len %d", event.Kind, event.Stream, len(event.Text), len(event.Raw))
	}
}

func TestStreamLinesChunksOversizedTextLine(t *testing.T) {
	line := strings.Repeat("z", maxStructuredOutputLineBytes+1024)
	sink := &recordingSink{}
	errCh := make(chan error, 1)

	streamLines(context.Background(), sink, ParserForKind("mock"), "stdout", strings.NewReader(line+"\n"), errCh)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if len(sink.events) < 2 {
		t.Fatalf("events = %d, want chunked oversized line", len(sink.events))
	}
	if !strings.Contains(sink.events[0].Text, "oversized log line chunk 1") {
		t.Fatalf("first chunk = %q", sink.events[0].Text[:min(len(sink.events[0].Text), 80)])
	}
	if !strings.Contains(sink.events[len(sink.events)-1].Text, "bytes 16777216-16778240+] ") {
		t.Fatalf("last chunk missing explicit byte range: %q", sink.events[len(sink.events)-1].Text[:min(len(sink.events[len(sink.events)-1].Text), 120)])
	}
}

func TestStreamLinesReportsOversizedJSONEventLine(t *testing.T) {
	line := `{"type":"item.completed","item":{"type":"agent_message","text":"` + strings.Repeat("x", maxStructuredOutputLineBytes) + `"}}`
	sink := &recordingSink{}
	errCh := make(chan error, 1)

	streamLines(context.Background(), sink, ParserForKind("codex"), "stdout", strings.NewReader(line+"\nnext log\n"), errCh)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 2 {
		t.Fatalf("events = %d, want oversized report and following log", len(sink.events))
	}
	if event := sink.events[0]; event.Kind != EventError || !strings.Contains(event.Text, "discarded oversized JSON event line from stdout") {
		t.Fatalf("oversized event = %+v", event)
	}
	if event := sink.events[1]; event.Text != "next log" {
		t.Fatalf("following log was poisoned: %+v", event)
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

func TestDefaultRunnerCapabilities(t *testing.T) {
	runners := DefaultRunners()
	if got := RunnerCapabilities(runners["codex"]); !got.ResumeSession || !got.PromptStdin || got.LiveSteering {
		t.Fatalf("codex capabilities = %+v, want resume without live steering", got)
	}
	if got := RunnerCapabilities(runners["claude"]); !got.ResumeSession || !got.PromptStdin || got.LiveSteering {
		t.Fatalf("claude capabilities = %+v, want resume without live steering", got)
	}
	if got := RunnerCapabilities(runners["shell"]); got.ResumeSession || got.PromptStdin || got.LiveSteering {
		t.Fatalf("shell capabilities = %+v, want no lifecycle extras", got)
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

func TestPromptStdinCommandRunnerWritesPromptWithoutDashArgument(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "stdin.txt")
	runner := NewPromptStdinCommandRunnerWithCapabilities("claude", Capabilities{ResumeSession: true}, func(Spec) []string {
		return []string{"/bin/sh", "-c", "cat > " + shellQuoteTest(outPath)}
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

func TestPluginRunnerStdinSerializesRunnerSpec(t *testing.T) {
	payload, err := PluginRunnerStdin(Spec{
		ID:              "worker-1",
		TaskID:          "task-1",
		Kind:            "review-plugin",
		Prompt:          "do the work",
		WorkDir:         "/repo",
		Command:         []string{"custom", "args"},
		ResumeSessionID: "session-1",
		ReasoningEffort: "high",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(payload, "\n") {
		t.Fatalf("payload should use encoder newline: %q", payload)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"id":              "worker-1",
		"taskId":          "task-1",
		"kind":            "review-plugin",
		"prompt":          "do the work",
		"workDir":         "/repo",
		"resumeSessionId": "session-1",
		"reasoningEffort": "high",
	} {
		if got[key] != want {
			t.Fatalf("%s = %v, want %q in %s", key, got[key], want, payload)
		}
	}
	if command, ok := got["command"].([]any); !ok || len(command) != 2 || command[0] != "custom" || command[1] != "args" {
		t.Fatalf("command = %#v", got["command"])
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
	want := []string{"codex", "exec", "--cd", "/tmp/aged-work", "resume", "--dangerously-bypass-approvals-and-sandbox", "--json", "thread-1", "-"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestDefaultClaudeRunnerUsesEffortFlag(t *testing.T) {
	runner := DefaultRunners()["claude"]
	got := runner.BuildCommand(Spec{Prompt: "review this", ReasoningEffort: "xhigh"})
	want := []string{"claude", "--print", "--output-format", "stream-json", "--verbose", "--dangerously-skip-permissions", "--effort", "xhigh"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
	if countArgs(got, "--dangerously-skip-permissions") != 1 {
		t.Fatalf("skip permissions flag count = %d in %#v", countArgs(got, "--dangerously-skip-permissions"), got)
	}
}

func TestDefaultClaudeRunnerResumesSession(t *testing.T) {
	runner := DefaultRunners()["claude"]
	got := runner.BuildCommand(Spec{Prompt: "continue", ResumeSessionID: "session-1"})
	want := []string{"claude", "--print", "--output-format", "stream-json", "--verbose", "--dangerously-skip-permissions", "--resume", "session-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
	if countArgs(got, "--dangerously-skip-permissions") != 1 {
		t.Fatalf("skip permissions flag count = %d in %#v", countArgs(got, "--dangerously-skip-permissions"), got)
	}
}

func TestAppendArgIfMissingDoesNotDuplicateClaudeSkipPermissions(t *testing.T) {
	args := appendArgIfMissing([]string{"claude", "--dangerously-skip-permissions"}, "--dangerously-skip-permissions")
	want := []string{"claude", "--dangerously-skip-permissions"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
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

func TestCodexOutputFilterSummarizesRepeatedInfrastructureWarnings(t *testing.T) {
	filter := NewOutputFilter(ParserForKind("codex"))

	lines := []string{
		"2026-05-08T10:00:00.000000Z WARN codex_core_skills::loader: icon path /missing/icon.png does not exist",
		"2026-05-08T10:00:01.000000Z WARN codex_core_skills::loader: icon path /missing/icon.png does not exist",
		"2026-05-08T10:00:02.000000Z WARN codex_core_skills::loader: icon path /missing/icon.png does not exist",
		"2026-05-08T10:00:03.000000Z WARN codex_core_plugins::manifest: defaultPrompt is deprecated",
		"2026-05-08T10:00:04.000000Z WARN codex_core_plugins::manifest: defaultPrompt is deprecated",
		"2026-05-08T10:00:05.000000Z WARN codex_app_server::in_process: queue full; dropping app event",
		"2026-05-08T10:00:06.000000Z WARN codex_app_server::in_process: queue full; dropping app event",
		"actual codex failure",
	}

	var events []Event
	sink := recordingSink{}
	for _, line := range lines {
		if err := filter.EmitLine(context.Background(), &sink, "stderr", line); err != nil {
			t.Fatal(err)
		}
	}
	if err := filter.EmitLine(context.Background(), &sink, "stdout", `{"type":"item.completed","item":{"type":"agent_message","text":"done"}}`); err != nil {
		t.Fatal(err)
	}
	if err := filter.Flush(context.Background(), &sink); err != nil {
		t.Fatal(err)
	}
	events = sink.events

	if len(events) != 6 {
		t.Fatalf("events = %d, want 6: %+v", len(events), events)
	}
	if events[0].Kind != EventLog || !strings.Contains(events[0].Text, "codex_core_skills::loader") {
		t.Fatalf("first warning event = %+v", events[0])
	}
	if events[3].Kind != EventError || events[3].Text != "actual codex failure" {
		t.Fatalf("real stderr event = %+v", events[3])
	}
	if events[4].Kind != EventResult || events[4].Text != "done" {
		t.Fatalf("result event = %+v", events[4])
	}
	summary := events[5]
	if summary.Kind != EventLog || !strings.Contains(summary.Text, "suppressed 4 repeated Codex infrastructure warnings") {
		t.Fatalf("summary event = %+v", summary)
	}
	for _, want := range []string{
		"codex_core_skills::loader icon path warning (2)",
		"codex_core_plugins::manifest defaultPrompt warning (1)",
		"codex_app_server::in_process queue-full warning (1)",
	} {
		if !strings.Contains(summary.Text, want) {
			t.Fatalf("summary %q missing %q", summary.Text, want)
		}
	}
}

func TestCodexOutputFilterLeavesUnknownStderrErrorsUnsuppressed(t *testing.T) {
	filter := NewOutputFilter(ParserForKind("codex"))

	var events []Event
	sink := recordingSink{}
	for i := 0; i < 3; i++ {
		if err := filter.EmitLine(context.Background(), &sink, "stderr", "actual codex failure"); err != nil {
			t.Fatal(err)
		}
	}
	if err := filter.Flush(context.Background(), &sink); err != nil {
		t.Fatal(err)
	}
	events = sink.events

	if len(events) != 3 {
		t.Fatalf("events = %d, want 3: %+v", len(events), events)
	}
	for _, event := range events {
		if event.Kind != EventError || event.Text != "actual codex failure" {
			t.Fatalf("event = %+v, want unsuppressed stderr error", event)
		}
	}
}

func TestBenchmarkCompareRunnerSuccessReports(t *testing.T) {
	tests := []struct {
		name   string
		prompt string
		want   []string
	}{
		{
			name: "improvement",
			prompt: `
command: go test -bench=Parser
baseline: 100
candidate: 112
threshold_percent: 5
higher_is_better: true
`,
			want: []string{"verdict: improved"},
		},
		{
			name: "repeated samples same command",
			prompt: `
baseline_command: go test -bench=Parser
candidate_command: go test -bench=Parser
baseline_samples: 100, 101, 99
candidate_samples: 108, 109, 107
min_samples: 3
threshold_percent: 5
higher_is_better: true
`,
			want: []string{"sample_count: 3", "verdict: improved"},
		},
		{
			name: "scientific notation scalars",
			prompt: `
command: go test -bench=Parser
baseline: 1e6
candidate: 1.25e+06
threshold_percent: 20
higher_is_better: true
`,
			want: []string{
				"baseline: 1e+06",
				"candidate: 1.25e+06",
				"delta_percent: 25",
				"verdict: improved",
			},
		},
		{
			name: "scientific notation samples",
			prompt: `
baseline_command: go test -bench=Parser
candidate_command: go test -bench=Parser
baseline_samples: 9.5E-3, +1.0e-2, .0105e+0
candidate_samples: 1.14e-2, 1.20E-02, +1.26e-2
min_samples: 3
threshold_percent: 15
higher_is_better: true
`,
			want: []string{
				"baseline: 0.01",
				"candidate: 0.012",
				"baseline_samples: 0.0095, 0.01, 0.0105",
				"candidate_samples: 0.0114, 0.012, 0.0126",
				"sample_count: 3",
				"delta_percent: 20",
				"verdict: improved",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := runBenchmarkCompare(t, tt.prompt)
			for _, want := range tt.want {
				if !strings.Contains(report, want) {
					t.Fatalf("report missing %q:\n%s", want, report)
				}
			}
		})
	}
}

func runBenchmarkCompare(t *testing.T, prompt string) string {
	t.Helper()
	sink := &recordingSink{}
	err := BenchmarkCompareRunner{}.Run(context.Background(), Spec{Prompt: prompt}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 || sink.events[0].Kind != EventResult {
		t.Fatalf("events = %+v", sink.events)
	}
	return sink.events[0].Text
}

func TestBenchmarkCompareNumberParserSupportsFloatNotation(t *testing.T) {
	got := numbers("-1e6, +1.25e+06, 9.5E-3, .75e1, 10.")
	want := []float64{-1e6, 1.25e6, 9.5e-3, 7.5, 10}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("numbers() = %#v, want %#v", got, want)
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

func countArgs(args []string, arg string) int {
	count := 0
	for _, existing := range args {
		if existing == arg {
			count++
		}
	}
	return count
}
