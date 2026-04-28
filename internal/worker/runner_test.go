package worker

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

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
