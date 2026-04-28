package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

type Spec struct {
	ID      string
	TaskID  string
	Kind    string
	Prompt  string
	WorkDir string
	Command []string
}

type Sink interface {
	Event(ctx context.Context, event Event) error
}

type EventKind string

const (
	EventLog        EventKind = "log"
	EventResult     EventKind = "result"
	EventError      EventKind = "error"
	EventNeedsInput EventKind = "needs_input"
)

type Event struct {
	Kind   EventKind       `json:"kind"`
	Stream string          `json:"stream,omitempty"`
	Text   string          `json:"text,omitempty"`
	Raw    json.RawMessage `json:"raw,omitempty"`
}

func LogEvent(stream string, text string) Event {
	kind := EventLog
	if stream == "stderr" {
		kind = EventError
	}
	return Event{Kind: kind, Stream: stream, Text: text}
}

type Runner interface {
	Kind() string
	BuildCommand(spec Spec) []string
	Run(ctx context.Context, spec Spec, sink Sink) error
}

type MockRunner struct{}

func (MockRunner) Kind() string {
	return "mock"
}

func (MockRunner) BuildCommand(Spec) []string {
	return nil
}

func (MockRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	if err := sink.Event(ctx, LogEvent("stdout", "mock worker received task")); err != nil {
		return err
	}
	if err := sink.Event(ctx, LogEvent("stdout", spec.Prompt)); err != nil {
		return err
	}
	return nil
}

type CommandRunner struct {
	kind    string
	command func(Spec) []string
}

func NewCommandRunner(kind string, command func(Spec) []string) CommandRunner {
	return CommandRunner{kind: kind, command: command}
}

func (r CommandRunner) Kind() string {
	return r.kind
}

func (r CommandRunner) BuildCommand(spec Spec) []string {
	return r.command(spec)
}

func (r CommandRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	argv := r.command(spec)
	if len(argv) == 0 {
		return errors.New("empty worker command")
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if spec.WorkDir != "" {
		cmd.Dir = spec.WorkDir
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	errCh := make(chan error, 2)
	parser := ParserForKind(r.kind)
	go streamLines(ctx, sink, parser, "stdout", stdout, errCh)
	go streamLines(ctx, sink, parser, "stderr", stderr, errCh)

	for i := 0; i < 2; i++ {
		if streamErr := <-errCh; streamErr != nil {
			_ = cmd.Wait()
			return streamErr
		}
	}
	waitErr := cmd.Wait()
	if waitErr != nil {
		return fmt.Errorf("worker command failed: %w", waitErr)
	}
	return nil
}

func streamLines(ctx context.Context, sink Sink, parser Parser, stream string, reader io.Reader, errCh chan<- error) {
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		event := parser.ParseLine(stream, scanner.Text())
		if err := sink.Event(ctx, event); err != nil {
			errCh <- err
			return
		}
	}
	errCh <- scanner.Err()
}

func DefaultRunners() map[string]Runner {
	runners := []Runner{
		MockRunner{},
		NewCommandRunner("codex", func(spec Spec) []string {
			return []string{"codex", "exec", "--json", "--cd", spec.WorkDir, spec.Prompt}
		}),
		NewCommandRunner("claude", func(spec Spec) []string {
			return []string{"claude", "--print", "--output-format", "stream-json", spec.Prompt}
		}),
		NewCommandRunner("shell", func(spec Spec) []string {
			return spec.Command
		}),
	}

	out := map[string]Runner{}
	for _, runner := range runners {
		out[runner.Kind()] = runner
	}
	return out
}

type Parser interface {
	ParseLine(stream string, line string) Event
}

type parserFunc func(stream string, line string) Event

func (f parserFunc) ParseLine(stream string, line string) Event {
	return f(stream, line)
}

func ParserForKind(kind string) Parser {
	switch kind {
	case "codex", "claude":
		return parserFunc(parseJSONWorkerLine)
	default:
		return parserFunc(func(stream string, line string) Event {
			return LogEvent(stream, line)
		})
	}
}

func parseJSONWorkerLine(stream string, line string) Event {
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		return LogEvent(stream, line)
	}

	event := Event{
		Kind:   classifyPayload(stream, payload),
		Stream: stream,
		Text:   extractText(payload),
		Raw:    json.RawMessage(line),
	}
	if event.Text == "" {
		event.Text = line
	}
	return event
}

func classifyPayload(stream string, payload map[string]any) EventKind {
	if stream == "stderr" {
		return EventError
	}
	value := strings.ToLower(strings.Join([]string{
		stringField(payload, "type"),
		stringField(payload, "event"),
		stringField(payload, "subtype"),
		stringField(payload, "status"),
	}, " "))
	if item, ok := payload["item"].(map[string]any); ok {
		value += " " + strings.ToLower(strings.Join([]string{
			stringField(item, "type"),
			stringField(item, "status"),
		}, " "))
	}

	switch {
	case strings.Contains(value, "error"), strings.Contains(value, "failed"):
		return EventError
	case strings.Contains(value, "approval"), strings.Contains(value, "input"), strings.Contains(value, "question"):
		return EventNeedsInput
	case strings.Contains(value, "result"), strings.Contains(value, "final"), strings.Contains(value, "complete"), strings.Contains(value, "completed"), strings.Contains(value, "success"):
		return EventResult
	default:
		return EventLog
	}
}

func extractText(payload map[string]any) string {
	for _, key := range []string{"text", "message", "content", "delta", "summary", "result", "error"} {
		if text := stringField(payload, key); text != "" {
			return text
		}
	}
	if message, ok := payload["message"].(map[string]any); ok {
		for _, key := range []string{"content", "text"} {
			if text := stringField(message, key); text != "" {
				return text
			}
		}
		if content, ok := message["content"].([]any); ok {
			var parts []string
			for _, part := range content {
				partMap, ok := part.(map[string]any)
				if !ok {
					continue
				}
				if text := stringField(partMap, "text"); text != "" {
					parts = append(parts, text)
				}
			}
			if len(parts) > 0 {
				return strings.Join(parts, "")
			}
		}
	}
	if item, ok := payload["item"].(map[string]any); ok {
		for _, key := range []string{"text", "message", "content"} {
			if text := stringField(item, key); text != "" {
				return text
			}
		}
	}
	return ""
}

func stringField(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}
