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
	"syscall"
)

const codexYoloFlag = "--dangerously-bypass-approvals-and-sandbox"

type Spec struct {
	ID              string
	TaskID          string
	Kind            string
	Prompt          string
	WorkDir         string
	Command         []string
	ResumeSessionID string
	ReasoningEffort string
	Steering        <-chan string
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

type SteeringSupport interface {
	SupportsSteering() bool
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
	kind              string
	command           func(Spec) []string
	steeringFormatter func(string) string
}

func NewCommandRunner(kind string, command func(Spec) []string) CommandRunner {
	return CommandRunner{kind: kind, command: command}
}

func NewSteerableCommandRunner(kind string, command func(Spec) []string, steeringFormatter func(string) string) CommandRunner {
	if steeringFormatter == nil {
		steeringFormatter = defaultSteeringFormatter
	}
	return CommandRunner{kind: kind, command: command, steeringFormatter: steeringFormatter}
}

func (r CommandRunner) Kind() string {
	return r.kind
}

func (r CommandRunner) SupportsSteering() bool {
	return r.steeringFormatter != nil
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	var stdin io.WriteCloser
	promptOnStdin := CommandUsesPromptStdin(argv)
	if promptOnStdin || (r.SupportsSteering() && spec.Steering != nil) {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return err
		}
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go killProcessGroupOnCancel(ctx, cmd, done)

	errCh := make(chan error, 2)
	parser := parserFunc(parseJSONWorkerLine)
	go streamLines(ctx, sink, parser, "stdout", stdout, errCh)
	go streamLines(ctx, sink, parser, "stderr", stderr, errCh)
	if promptOnStdin && stdin != nil {
		go func() {
			_, _ = io.WriteString(stdin, spec.Prompt)
			_ = stdin.Close()
		}()
	} else if stdin != nil {
		go forwardSteering(ctx, sink, spec.Steering, stdin, r.steeringFormatter)
	}

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

func CommandUsesPromptStdin(argv []string) bool {
	return len(argv) > 0 && argv[len(argv)-1] == "-"
}

func forwardSteering(ctx context.Context, sink Sink, steering <-chan string, stdin io.WriteCloser, formatter func(string) string) {
	defer stdin.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-steering:
			if !ok {
				return
			}
			message = strings.TrimSpace(message)
			if message == "" {
				continue
			}
			if _, err := fmt.Fprintln(stdin, formatter(message)); err != nil {
				_ = sink.Event(ctx, Event{Kind: EventError, Stream: "stdin", Text: "failed to deliver steering: " + err.Error()})
				return
			}
			_ = sink.Event(ctx, Event{Kind: EventLog, Stream: "stdin", Text: "delivered steering to worker stdin"})
		}
	}
}

func defaultSteeringFormatter(message string) string {
	return "\n[orchestrator steering]\n" + message + "\n[/orchestrator steering]"
}

type PluginRunner struct {
	kind    string
	command []string
}

func NewPluginRunner(kind string, command []string) PluginRunner {
	return PluginRunner{kind: strings.TrimSpace(kind), command: append([]string{}, command...)}
}

func (r PluginRunner) Kind() string {
	return r.kind
}

func (r PluginRunner) BuildCommand(Spec) []string {
	return append(append([]string{}, r.command...), "run")
}

func (r PluginRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	argv := r.BuildCommand(spec)
	if len(argv) == 0 {
		return errors.New("empty plugin runner command")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if spec.WorkDir != "" {
		cmd.Dir = spec.WorkDir
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
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
	done := make(chan struct{})
	defer close(done)
	go killProcessGroupOnCancel(ctx, cmd, done)
	payload := struct {
		ID              string   `json:"id"`
		TaskID          string   `json:"taskId"`
		Kind            string   `json:"kind"`
		Prompt          string   `json:"prompt"`
		WorkDir         string   `json:"workDir,omitempty"`
		Command         []string `json:"command,omitempty"`
		ResumeSessionID string   `json:"resumeSessionId,omitempty"`
		ReasoningEffort string   `json:"reasoningEffort,omitempty"`
	}{
		ID:              spec.ID,
		TaskID:          spec.TaskID,
		Kind:            spec.Kind,
		Prompt:          spec.Prompt,
		WorkDir:         spec.WorkDir,
		Command:         spec.Command,
		ResumeSessionID: spec.ResumeSessionID,
		ReasoningEffort: spec.ReasoningEffort,
	}
	if err := json.NewEncoder(stdin).Encode(payload); err != nil {
		_ = stdin.Close()
		return err
	}
	_ = stdin.Close()
	errCh := make(chan error, 2)
	parser := ParserForKind(r.kind)
	go streamLines(ctx, sink, parser, "stdout", stdout, errCh)
	go streamLines(ctx, sink, parser, "stderr", stderr, errCh)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			_ = cmd.Wait()
			return err
		}
	}
	return cmd.Wait()
}

func killProcessGroupOnCancel(ctx context.Context, cmd *exec.Cmd, done <-chan struct{}) {
	select {
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	case <-done:
	}
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
		BenchmarkCompareRunner{},
		NewCommandRunner("codex", func(spec Spec) []string {
			if strings.TrimSpace(spec.ResumeSessionID) != "" {
				args := []string{"codex", "exec", "resume", codexYoloFlag, "--json"}
				if effort := CodexReasoningEffort(spec.ReasoningEffort); effort != "" {
					args = append(args, "-c", "model_reasoning_effort=\""+effort+"\"")
				}
				return append(args, strings.TrimSpace(spec.ResumeSessionID), "-")
			}
			args := []string{"codex", "exec", codexYoloFlag, "--json", "--cd", spec.WorkDir}
			if effort := CodexReasoningEffort(spec.ReasoningEffort); effort != "" {
				args = append(args, "-c", "model_reasoning_effort=\""+effort+"\"")
			}
			return append(args, "-")
		}),
		NewCommandRunner("claude", func(spec Spec) []string {
			args := []string{"claude", "--print", "--output-format", "stream-json", "--verbose"}
			if strings.TrimSpace(spec.ResumeSessionID) != "" {
				args = append(args, "--resume", strings.TrimSpace(spec.ResumeSessionID))
			}
			if effort := ReasoningEffort(spec.ReasoningEffort); effort != "" {
				args = append(args, "--effort", effort)
			}
			return append(args, spec.Prompt)
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

func ReasoningEffort(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "low", "medium", "high", "xhigh", "max":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func CodexReasoningEffort(value string) string {
	effort := ReasoningEffort(value)
	if effort == "max" {
		return "xhigh"
	}
	return effort
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
	case "codex":
		return parserFunc(parseCodexWorkerLine)
	case "claude":
		return parserFunc(parseJSONWorkerLine)
	default:
		return parserFunc(func(stream string, line string) Event {
			return LogEvent(stream, line)
		})
	}
}

func parseCodexWorkerLine(stream string, line string) Event {
	event := parseJSONWorkerLine(stream, line)
	if stream == "stderr" && strings.Contains(line, "failed to record rollout items: thread") {
		event.Kind = EventLog
	}
	return event
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
	if stringField(payload, "type") == "turn.completed" {
		return EventLog
	}
	if item, ok := payload["item"].(map[string]any); ok {
		itemType := stringField(item, "type")
		switch itemType {
		case "agent_message":
			return EventResult
		case "command_execution":
			if exitCode, ok := item["exit_code"].(float64); ok && exitCode != 0 {
				return EventError
			}
			if stringField(item, "status") == "failed" {
				return EventError
			}
			return EventLog
		case "file_change":
			return EventLog
		}
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
