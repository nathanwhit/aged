package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
)

const codexYoloFlag = "--dangerously-bypass-approvals-and-sandbox"
const claudeSkipPermissionsFlag = "--dangerously-skip-permissions"
const outputLineReadBufferBytes = 64 * 1024
const maxStructuredOutputLineBytes = 16 * 1024 * 1024

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

type RemoteStdinProvider interface {
	RemoteStdin(spec Spec) (string, error)
}

type Capabilities struct {
	LiveSteering  bool
	PromptStdin   bool
	ResumeSession bool
}

type CapabilityProvider interface {
	Capabilities() Capabilities
}

type SteeringSupport interface {
	SupportsSteering() bool
}

func RunnerCapabilities(runner Runner) Capabilities {
	if runner == nil {
		return Capabilities{}
	}
	if provider, ok := runner.(CapabilityProvider); ok {
		return provider.Capabilities()
	}
	capabilities := Capabilities{}
	if support, ok := runner.(SteeringSupport); ok {
		capabilities.LiveSteering = support.SupportsSteering()
	}
	return capabilities
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
	capabilities      Capabilities
	promptOnStdin     bool
}

func NewCommandRunner(kind string, command func(Spec) []string) CommandRunner {
	return CommandRunner{kind: kind, command: command}
}

func NewCommandRunnerWithCapabilities(kind string, capabilities Capabilities, command func(Spec) []string) CommandRunner {
	return CommandRunner{kind: kind, command: command, capabilities: capabilities}
}

func NewPromptStdinCommandRunnerWithCapabilities(kind string, capabilities Capabilities, command func(Spec) []string) CommandRunner {
	capabilities.PromptStdin = true
	return CommandRunner{kind: kind, command: command, capabilities: capabilities, promptOnStdin: true}
}

func NewSteerableCommandRunner(kind string, command func(Spec) []string, steeringFormatter func(string) string) CommandRunner {
	if steeringFormatter == nil {
		steeringFormatter = defaultSteeringFormatter
	}
	return CommandRunner{kind: kind, command: command, steeringFormatter: steeringFormatter, capabilities: Capabilities{LiveSteering: true}}
}

func (r CommandRunner) Kind() string {
	return r.kind
}

func (r CommandRunner) SupportsSteering() bool {
	return r.Capabilities().LiveSteering
}

func (r CommandRunner) Capabilities() Capabilities {
	capabilities := r.capabilities
	if r.steeringFormatter != nil {
		capabilities.LiveSteering = true
	}
	return capabilities
}

func (r CommandRunner) BuildCommand(spec Spec) []string {
	return r.command(spec)
}

func (r CommandRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	argv := r.command(spec)
	if len(argv) == 0 {
		return errors.New("empty worker command")
	}

	cmd := newWorkerCommand(ctx, argv, spec.WorkDir)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	var stdin io.WriteCloser
	promptOnStdin := r.promptOnStdin || CommandUsesPromptStdin(argv)
	if promptOnStdin || (r.SupportsSteering() && spec.Steering != nil) {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return err
		}
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	defer killOnCancel(ctx, cmd)()

	var parser Parser = parserFunc(parseJSONWorkerLine)
	if r.kind == "codex" || r.kind == "claude" {
		parser = ParserForKind(r.kind)
	}
	if promptOnStdin && stdin != nil {
		go func() {
			_, _ = io.WriteString(stdin, spec.Prompt)
			_ = stdin.Close()
		}()
	} else if stdin != nil {
		go forwardSteering(ctx, sink, spec.Steering, stdin, r.steeringFormatter)
	}

	if err := waitCommandOutput(ctx, sink, parser, cmd, stdout, stderr); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("worker command failed: %w", err)
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

type pluginRunnerPayload struct {
	ID              string   `json:"id"`
	TaskID          string   `json:"taskId"`
	Kind            string   `json:"kind"`
	Prompt          string   `json:"prompt"`
	WorkDir         string   `json:"workDir,omitempty"`
	Command         []string `json:"command,omitempty"`
	ResumeSessionID string   `json:"resumeSessionId,omitempty"`
	ReasoningEffort string   `json:"reasoningEffort,omitempty"`
}

func PluginRunnerStdin(spec Spec) (string, error) {
	payload := pluginRunnerPayload{
		ID:              spec.ID,
		TaskID:          spec.TaskID,
		Kind:            spec.Kind,
		Prompt:          spec.Prompt,
		WorkDir:         spec.WorkDir,
		Command:         spec.Command,
		ResumeSessionID: spec.ResumeSessionID,
		ReasoningEffort: spec.ReasoningEffort,
	}
	var out bytes.Buffer
	if err := json.NewEncoder(&out).Encode(payload); err != nil {
		return "", err
	}
	return out.String(), nil
}

func (r PluginRunner) RemoteStdin(spec Spec) (string, error) {
	return PluginRunnerStdin(spec)
}

func (r PluginRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	argv := r.BuildCommand(spec)
	if len(argv) == 0 {
		return errors.New("empty plugin runner command")
	}
	cmd := newWorkerCommand(ctx, argv, spec.WorkDir)
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
	defer killOnCancel(ctx, cmd)()
	payload, err := r.RemoteStdin(spec)
	if err != nil {
		_ = stdin.Close()
		return err
	}
	if _, err := io.WriteString(stdin, payload); err != nil {
		_ = stdin.Close()
		return err
	}
	_ = stdin.Close()
	if err := waitCommandOutput(ctx, sink, ParserForKind(r.kind), cmd, stdout, stderr); err != nil {
		return err
	}
	return cmd.Wait()
}

func newWorkerCommand(ctx context.Context, argv []string, workDir string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if workDir != "" {
		cmd.Dir = workDir
	}
	return cmd
}

func killOnCancel(ctx context.Context, cmd *exec.Cmd) func() {
	done := make(chan struct{})
	go killProcessGroupOnCancel(ctx, cmd, done)
	return func() { close(done) }
}

func waitCommandOutput(ctx context.Context, sink Sink, parser Parser, cmd *exec.Cmd, stdout io.Reader, stderr io.Reader) error {
	errCh := make(chan error, 2)
	go streamLines(ctx, sink, parser, "stdout", stdout, errCh)
	go streamLines(ctx, sink, parser, "stderr", stderr, errCh)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			_ = cmd.Wait()
			return err
		}
	}
	return nil
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
	filter := NewOutputFilter(parser)
	if err := StreamReaderLines(ctx, stream, reader, func(line string) error {
		return filter.EmitLine(ctx, sink, stream, line)
	}, func(stream string, discarded int) error {
		return sink.Event(ctx, Event{
			Kind:   EventError,
			Stream: stream,
			Text:   fmt.Sprintf("discarded oversized JSON event line from %s after %d bytes; maximum supported line length is %d bytes", stream, discarded, maxStructuredOutputLineBytes),
		})
	}); err != nil {
		errCh <- err
		return
	}
	errCh <- filter.Flush(ctx, sink)
}

// StreamReaderLines emits newline-delimited output without bufio.Scanner's token cap.
func StreamReaderLines(ctx context.Context, stream string, reader io.Reader, emit func(string) error, oversizedJSON func(string, int) error) error {
	bufReader := bufio.NewReaderSize(reader, outputLineReadBufferBytes)
	var line []byte
	discardingJSON := false
	discarded := 0
	chunkingText := false
	textChunkIndex := 0
	textLineBytes := 0
	for {
		fragment, isPrefix, err := bufReader.ReadLine()
		if len(fragment) > 0 {
			if discardingJSON {
				discarded += len(fragment)
			} else if chunkingText {
				if emitErr := emitOversizedTextBytes(ctx, emit, fragment, &textChunkIndex, &textLineBytes); emitErr != nil {
					return emitErr
				}
			} else {
				line = append(line, fragment...)
				if len(line) > maxStructuredOutputLineBytes && lineLooksLikeJSON(line) {
					discardingJSON = true
					discarded = len(line)
					line = nil
				} else if len(line) > maxStructuredOutputLineBytes {
					chunkingText = true
					if emitErr := emitOversizedTextBytes(ctx, emit, line, &textChunkIndex, &textLineBytes); emitErr != nil {
						return emitErr
					}
					line = nil
				}
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if !isPrefix {
			if discardingJSON {
				if oversizedJSON != nil {
					if emitErr := oversizedJSON(stream, discarded); emitErr != nil {
						return emitErr
					}
				} else if emitErr := emit(fmt.Sprintf("[discarded oversized JSON-looking line from %s after %d bytes; maximum supported line length is %d bytes]", stream, discarded, maxStructuredOutputLineBytes)); emitErr != nil {
					return emitErr
				}
				discardingJSON = false
				discarded = 0
			} else if chunkingText {
				chunkingText = false
				textChunkIndex = 0
				textLineBytes = 0
			} else if len(line) > 0 {
				if emitErr := emit(string(line)); emitErr != nil {
					return emitErr
				}
				line = nil
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func emitOversizedTextBytes(ctx context.Context, emit func(string) error, data []byte, chunkIndex *int, lineBytes *int) error {
	const chunkBytes = 256 * 1024
	for len(data) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		size := chunkBytes
		if size > len(data) {
			size = len(data)
		}
		start := *lineBytes
		*lineBytes += size
		*chunkIndex = *chunkIndex + 1
		text := fmt.Sprintf("[oversized log line chunk %d, bytes %d-%d+] %s", *chunkIndex, start, *lineBytes, string(data[:size]))
		if err := emit(text); err != nil {
			return err
		}
		data = data[size:]
	}
	return nil
}

func lineLooksLikeJSON(line []byte) bool {
	for _, b := range line {
		switch b {
		case ' ', '\t', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

func DefaultRunners() map[string]Runner {
	runners := []Runner{
		MockRunner{},
		BenchmarkCompareRunner{},
		NewPromptStdinCommandRunnerWithCapabilities("codex", Capabilities{ResumeSession: true}, func(spec Spec) []string {
			resumeSessionID := strings.TrimSpace(spec.ResumeSessionID)
			effortArgs := []string{}
			if effort := CodexReasoningEffort(spec.ReasoningEffort); effort != "" {
				effortArgs = []string{"-c", "model_reasoning_effort=\"" + effort + "\""}
			}
			if resumeSessionID != "" {
				args := []string{"codex", "exec"}
				if strings.TrimSpace(spec.WorkDir) != "" {
					args = append(args, "--cd", spec.WorkDir)
				}
				args = append(args, "resume", codexYoloFlag, "--json")
				return append(append(args, effortArgs...), resumeSessionID, "-")
			}
			args := []string{"codex", "exec", codexYoloFlag, "--json", "--cd", spec.WorkDir}
			return append(append(args, effortArgs...), "-")
		}),
		NewPromptStdinCommandRunnerWithCapabilities("claude", Capabilities{ResumeSession: true}, func(spec Spec) []string {
			args := []string{"claude", "--print", "--output-format", "stream-json", "--verbose"}
			args = appendArgIfMissing(args, claudeSkipPermissionsFlag)
			if strings.TrimSpace(spec.ResumeSessionID) != "" {
				args = append(args, "--resume", strings.TrimSpace(spec.ResumeSessionID))
			}
			if effort := ReasoningEffort(spec.ReasoningEffort); effort != "" {
				args = append(args, "--effort", effort)
			}
			return args
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

func appendArgIfMissing(args []string, arg string) []string {
	for _, existing := range args {
		if existing == arg {
			return args
		}
	}
	return append(args, arg)
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

type OutputFilter struct {
	parser     Parser
	mu         sync.Mutex
	suppressed map[string]int
}

func NewOutputFilter(parser Parser) *OutputFilter {
	return &OutputFilter{parser: parser}
}

func (f *OutputFilter) EmitLine(ctx context.Context, sink Sink, stream string, line string) error {
	event := f.parser.ParseLine(stream, line)
	if key, ok := infrastructureWarningKey(stream, line, event); ok {
		f.mu.Lock()
		if f.suppressed == nil {
			f.suppressed = map[string]int{}
		}
		if _, seen := f.suppressed[key]; seen {
			f.suppressed[key]++
			f.mu.Unlock()
			return nil
		}
		f.suppressed[key] = 0
		f.mu.Unlock()
	}
	return sink.Event(ctx, event)
}

func (f *OutputFilter) Flush(ctx context.Context, sink Sink) error {
	f.mu.Lock()
	var events []Event
	var parts []string
	total := 0
	for key, count := range f.suppressed {
		if count > 0 {
			total += count
			parts = append(parts, fmt.Sprintf("%s (%d)", key, count))
		}
	}
	f.suppressed = nil
	f.mu.Unlock()
	if total > 0 {
		sort.Strings(parts)
		events = append(events, Event{
			Kind:   EventLog,
			Stream: "stderr",
			Text:   fmt.Sprintf("suppressed %d repeated Codex infrastructure warnings: %s", total, strings.Join(parts, "; ")),
		})
	}
	for _, event := range events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func infrastructureWarningKey(stream string, line string, event Event) (string, bool) {
	if stream != "stderr" || event.Kind != EventLog {
		return "", false
	}
	if category, ok := knownCodexInfrastructureWarning(stream, line); ok {
		return category.label, true
	}
	return "", false
}

type codexInfrastructureWarning struct {
	label string
}

func knownCodexInfrastructureWarning(stream string, line string) (codexInfrastructureWarning, bool) {
	if stream != "stderr" {
		return codexInfrastructureWarning{}, false
	}
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(line, "codex_core_skills::loader") && strings.Contains(lower, "icon"):
		return codexInfrastructureWarning{label: "codex_core_skills::loader icon path warning"}, true
	case strings.Contains(line, "codex_core_plugins::manifest") && strings.Contains(line, "defaultPrompt"):
		return codexInfrastructureWarning{label: "codex_core_plugins::manifest defaultPrompt warning"}, true
	case strings.Contains(line, "codex_app_server::in_process") && (strings.Contains(lower, "queue full") || strings.Contains(lower, "queue-full")):
		return codexInfrastructureWarning{label: "codex_app_server::in_process queue-full warning"}, true
	case strings.Contains(line, "failed to record rollout items: thread") && strings.Contains(line, "codex_core::session"):
		return codexInfrastructureWarning{label: "failed to record Codex rollout items"}, true
	default:
		return codexInfrastructureWarning{}, false
	}
}

type parserFunc func(stream string, line string) Event

func (f parserFunc) ParseLine(stream string, line string) Event {
	return f(stream, line)
}

func ParserForKind(kind string) Parser {
	switch kind {
	case "codex":
		return codexWorkerParser{}
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
	if _, ok := knownCodexInfrastructureWarning(stream, line); ok {
		event.Kind = EventLog
	}
	return event
}

type codexWorkerParser struct{}

func (codexWorkerParser) ParseLine(stream string, line string) Event {
	return parseCodexWorkerLine(stream, line)
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
