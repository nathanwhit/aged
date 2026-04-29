package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

type CLIAssistantConfig struct {
	Kind       string
	CodexPath  string
	ClaudePath string
	WorkDir    string
	Timeout    time.Duration
}

type CLIAssistant struct {
	kind       string
	codexPath  string
	claudePath string
	workDir    string
	timeout    time.Duration
}

func NewCLIAssistant(config CLIAssistantConfig) (*CLIAssistant, error) {
	kind := strings.TrimSpace(config.Kind)
	if kind == "" || kind == "none" {
		return nil, nil
	}
	if kind != "codex" && kind != "claude" {
		return nil, fmt.Errorf("assistant must be one of none, codex, or claude")
	}
	codexPath := strings.TrimSpace(config.CodexPath)
	if codexPath == "" {
		codexPath = "codex"
	}
	claudePath := strings.TrimSpace(config.ClaudePath)
	if claudePath == "" {
		claudePath = "claude"
	}
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return &CLIAssistant{
		kind:       kind,
		codexPath:  codexPath,
		claudePath: claudePath,
		workDir:    config.WorkDir,
		timeout:    timeout,
	}, nil
}

func (a *CLIAssistant) Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	if a == nil {
		return core.AssistantResponse{}, errors.New("assistant is not configured")
	}
	prompt := interactiveAssistantPrompt(req)
	runCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	switch a.kind {
	case "codex":
		return a.askCodex(runCtx, req, prompt)
	case "claude":
		return a.askClaude(runCtx, req, prompt)
	default:
		return core.AssistantResponse{}, fmt.Errorf("unsupported assistant %q", a.kind)
	}
}

func (a *CLIAssistant) askCodex(ctx context.Context, req core.AssistantRequest, prompt string) (core.AssistantResponse, error) {
	workDir := nonEmpty(strings.TrimSpace(req.WorkDir), a.workDir)
	args := []string{"exec", "--sandbox", "read-only", "--json", "--cd", workDir, prompt}
	if strings.TrimSpace(req.ProviderSessionID) != "" {
		args = []string{"exec", "resume", "--json", req.ProviderSessionID, prompt}
	}
	cmd := exec.CommandContext(ctx, a.codexPath, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return core.AssistantResponse{}, fmt.Errorf("codex assistant command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	content, sessionID, err := extractCodexAssistantOutput(stdout.Bytes())
	if err != nil {
		return core.AssistantResponse{}, err
	}
	sessionID = nonEmpty(sessionID, req.ProviderSessionID)
	return core.AssistantResponse{
		ConversationID:    req.ConversationID,
		Message:           strings.TrimSpace(content),
		Provider:          "codex",
		ProviderSessionID: sessionID,
		Metadata: core.MustJSON(map[string]any{
			"assistant":         "codex",
			"providerSessionId": sessionID,
			"resumed":           req.ProviderSessionID != "",
		}),
	}, nil
}

func (a *CLIAssistant) askClaude(ctx context.Context, req core.AssistantRequest, prompt string) (core.AssistantResponse, error) {
	workDir := nonEmpty(strings.TrimSpace(req.WorkDir), a.workDir)
	args := []string{"--print", "--output-format", "stream-json", prompt}
	if strings.TrimSpace(req.ProviderSessionID) != "" {
		args = []string{"--resume", req.ProviderSessionID, "--print", "--output-format", "stream-json", prompt}
	}
	cmd := exec.CommandContext(ctx, a.claudePath, args...)
	cmd.Dir = workDir
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return core.AssistantResponse{}, fmt.Errorf("claude assistant command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	output := stdout.String()
	message := extractLastParsedResult("claude", output)
	if message == "" {
		message = strings.TrimSpace(output)
	}
	sessionID := nonEmpty(extractClaudeSessionID(output), req.ProviderSessionID)
	return core.AssistantResponse{
		ConversationID:    req.ConversationID,
		Message:           message,
		Provider:          "claude",
		ProviderSessionID: sessionID,
		Metadata: core.MustJSON(map[string]any{
			"assistant":         "claude",
			"providerSessionId": sessionID,
			"resumed":           req.ProviderSessionID != "",
		}),
	}, nil
}

func interactiveAssistantPrompt(req core.AssistantRequest) string {
	var builder strings.Builder
	builder.WriteString("You are the interactive assistant for aged, a local autonomous development orchestrator.\n")
	builder.WriteString("Answer the user directly and concisely. You may inspect files in the current project checkout to answer questions, but do not edit files or run mutating commands. If the request should become long-running code work, say what task should be started. Do not invent execution results.\n\n")
	if strings.TrimSpace(req.WorkDir) != "" {
		builder.WriteString("Current read-only project checkout:\n")
		builder.WriteString(req.WorkDir)
		builder.WriteString("\n\n")
	}
	if len(req.Context) > 0 {
		builder.WriteString("Context JSON:\n")
		builder.Write(req.Context)
		builder.WriteString("\n\n")
	}
	builder.WriteString("User message:\n")
	builder.WriteString(req.Message)
	return builder.String()
}

func extractLastParsedResult(kind string, output string) string {
	parser := worker.ParserForKind(kind)
	scanner := bufio.NewScanner(strings.NewReader(output))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	var last string
	for scanner.Scan() {
		event := parser.ParseLine("stdout", scanner.Text())
		if strings.TrimSpace(event.Text) == "" {
			continue
		}
		if event.Kind == worker.EventResult {
			last = event.Text
		} else if last == "" && event.Kind == worker.EventLog {
			last = event.Text
		}
	}
	return strings.TrimSpace(last)
}

func extractCodexAssistantOutput(data []byte) (string, string, error) {
	content, err := extractCodexAgentMessage(data)
	if err != nil {
		return "", "", err
	}
	return content, extractCodexThreadID(data), nil
}

func extractCodexThreadID(data []byte) string {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var payload map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &payload); err != nil {
			continue
		}
		if payload["type"] == "thread.started" {
			return stringMetadataValue(payload["thread_id"])
		}
	}
	return ""
}

func extractClaudeSessionID(output string) string {
	scanner := bufio.NewScanner(strings.NewReader(output))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var payload map[string]any
		if err := json.Unmarshal([]byte(scanner.Text()), &payload); err != nil {
			continue
		}
		if payload["type"] == "system" && payload["subtype"] == "init" {
			return stringMetadataValue(payload["session_id"])
		}
	}
	return ""
}
