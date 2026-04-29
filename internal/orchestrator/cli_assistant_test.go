package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"aged/internal/core"
)

func TestCLIAssistantUsesCodexJSONOutput(t *testing.T) {
	argsPath := filepath.Join(t.TempDir(), "args.txt")
	script := writeExecutable(t, `#!/bin/sh
printf '%s\n' "$*" > `+shellQuoteTest(argsPath)+`
printf '%s\n' '{"type":"thread.started","thread_id":"thread-1"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"msg","type":"agent_message","text":"codex answer"}}'
`)
	projectDir := t.TempDir()
	assistant, err := NewCLIAssistant(CLIAssistantConfig{
		Kind:      "codex",
		CodexPath: script,
		WorkDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := assistant.Ask(context.Background(), core.AssistantRequest{ConversationID: "c1", Message: "hello", WorkDir: projectDir})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != "codex answer" {
		t.Fatalf("message = %q", response.Message)
	}
	if response.ProviderSessionID != "thread-1" {
		t.Fatalf("session = %q, want thread-1", response.ProviderSessionID)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--sandbox read-only") || !strings.Contains(string(args), "--cd "+projectDir) {
		t.Fatalf("args = %s", args)
	}
}

func TestCLIAssistantUsesClaudeStreamResult(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"session-1"}' '{"type":"assistant","message":{"content":[{"type":"text","text":"draft answer"}]}}' '{"type":"result","subtype":"success","result":"claude answer"}'
`)
	assistant, err := NewCLIAssistant(CLIAssistantConfig{
		Kind:       "claude",
		ClaudePath: script,
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := assistant.Ask(context.Background(), core.AssistantRequest{ConversationID: "c1", Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != "claude answer" {
		t.Fatalf("message = %q", response.Message)
	}
	if response.ProviderSessionID != "session-1" {
		t.Fatalf("session = %q, want session-1", response.ProviderSessionID)
	}
}

func TestCLIAssistantResumesCodexSession(t *testing.T) {
	argsPath := filepath.Join(t.TempDir(), "args.txt")
	script := writeExecutable(t, `#!/bin/sh
printf '%s\n' "$*" > `+shellQuoteTest(argsPath)+`
printf '%s\n' '{"type":"item.completed","item":{"id":"msg","type":"agent_message","text":"resumed codex"}}'
`)
	assistant, err := NewCLIAssistant(CLIAssistantConfig{
		Kind:      "codex",
		CodexPath: script,
		WorkDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := assistant.Ask(context.Background(), core.AssistantRequest{ConversationID: "c1", ProviderSessionID: "thread-1", Message: "again"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != "resumed codex" || response.ProviderSessionID != "thread-1" {
		t.Fatalf("response = %+v", response)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "exec resume") || !strings.Contains(string(args), "thread-1") {
		t.Fatalf("args = %s", args)
	}
}

func TestCLIAssistantResumesClaudeSession(t *testing.T) {
	argsPath := filepath.Join(t.TempDir(), "args.txt")
	script := writeExecutable(t, `#!/bin/sh
printf '%s\n' "$*" > `+shellQuoteTest(argsPath)+`
printf '%s\n' '{"type":"result","subtype":"success","result":"resumed claude"}'
`)
	assistant, err := NewCLIAssistant(CLIAssistantConfig{
		Kind:       "claude",
		ClaudePath: script,
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := assistant.Ask(context.Background(), core.AssistantRequest{ConversationID: "c1", ProviderSessionID: "session-1", Message: "again"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != "resumed claude" || response.ProviderSessionID != "session-1" {
		t.Fatalf("response = %+v", response)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--resume session-1") {
		t.Fatalf("args = %s", args)
	}
}

func writeExecutable(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "assistant")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuoteTest(value string) string {
	return strconv.Quote(value)
}
