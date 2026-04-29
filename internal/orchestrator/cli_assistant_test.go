package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"aged/internal/core"
)

func TestCLIAssistantUsesCodexJSONOutput(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
printf '%s\n' '{"type":"item.completed","item":{"id":"msg","type":"agent_message","text":"codex answer"}}'
`)
	assistant, err := NewCLIAssistant(CLIAssistantConfig{
		Kind:      "codex",
		CodexPath: script,
		WorkDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := assistant.Ask(context.Background(), core.AssistantRequest{ConversationID: "c1", Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != "codex answer" {
		t.Fatalf("message = %q", response.Message)
	}
}

func TestCLIAssistantUsesClaudeStreamResult(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"draft answer"}]}}' '{"type":"result","subtype":"success","result":"claude answer"}'
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
}

func writeExecutable(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "assistant")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}
