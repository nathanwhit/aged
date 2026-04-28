package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"aged/internal/core"
)

func TestCodexBrainPlansFromAgentMessage(t *testing.T) {
	brain := newTestCodexBrain(t, "valid", nil)
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Implement task",
		Prompt: "Make the scheduler use Codex.",
	}, []string{"keep it small"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerKind != "codex" {
		t.Fatalf("WorkerKind = %q", plan.WorkerKind)
	}
	if plan.Prompt != "Implement the requested scheduler change." {
		t.Fatalf("Prompt = %q", plan.Prompt)
	}
	if plan.Metadata["brain"] != "codex" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
}

func TestCodexBrainFallsBackOnInvalidPlan(t *testing.T) {
	brain := newTestCodexBrain(t, "invalid", StaticBrain{WorkerKind: "mock"})
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Implement task",
		Prompt: "Make the scheduler use Codex.",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerKind != "mock" {
		t.Fatalf("WorkerKind = %q", plan.WorkerKind)
	}
	if plan.Metadata["brain"] != "codex-fallback" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
	if plan.Metadata["fallbackReason"] == "" {
		t.Fatalf("missing fallback reason")
	}
}

func TestDecodeCodexPlanAcceptsStringLists(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`{
		"workerKind": "mock",
		"workerPrompt": "Run a smoke test.",
		"rationale": "The request asks for scheduler validation.",
		"steps": ["Run mock worker"],
		"requiredApprovals": ["Confirm external upload"],
		"spawns": ["reviewer"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Steps[0] != (PlanStep{Title: "Run mock worker", Description: "Run mock worker"}) {
		t.Fatalf("steps = %+v", plan.Steps)
	}
	if plan.RequiredApprovals[0] != (ApprovalRequest{Title: "Confirm external upload", Reason: "Confirm external upload"}) {
		t.Fatalf("approvals = %+v", plan.RequiredApprovals)
	}
	if plan.Spawns[0] != (SpawnRequest{Role: "reviewer", Reason: "reviewer"}) {
		t.Fatalf("spawns = %+v", plan.Spawns)
	}
}

func TestDecodeCodexPlanAcceptsObjectLists(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`{
		"workerKind": "mock",
		"workerPrompt": "Run a smoke test.",
		"rationale": "The request asks for scheduler validation.",
		"steps": [{"title": "Run", "description": "Run mock worker"}],
		"requiredApprovals": [{"title": "Approval", "reason": "Needed"}],
		"spawns": [{"role": "reviewer", "reason": "Check output"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Steps[0] != (PlanStep{Title: "Run", Description: "Run mock worker"}) {
		t.Fatalf("steps = %+v", plan.Steps)
	}
	if plan.RequiredApprovals[0] != (ApprovalRequest{Title: "Approval", Reason: "Needed"}) {
		t.Fatalf("approvals = %+v", plan.RequiredApprovals)
	}
	if plan.Spawns[0] != (SpawnRequest{Role: "reviewer", Reason: "Check output"}) {
		t.Fatalf("spawns = %+v", plan.Spawns)
	}
}

func newTestCodexBrain(t *testing.T, mode string, fallback BrainProvider) *CodexBrain {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "scheduler.md")
	if err := os.WriteFile(path, []byte("schedule the work"), 0o644); err != nil {
		t.Fatal(err)
	}
	codexPath := filepath.Join(dir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nprintf '%s\\n' "+strconv.Quote(testCodexBrainOutput(t, mode))+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	brain, err := NewCodexBrain(CodexBrainConfig{
		CodexPath:    codexPath,
		TemplatePath: path,
		WorkDir:      dir,
		Fallback:     fallback,
	})
	if err != nil {
		t.Fatal(err)
	}
	return brain
}

func testCodexBrainOutput(t *testing.T, mode string) string {
	t.Helper()
	switch mode {
	case "valid":
		plan := Plan{
			WorkerKind: "codex",
			Prompt:     "Implement the requested scheduler change.",
			Rationale:  "The task edits this Go codebase.",
			Steps: []PlanStep{{
				Title:       "Implement",
				Description: "Make the scheduler run through Codex.",
			}},
			RequiredApprovals: []ApprovalRequest{},
			Spawns:            []SpawnRequest{},
		}
		return codexAgentMessageLine(t, plan)
	case "invalid":
		return `{"type":"item.completed","item":{"type":"agent_message","text":"{\"workerKind\":\"codex\"}"}}`
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	return ""
}

func codexAgentMessageLine(t *testing.T, plan Plan) string {
	t.Helper()
	content, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal(map[string]any{
		"type": "item.completed",
		"item": map[string]any{
			"type": "agent_message",
			"text": string(content),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(line)
}
