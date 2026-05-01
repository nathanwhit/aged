package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
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

func TestCodexBrainExecArgsUseYoloPermissions(t *testing.T) {
	brain := &CodexBrain{workDir: "/tmp/aged-work"}
	got := brain.execArgs()
	want := []string{
		"exec",
		"--dangerously-bypass-approvals-and-sandbox",
		"--json",
		"--cd",
		"/tmp/aged-work",
		"-",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestCodexBrainSendsSchedulerPromptOnStdin(t *testing.T) {
	dir := t.TempDir()
	templatePath := filepath.Join(dir, "scheduler.md")
	if err := os.WriteFile(templatePath, []byte("schedule the work"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdinPath := filepath.Join(dir, "stdin.txt")
	codexPath := filepath.Join(dir, "codex")
	script := "#!/bin/sh\n" +
		"cat > " + shellQuoteTest(stdinPath) + "\n" +
		"printf '%s\\n' " + strconv.Quote(testCodexBrainOutput(t, "valid")) + "\n"
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	brain, err := NewCodexBrain(CodexBrainConfig{
		CodexPath:    codexPath,
		TemplatePath: templatePath,
		WorkDir:      dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = brain.Plan(context.Background(), core.Task{
		ID:     "task-stdin",
		Title:  "Stdin",
		Prompt: strings.Repeat("large prompt ", 1000),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stdin), "large prompt large prompt") {
		t.Fatalf("scheduler prompt was not sent on stdin: %.200q", stdin)
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
	if plan.Spawns[0].Role != "reviewer" || plan.Spawns[0].Reason != "reviewer" {
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
		"spawns": [{"id": "review", "role": "reviewer", "reason": "Check output", "workerKind": "claude", "dependsOn": ["test"]}]
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
	if !reflect.DeepEqual(plan.Spawns[0], SpawnRequest{ID: "review", Role: "reviewer", Reason: "Check output", WorkerKind: "claude", DependsOn: []string{"test"}}) {
		t.Fatalf("spawns = %+v", plan.Spawns)
	}
}

func TestDecodeReplanDecisionContinue(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "continue",
		"rationale": "review found a missing case",
		"message": "run an incorporation worker",
		"plan": {
			"workerKind": "codex",
			"workerPrompt": "incorporate review feedback",
			"rationale": "review found a missing case",
			"steps": [{"title": "Fix", "description": "Patch the missing case"}],
			"requiredApprovals": [],
			"spawns": []
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err != nil {
		t.Fatal(err)
	}
	if decision.Action != "continue" {
		t.Fatalf("action = %q", decision.Action)
	}
	if decision.Plan == nil || decision.Plan.Prompt != "incorporate review feedback" {
		t.Fatalf("plan = %+v", decision.Plan)
	}
}

func TestDecodeReplanDecisionComplete(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "complete",
		"finalCandidateWorkerId": "worker-123",
		"rationale": "all follow-up work is done",
		"message": "ready for user review",
		"plan": null
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err != nil {
		t.Fatal(err)
	}
	if decision.Action != "complete" {
		t.Fatalf("action = %q", decision.Action)
	}
	if decision.Plan != nil {
		t.Fatalf("plan = %+v", decision.Plan)
	}
	if decision.FinalCandidateWorkerID != "worker-123" {
		t.Fatalf("final candidate = %q", decision.FinalCandidateWorkerID)
	}
}

func TestDecodeReplanDecisionIgnoresTrailingJunk(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "complete",
		"rationale": "done",
		"plan": null
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != "complete" {
		t.Fatalf("action = %q", decision.Action)
	}
}

func TestDecodeCodexPlanExtractsObjectFromProse(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`Here is the plan:
	{
		"workerKind": "mock",
		"workerPrompt": "Run a smoke test.",
		"rationale": "test",
		"steps": [],
		"requiredApprovals": [],
		"spawns": []
	}
	Thanks.`))
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerKind != "mock" || plan.Prompt != "Run a smoke test." {
		t.Fatalf("plan = %+v", plan)
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
	script := "#!/bin/sh\n" +
		"case \" $* \" in *\" --dangerously-bypass-approvals-and-sandbox \"*) ;; *) echo missing yolo permissions >&2; exit 42;; esac\n" +
		"printf '%s\\n' " + strconv.Quote(testCodexBrainOutput(t, mode)) + "\n"
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
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
