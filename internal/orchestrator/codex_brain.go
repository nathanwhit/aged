package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"aged/internal/core"
)

const codexYoloFlag = "--dangerously-bypass-approvals-and-sandbox"

type CodexBrainConfig struct {
	CodexPath    string
	TemplatePath string
	WorkDir      string
	Timeout      time.Duration
	Fallback     BrainProvider
}

type CodexBrain struct {
	codexPath string
	template  string
	workDir   string
	timeout   time.Duration
	fallback  BrainProvider
}

func NewCodexBrain(config CodexBrainConfig) (*CodexBrain, error) {
	template, err := os.ReadFile(config.TemplatePath)
	if err != nil {
		return nil, err
	}
	codexPath := strings.TrimSpace(config.CodexPath)
	if codexPath == "" {
		codexPath = "codex"
	}
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	return &CodexBrain{
		codexPath: codexPath,
		template:  string(template),
		workDir:   config.WorkDir,
		timeout:   timeout,
		fallback:  config.Fallback,
	}, nil
}

func (b *CodexBrain) Plan(ctx context.Context, task core.Task, steering []string) (Plan, error) {
	plan, err := b.plan(ctx, task, steering)
	if err == nil {
		return plan, nil
	}
	if b.fallback == nil {
		return Plan{}, err
	}
	fallbackPlan, fallbackErr := b.fallback.Plan(ctx, task, steering)
	if fallbackErr != nil {
		return Plan{}, fmt.Errorf("codex brain failed: %w; fallback failed: %w", err, fallbackErr)
	}
	if fallbackPlan.Metadata == nil {
		fallbackPlan.Metadata = map[string]any{}
	}
	fallbackPlan.Metadata["brain"] = "codex-fallback"
	fallbackPlan.Metadata["fallbackReason"] = err.Error()
	return fallbackPlan, nil
}

func (b *CodexBrain) Replan(ctx context.Context, task core.Task, state OrchestrationState) (ReplanDecision, error) {
	runCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, b.codexPath, b.execArgs(b.replanPrompt(task, state))...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return ReplanDecision{}, fmt.Errorf("codex replan command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	content, err := extractCodexAgentMessage(stdout.Bytes())
	if err != nil {
		return ReplanDecision{}, err
	}
	content = trimJSONFence(content)
	decision, err := decodeReplanDecision([]byte(content))
	if err != nil {
		return ReplanDecision{}, fmt.Errorf("decode codex replan decision: %w", err)
	}
	if err := decision.Validate(); err != nil {
		return ReplanDecision{}, err
	}
	if decision.Metadata == nil {
		decision.Metadata = map[string]any{}
	}
	decision.Metadata["brain"] = "codex"
	decision.Metadata["scheduler"] = "orchestrator"
	return decision, nil
}

func (b *CodexBrain) Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	runCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	prompt := b.assistantPrompt(req)
	workDir := nonEmpty(strings.TrimSpace(req.WorkDir), b.workDir)
	args := []string{"exec", "--sandbox", "read-only", "--json", "--cd", workDir, prompt}
	if strings.TrimSpace(req.ProviderSessionID) != "" {
		args = []string{"exec", "resume", "--json", req.ProviderSessionID, prompt}
	}
	cmd := exec.CommandContext(runCtx, b.codexPath, args...)
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
			"brain":             "codex",
			"providerSessionId": sessionID,
			"resumed":           req.ProviderSessionID != "",
		}),
	}, nil
}

func (b *CodexBrain) plan(ctx context.Context, task core.Task, steering []string) (Plan, error) {
	runCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, b.codexPath, b.execArgs(b.prompt(task, steering))...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return Plan{}, fmt.Errorf("codex brain command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	content, err := extractCodexAgentMessage(stdout.Bytes())
	if err != nil {
		return Plan{}, err
	}
	content = trimJSONFence(content)
	plan, err := decodeCodexPlan([]byte(content))
	if err != nil {
		return Plan{}, fmt.Errorf("decode codex brain plan: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["brain"] = "codex"
	plan.Metadata["scheduler"] = "orchestrator"
	return plan, nil
}

func (b *CodexBrain) assistantPrompt(req core.AssistantRequest) string {
	var builder strings.Builder
	builder.WriteString("You are the interactive assistant for aged, a local autonomous development orchestrator.\n")
	builder.WriteString("Answer the user's question directly. You may inspect files in the current project checkout to answer questions, but do not edit files or run mutating commands. If the request needs code execution or a long-running task, say what task should be started.\n\n")
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

func (b *CodexBrain) execArgs(prompt string) []string {
	return []string{"exec", codexYoloFlag, "--json", "--cd", b.workDir, prompt}
}

func decodeReplanDecision(data []byte) (ReplanDecision, error) {
	var raw struct {
		Action    string          `json:"action"`
		Plan      json.RawMessage `json:"plan,omitempty"`
		Rationale string          `json:"rationale,omitempty"`
		Message   string          `json:"message,omitempty"`
		Metadata  map[string]any  `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ReplanDecision{}, err
	}
	decision := ReplanDecision{
		Action:    raw.Action,
		Rationale: raw.Rationale,
		Message:   raw.Message,
		Metadata:  raw.Metadata,
	}
	if len(raw.Plan) > 0 && string(raw.Plan) != "null" {
		plan, err := decodeCodexPlan(raw.Plan)
		if err != nil {
			return ReplanDecision{}, fmt.Errorf("decode plan: %w", err)
		}
		decision.Plan = &plan
	}
	return decision, nil
}

func decodeCodexPlan(data []byte) (Plan, error) {
	var raw struct {
		WorkerKind        string          `json:"workerKind"`
		Prompt            string          `json:"workerPrompt"`
		ReasoningEffort   string          `json:"reasoningEffort,omitempty"`
		Rationale         string          `json:"rationale,omitempty"`
		Steps             json.RawMessage `json:"steps,omitempty"`
		RequiredApprovals json.RawMessage `json:"requiredApprovals,omitempty"`
		Spawns            json.RawMessage `json:"spawns,omitempty"`
		Metadata          map[string]any  `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Plan{}, err
	}
	steps, err := decodePlanSteps(raw.Steps)
	if err != nil {
		return Plan{}, fmt.Errorf("decode steps: %w", err)
	}
	approvals, err := decodeApprovalRequests(raw.RequiredApprovals)
	if err != nil {
		return Plan{}, fmt.Errorf("decode requiredApprovals: %w", err)
	}
	spawns, err := decodeSpawnRequests(raw.Spawns)
	if err != nil {
		return Plan{}, fmt.Errorf("decode spawns: %w", err)
	}
	return Plan{
		WorkerKind:        raw.WorkerKind,
		Prompt:            raw.Prompt,
		ReasoningEffort:   raw.ReasoningEffort,
		Rationale:         raw.Rationale,
		Steps:             steps,
		RequiredApprovals: approvals,
		Spawns:            spawns,
		Metadata:          raw.Metadata,
	}, nil
}

func decodePlanSteps(data json.RawMessage) ([]PlanStep, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var steps []PlanStep
	if err := json.Unmarshal(data, &steps); err == nil {
		return steps, nil
	}
	var labels []string
	if err := json.Unmarshal(data, &labels); err != nil {
		return nil, err
	}
	steps = make([]PlanStep, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		steps = append(steps, PlanStep{Title: label, Description: label})
	}
	return steps, nil
}

func decodeApprovalRequests(data json.RawMessage) ([]ApprovalRequest, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var approvals []ApprovalRequest
	if err := json.Unmarshal(data, &approvals); err == nil {
		return approvals, nil
	}
	var labels []string
	if err := json.Unmarshal(data, &labels); err != nil {
		return nil, err
	}
	approvals = make([]ApprovalRequest, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		approvals = append(approvals, ApprovalRequest{Title: label, Reason: label})
	}
	return approvals, nil
}

func decodeSpawnRequests(data json.RawMessage) ([]SpawnRequest, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var spawns []SpawnRequest
	if err := json.Unmarshal(data, &spawns); err == nil {
		return spawns, nil
	}
	var labels []string
	if err := json.Unmarshal(data, &labels); err != nil {
		return nil, err
	}
	spawns = make([]SpawnRequest, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		spawns = append(spawns, SpawnRequest{Role: label, Reason: label})
	}
	return spawns, nil
}

func (b *CodexBrain) prompt(task core.Task, steering []string) string {
	return strings.TrimSpace(b.template) + "\n\n" + b.taskMessage(task, steering)
}

func (b *CodexBrain) taskMessage(task core.Task, steering []string) string {
	payload := map[string]any{
		"task": map[string]any{
			"id":     task.ID,
			"title":  task.Title,
			"prompt": task.Prompt,
		},
		"availableWorkers": []map[string]string{
			{"kind": "codex", "description": "Autonomous software engineering worker using Codex CLI headless mode."},
			{"kind": "claude", "description": "Autonomous software engineering worker using Claude Code headless mode."},
			{"kind": "benchmark_compare", "description": "Deterministic benchmark comparison worker for prompts containing explicit baseline and candidate numeric values."},
			{"kind": "mock", "description": "No-op deterministic worker for smoke tests and scheduler validation."},
		},
		"steering": steering,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return task.Prompt
	}
	return "Schedule this task. Return only the JSON plan, with no prose or markdown.\n\n" + string(data)
}

func (b *CodexBrain) replanPrompt(task core.Task, state OrchestrationState) string {
	payload := map[string]any{
		"task": map[string]any{
			"id":     task.ID,
			"title":  task.Title,
			"prompt": task.Prompt,
		},
		"state": state,
		"availableWorkers": []map[string]string{
			{"kind": "codex", "description": "Autonomous software engineering worker using Codex CLI headless mode."},
			{"kind": "claude", "description": "Autonomous software engineering worker using Claude Code headless mode."},
			{"kind": "mock", "description": "No-op deterministic worker for smoke tests and scheduler validation."},
		},
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return task.Prompt
	}
	return strings.TrimSpace(b.template) + `	

You are making a dynamic replanning decision after one or more worker turns.

Return exactly one JSON object and nothing else. Do not wrap it in markdown.

The JSON object must have exactly these top-level fields:

{
  "action": "complete",
  "rationale": "string",
  "message": "string",
  "plan": null
}

Field rules:
- "action" must be exactly one of "continue", "complete", "wait", or "fail".
- Use "complete" when the task appears done.
- Use "continue" when another worker turn is needed.
- Use "wait" when user input or approval is needed.
- Use "fail" when the task cannot continue.
- When action is "continue", "plan" must be an object with the same exact schema as the scheduler plan: workerKind, workerPrompt, rationale, steps, requiredApprovals, spawns.
- Each spawn object must include role and reason, and may include id, workerKind, and dependsOn. Use id and dependsOn to express parallel/dependency scheduling between spawned workers.
- Spawn objects with no dependsOn may run in parallel. Spawn objects with dependsOn wait for those spawn ids to succeed.
- When action is not "continue", "plan" must be null or omitted.
- "steps", "requiredApprovals", and "spawns" inside plan must be arrays of objects, never arrays of strings.

Dynamic replanning input:

` + string(data)
}

func extractCodexAgentMessage(output []byte) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var result string
	for scanner.Scan() {
		var payload map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &payload); err != nil {
			continue
		}
		item, ok := payload["item"].(map[string]any)
		if !ok || codexStringField(item, "type") != "agent_message" {
			continue
		}
		if text := codexStringField(item, "text"); text != "" {
			result = text
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(result) == "" {
		return "", errors.New("codex brain returned no agent message")
	}
	return result, nil
}

func codexStringField(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
