package orchestrator

import (
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

const (
	codexBrainKind             = "codex"
	claudeBrainKind            = "claude"
	codexYoloFlag              = "--dangerously-bypass-approvals-and-sandbox"
	claudeSkipPermissionsFlag  = "--dangerously-skip-permissions"
	defaultBrainCommandTimeout = 2 * time.Minute
)

type CodexBrainConfig struct {
	CodexPath    string
	TemplatePath string
	PromptSets   *PromptSetRegistry
	WorkDir      string
	Timeout      time.Duration
	Fallback     BrainProvider
}

type ClaudeBrainConfig struct {
	ClaudePath   string
	TemplatePath string
	PromptSets   *PromptSetRegistry
	WorkDir      string
	Timeout      time.Duration
	Fallback     BrainProvider
}

type cliBrainConfig struct {
	Provider     string
	CodexPath    string
	ClaudePath   string
	TemplatePath string
	PromptSets   *PromptSetRegistry
	WorkDir      string
	Timeout      time.Duration
	Fallback     BrainProvider
}

type CodexBrain struct {
	provider   string
	codexPath  string
	claudePath string
	template   string
	promptSets *PromptSetRegistry
	workDir    string
	timeout    time.Duration
	fallback   BrainProvider
}

func NewCodexBrain(config CodexBrainConfig) (*CodexBrain, error) {
	return newCLIBrain(cliBrainConfig{
		Provider:     codexBrainKind,
		CodexPath:    config.CodexPath,
		TemplatePath: config.TemplatePath,
		PromptSets:   config.PromptSets,
		WorkDir:      config.WorkDir,
		Timeout:      config.Timeout,
		Fallback:     config.Fallback,
	})
}

func NewClaudeBrain(config ClaudeBrainConfig) (*CodexBrain, error) {
	return newCLIBrain(cliBrainConfig{
		Provider:     claudeBrainKind,
		ClaudePath:   config.ClaudePath,
		TemplatePath: config.TemplatePath,
		PromptSets:   config.PromptSets,
		WorkDir:      config.WorkDir,
		Timeout:      config.Timeout,
		Fallback:     config.Fallback,
	})
}

func newCLIBrain(config cliBrainConfig) (*CodexBrain, error) {
	template, err := os.ReadFile(config.TemplatePath)
	if err != nil {
		return nil, err
	}
	provider := strings.TrimSpace(config.Provider)
	if provider == "" {
		provider = codexBrainKind
	}
	if provider != codexBrainKind && provider != claudeBrainKind {
		return nil, fmt.Errorf("CLI brain provider must be one of %q or %q", codexBrainKind, claudeBrainKind)
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
		timeout = defaultBrainCommandTimeout
	}
	return &CodexBrain{
		provider:   provider,
		codexPath:  codexPath,
		claudePath: claudePath,
		template:   string(template),
		promptSets: config.PromptSets,
		workDir:    config.WorkDir,
		timeout:    timeout,
		fallback:   config.Fallback,
	}, nil
}

func (b *CodexBrain) kind() string {
	if b == nil || strings.TrimSpace(b.provider) == "" {
		return codexBrainKind
	}
	return strings.TrimSpace(b.provider)
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
		return Plan{}, fmt.Errorf("%s brain failed: %w; fallback failed: %w", b.kind(), err, fallbackErr)
	}
	if fallbackPlan.Metadata == nil {
		fallbackPlan.Metadata = map[string]any{}
	}
	fallbackPlan.Metadata["brain"] = b.kind() + "-fallback"
	fallbackPlan.Metadata["fallbackReason"] = err.Error()
	return fallbackPlan, nil
}

func (b *CodexBrain) Replan(ctx context.Context, task core.Task, state OrchestrationState) (ReplanDecision, error) {
	builtinPrompt := b.replanPrompt(task, state)
	rendered, custom := b.customPrompt(task, "replan", replanPromptPayload(task, state))
	decision, fallbackReason, err := b.replanWithFallback(ctx, rendered, custom, builtinPrompt)
	if err != nil {
		return ReplanDecision{}, err
	}
	if decision.Metadata == nil {
		decision.Metadata = map[string]any{}
	}
	decision.Metadata["brain"] = b.kind()
	decision.Metadata["scheduler"] = "orchestrator"
	for key, value := range promptMetadata(rendered, fallbackReason) {
		decision.Metadata[key] = value
	}
	return decision, nil
}

func (b *CodexBrain) ReviewCompletion(ctx context.Context, task core.Task, candidate WorkerTurnResult, reason string) (CompletionReview, error) {
	builtinPrompt := b.completionReviewPrompt(task, candidate, reason)
	rendered, custom := b.customPrompt(task, "completion_review", completionReviewPayload(task, candidate, reason))
	review, fallbackReason, err := b.completionReviewWithFallback(ctx, rendered, custom, builtinPrompt)
	if err != nil {
		return CompletionReview{}, err
	}
	if review.Metadata == nil {
		review.Metadata = map[string]any{}
	}
	review.Metadata["brain"] = b.kind()
	review.Metadata["scheduler"] = "orchestrator"
	for key, value := range promptMetadata(rendered, fallbackReason) {
		review.Metadata[key] = value
	}
	return review, nil
}

func (b *CodexBrain) ReviewPublication(ctx context.Context, task core.Task, candidate WorkerTurnResult, action PlanAction) (PublicationReview, error) {
	builtinPrompt := b.publicationReviewPrompt(task, candidate, action)
	rendered, custom := b.customPrompt(task, "publication_review", publicationReviewPayload(task, candidate, action))
	review, fallbackReason, err := b.publicationReviewWithFallback(ctx, rendered, custom, builtinPrompt)
	if err != nil {
		return PublicationReview{}, err
	}
	if review.Metadata == nil {
		review.Metadata = map[string]any{}
	}
	review.Metadata["brain"] = b.kind()
	review.Metadata["scheduler"] = "orchestrator"
	for key, value := range promptMetadata(rendered, fallbackReason) {
		review.Metadata[key] = value
	}
	return review, nil
}

func (b *CodexBrain) CodeReviewPrompt(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string) string {
	payload := codeReviewPromptPayload(task, candidate, policy, phase)
	if promptSetID := strings.TrimSpace(policy.PromptSetID); promptSetID != "" {
		metadata := map[string]any{}
		if len(task.Metadata) > 0 {
			_ = json.Unmarshal(task.Metadata, &metadata)
		}
		metadata["promptSetId"] = promptSetID
		task.Metadata = core.MustJSON(metadata)
	}
	if rendered, custom := b.customPrompt(task, PromptTemplateCodeReview, payload); custom {
		return rendered.Prompt
	}
	return b.codeReviewPrompt(task, candidate, policy, phase)
}

func (b *CodexBrain) Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	runCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	prompt := b.assistantPrompt(req)
	workDir := nonEmpty(strings.TrimSpace(req.WorkDir), b.workDir)
	if b.kind() == claudeBrainKind {
		args := []string{"--print", "--output-format", "stream-json", "--verbose"}
		if strings.TrimSpace(req.ProviderSessionID) != "" {
			args = []string{"--resume", req.ProviderSessionID, "--print", "--output-format", "stream-json", "--verbose"}
		}
		stdout, err := runPromptCommand(runCtx, b.claudePath, args, workDir, prompt, "claude assistant command failed")
		if err != nil {
			return core.AssistantResponse{}, err
		}
		output := string(stdout)
		content := extractLastParsedResult(claudeBrainKind, output)
		if content == "" {
			content = strings.TrimSpace(output)
		}
		sessionID := nonEmpty(extractClaudeSessionID(output), req.ProviderSessionID)
		return core.AssistantResponse{
			ConversationID:    req.ConversationID,
			Message:           strings.TrimSpace(content),
			Provider:          claudeBrainKind,
			ProviderSessionID: sessionID,
			Metadata: core.MustJSON(map[string]any{
				"brain":             claudeBrainKind,
				"providerSessionId": sessionID,
				"resumed":           req.ProviderSessionID != "",
			}),
		}, nil
	}

	args := []string{"exec", "--sandbox", "read-only", "--json", "--cd", workDir, "-"}
	if strings.TrimSpace(req.ProviderSessionID) != "" {
		args = []string{"exec", "resume", "--json", req.ProviderSessionID, "-"}
	}
	cmd := exec.CommandContext(runCtx, b.codexPath, args...)
	cmd.Stdin = strings.NewReader(prompt)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return core.AssistantResponse{}, fmt.Errorf("codex assistant command failed: %w: %s", err, commandFailureDetail(stdout.String(), stderr.String()))
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
			"brain":             codexBrainKind,
			"providerSessionId": sessionID,
			"resumed":           req.ProviderSessionID != "",
		}),
	}, nil
}

func (b *CodexBrain) plan(ctx context.Context, task core.Task, steering []string) (Plan, error) {
	var customErrors []string
	var failedPrompt RenderedPrompt
	for _, rendered := range b.customPrompts(task, planTemplateNames(task), b.taskMessage(task, steering)) {
		plan, err := b.planWithPrompt(ctx, rendered.Prompt)
		if err == nil {
			if plan.Metadata == nil {
				plan.Metadata = map[string]any{}
			}
			for key, value := range promptMetadata(rendered, "") {
				plan.Metadata[key] = value
			}
			plan.Metadata["brain"] = b.kind()
			plan.Metadata["scheduler"] = "orchestrator"
			return plan, nil
		}
		failedPrompt = rendered
		customErrors = append(customErrors, fmt.Sprintf("%s: %v", rendered.Template, err))
	}
	plan, err := b.planWithPrompt(ctx, b.prompt(task, steering))
	if err != nil {
		if len(customErrors) > 0 {
			return Plan{}, fmt.Errorf("custom %s plan prompts failed: %s; built-in prompt failed: %w", b.kind(), strings.Join(customErrors, "; "), err)
		}
		return Plan{}, err
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	if len(customErrors) > 0 {
		for key, value := range promptMetadata(failedPrompt, strings.Join(customErrors, "; ")) {
			plan.Metadata[key] = value
		}
	}
	plan.Metadata["brain"] = b.kind()
	plan.Metadata["scheduler"] = "orchestrator"
	return plan, nil
}

func (b *CodexBrain) replanWithFallback(ctx context.Context, rendered RenderedPrompt, custom bool, builtinPrompt string) (ReplanDecision, string, error) {
	if custom {
		decision, err := b.replanWithPrompt(ctx, rendered.Prompt)
		if err == nil {
			return decision, "", nil
		}
		fallback, fallbackErr := b.replanWithPrompt(ctx, builtinPrompt)
		if fallbackErr != nil {
			return ReplanDecision{}, "", fmt.Errorf("custom %s replan prompt failed: %w; built-in prompt failed: %w", b.kind(), err, fallbackErr)
		}
		return fallback, err.Error(), nil
	}
	decision, err := b.replanWithPrompt(ctx, builtinPrompt)
	return decision, "", err
}

func (b *CodexBrain) completionReviewWithFallback(ctx context.Context, rendered RenderedPrompt, custom bool, builtinPrompt string) (CompletionReview, string, error) {
	if custom {
		review, err := b.completionReviewWithPrompt(ctx, rendered.Prompt)
		if err == nil {
			return review, "", nil
		}
		fallback, fallbackErr := b.completionReviewWithPrompt(ctx, builtinPrompt)
		if fallbackErr != nil {
			return CompletionReview{}, "", fmt.Errorf("custom %s completion review prompt failed: %w; built-in prompt failed: %w", b.kind(), err, fallbackErr)
		}
		return fallback, err.Error(), nil
	}
	review, err := b.completionReviewWithPrompt(ctx, builtinPrompt)
	return review, "", err
}

func (b *CodexBrain) publicationReviewWithFallback(ctx context.Context, rendered RenderedPrompt, custom bool, builtinPrompt string) (PublicationReview, string, error) {
	if custom {
		review, err := b.publicationReviewWithPrompt(ctx, rendered.Prompt)
		if err == nil {
			return review, "", nil
		}
		fallback, fallbackErr := b.publicationReviewWithPrompt(ctx, builtinPrompt)
		if fallbackErr != nil {
			return PublicationReview{}, "", fmt.Errorf("custom %s publication review prompt failed: %w; built-in prompt failed: %w", b.kind(), err, fallbackErr)
		}
		return fallback, err.Error(), nil
	}
	review, err := b.publicationReviewWithPrompt(ctx, builtinPrompt)
	return review, "", err
}

func (b *CodexBrain) planWithPrompt(ctx context.Context, prompt string) (Plan, error) {
	content, err := b.runBrainPrompt(ctx, prompt, b.kind()+" brain command failed")
	if err != nil {
		return Plan{}, err
	}
	plan, err := decodeCodexPlan([]byte(content))
	if err != nil {
		return Plan{}, fmt.Errorf("decode %s brain plan: %w", b.kind(), err)
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (b *CodexBrain) replanWithPrompt(ctx context.Context, prompt string) (ReplanDecision, error) {
	content, err := b.runBrainPrompt(ctx, prompt, b.kind()+" replan command failed")
	if err != nil {
		return ReplanDecision{}, err
	}
	decision, err := decodeReplanDecision([]byte(content))
	if err != nil {
		return ReplanDecision{}, fmt.Errorf("decode %s replan decision: %w", b.kind(), err)
	}
	if err := decision.Validate(); err != nil {
		return ReplanDecision{}, err
	}
	return decision, nil
}

func (b *CodexBrain) completionReviewWithPrompt(ctx context.Context, prompt string) (CompletionReview, error) {
	content, err := b.runBrainPrompt(ctx, prompt, b.kind()+" completion review command failed")
	if err != nil {
		return CompletionReview{}, err
	}
	review, err := decodeCompletionReview([]byte(content))
	if err != nil {
		return CompletionReview{}, fmt.Errorf("decode %s completion review: %w", b.kind(), err)
	}
	return review, nil
}

func (b *CodexBrain) publicationReviewWithPrompt(ctx context.Context, prompt string) (PublicationReview, error) {
	content, err := b.runBrainPrompt(ctx, prompt, b.kind()+" publication review command failed")
	if err != nil {
		return PublicationReview{}, err
	}
	review, err := decodePublicationReview([]byte(content))
	if err != nil {
		return PublicationReview{}, fmt.Errorf("decode %s publication review: %w", b.kind(), err)
	}
	return review, nil
}

func (b *CodexBrain) runBrainPrompt(ctx context.Context, prompt string, failurePrefix string) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	var cmd *exec.Cmd
	switch b.kind() {
	case claudeBrainKind:
		cmd = exec.CommandContext(runCtx, b.claudePath, b.claudeArgs()...)
		cmd.Dir = b.workDir
	default:
		cmd = exec.CommandContext(runCtx, b.codexPath, b.execArgs()...)
	}
	cmd.Stdin = strings.NewReader(prompt)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s: %w: %s", failurePrefix, err, commandFailureDetail(stdout.String(), stderr.String()))
	}
	var content string
	switch b.kind() {
	case claudeBrainKind:
		content = extractLastParsedResult(claudeBrainKind, stdout.String())
		if strings.TrimSpace(content) == "" {
			return "", errors.New("claude brain returned no result")
		}
	default:
		var err error
		content, err = extractCodexAgentMessage(stdout.Bytes())
		if err != nil {
			return "", err
		}
	}
	return trimJSONFence(content), nil
}

func commandFailureDetail(stdout string, stderr string) string {
	detail := strings.TrimSpace(stderr)
	if detail == "" {
		detail = strings.TrimSpace(stdout)
	}
	if detail == "" {
		return "no command output"
	}
	const maxDetailBytes = 4000
	if len(detail) <= maxDetailBytes {
		return detail
	}
	return "..." + detail[len(detail)-maxDetailBytes:]
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

func (b *CodexBrain) execArgs() []string {
	return []string{"exec", codexYoloFlag, "--json", "--cd", b.workDir, "-"}
}

func (b *CodexBrain) claudeArgs() []string {
	return []string{"--print", "--output-format", "stream-json", "--verbose", claudeSkipPermissionsFlag}
}

func decodeReplanDecision(data []byte) (ReplanDecision, error) {
	var raw struct {
		Action                 string          `json:"action"`
		Plan                   json.RawMessage `json:"plan,omitempty"`
		FinalCandidateWorkerID string          `json:"finalCandidateWorkerId,omitempty"`
		PullRequestBody        string          `json:"pullRequestBody,omitempty"`
		Rationale              string          `json:"rationale,omitempty"`
		Message                string          `json:"message,omitempty"`
		WorkPlan               *core.WorkPlan  `json:"workPlan,omitempty"`
		Metadata               map[string]any  `json:"metadata,omitempty"`
	}
	if err := unmarshalPossiblyWrappedJSONObject(data, &raw); err != nil {
		return ReplanDecision{}, err
	}
	decision := ReplanDecision{
		Action:                 raw.Action,
		FinalCandidateWorkerID: raw.FinalCandidateWorkerID,
		PullRequestBody:        raw.PullRequestBody,
		Rationale:              raw.Rationale,
		Message:                raw.Message,
		WorkPlan:               raw.WorkPlan,
		Metadata:               raw.Metadata,
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

func decodeCompletionReview(data []byte) (CompletionReview, error) {
	var raw struct {
		Ready    bool           `json:"ready"`
		Reason   string         `json:"reason,omitempty"`
		Metadata map[string]any `json:"metadata,omitempty"`
	}
	if err := unmarshalPossiblyWrappedJSONObject(data, &raw); err != nil {
		return CompletionReview{}, err
	}
	return CompletionReview{
		Ready:    raw.Ready,
		Reason:   raw.Reason,
		Metadata: raw.Metadata,
	}, nil
}

func decodePublicationReview(data []byte) (PublicationReview, error) {
	var raw struct {
		Ready    bool           `json:"ready"`
		Reason   string         `json:"reason,omitempty"`
		Metadata map[string]any `json:"metadata,omitempty"`
	}
	if err := unmarshalPossiblyWrappedJSONObject(data, &raw); err != nil {
		return PublicationReview{}, err
	}
	return PublicationReview{
		Ready:    raw.Ready,
		Reason:   raw.Reason,
		Metadata: raw.Metadata,
	}, nil
}

func decodeCodexPlan(data []byte) (Plan, error) {
	var raw struct {
		WorkerKind        string          `json:"workerKind"`
		Prompt            string          `json:"workerPrompt"`
		ReasoningEffort   string          `json:"reasoningEffort,omitempty"`
		Rationale         string          `json:"rationale,omitempty"`
		WorkPlan          *core.WorkPlan  `json:"workPlan,omitempty"`
		Steps             json.RawMessage `json:"steps,omitempty"`
		RequiredApprovals json.RawMessage `json:"requiredApprovals,omitempty"`
		Actions           json.RawMessage `json:"actions,omitempty"`
		Workers           json.RawMessage `json:"workers,omitempty"`
		Spawns            json.RawMessage `json:"spawns,omitempty"`
		Metadata          map[string]any  `json:"metadata,omitempty"`
	}
	if err := unmarshalPossiblyWrappedJSONObject(data, &raw); err != nil {
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
	actions, err := decodePlanActions(raw.Actions)
	if err != nil {
		return Plan{}, fmt.Errorf("decode actions: %w", err)
	}
	workers, err := decodeWorkerRequests(raw.Workers)
	if err != nil {
		return Plan{}, fmt.Errorf("decode workers: %w", err)
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
		WorkPlan:          raw.WorkPlan,
		Steps:             steps,
		RequiredApprovals: approvals,
		Actions:           actions,
		Workers:           workers,
		Spawns:            spawns,
		Metadata:          raw.Metadata,
	}, nil
}

func unmarshalPossiblyWrappedJSONObject(data []byte, value any) error {
	trimmed := bytes.TrimSpace(data)
	if err := json.Unmarshal(trimmed, value); err == nil {
		return nil
	} else {
		object, extractErr := firstJSONObject(trimmed)
		if extractErr != nil || bytes.Equal(object, trimmed) {
			return err
		}
		if retryErr := json.Unmarshal(object, value); retryErr != nil {
			return fmt.Errorf("%w; extracted JSON object also failed: %v", err, retryErr)
		}
		return nil
	}
}

func firstJSONObject(data []byte) ([]byte, error) {
	start := bytes.IndexByte(data, '{')
	if start < 0 {
		return nil, errors.New("no JSON object found")
	}
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(data); index++ {
		ch := data[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch ch {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return data[start : index+1], nil
			}
			if depth < 0 {
				return nil, errors.New("unbalanced JSON object")
			}
		}
	}
	return nil, errors.New("unterminated JSON object")
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

func decodePlanActions(data json.RawMessage) ([]PlanAction, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var actions []PlanAction
	if err := json.Unmarshal(data, &actions); err != nil {
		return nil, err
	}
	return actions, nil
}

func decodeWorkerRequests(data json.RawMessage) ([]WorkerRequest, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var workers []WorkerRequest
	if err := json.Unmarshal(data, &workers); err != nil {
		return nil, err
	}
	return workers, nil
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
	taskPayload := map[string]any{
		"id":             task.ID,
		"title":          task.Title,
		"prompt":         task.Prompt,
		"completionMode": taskCompletionModeFromTask(task),
	}
	if budget := taskBudgetPayload(task); budget != nil {
		taskPayload["budget"] = budget
	}
	payload := map[string]any{
		"task": taskPayload,
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

func (b *CodexBrain) customPrompt(task core.Task, templateName string, input any) (RenderedPrompt, bool) {
	rendered := b.customPrompts(task, []string{templateName}, input)
	if len(rendered) == 0 {
		return RenderedPrompt{}, false
	}
	return rendered[0], true
}

func (b *CodexBrain) customPrompts(task core.Task, templateNames []string, input any) []RenderedPrompt {
	if b.promptSets == nil {
		return nil
	}
	taskJSON := map[string]any{
		"id":             task.ID,
		"title":          task.Title,
		"prompt":         task.Prompt,
		"completionMode": taskCompletionModeFromTask(task),
	}
	if budget := taskBudgetPayload(task); budget != nil {
		taskJSON["budget"] = budget
	}
	data := map[string]any{
		"system":      strings.TrimSpace(b.template),
		"input_json":  input,
		"task_id":     task.ID,
		"task_title":  task.Title,
		"task_prompt": task.Prompt,
		"task_json":   taskJSON,
	}
	var rendered []RenderedPrompt
	for _, templateName := range templateNames {
		if prompt, ok := b.promptSets.Render(task, templateName, data); ok {
			rendered = append(rendered, prompt)
		}
	}
	return rendered
}

func planTemplateNames(task core.Task) []string {
	if isGitHubReviewRequestTask(task) {
		return []string{PromptTemplateGitHubReview, PromptTemplatePlan}
	}
	return []string{PromptTemplatePlan}
}

func isGitHubReviewRequestTask(task core.Task) bool {
	if len(task.Metadata) == 0 {
		return false
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return false
	}
	return strings.EqualFold(stringMetadataValue(metadata["source"]), "github-mention") &&
		strings.EqualFold(stringMetadataValue(metadata["reason"]), "review_requested") &&
		strings.EqualFold(stringMetadataValue(metadata["subjectType"]), "PullRequest")
}

func replanPromptPayload(task core.Task, state OrchestrationState) map[string]any {
	taskPayload := map[string]any{
		"id":             task.ID,
		"title":          task.Title,
		"prompt":         task.Prompt,
		"completionMode": taskCompletionModeFromTask(task),
	}
	if budget := taskBudgetPayload(task); budget != nil {
		taskPayload["budget"] = budget
	}
	return map[string]any{
		"task":  taskPayload,
		"state": compactOrchestrationStateForPrompt(state),
		"availableWorkers": []map[string]string{
			{"kind": "codex", "description": "Autonomous software engineering worker using Codex CLI headless mode."},
			{"kind": "claude", "description": "Autonomous software engineering worker using Claude Code headless mode."},
			{"kind": "mock", "description": "No-op deterministic worker for smoke tests and scheduler validation."},
		},
	}
}

func completionReviewPayload(task core.Task, candidate WorkerTurnResult, reason string) map[string]any {
	return map[string]any{
		"task": map[string]any{
			"id":     task.ID,
			"title":  task.Title,
			"prompt": task.Prompt,
		},
		"selectedCandidate": candidate,
		"completionReason":  reason,
	}
}

func publicationReviewPayload(task core.Task, candidate WorkerTurnResult, action PlanAction) map[string]any {
	return map[string]any{
		"task": map[string]any{
			"id":     task.ID,
			"title":  task.Title,
			"prompt": task.Prompt,
		},
		"candidate":         candidate,
		"publicationAction": action,
	}
}

func codeReviewPromptPayload(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string) map[string]any {
	return map[string]any{
		"task": map[string]any{
			"id":     task.ID,
			"title":  task.Title,
			"prompt": task.Prompt,
		},
		"candidate":    candidate,
		"reviewPolicy": policy,
		"phase":        phase,
	}
}

func (b *CodexBrain) replanPrompt(task core.Task, state OrchestrationState) string {
	payload := replanPromptPayload(task, state)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return task.Prompt
	}
	return strings.TrimSpace(b.template) + `	

You are making a dynamic replanning decision after one or more worker turns.

Return exactly one JSON object and nothing else. Do not wrap it in markdown.
The first non-whitespace character of your response must be "{", and the last non-whitespace character must be "}".
Do not include prose before or after the JSON object. Do not include markdown fences. Do not include comments. Do not emit more than one JSON object. Do not add an extra closing brace after the object.

The JSON object must have exactly these top-level fields:

{
  "action": "complete",
  "finalCandidateWorkerId": "worker-id-or-empty",
  "pullRequestBody": "string",
  "rationale": "string",
  "message": "string",
  "workPlan": null,
  "plan": null
}

Field rules:
- "action" must be exactly one of "continue", "complete", "wait", or "fail".
- Use "complete" when the task appears done.
- When action is "complete" and the task completionMode is "github", write the pull request description in "pullRequestBody". Write it the way a human contributor opening this PR would write it, not as a status report to the orchestrator. Describe what the code changes do and any notable behavior, API, or migration impact a reviewer should know, and list the tests or commands actually run to validate the change under a "## Test plan" or "## Validation" heading. Prefer a short "## Summary" with bullet points covering the substantive code changes. Do not restate or paraphrase the user's task prompt, the orchestrator's framing, or the worker's instructions; reviewers will read the PR diff, not the task description. Do not mention orchestration internals such as worker ids, task ids, replan or scheduler rationale, "remote worker", "candidate", "aged", or how the change was scheduled. Do not include changed-file lists, file paths in headings, or diffstats because the PR diff already shows them. Keep the body tight: omit a section rather than padding it.
- When action is not "complete" or the task completionMode is not "github", set "pullRequestBody" to an empty string.
- When action is "complete" and more than one successful worker produced candidate changes, set "finalCandidateWorkerId" to the worker id whose changes should be the final task result. If no existing changed candidate should be final, use "continue" to schedule a consolidation, validation, or fix worker instead.
- When action is "complete" and there is only one changed candidate lineage, "finalCandidateWorkerId" may be empty; do not set it to a no-change review or validation worker unless the correct final result is to complete without publishing changes.
- When the task is already satisfied and no code changes or pull request are needed, use "complete", set "finalCandidateWorkerId" to the successful no-change worker that established that result, and set "pullRequestBody" to an empty string even when completionMode is "github".
- Use "continue" when another worker turn is needed.
- Use state.contextLedger as compact durable memory for older high-signal facts from persisted task events. The state.results list may omit routine old worker turns to keep the prompt bounded.
- Use state.budget, when present, as a hard budget for active orchestration. Do not schedule a continue plan whose workers or spawns exceed the remaining worker turns. When the budget is exhausted and no existing candidate can satisfy the task, use "wait" with a clear message asking for more budget or steering. Do not fail a task merely because it is waiting on external pull request state; preserve PR monitoring and external wait states unless an existing policy says to stop.
- For broad performance-improvement investigations, use "continue" unless there is a real product optimization with credible before/after evidence outside measured noise, or the user explicitly asked for a bounded one-shot result. Benchmark harnesses, profiler notes, noisy measurements, and small cleanup patches are intermediate artifacts.
- Use "wait" when user input, approval, or external setup is needed. Put the exact user-facing question or setup request in "message".
- Use "fail" when the task cannot continue.
- Use "workPlan" for the durable engineering decomposition of the whole objective. Set it to null when the existing work plan is still accurate. Include a full updated work plan when worker results change progress, split the task differently, reveal a blocker, finish a stream, add validation work, or change risks. The updated work plan should include summary, workstreams, validation, and risks.
- "workPlan" item statuses should usually be "pending", "running", "blocked", "done", or "dropped". Keep ids stable across turns when they still refer to the same workstream.
- When action is "continue", "plan" must be an object with the same exact schema as the scheduler plan: reasoningEffort, rationale, workPlan, steps, requiredApprovals, actions, workers, spawns.
- The top-level "workPlan" is the durable task update. The continue plan's "workPlan" exists because continue plans use the scheduler schema; it should match the top-level "workPlan" when you are changing the durable plan for the next turn. If the current durable plan remains accurate, include the current work plan in plan.workPlan and set top-level "workPlan" to null.
- The continue plan must use workers for initial execution. Each workers[] object must include id, role, reason, workerKind, workerPrompt, reasoningEffort, and dependsOn. Root workers with empty dependsOn can run in parallel immediately. Workers with dependencies wait until all dependency worker ids finish.
- Top-level workerKind and workerPrompt are legacy compatibility fallback fields only when workers is absent. Do not use them for new continue plans.
- The continue plan may include actions. Use action kind "publish_pull_request" to publish the latest candidate worker as a durable intermediate PR artifact, then wait for GitHub state. A publish_pull_request action must include inputs.body with the PR description to publish; do not rely on aged to generate one. Write inputs.body the same way a human contributor would write the PR description: describe what the code changes do and any notable behavior, API, or migration impact, and list the validation commands actually run, under "## Summary" and "## Test plan" or "## Validation" headings. Do not restate the user's task prompt, mention orchestration internals (worker ids, task ids, replan rationale, "candidate", "aged"), or include changed-file lists or diffstats; the PR diff already shows them. Use action kind "update_pull_request" when the latest candidate worker should update an existing PR branch or PR metadata before returning to monitoring. Use action kind "watch_pull_requests" with when "immediate" when the user only wants to babysit existing PRs. Use "wait_external" when the task should pause for an external event. Use "ask_user" when the task needs user setup, credentials, permissions, VM changes, or another human-provided answer before continuing.
- Plan actions must be objects with kind, when, reason, workerId, and inputs. Use when "after_success" for worker-result actions and "immediate" for standalone existing-PR watch tasks. Use workerId "" to mean the final successful candidate worker when unambiguous; when multiple workers can produce competing candidates, schedule consolidation or validation before publishing. Use inputs {} when no extra inputs are needed for non-publish actions.
- Each spawn object must include role and reason, and may include id, workerKind, and dependsOn. Use id and dependsOn to express parallel/dependency scheduling between spawned workers.
- Spawn objects with no dependsOn may run in parallel. Spawn objects with dependsOn wait for those spawn ids to succeed.
- When action is not "continue", "plan" must be null or omitted.
- "reasoningEffort" inside plan must be one of "default", "low", "medium", "high", "xhigh", or "max".
- "steps", "requiredApprovals", "workers", and "spawns" inside plan must be arrays of objects, never arrays of strings.

Dynamic replanning input:

` + string(data)
}

func (b *CodexBrain) completionReviewPrompt(task core.Task, candidate WorkerTurnResult, reason string) string {
	payload := completionReviewPayload(task, candidate, reason)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return task.Prompt
	}
	return strings.TrimSpace(b.template) + `

You are reviewing whether the selected final candidate actually satisfies the user's task objective.

Return exactly one JSON object and nothing else. Do not wrap it in markdown.
The first non-whitespace character of your response must be "{", and the last non-whitespace character must be "}".

The JSON object must have exactly these top-level fields:

{
  "ready": true,
  "reason": "string"
}

Readiness rules:
- Set "ready": true only when the selected candidate is an appropriate final result for the task as the user stated it.
- Set "ready": false when the task describes an ongoing, multi-turn, keep-working, babysitting, monitoring, or open-ended objective and the candidate is only an intermediate artifact.
- Set "ready": false when the candidate or completion reason says more implementation, validation, review response, benchmarking, or follow-up work is still needed.
- Set "ready": false when the candidate does not address the actual task objective, even if it produced useful setup, test, benchmark, documentation, or diagnostic artifacts.
- Set "ready": false when the user asked to fix, implement, repair, or address a product/code issue but the candidate only adds or changes tests, snapshots, fixtures, benchmarks, or diagnostics. Such a candidate is ready only when the user explicitly asked for tests-only coverage or the issue itself is in the test infrastructure.
- Set "ready": true for bounded one-shot tasks when the candidate appears to satisfy that bounded request, including tasks where tests, documentation, or diagnostic artifacts are the requested output.
- Do not require perfection. This is a task-contract review, not a general code review.

Completion review input:

` + string(data)
}

func (b *CodexBrain) publicationReviewPrompt(task core.Task, candidate WorkerTurnResult, action PlanAction) string {
	payload := publicationReviewPayload(task, candidate, action)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return task.Prompt
	}
	return strings.TrimSpace(b.template) + `

You are reviewing whether the orchestrator should publish the selected worker result as a pull request right now.

Return exactly one JSON object and nothing else. Do not wrap it in markdown.
The first non-whitespace character of your response must be "{", and the last non-whitespace character must be "}".

The JSON object must have exactly these top-level fields:

{
  "ready": true,
  "reason": "string"
}

Publication readiness rules:
- Set "ready": true only when the candidate contains real, task-relevant changes that are appropriate to expose as a pull request now.
- A candidate may be publishable even when the broader task should continue, but only if this PR would contain a coherent useful unit of work on its own.
- Set "ready": false when the candidate summary says the requested work is not done, the work should continue before review, validation is missing for the claimed change, or the candidate is only diagnostic setup for a broader implementation task.
- Set "ready": false when the changed files do not address the user's actual task objective, even if they are useful for another task.
- Set "ready": false when the user asked to fix, implement, repair, or address a product/code issue but the pull request would only add or change tests, snapshots, fixtures, benchmarks, or diagnostics. Publish that only when the user explicitly asked for tests-only coverage or the issue itself is in the test infrastructure.
- Set "ready": false when the action would publish a branch without the worker's requested changes.
- Do not perform a general code review. Decide only whether opening a PR now matches the task, candidate, and planned publication action.

Publication review input:

	` + string(data)
}

func (b *CodexBrain) codeReviewPrompt(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string) string {
	payload := codeReviewPromptPayload(task, candidate, policy, phase)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return buildCodeReviewGatePrompt(task, candidate, policy, phase)
	}
	return strings.TrimSpace(b.template) + `

You are performing a blocking pre-publication code review for aged.

Review the selected candidate before aged publishes it as a pull request. This is a code review, not a task-completion readiness check.

Review rules:
- Inspect the actual diff and surrounding code in the workspace.
- Look for correctness bugs, lifecycle/state regressions, missing regression coverage, unsafe assumptions, and mismatches between the PR claim and the implemented/tested behavior.
- Treat missing tests as blocking when the changed behavior is risky or the PR explicitly claims coverage for a path that is not actually tested.
- Do not make code changes. Report findings only.
- Use severity labels like P0, P1, P2, or P3. Any finding at a configured blocking severity must use "Decision: request_changes".
- If the project instructions name additional checks, apply them.

Respond in markdown with exactly these sections:
Decision: approve OR request_changes
Findings:
Commands Run:
Residual Risk:

Code review input:

` + string(data)
}

const (
	maxReplanPromptResults      = 30
	maxPromptPlanTextBytes      = 12000
	maxPromptRationaleBytes     = 4000
	maxPromptResultSummaryBytes = 3000
	maxPromptResultErrorBytes   = 2000
	maxPromptStatusBytes        = 1000
	maxPromptDiffStatBytes      = 2000
	maxPromptArtifactBytes      = 2000
	maxPromptChangedFiles       = 40
	maxPromptArtifacts          = 20
)

func compactOrchestrationStateForPrompt(state OrchestrationState) OrchestrationState {
	state.InitialPlan = compactPlanForPrompt(state.InitialPlan)
	state.WorkPlan = compactWorkPlanForPrompt(state.WorkPlan)
	state.RecoveryHint = truncateStringForPrompt(state.RecoveryHint, maxPromptResultErrorBytes)
	state.ContextLedger = compactContextLedgerForPrompt(state.ContextLedger)

	blocked := map[string]bool{}
	for _, id := range state.BlockedFinalCandidateIDs {
		blocked[id] = true
	}
	keepFrom := 0
	if len(state.Results) > maxReplanPromptResults {
		keepFrom = len(state.Results) - maxReplanPromptResults
	}
	results := make([]WorkerTurnResult, 0, len(state.Results)-keepFrom)
	for index, result := range state.Results {
		if index < keepFrom && !blocked[result.WorkerID] {
			continue
		}
		results = append(results, compactWorkerTurnResultForPrompt(result))
	}
	state.Results = results
	return state
}

func compactPlanForPrompt(plan Plan) Plan {
	plan.Prompt = truncateStringForPrompt(plan.Prompt, maxPromptPlanTextBytes)
	plan.Rationale = truncateStringForPrompt(plan.Rationale, maxPromptRationaleBytes)
	plan.WorkPlan = compactWorkPlanForPrompt(plan.WorkPlan)
	for index := range plan.Steps {
		plan.Steps[index].Title = truncateStringForPrompt(plan.Steps[index].Title, maxPromptRationaleBytes)
		plan.Steps[index].Description = truncateStringForPrompt(plan.Steps[index].Description, maxPromptRationaleBytes)
	}
	for index := range plan.RequiredApprovals {
		plan.RequiredApprovals[index].Title = truncateStringForPrompt(plan.RequiredApprovals[index].Title, maxPromptRationaleBytes)
		plan.RequiredApprovals[index].Reason = truncateStringForPrompt(plan.RequiredApprovals[index].Reason, maxPromptRationaleBytes)
	}
	for index := range plan.Actions {
		plan.Actions[index].Reason = truncateStringForPrompt(plan.Actions[index].Reason, maxPromptRationaleBytes)
	}
	for index := range plan.Workers {
		plan.Workers[index].Role = truncateStringForPrompt(plan.Workers[index].Role, maxPromptRationaleBytes)
		plan.Workers[index].Reason = truncateStringForPrompt(plan.Workers[index].Reason, maxPromptRationaleBytes)
		plan.Workers[index].Prompt = truncateStringForPrompt(plan.Workers[index].Prompt, maxPromptPlanTextBytes)
	}
	for index := range plan.Spawns {
		plan.Spawns[index].Role = truncateStringForPrompt(plan.Spawns[index].Role, maxPromptRationaleBytes)
		plan.Spawns[index].Reason = truncateStringForPrompt(plan.Spawns[index].Reason, maxPromptRationaleBytes)
	}
	return plan
}

func compactWorkPlanForPrompt(workPlan *core.WorkPlan) *core.WorkPlan {
	if workPlan == nil {
		return nil
	}
	compact := &core.WorkPlan{
		Summary:     truncateStringForPrompt(workPlan.Summary, maxPromptRationaleBytes),
		Workstreams: compactWorkPlanItemsForPrompt(workPlan.Workstreams),
		Validation:  compactWorkPlanItemsForPrompt(workPlan.Validation),
	}
	if len(workPlan.Risks) > maxPromptArtifacts {
		compact.Risks = append([]string{}, workPlan.Risks[:maxPromptArtifacts]...)
		compact.Risks = append(compact.Risks, fmt.Sprintf("... %d additional risks omitted ...", len(workPlan.Risks)-maxPromptArtifacts))
	} else {
		compact.Risks = append([]string{}, workPlan.Risks...)
	}
	for index := range compact.Risks {
		compact.Risks[index] = truncateStringForPrompt(compact.Risks[index], maxPromptRationaleBytes)
	}
	return compact
}

func compactWorkPlanItemsForPrompt(items []core.WorkPlanItem) []core.WorkPlanItem {
	const maxPromptWorkPlanItems = 30
	if len(items) > maxPromptWorkPlanItems {
		omitted := len(items) - maxPromptWorkPlanItems
		items = append(append([]core.WorkPlanItem{}, items[:maxPromptWorkPlanItems]...), core.WorkPlanItem{
			ID:     "omitted",
			Goal:   fmt.Sprintf("... %d additional work plan items omitted ...", omitted),
			Status: "omitted",
		})
	} else {
		items = append([]core.WorkPlanItem{}, items...)
	}
	for index := range items {
		items[index].ID = truncateStringForPrompt(items[index].ID, maxPromptRationaleBytes)
		items[index].Goal = truncateStringForPrompt(items[index].Goal, maxPromptRationaleBytes)
		items[index].Status = truncateStringForPrompt(items[index].Status, maxPromptRationaleBytes)
		items[index].DoneWhen = truncateStringForPrompt(items[index].DoneWhen, maxPromptRationaleBytes)
		items[index].DependsOn = append([]string{}, items[index].DependsOn...)
		for depIndex := range items[index].DependsOn {
			items[index].DependsOn[depIndex] = truncateStringForPrompt(items[index].DependsOn[depIndex], maxPromptRationaleBytes)
		}
	}
	return items
}

func compactWorkerTurnResultForPrompt(result WorkerTurnResult) WorkerTurnResult {
	result.Summary = truncateStringForPrompt(result.Summary, maxPromptResultSummaryBytes)
	result.Error = truncateStringForPrompt(result.Error, maxPromptResultErrorBytes)
	result.Changes.Status = truncateStringForPrompt(result.Changes.Status, maxPromptStatusBytes)
	result.Changes.DiffStat = truncateStringForPrompt(result.Changes.DiffStat, maxPromptDiffStatBytes)
	result.Changes.Diff = ""
	result.Changes.Error = truncateStringForPrompt(result.Changes.Error, maxPromptResultErrorBytes)
	if len(result.Changes.ChangedFiles) > maxPromptChangedFiles {
		omitted := len(result.Changes.ChangedFiles) - maxPromptChangedFiles
		result.Changes.ChangedFiles = append(result.Changes.ChangedFiles[:maxPromptChangedFiles], WorkspaceChangedFile{
			Path:   fmt.Sprintf("... %d additional changed files omitted ...", omitted),
			Status: "omitted",
		})
	}
	if len(result.Changes.Artifacts) > maxPromptArtifacts {
		result.Changes.Artifacts = result.Changes.Artifacts[:maxPromptArtifacts]
	}
	for index := range result.Changes.Artifacts {
		result.Changes.Artifacts[index].Content = truncateStringForPrompt(result.Changes.Artifacts[index].Content, maxPromptArtifactBytes)
	}
	return result
}

func truncateStringForPrompt(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	const marker = "\n... truncated for replanning prompt ...\n"
	if maxBytes <= len(marker) {
		return value[:maxBytes]
	}
	head := (maxBytes - len(marker)) / 2
	tail := maxBytes - len(marker) - head
	return value[:head] + marker + value[len(value)-tail:]
}

func extractCodexAgentMessage(output []byte) (string, error) {
	var result string
	if err := forEachBufferedLine(bytes.NewReader(output), func(line []byte) error {
		var payload map[string]any
		if err := json.Unmarshal(line, &payload); err != nil {
			return nil
		}
		item, ok := payload["item"].(map[string]any)
		if !ok || codexStringField(item, "type") != "agent_message" {
			return nil
		}
		if text := codexStringField(item, "text"); text != "" {
			result = text
		}
		return nil
	}); err != nil {
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
