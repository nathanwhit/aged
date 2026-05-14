package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"aged/internal/core"
)

type BrainProvider interface {
	Plan(ctx context.Context, task core.Task, steering []string) (Plan, error)
}

type ReplanProvider interface {
	Replan(ctx context.Context, task core.Task, state OrchestrationState) (ReplanDecision, error)
}

type CompletionReviewProvider interface {
	ReviewCompletion(ctx context.Context, task core.Task, candidate WorkerTurnResult, reason string) (CompletionReview, error)
}

type PublicationReviewProvider interface {
	ReviewPublication(ctx context.Context, task core.Task, candidate WorkerTurnResult, action PlanAction) (PublicationReview, error)
}

type CodeReviewPromptProvider interface {
	CodeReviewPrompt(task core.Task, candidate WorkerTurnResult, policy core.ReviewPolicy, phase string) string
}

type AssistantProvider interface {
	Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error)
}

type Plan struct {
	WorkerKind        string            `json:"workerKind"`
	Prompt            string            `json:"workerPrompt"`
	ReasoningEffort   string            `json:"reasoningEffort,omitempty"`
	Rationale         string            `json:"rationale,omitempty"`
	WorkPlan          *core.WorkPlan    `json:"workPlan,omitempty"`
	Steps             []PlanStep        `json:"steps,omitempty"`
	RequiredApprovals []ApprovalRequest `json:"requiredApprovals,omitempty"`
	Actions           []PlanAction      `json:"actions,omitempty"`
	Workers           []WorkerRequest   `json:"workers,omitempty"`
	Spawns            []SpawnRequest    `json:"spawns,omitempty"`
	Metadata          map[string]any    `json:"metadata,omitempty"`
}

type PlanStep struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

type ApprovalRequest struct {
	Title  string `json:"title"`
	Reason string `json:"reason"`
}

type PlanAction struct {
	Kind     string         `json:"kind"`
	When     string         `json:"when"`
	Reason   string         `json:"reason"`
	WorkerID string         `json:"workerId,omitempty"`
	Inputs   map[string]any `json:"inputs,omitempty"`
}

type WorkerRequest struct {
	ID              string   `json:"id,omitempty"`
	Role            string   `json:"role,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	WorkerKind      string   `json:"workerKind"`
	Prompt          string   `json:"workerPrompt"`
	ReasoningEffort string   `json:"reasoningEffort,omitempty"`
	DependsOn       []string `json:"dependsOn,omitempty"`
}

type SpawnRequest struct {
	ID              string   `json:"id,omitempty"`
	Role            string   `json:"role"`
	Reason          string   `json:"reason"`
	WorkerKind      string   `json:"workerKind,omitempty"`
	ReasoningEffort string   `json:"reasoningEffort,omitempty"`
	DependsOn       []string `json:"dependsOn,omitempty"`
}

type OrchestrationState struct {
	InitialPlan              Plan               `json:"initialPlan"`
	WorkPlan                 *core.WorkPlan     `json:"workPlan,omitempty"`
	Results                  []WorkerTurnResult `json:"results"`
	Turn                     int                `json:"turn"`
	BlockedFinalCandidateIDs []string           `json:"blockedFinalCandidateIds,omitempty"`
	RecoveryHint             string             `json:"recoveryHint,omitempty"`
}

type ReplanDecision struct {
	Action                 string         `json:"action"`
	Plan                   *Plan          `json:"plan,omitempty"`
	FinalCandidateWorkerID string         `json:"finalCandidateWorkerId,omitempty"`
	PullRequestBody        string         `json:"pullRequestBody,omitempty"`
	Rationale              string         `json:"rationale,omitempty"`
	Message                string         `json:"message,omitempty"`
	WorkPlan               *core.WorkPlan `json:"workPlan,omitempty"`
	Metadata               map[string]any `json:"metadata,omitempty"`
}

type CompletionReview struct {
	Ready    bool           `json:"ready"`
	Reason   string         `json:"reason,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type PublicationReview struct {
	Ready    bool           `json:"ready"`
	Reason   string         `json:"reason,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func (p Plan) Validate() error {
	if len(p.Workers) == 0 {
		if strings.TrimSpace(p.WorkerKind) == "" {
			return errors.New("plan workerKind is required")
		}
		if strings.TrimSpace(p.Prompt) == "" {
			return errors.New("plan workerPrompt is required")
		}
	} else if err := validateWorkerRequests(p.Workers); err != nil {
		return err
	}
	for index, action := range p.Actions {
		if err := action.Validate(); err != nil {
			return fmt.Errorf("plan actions[%d]: %w", index, err)
		}
	}
	return nil
}

func validateWorkerRequests(workers []WorkerRequest) error {
	ids := map[string]bool{}
	for index, worker := range workers {
		id := workerRequestID(worker, index)
		if ids[id] {
			return fmt.Errorf("plan workers[%d]: duplicate worker id %q", index, id)
		}
		ids[id] = true
		if strings.TrimSpace(worker.WorkerKind) == "" {
			return fmt.Errorf("plan workers[%d]: workerKind is required", index)
		}
		if strings.TrimSpace(worker.Prompt) == "" {
			return fmt.Errorf("plan workers[%d]: workerPrompt is required", index)
		}
	}
	for index, worker := range workers {
		id := workerRequestID(worker, index)
		for _, dep := range worker.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep == "" {
				continue
			}
			if dep == id {
				return fmt.Errorf("plan workers[%d]: worker %q depends on itself", index, id)
			}
			if !ids[dep] {
				return fmt.Errorf("plan workers[%d]: worker %q depends on unknown worker %q", index, id, dep)
			}
		}
	}
	return nil
}

func workerRequestID(worker WorkerRequest, index int) string {
	if strings.TrimSpace(worker.ID) != "" {
		return strings.TrimSpace(worker.ID)
	}
	return fmt.Sprintf("worker-%d", index+1)
}

func (a PlanAction) Validate() error {
	switch strings.TrimSpace(a.Kind) {
	case "publish_pull_request", "update_pull_request", "watch_pull_requests", "wait_external", "ask_user":
	default:
		return errors.New("kind must be one of publish_pull_request, update_pull_request, watch_pull_requests, wait_external, or ask_user")
	}
	switch strings.TrimSpace(a.When) {
	case "", "after_success", "immediate":
	default:
		return errors.New("when must be immediate or after_success")
	}
	if strings.TrimSpace(a.Reason) == "" {
		return errors.New("reason is required")
	}
	if strings.TrimSpace(a.Kind) == "publish_pull_request" && strings.TrimSpace(stringMetadata(a.Inputs, "body")) == "" {
		return errors.New("publish_pull_request inputs.body is required")
	}
	return nil
}

func (d ReplanDecision) Validate() error {
	switch strings.TrimSpace(d.Action) {
	case "continue":
		if d.Plan == nil {
			return errors.New("replan continue action requires plan")
		}
		return d.Plan.Validate()
	case "complete", "wait", "fail":
		return nil
	default:
		return errors.New("replan action must be one of continue, complete, wait, or fail")
	}
}

type PromptBrain struct {
	defaultKind string
	template    string
}

func NewPromptBrain(defaultKind string, templatePath string) (*PromptBrain, error) {
	template, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, err
	}
	if defaultKind == "" {
		defaultKind = "mock"
	}
	return &PromptBrain{
		defaultKind: defaultKind,
		template:    string(template),
	}, nil
}

func (b *PromptBrain) Plan(_ context.Context, task core.Task, steering []string) (Plan, error) {
	prompt := b.template
	prompt = strings.ReplaceAll(prompt, "{{title}}", task.Title)
	prompt = strings.ReplaceAll(prompt, "{{prompt}}", task.Prompt)
	prompt = strings.ReplaceAll(prompt, "{{steering}}", strings.Join(steering, "\n"))

	return Plan{
		WorkerKind: b.defaultKind,
		Prompt:     strings.TrimSpace(prompt),
		Rationale:  "fallback prompt brain selected the configured default worker",
		Steps: []PlanStep{{
			Title:       "Execute requested work",
			Description: "Run one worker with the user request and current steering context.",
		}},
		Metadata: map[string]any{
			"brain":     "prompt",
			"scheduler": "orchestrator",
		},
	}, nil
}

func (b *PromptBrain) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	return noInteractiveAssistantResponse(req.ConversationID, "prompt"), nil
}

type StaticBrain struct {
	WorkerKind string
}

func (b StaticBrain) Plan(_ context.Context, task core.Task, steering []string) (Plan, error) {
	kind := b.WorkerKind
	if kind == "" {
		kind = "mock"
	}
	extra := ""
	if len(steering) > 0 {
		extra = "\n\nUser steering:\n" + strings.Join(steering, "\n")
	}
	return Plan{
		WorkerKind: kind,
		Prompt:     fmt.Sprintf("%s\n\n%s%s", task.Title, task.Prompt, extra),
		Rationale:  "static brain selected the configured default worker",
		Steps: []PlanStep{{
			Title:       "Execute requested work",
			Description: "Run one worker with the user request.",
		}},
		Metadata: map[string]any{
			"brain":     "static",
			"scheduler": "orchestrator",
		},
	}, nil
}

func (b StaticBrain) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	return noInteractiveAssistantResponse(req.ConversationID, "static"), nil
}

func noInteractiveAssistantResponse(conversationID string, brain string) core.AssistantResponse {
	return core.AssistantResponse{
		ConversationID: conversationID,
		Message:        "No interactive assistant brain is configured. Start a task when you want the orchestrator to schedule worker execution.",
		Metadata: core.MustJSON(map[string]any{
			"brain": brain,
		}),
	}
}
