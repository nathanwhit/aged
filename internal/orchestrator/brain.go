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
	WorkerKind        string            `json:"-"`
	Prompt            string            `json:"-"`
	ReasoningEffort   string            `json:"reasoningEffort,omitempty"`
	Rationale         string            `json:"rationale,omitempty"`
	WorkPlan          *core.WorkPlan    `json:"workPlan,omitempty"`
	Steps             []PlanStep        `json:"steps,omitempty"`
	RequiredApprovals []ApprovalRequest `json:"requiredApprovals,omitempty"`
	Actions           []PlanAction      `json:"actions,omitempty"`
	WorkItems         []WorkItemRequest `json:"workItems,omitempty"`
	Workers           []WorkerRequest   `json:"-"`
	Spawns            []SpawnRequest    `json:"-"`
	Metadata          map[string]any    `json:"metadata,omitempty"`
}

func normalizePlanShape(plan *Plan) {
	if plan == nil {
		return
	}
	for index := range plan.Actions {
		action := &plan.Actions[index]
		action.Kind = strings.TrimSpace(action.Kind)
		action.When = strings.TrimSpace(action.When)
		action.WorkerID = strings.TrimSpace(action.WorkerID)
		action.Reason = strings.TrimSpace(action.Reason)
		if action.Inputs == nil {
			action.Inputs = map[string]any{}
		}
		if action.Reason == "" {
			action.Reason = defaultPlanActionReason(*action)
		}
	}
}

func defaultPlanActionReason(action PlanAction) string {
	switch strings.TrimSpace(action.Kind) {
	case "publish_pull_request":
		if title := strings.TrimSpace(stringMetadata(action.Inputs, "title")); title != "" {
			return "Publish pull request: " + title
		}
		return "Publish pull request."
	case "update_pull_request":
		if title := strings.TrimSpace(stringMetadata(action.Inputs, "title")); title != "" {
			return "Update pull request: " + title
		}
		return "Update pull request."
	case "watch_pull_requests":
		return "Watch pull requests."
	case "wait_external":
		return "Wait for external state."
	case "ask_user":
		return "Ask the user for input."
	case "spawn_work":
		return "Spawn objective work."
	case "create_tasks":
		return "Create follow-up tasks."
	case "finish_objective":
		return "Finish objective."
	default:
		return "Run planned action."
	}
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

type WorkItemRequest struct {
	ID              string         `json:"id,omitempty"`
	Kind            string         `json:"kind"`
	Reason          string         `json:"reason"`
	Prompt          string         `json:"prompt,omitempty"`
	TargetKind      string         `json:"targetKind,omitempty"`
	TargetID        string         `json:"targetId,omitempty"`
	WorkerKind      string         `json:"workerKind"`
	ReasoningEffort string         `json:"reasoningEffort,omitempty"`
	DependsOn       []string       `json:"dependsOn,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
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
	InitialPlan                Plan                      `json:"initialPlan"`
	WorkPlan                   *core.WorkPlan            `json:"workPlan,omitempty"`
	Results                    []WorkerTurnResult        `json:"results"`
	ContextLedger              []ContextLedgerEntry      `json:"contextLedger,omitempty"`
	Artifacts                  []core.TaskArtifact       `json:"artifacts,omitempty"`
	PullRequests               []ReplanPullRequestState  `json:"pullRequests,omitempty"`
	TaskSteering               []string                  `json:"taskSteering,omitempty"`
	PendingPullRequestFeedback []PullRequestFeedbackItem `json:"pendingPullRequestFeedback,omitempty"`
	PendingWorkerSteering      []WorkerSteeringItem      `json:"pendingWorkerSteering,omitempty"`
	Turn                       int                       `json:"turn"`
	RecoveryHint               string                    `json:"recoveryHint,omitempty"`
}

type ReplanPullRequestState struct {
	ID                   string `json:"id"`
	Repo                 string `json:"repo,omitempty"`
	Number               int    `json:"number,omitempty"`
	URL                  string `json:"url,omitempty"`
	Branch               string `json:"branch,omitempty"`
	Base                 string `json:"base,omitempty"`
	Title                string `json:"title,omitempty"`
	State                string `json:"state,omitempty"`
	Draft                bool   `json:"draft,omitempty"`
	ChecksStatus         string `json:"checksStatus,omitempty"`
	ChecksConclusion     string `json:"checksConclusion,omitempty"`
	MergeStatus          string `json:"mergeStatus,omitempty"`
	Mergeable            string `json:"mergeable,omitempty"`
	ReviewStatus         string `json:"reviewStatus,omitempty"`
	ContinueAfterPublish bool   `json:"continueAfterPublish,omitempty"`
	PublicationPhase     string `json:"publicationPhase,omitempty"`
}

type PullRequestFeedbackItem = core.PullRequestFeedback

type WorkerSteeringItem struct {
	EventID           int64  `json:"eventId"`
	WorkerID          string `json:"workerId"`
	NodeID            string `json:"nodeId,omitempty"`
	WorkerKind        string `json:"workerKind,omitempty"`
	Role              string `json:"role,omitempty"`
	SpawnID           string `json:"spawnId,omitempty"`
	CandidateWorkerID string `json:"candidateWorkerId,omitempty"`
	ReviewPhase       string `json:"reviewPhase,omitempty"`
	Status            string `json:"status,omitempty"`
	Reason            string `json:"reason,omitempty"`
	Message           string `json:"message"`
}

type ContextLedgerEntry struct {
	Kind         string                 `json:"kind"`
	SourceEvent  string                 `json:"sourceEvent,omitempty"`
	WorkerID     string                 `json:"workerId,omitempty"`
	NodeID       string                 `json:"nodeId,omitempty"`
	WorkerKind   string                 `json:"workerKind,omitempty"`
	Role         string                 `json:"role,omitempty"`
	SpawnID      string                 `json:"spawnId,omitempty"`
	BaseWorkerID string                 `json:"baseWorkerId,omitempty"`
	Status       string                 `json:"status,omitempty"`
	Summary      string                 `json:"summary,omitempty"`
	Error        string                 `json:"error,omitempty"`
	ChangedFiles []WorkspaceChangedFile `json:"changedFiles,omitempty"`
	Metadata     map[string]any         `json:"metadata,omitempty"`
}

type ReplanDecision struct {
	Action          string         `json:"action"`
	Plan            *Plan          `json:"plan,omitempty"`
	PullRequestBody string         `json:"pullRequestBody,omitempty"`
	Rationale       string         `json:"rationale,omitempty"`
	Message         string         `json:"message,omitempty"`
	WorkPlan        *core.WorkPlan `json:"workPlan,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
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
	normalizePlanShape(&p)
	if len(p.Workers) > 0 {
		return errors.New("plan workers are no longer supported; use workItems")
	}
	if len(p.Spawns) > 0 {
		return errors.New("plan spawns are no longer supported; use workItems")
	}
	if len(p.WorkItems) == 0 {
		if len(p.Actions) == 0 {
			return errors.New("plan workItems must contain at least one work item")
		}
		for _, action := range p.Actions {
			if !actionAllowedWithoutWorkItem(action) {
				return errors.New("work-itemless plans may only contain immediate actions or worker-bound PR actions")
			}
		}
	} else if err := validateWorkItemRequests(p.WorkItems); err != nil {
		return err
	}
	for index, action := range p.Actions {
		if err := action.Validate(); err != nil {
			return fmt.Errorf("plan actions[%d]: %w", index, err)
		}
	}
	return nil
}

func actionAllowedWithoutWorkItem(action PlanAction) bool {
	if strings.TrimSpace(action.When) == "immediate" {
		return true
	}
	if strings.TrimSpace(action.WorkerID) == "" {
		return false
	}
	switch strings.TrimSpace(action.Kind) {
	case "publish_pull_request", "update_pull_request":
		return strings.TrimSpace(action.When) == "after_success"
	default:
		return false
	}
}

func validateWorkItemRequests(items []WorkItemRequest) error {
	ids := map[string]bool{}
	for index, item := range items {
		id := workItemRequestID(item, index)
		if ids[id] {
			return fmt.Errorf("plan workItems[%d]: duplicate work item id %q", index, id)
		}
		ids[id] = true
		if strings.TrimSpace(item.Kind) == "" {
			return fmt.Errorf("plan workItems[%d]: kind is required", index)
		}
		if strings.TrimSpace(item.WorkerKind) == "" {
			return fmt.Errorf("plan workItems[%d]: workerKind is required", index)
		}
		if strings.TrimSpace(item.Prompt) == "" {
			return fmt.Errorf("plan workItems[%d]: prompt is required", index)
		}
	}
	for index, item := range items {
		id := workItemRequestID(item, index)
		for _, dep := range item.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep == "" {
				continue
			}
			if dep == id {
				return fmt.Errorf("plan workItems[%d]: work item %q depends on itself", index, id)
			}
			if !ids[dep] {
				return fmt.Errorf("plan workItems[%d]: work item %q depends on unknown work item %q", index, id, dep)
			}
		}
	}
	return nil
}

func workItemRequestID(item WorkItemRequest, index int) string {
	if strings.TrimSpace(item.ID) != "" {
		return strings.TrimSpace(item.ID)
	}
	return fmt.Sprintf("work-item-%d", index+1)
}

func (a PlanAction) Validate() error {
	switch strings.TrimSpace(a.Kind) {
	case "publish_pull_request", "update_pull_request", "watch_pull_requests", "wait_external", "ask_user", "spawn_work", "create_tasks", "finish_objective":
	default:
		return errors.New("kind must be one of publish_pull_request, update_pull_request, watch_pull_requests, wait_external, ask_user, spawn_work, create_tasks, or finish_objective")
	}
	switch strings.TrimSpace(a.When) {
	case "", "after_success", "immediate":
	default:
		return errors.New("when must be immediate or after_success")
	}
	reason := strings.TrimSpace(a.Reason)
	if reason == "" {
		reason = defaultPlanActionReason(a)
	}
	if reason == "" {
		return errors.New("reason is required")
	}
	if strings.TrimSpace(a.Kind) == "publish_pull_request" && strings.TrimSpace(stringMetadata(a.Inputs, "body")) == "" {
		return errors.New("publish_pull_request inputs.body is required")
	}
	if strings.TrimSpace(a.Kind) == "create_tasks" && len(anySliceMetadata(a.Inputs, "tasks")) == 0 {
		return errors.New("create_tasks inputs.tasks is required")
	}
	if strings.TrimSpace(a.Kind) == "spawn_work" && len(anySliceMetadata(a.Inputs, "items")) == 0 {
		return errors.New("spawn_work inputs.items is required")
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
	case "complete", "finish_objective", "wait", "fail":
		return nil
	default:
		return errors.New("replan action must be one of continue, complete, finish_objective, wait, or fail")
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
		Rationale: "fallback prompt brain selected the configured default worker",
		Steps: []PlanStep{{
			Title:       "Execute requested work",
			Description: "Run one worker with the user request and current steering context.",
		}},
		WorkItems: []WorkItemRequest{{
			ID:              "main",
			Kind:            "objective.implement",
			Reason:          "Run the requested work.",
			Prompt:          strings.TrimSpace(prompt),
			TargetKind:      "objective",
			TargetID:        task.ID,
			WorkerKind:      b.defaultKind,
			ReasoningEffort: "default",
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
		Rationale: "static brain selected the configured default worker",
		Steps: []PlanStep{{
			Title:       "Execute requested work",
			Description: "Run one worker with the user request.",
		}},
		WorkItems: []WorkItemRequest{{
			ID:              "main",
			Kind:            "objective.implement",
			Reason:          "Run the requested work.",
			Prompt:          fmt.Sprintf("%s\n\n%s%s", task.Title, task.Prompt, extra),
			TargetKind:      "objective",
			TargetID:        task.ID,
			WorkerKind:      kind,
			ReasoningEffort: "default",
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
