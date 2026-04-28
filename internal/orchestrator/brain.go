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

type Plan struct {
	WorkerKind        string            `json:"workerKind"`
	Prompt            string            `json:"workerPrompt"`
	Rationale         string            `json:"rationale,omitempty"`
	Steps             []PlanStep        `json:"steps,omitempty"`
	RequiredApprovals []ApprovalRequest `json:"requiredApprovals,omitempty"`
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

type SpawnRequest struct {
	Role   string `json:"role"`
	Reason string `json:"reason"`
}

func (p Plan) Validate() error {
	if strings.TrimSpace(p.WorkerKind) == "" {
		return errors.New("plan workerKind is required")
	}
	if strings.TrimSpace(p.Prompt) == "" {
		return errors.New("plan workerPrompt is required")
	}
	return nil
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
