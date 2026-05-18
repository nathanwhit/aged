package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"aged/internal/core"
)

type APIBrainConfig struct {
	Endpoint     string
	APIKey       string
	Model        string
	TemplatePath string
	HTTPClient   *http.Client
	Fallback     BrainProvider
}

type APIBrain struct {
	endpoint   string
	apiKey     string
	model      string
	template   string
	httpClient *http.Client
	fallback   BrainProvider
}

func NewAPIBrain(config APIBrainConfig) (*APIBrain, error) {
	if strings.TrimSpace(config.Endpoint) == "" {
		return nil, errors.New("api brain endpoint is required")
	}
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, errors.New("api brain API key is required")
	}
	if strings.TrimSpace(config.Model) == "" {
		return nil, errors.New("api brain model is required")
	}
	template, err := os.ReadFile(config.TemplatePath)
	if err != nil {
		return nil, err
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &APIBrain{
		endpoint:   config.Endpoint,
		apiKey:     config.APIKey,
		model:      config.Model,
		template:   string(template),
		httpClient: client,
		fallback:   config.Fallback,
	}, nil
}

func (b *APIBrain) Plan(ctx context.Context, task core.Task, steering []string) (Plan, error) {
	plan, err := b.plan(ctx, task, steering)
	if err == nil {
		return plan, nil
	}
	if b.fallback == nil {
		return Plan{}, err
	}
	fallbackPlan, fallbackErr := b.fallback.Plan(ctx, task, steering)
	if fallbackErr != nil {
		return Plan{}, fmt.Errorf("api brain failed: %w; fallback failed: %w", err, fallbackErr)
	}
	if fallbackPlan.Metadata == nil {
		fallbackPlan.Metadata = map[string]any{}
	}
	fallbackPlan.Metadata["brain"] = "api-fallback"
	fallbackPlan.Metadata["fallbackReason"] = err.Error()
	return fallbackPlan, nil
}

func (b *APIBrain) plan(ctx context.Context, task core.Task, steering []string) (Plan, error) {
	content, err := b.chatCompletion(ctx, "api brain", chatCompletionRequest{
		Model: b.model,
		Messages: []chatMessage{
			{
				Role:    "system",
				Content: strings.TrimSpace(b.template),
			},
			{
				Role:    "user",
				Content: b.taskMessage(task, steering),
			},
		},
		ResponseFormat: planResponseFormat(),
	})
	if err != nil {
		return Plan{}, err
	}
	content = trimJSONFence(content)
	var plan Plan
	if err := json.Unmarshal([]byte(content), &plan); err != nil {
		return Plan{}, fmt.Errorf("decode api brain plan: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["brain"] = "api"
	plan.Metadata["scheduler"] = "orchestrator"
	plan.Metadata["model"] = b.model
	return plan, nil
}

func (b *APIBrain) Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	content, err := b.chatCompletion(ctx, "api assistant", chatCompletionRequest{
		Model: b.model,
		Messages: []chatMessage{
			{
				Role:    "system",
				Content: "You are the interactive assistant for aged, a local autonomous development orchestrator. Answer directly. If a request requires long-running code work, explain the task the user should start.",
			},
			{
				Role:    "user",
				Content: b.assistantMessage(req),
			},
		},
	})
	if err != nil {
		return core.AssistantResponse{}, err
	}
	return core.AssistantResponse{
		ConversationID: req.ConversationID,
		Message:        content,
		Metadata: core.MustJSON(map[string]any{
			"brain": "api",
			"model": b.model,
		}),
	}, nil
}

func (b *APIBrain) chatCompletion(ctx context.Context, caller string, request chatCompletionRequest) (string, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Authorization", "Bearer "+b.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpRes, err := b.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer httpRes.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(httpRes.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if httpRes.StatusCode < 200 || httpRes.StatusCode >= 300 {
		return "", fmt.Errorf("%s returned %s: %s", caller, httpRes.Status, strings.TrimSpace(string(responseBody)))
	}
	var response chatCompletionResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return "", fmt.Errorf("decode %s response: %w", caller, err)
	}
	if len(response.Choices) == 0 {
		return "", fmt.Errorf("%s returned no choices", caller)
	}
	return strings.TrimSpace(response.Choices[0].Message.Content), nil
}

func (b *APIBrain) taskMessage(task core.Task, steering []string) string {
	payload := map[string]any{
		"task": taskPromptPayload(task),
		"availableWorkers": []map[string]string{
			{"kind": "codex", "description": "Autonomous software engineering worker using Codex CLI headless mode."},
			{"kind": "claude", "description": "Autonomous software engineering worker using Claude Code headless mode."},
			{"kind": "mock", "description": "No-op deterministic worker for smoke tests and scheduler validation."},
		},
		"steering": steering,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return task.Prompt
	}
	return string(data)
}

func (b *APIBrain) assistantMessage(req core.AssistantRequest) string {
	payload := map[string]any{
		"conversationId": req.ConversationID,
		"message":        req.Message,
	}
	if len(req.Context) > 0 {
		var contextValue any
		if err := json.Unmarshal(req.Context, &contextValue); err == nil {
			payload["context"] = contextValue
		} else {
			payload["context"] = string(req.Context)
		}
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return req.Message
	}
	return string(data)
}

type chatCompletionRequest struct {
	Model          string         `json:"model"`
	Messages       []chatMessage  `json:"messages"`
	ResponseFormat map[string]any `json:"response_format,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

func planResponseFormat() map[string]any {
	return map[string]any{
		"type": "json_schema",
		"json_schema": map[string]any{
			"name":   "orchestration_plan",
			"strict": true,
			"schema": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"reasoningEffort", "rationale", "workPlan", "steps", "requiredApprovals", "actions", "workers", "spawns"},
				"properties": map[string]any{
					"workerKind": map[string]any{
						"type":        "string",
						"description": "Legacy fallback worker kind. Prefer workers[].workerKind for new plans.",
					},
					"workerPrompt": map[string]any{
						"type":        "string",
						"description": "Legacy fallback worker prompt. Prefer workers[].workerPrompt for new plans.",
					},
					"reasoningEffort": map[string]any{
						"type": "string",
						"enum": []string{"default", "low", "medium", "high", "xhigh", "max"},
					},
					"rationale": map[string]any{
						"type": "string",
					},
					"workPlan": workPlanSchema(),
					"steps": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"title", "description"},
							"properties": map[string]any{
								"title":       map[string]any{"type": "string"},
								"description": map[string]any{"type": "string"},
							},
						},
					},
					"requiredApprovals": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"title", "reason"},
							"properties": map[string]any{
								"title":  map[string]any{"type": "string"},
								"reason": map[string]any{"type": "string"},
							},
						},
					},
					"actions": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"kind", "when", "reason", "workerId", "inputs"},
							"properties": map[string]any{
								"kind":     map[string]any{"type": "string", "enum": []string{"publish_pull_request", "update_pull_request", "watch_pull_requests", "wait_external", "ask_user", "create_tasks"}},
								"when":     map[string]any{"type": "string", "enum": []string{"immediate", "after_success"}},
								"reason":   map[string]any{"type": "string"},
								"workerId": map[string]any{"type": "string"},
								"inputs": map[string]any{
									"type":                 "object",
									"additionalProperties": true,
								},
							},
						},
					},
					"metadata": map[string]any{
						"type":                 "object",
						"additionalProperties": true,
					},
					"workers": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"id", "role", "reason", "workerKind", "workerPrompt", "reasoningEffort", "dependsOn"},
							"properties": map[string]any{
								"id":              map[string]any{"type": "string"},
								"role":            map[string]any{"type": "string"},
								"reason":          map[string]any{"type": "string"},
								"workerKind":      map[string]any{"type": "string", "description": "Configured worker kind, including enabled aged-runner-v1 plugin kinds."},
								"workerPrompt":    map[string]any{"type": "string", "minLength": 1},
								"reasoningEffort": map[string]any{"type": "string", "enum": []string{"default", "low", "medium", "high", "xhigh", "max"}},
								"dependsOn": map[string]any{
									"type":  "array",
									"items": map[string]any{"type": "string"},
								},
							},
						},
					},
					"spawns": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"role", "reason"},
							"properties": map[string]any{
								"id":              map[string]any{"type": "string"},
								"role":            map[string]any{"type": "string"},
								"reason":          map[string]any{"type": "string"},
								"workerKind":      map[string]any{"type": "string", "description": "Configured worker kind, including enabled aged-runner-v1 plugin kinds."},
								"reasoningEffort": map[string]any{"type": "string", "enum": []string{"default", "low", "medium", "high", "xhigh", "max"}},
								"dependsOn": map[string]any{
									"type":  "array",
									"items": map[string]any{"type": "string"},
								},
							},
						},
					},
				},
			},
		},
	}
}

func workPlanSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"summary", "workstreams", "validation", "risks"},
		"properties": map[string]any{
			"summary":     map[string]any{"type": "string"},
			"workstreams": workPlanItemArraySchema(),
			"validation":  workPlanItemArraySchema(),
			"risks": map[string]any{
				"type":  "array",
				"items": map[string]any{"type": "string"},
			},
		},
	}
}

func workPlanItemArraySchema() map[string]any {
	return map[string]any{
		"type": "array",
		"items": map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []string{"id", "goal", "status", "doneWhen", "dependsOn"},
			"properties": map[string]any{
				"id":       map[string]any{"type": "string"},
				"goal":     map[string]any{"type": "string"},
				"status":   map[string]any{"type": "string"},
				"doneWhen": map[string]any{"type": "string"},
				"dependsOn": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
			},
		},
	}
}

func trimJSONFence(value string) string {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "```") {
		return value
	}
	value = strings.TrimPrefix(value, "```json")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	return strings.TrimSpace(value)
}
