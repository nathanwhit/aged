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
	request := chatCompletionRequest{
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
	}
	body, err := json.Marshal(request)
	if err != nil {
		return Plan{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint, bytes.NewReader(body))
	if err != nil {
		return Plan{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+b.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	httpRes, err := b.httpClient.Do(httpReq)
	if err != nil {
		return Plan{}, err
	}
	defer httpRes.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(httpRes.Body, 4<<20))
	if err != nil {
		return Plan{}, err
	}
	if httpRes.StatusCode < 200 || httpRes.StatusCode >= 300 {
		return Plan{}, fmt.Errorf("api brain returned %s: %s", httpRes.Status, strings.TrimSpace(string(responseBody)))
	}

	var response chatCompletionResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return Plan{}, fmt.Errorf("decode api brain response: %w", err)
	}
	if len(response.Choices) == 0 {
		return Plan{}, errors.New("api brain returned no choices")
	}

	content := strings.TrimSpace(response.Choices[0].Message.Content)
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
	request := chatCompletionRequest{
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
	}
	body, err := json.Marshal(request)
	if err != nil {
		return core.AssistantResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint, bytes.NewReader(body))
	if err != nil {
		return core.AssistantResponse{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+b.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpRes, err := b.httpClient.Do(httpReq)
	if err != nil {
		return core.AssistantResponse{}, err
	}
	defer httpRes.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(httpRes.Body, 4<<20))
	if err != nil {
		return core.AssistantResponse{}, err
	}
	if httpRes.StatusCode < 200 || httpRes.StatusCode >= 300 {
		return core.AssistantResponse{}, fmt.Errorf("api assistant returned %s: %s", httpRes.Status, strings.TrimSpace(string(responseBody)))
	}
	var response chatCompletionResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return core.AssistantResponse{}, fmt.Errorf("decode api assistant response: %w", err)
	}
	if len(response.Choices) == 0 {
		return core.AssistantResponse{}, errors.New("api assistant returned no choices")
	}
	return core.AssistantResponse{
		ConversationID: req.ConversationID,
		Message:        strings.TrimSpace(response.Choices[0].Message.Content),
		Metadata: core.MustJSON(map[string]any{
			"brain": "api",
			"model": b.model,
		}),
	}, nil
}

func (b *APIBrain) taskMessage(task core.Task, steering []string) string {
	payload := map[string]any{
		"task": map[string]any{
			"id":        task.ID,
			"projectId": task.ProjectID,
			"title":     task.Title,
			"prompt":    task.Prompt,
		},
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
				"required":             []string{"workerKind", "workerPrompt", "reasoningEffort", "rationale", "steps", "requiredApprovals", "actions", "spawns"},
				"properties": map[string]any{
					"workerKind": map[string]any{
						"type":        "string",
						"description": "Configured worker kind, such as codex, claude, mock, benchmark_compare, or an enabled aged-runner-v1 plugin kind.",
					},
					"workerPrompt": map[string]any{
						"type":      "string",
						"minLength": 1,
					},
					"reasoningEffort": map[string]any{
						"type": "string",
						"enum": []string{"default", "low", "medium", "high", "xhigh", "max"},
					},
					"rationale": map[string]any{
						"type": "string",
					},
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
								"kind":     map[string]any{"type": "string", "enum": []string{"publish_pull_request", "watch_pull_requests", "wait_external", "ask_user"}},
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
