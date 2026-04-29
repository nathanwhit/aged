package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"aged/internal/core"
	"aged/internal/eventstore"
)

const mcpProtocolVersion = "2025-11-25"

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpError       `json:"error,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type mcpTool struct {
	Name        string         `json:"name"`
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
}

type mcpResource struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

func (s *Server) mcpInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":            "aged",
		"protocolVersion": mcpProtocolVersion,
		"endpoint":        "/mcp",
		"transport":       "streamable-http",
	})
}

func (s *Server) mcp(w http.ResponseWriter, r *http.Request) {
	var req mcpRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, mcpResponse{
			JSONRPC: "2.0",
			Error:   &mcpError{Code: -32700, Message: "parse error", Data: err.Error()},
		})
		return
	}
	if req.JSONRPC != "2.0" {
		writeJSON(w, http.StatusOK, mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &mcpError{Code: -32600, Message: "invalid request"},
		})
		return
	}

	result, err := s.handleMCP(r, req)
	if len(req.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusOK, mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   mcpErrorFor(err),
		})
		return
	}
	if req.Method == "initialize" {
		w.Header().Set("Mcp-Session-Id", "aged")
	}
	writeJSON(w, http.StatusOK, mcpResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	})
}

func (s *Server) handleMCP(r *http.Request, req mcpRequest) (any, error) {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities": map[string]any{
				"tools":     map[string]any{"listChanged": false},
				"resources": map[string]any{"subscribe": false, "listChanged": false},
			},
			"serverInfo": map[string]any{
				"name":        "aged",
				"title":       "aged",
				"version":     "0.1.0",
				"description": "Durable autonomous development orchestration daemon.",
			},
			"instructions": "Use aged tools to inspect orchestration state, create durable work tasks, steer running work, publish pull requests, and babysit PRs. For conversational follow-ups like 'do it', synthesize a concrete aged_create_task prompt from the prior conversation.",
		}, nil
	case "notifications/initialized":
		return map[string]any{}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": mcpTools()}, nil
	case "tools/call":
		return s.mcpToolCall(r, req.Params)
	case "resources/list":
		return s.mcpResources(r)
	case "resources/read":
		return s.mcpResourceRead(r, req.Params)
	default:
		return nil, mcpMethodNotFound(req.Method)
	}
}

func (s *Server) mcpToolCall(r *http.Request, raw json.RawMessage) (any, error) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, invalidMCPParams(err)
	}
	var result any
	var err error
	switch params.Name {
	case "aged_snapshot":
		result, err = s.service.Snapshot(r.Context())
	case "aged_list_projects":
		var snapshot core.Snapshot
		snapshot, err = s.service.Snapshot(r.Context())
		if err == nil {
			result = snapshot.Projects
		}
	case "aged_create_task":
		var req core.CreateTaskRequest
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.CreateTask(r.Context(), req)
		}
	case "aged_create_project":
		var req core.Project
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.CreateProject(r.Context(), req)
		}
	case "aged_publish_pr":
		var req struct {
			TaskID string `json:"taskId"`
			core.PublishPullRequestRequest
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil && strings.TrimSpace(req.TaskID) == "" {
			err = errors.New("taskId is required")
		}
		if err == nil {
			result, err = s.service.PublishTaskPullRequest(r.Context(), req.TaskID, req.PublishPullRequestRequest)
		}
	case "aged_refresh_pr":
		var req struct {
			PullRequestID string `json:"pullRequestId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.RefreshPullRequest(r.Context(), req.PullRequestID)
		}
	case "aged_babysit_pr":
		var req struct {
			PullRequestID string `json:"pullRequestId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.StartPullRequestBabysitter(r.Context(), req.PullRequestID)
		}
	case "aged_retry_task":
		var req struct {
			TaskID string `json:"taskId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.RetryTask(r.Context(), req.TaskID)
		}
	case "aged_steer_task":
		var req struct {
			TaskID  string `json:"taskId"`
			Message string `json:"message"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.SteerTask(r.Context(), req.TaskID, core.SteeringRequest{Message: req.Message})
			result = map[string]any{"ok": true}
		}
	case "aged_cancel_task":
		var req struct {
			TaskID string `json:"taskId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.CancelTask(r.Context(), req.TaskID)
			result = map[string]any{"ok": true}
		}
	default:
		return nil, mcpToolNotFound(params.Name)
	}
	if err != nil {
		return nil, err
	}
	return mcpToolResult(result), nil
}

func (s *Server) mcpResources(r *http.Request) (any, error) {
	snapshot, err := s.service.Snapshot(r.Context())
	if err != nil {
		return nil, err
	}
	resources := []mcpResource{{
		URI:         "aged://snapshot",
		Name:        "snapshot",
		Title:       "Current aged snapshot",
		Description: "Current tasks, workers, projects, pull requests, execution graph, plugins, targets, and recent events.",
		MimeType:    "application/json",
	}}
	for _, task := range snapshot.Tasks {
		resources = append(resources, mcpResource{
			URI:         "aged://tasks/" + task.ID,
			Name:        "task-" + task.ID,
			Title:       task.Title,
			Description: "aged task " + string(task.Status),
			MimeType:    "application/json",
		})
	}
	for _, worker := range snapshot.Workers {
		resources = append(resources, mcpResource{
			URI:         "aged://workers/" + worker.ID,
			Name:        "worker-" + worker.ID,
			Title:       worker.Kind + " worker",
			Description: "aged worker " + string(worker.Status),
			MimeType:    "application/json",
		})
	}
	for _, pr := range snapshot.PullRequests {
		resources = append(resources, mcpResource{
			URI:         "aged://pull-requests/" + pr.ID,
			Name:        "pull-request-" + pr.ID,
			Title:       pr.Title,
			Description: fmt.Sprintf("%s#%d %s", pr.Repo, pr.Number, pr.State),
			MimeType:    "application/json",
		})
	}
	return map[string]any{"resources": resources}, nil
}

func (s *Server) mcpResourceRead(r *http.Request, raw json.RawMessage) (any, error) {
	var params struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, invalidMCPParams(err)
	}
	snapshot, err := s.service.Snapshot(r.Context())
	if err != nil {
		return nil, err
	}
	value, ok := findMCPResource(snapshot, params.URI)
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	text, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"contents": []map[string]any{{
			"uri":      params.URI,
			"mimeType": "application/json",
			"text":     string(text),
		}},
	}, nil
}

func findMCPResource(snapshot core.Snapshot, uri string) (any, bool) {
	switch {
	case uri == "aged://snapshot":
		return snapshot, true
	case strings.HasPrefix(uri, "aged://tasks/"):
		id := strings.TrimPrefix(uri, "aged://tasks/")
		for _, task := range snapshot.Tasks {
			if task.ID == id {
				return task, true
			}
		}
	case strings.HasPrefix(uri, "aged://workers/"):
		id := strings.TrimPrefix(uri, "aged://workers/")
		for _, worker := range snapshot.Workers {
			if worker.ID == id {
				return worker, true
			}
		}
	case strings.HasPrefix(uri, "aged://pull-requests/"):
		id := strings.TrimPrefix(uri, "aged://pull-requests/")
		for _, pr := range snapshot.PullRequests {
			if pr.ID == id {
				return pr, true
			}
		}
	}
	return nil, false
}

func mcpToolResult(value any) any {
	text, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		text = []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
	}
	return map[string]any{
		"content": []map[string]string{{
			"type": "text",
			"text": string(text),
		}},
	}
}

func decodeMCPArgs(raw json.RawMessage, out any) error {
	if len(raw) == 0 || string(raw) == "null" {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return invalidMCPParams(err)
	}
	return nil
}

func mcpTools() []mcpTool {
	return []mcpTool{
		{
			Name:        "aged_snapshot",
			Title:       "Get aged snapshot",
			Description: "Return the current aged state: tasks, workers, projects, PRs, targets, plugins, execution graphs, and recent events.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "aged_create_task",
			Title:       "Create aged task",
			Description: "Create a durable orchestrated task from a concrete work prompt. Use this for 'do it' after synthesizing the actual task from conversation context.",
			InputSchema: objectSchema(map[string]any{
				"projectId":  stringSchema("Optional project id."),
				"title":      stringSchema("Short task title. Optional; aged can generate one when omitted."),
				"prompt":     stringSchema("Concrete work request for the orchestrator."),
				"source":     stringSchema("Optional external source name for idempotency."),
				"externalId": stringSchema("Optional external id paired with source."),
				"metadata":   objectUntypedSchema("Optional structured metadata."),
			}, []string{"prompt"}),
		},
		{
			Name:        "aged_list_projects",
			Title:       "List aged projects",
			Description: "Return configured projects/repositories known to the daemon.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "aged_create_project",
			Title:       "Create aged project",
			Description: "Add a project/repository to the daemon.",
			InputSchema: objectSchema(map[string]any{
				"id":            stringSchema("Project id."),
				"name":          stringSchema("Display name."),
				"localPath":     stringSchema("Absolute path to local checkout on the daemon host."),
				"repo":          stringSchema("GitHub repo such as owner/name."),
				"vcs":           stringSchema("VCS kind: auto, jj, or git."),
				"defaultBase":   stringSchema("Default PR base branch."),
				"workspaceRoot": stringSchema("Optional workspace root override."),
				"targetLabels":  objectUntypedSchema("Optional target label policy."),
			}, []string{"id", "name", "localPath"}),
		},
		{
			Name:        "aged_publish_pr",
			Title:       "Publish task PR",
			Description: "Apply a completed task's selected worker changes if needed, push a branch, and open a GitHub PR.",
			InputSchema: objectSchema(map[string]any{
				"taskId":   stringSchema("Task id to publish."),
				"workerId": stringSchema("Optional worker id when multiple candidates exist."),
				"repo":     stringSchema("Optional GitHub repo override."),
				"base":     stringSchema("Optional base branch override."),
				"branch":   stringSchema("Optional branch name override."),
				"title":    stringSchema("Optional PR title."),
				"body":     stringSchema("Optional PR body."),
				"draft":    map[string]any{"type": "boolean", "description": "Create as draft."},
			}, []string{"taskId"}),
		},
		{
			Name:        "aged_refresh_pr",
			Title:       "Refresh PR",
			Description: "Refresh GitHub PR state, checks, merge status, and review decision.",
			InputSchema: objectSchema(map[string]any{"pullRequestId": stringSchema("aged pull request id.")}, []string{"pullRequestId"}),
		},
		{
			Name:        "aged_babysit_pr",
			Title:       "Babysit PR",
			Description: "Create or return an active task to monitor and fix a PR until it is ready.",
			InputSchema: objectSchema(map[string]any{"pullRequestId": stringSchema("aged pull request id.")}, []string{"pullRequestId"}),
		},
		{
			Name:        "aged_retry_task",
			Title:       "Retry task",
			Description: "Retry a failed task from its persisted plan.",
			InputSchema: objectSchema(map[string]any{"taskId": stringSchema("Task id.")}, []string{"taskId"}),
		},
		{
			Name:        "aged_steer_task",
			Title:       "Steer task",
			Description: "Send steering or feedback to a running/waiting task.",
			InputSchema: objectSchema(map[string]any{
				"taskId":  stringSchema("Task id."),
				"message": stringSchema("Steering message."),
			}, []string{"taskId", "message"}),
		},
		{
			Name:        "aged_cancel_task",
			Title:       "Cancel task",
			Description: "Cancel active workers for a task and mark it canceled.",
			InputSchema: objectSchema(map[string]any{"taskId": stringSchema("Task id.")}, []string{"taskId"}),
		},
	}
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func stringSchema(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func objectUntypedSchema(description string) map[string]any {
	return map[string]any{"type": "object", "description": description, "additionalProperties": true}
}

type mcpRPCError struct {
	code    int
	message string
}

func (e mcpRPCError) Error() string {
	return e.message
}

func mcpMethodNotFound(method string) error {
	return mcpRPCError{code: -32601, message: "method not found: " + method}
}

func mcpToolNotFound(name string) error {
	return mcpRPCError{code: -32602, message: "unknown tool: " + name}
}

func invalidMCPParams(err error) error {
	return mcpRPCError{code: -32602, message: "invalid params: " + err.Error()}
}

func mcpErrorFor(err error) *mcpError {
	var rpcErr mcpRPCError
	if errors.As(err, &rpcErr) {
		return &mcpError{Code: rpcErr.code, Message: rpcErr.message}
	}
	if errors.Is(err, eventstore.ErrNotFound) {
		return &mcpError{Code: -32004, Message: err.Error()}
	}
	return &mcpError{Code: -32000, Message: err.Error()}
}
