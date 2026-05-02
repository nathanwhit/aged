package httpapi

import (
	"context"
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

type mcpProjectPatch struct {
	ID                *string                    `json:"id"`
	Name              *string                    `json:"name"`
	LocalPath         *string                    `json:"localPath"`
	Repo              *string                    `json:"repo"`
	UpstreamRepo      *string                    `json:"upstreamRepo"`
	HeadRepoOwner     *string                    `json:"headRepoOwner"`
	PushRemote        *string                    `json:"pushRemote"`
	VCS               *string                    `json:"vcs"`
	DefaultBase       *string                    `json:"defaultBase"`
	WorkspaceRoot     *string                    `json:"workspaceRoot"`
	TargetLabels      *map[string]string         `json:"targetLabels"`
	PullRequestPolicy *mcpPullRequestPolicyPatch `json:"pullRequestPolicy"`
}

type mcpPullRequestPolicyPatch struct {
	BranchPrefix *string `json:"branchPrefix"`
	Draft        *bool   `json:"draft"`
	AllowMerge   *bool   `json:"allowMerge"`
	AutoMerge    *bool   `json:"autoMerge"`
}

type mcpTargetPatch struct {
	ID                    *string                 `json:"id"`
	Kind                  *string                 `json:"kind"`
	Host                  *string                 `json:"host"`
	User                  *string                 `json:"user"`
	Port                  *int                    `json:"port"`
	IdentityFile          *string                 `json:"identityFile"`
	InsecureIgnoreHostKey *bool                   `json:"insecureIgnoreHostKey"`
	WorkDir               *string                 `json:"workDir"`
	WorkRoot              *string                 `json:"workRoot"`
	Labels                *map[string]string      `json:"labels"`
	Capacity              *mcpTargetCapacityPatch `json:"capacity"`
}

type mcpTargetCapacityPatch struct {
	MaxWorkers *int     `json:"maxWorkers"`
	CPUWeight  *float64 `json:"cpuWeight"`
	MemoryGB   *float64 `json:"memoryGB"`
}

type mcpPluginPatch struct {
	ID           *string            `json:"id"`
	Name         *string            `json:"name"`
	Kind         *string            `json:"kind"`
	Protocol     *string            `json:"protocol"`
	Enabled      *bool              `json:"enabled"`
	Command      *[]string          `json:"command"`
	Endpoint     *string            `json:"endpoint"`
	Capabilities *[]string          `json:"capabilities"`
	Config       *map[string]string `json:"config"`
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
	case "aged_task_detail":
		var req struct {
			TaskID string `json:"taskId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil && strings.TrimSpace(req.TaskID) == "" {
			err = errors.New("taskId is required")
		}
		if err == nil {
			result, err = s.service.TaskDetail(r.Context(), req.TaskID)
		}
	case "aged_worker_detail":
		var req struct {
			WorkerID string `json:"workerId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil && strings.TrimSpace(req.WorkerID) == "" {
			err = errors.New("workerId is required")
		}
		if err == nil {
			result, err = s.service.WorkerDetail(r.Context(), req.WorkerID)
		}
	case "aged_list_projects":
		var snapshot core.Snapshot
		snapshot, err = s.service.Snapshot(r.Context())
		if err == nil {
			result = snapshot.Projects
		}
	case "aged_list_targets":
		var snapshot core.Snapshot
		snapshot, err = s.service.Snapshot(r.Context())
		if err == nil {
			result = snapshot.Targets
		}
	case "aged_create_target":
		var req core.TargetConfig
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.RegisterTarget(r.Context(), req)
		}
	case "aged_update_target":
		var req mcpTargetPatch
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.updateMCPTarget(r.Context(), req)
		}
	case "aged_delete_target":
		var req struct {
			TargetID string `json:"targetId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.DeleteTarget(r.Context(), req.TargetID)
			result = map[string]any{"ok": true}
		}
	case "aged_target_health":
		var req struct {
			TargetID string `json:"targetId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.refreshMCPTargetHealth(r.Context(), req.TargetID)
		}
	case "aged_list_plugins":
		var snapshot core.Snapshot
		snapshot, err = s.service.Snapshot(r.Context())
		if err == nil {
			result = snapshot.Plugins
		}
	case "aged_create_plugin":
		var req core.Plugin
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.RegisterPlugin(r.Context(), req)
		}
	case "aged_update_plugin":
		var req mcpPluginPatch
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.updateMCPPlugin(r.Context(), req)
		}
	case "aged_delete_plugin":
		var req struct {
			PluginID string `json:"pluginId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.DeletePlugin(r.Context(), req.PluginID)
			result = map[string]any{"ok": true}
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
	case "aged_update_project":
		var req mcpProjectPatch
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.updateMCPProject(r.Context(), req)
		}
	case "aged_delete_project":
		var req struct {
			ProjectID string `json:"projectId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.DeleteProject(r.Context(), req.ProjectID)
			result = map[string]any{"ok": true}
		}
	case "aged_project_health":
		var req struct {
			ProjectID string `json:"projectId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.ProjectHealth(r.Context(), req.ProjectID)
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
	case "aged_watch_prs":
		var req struct {
			TaskID string `json:"taskId"`
			core.WatchPullRequestsRequest
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil && strings.TrimSpace(req.TaskID) == "" {
			err = errors.New("taskId is required")
		}
		if err == nil {
			result, err = s.service.WatchPullRequests(r.Context(), req.TaskID, req.WatchPullRequestsRequest)
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
	case "aged_clear_task":
		var req struct {
			TaskID string `json:"taskId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.ClearTask(r.Context(), req.TaskID)
			result = map[string]any{"ok": true}
		}
	case "aged_clear_finished_tasks":
		err = decodeMCPArgs(params.Arguments, &struct{}{})
		if err == nil {
			result, err = s.service.ClearTerminalTasks(r.Context())
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
	case "aged_cancel_worker":
		var req struct {
			WorkerID string `json:"workerId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			err = s.service.CancelWorker(r.Context(), req.WorkerID)
			result = map[string]any{"ok": true}
		}
	case "aged_review_worker_changes":
		var req struct {
			WorkerID string `json:"workerId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.ReviewWorkerChanges(r.Context(), req.WorkerID)
		}
	case "aged_apply_worker_changes":
		var req struct {
			WorkerID string `json:"workerId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.ApplyWorkerChanges(r.Context(), req.WorkerID)
		}
	case "aged_apply_task_result":
		var req struct {
			TaskID string `json:"taskId"`
		}
		err = decodeMCPArgs(params.Arguments, &req)
		if err == nil {
			result, err = s.service.ApplyTaskResult(r.Context(), req.TaskID)
		}
	default:
		return nil, mcpToolNotFound(params.Name)
	}
	if err != nil {
		return nil, err
	}
	return mcpToolResult(result), nil
}

func (s *Server) updateMCPProject(ctx context.Context, patch mcpProjectPatch) (core.Project, error) {
	projectID := ""
	if patch.ID != nil {
		projectID = strings.TrimSpace(*patch.ID)
	}
	if projectID == "" {
		return core.Project{}, errors.New("id is required")
	}
	snapshot, err := s.service.Snapshot(ctx)
	if err != nil {
		return core.Project{}, err
	}
	for _, project := range snapshot.Projects {
		if project.ID == projectID {
			return s.service.UpdateProject(ctx, projectID, mergeMCPProjectPatch(project, patch))
		}
	}
	return core.Project{}, eventstore.ErrNotFound
}

func mergeMCPProjectPatch(current core.Project, patch mcpProjectPatch) core.Project {
	updated := current
	if patch.Name != nil {
		updated.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.LocalPath != nil {
		updated.LocalPath = strings.TrimSpace(*patch.LocalPath)
	}
	if patch.Repo != nil {
		updated.Repo = strings.TrimSpace(*patch.Repo)
	}
	if patch.UpstreamRepo != nil {
		updated.UpstreamRepo = strings.TrimSpace(*patch.UpstreamRepo)
	}
	if patch.HeadRepoOwner != nil {
		updated.HeadRepoOwner = strings.TrimSpace(*patch.HeadRepoOwner)
	}
	if patch.PushRemote != nil {
		updated.PushRemote = strings.TrimSpace(*patch.PushRemote)
	}
	if patch.VCS != nil {
		updated.VCS = strings.TrimSpace(*patch.VCS)
	}
	if patch.DefaultBase != nil {
		updated.DefaultBase = strings.TrimSpace(*patch.DefaultBase)
	}
	if patch.WorkspaceRoot != nil {
		updated.WorkspaceRoot = strings.TrimSpace(*patch.WorkspaceRoot)
	}
	if patch.TargetLabels != nil {
		updated.TargetLabels = *patch.TargetLabels
	}
	if patch.PullRequestPolicy != nil {
		if patch.PullRequestPolicy.BranchPrefix != nil {
			updated.PullRequestPolicy.BranchPrefix = strings.TrimSpace(*patch.PullRequestPolicy.BranchPrefix)
		}
		if patch.PullRequestPolicy.Draft != nil {
			updated.PullRequestPolicy.Draft = *patch.PullRequestPolicy.Draft
		}
		if patch.PullRequestPolicy.AllowMerge != nil {
			updated.PullRequestPolicy.AllowMerge = *patch.PullRequestPolicy.AllowMerge
		}
		if patch.PullRequestPolicy.AutoMerge != nil {
			updated.PullRequestPolicy.AutoMerge = *patch.PullRequestPolicy.AutoMerge
		}
	}
	updated.ID = current.ID
	return updated
}

func (s *Server) refreshMCPTargetHealth(ctx context.Context, targetID string) (core.TargetState, error) {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return core.TargetState{}, errors.New("targetId is required")
	}
	s.service.RefreshTargetHealthFor(ctx, targetID)
	snapshot, err := s.service.Snapshot(ctx)
	if err != nil {
		return core.TargetState{}, err
	}
	for _, target := range snapshot.Targets {
		if target.ID == targetID {
			return target, nil
		}
	}
	return core.TargetState{}, eventstore.ErrNotFound
}

func (s *Server) updateMCPTarget(ctx context.Context, patch mcpTargetPatch) (core.TargetConfig, error) {
	targetID := ""
	if patch.ID != nil {
		targetID = strings.TrimSpace(*patch.ID)
	}
	if targetID == "" {
		return core.TargetConfig{}, errors.New("id is required")
	}
	snapshot, err := s.service.Snapshot(ctx)
	if err != nil {
		return core.TargetConfig{}, err
	}
	for _, target := range snapshot.Targets {
		if target.ID == targetID {
			return s.service.RegisterTarget(ctx, mergeMCPTargetPatch(target.TargetConfig, patch))
		}
	}
	return core.TargetConfig{}, eventstore.ErrNotFound
}

func mergeMCPTargetPatch(current core.TargetConfig, patch mcpTargetPatch) core.TargetConfig {
	updated := current
	if patch.Kind != nil {
		updated.Kind = strings.TrimSpace(*patch.Kind)
	}
	if patch.Host != nil {
		updated.Host = strings.TrimSpace(*patch.Host)
	}
	if patch.User != nil {
		updated.User = strings.TrimSpace(*patch.User)
	}
	if patch.Port != nil {
		updated.Port = *patch.Port
	}
	if patch.IdentityFile != nil {
		updated.IdentityFile = strings.TrimSpace(*patch.IdentityFile)
	}
	if patch.InsecureIgnoreHostKey != nil {
		updated.InsecureIgnoreHostKey = *patch.InsecureIgnoreHostKey
	}
	if patch.WorkDir != nil {
		updated.WorkDir = strings.TrimSpace(*patch.WorkDir)
	}
	if patch.WorkRoot != nil {
		updated.WorkRoot = strings.TrimSpace(*patch.WorkRoot)
	}
	if patch.Labels != nil {
		updated.Labels = *patch.Labels
	}
	if patch.Capacity != nil {
		if patch.Capacity.MaxWorkers != nil {
			updated.Capacity.MaxWorkers = *patch.Capacity.MaxWorkers
		}
		if patch.Capacity.CPUWeight != nil {
			updated.Capacity.CPUWeight = *patch.Capacity.CPUWeight
		}
		if patch.Capacity.MemoryGB != nil {
			updated.Capacity.MemoryGB = *patch.Capacity.MemoryGB
		}
	}
	updated.ID = current.ID
	return updated
}

func (s *Server) updateMCPPlugin(ctx context.Context, patch mcpPluginPatch) (core.Plugin, error) {
	pluginID := ""
	if patch.ID != nil {
		pluginID = strings.TrimSpace(*patch.ID)
	}
	if pluginID == "" {
		return core.Plugin{}, errors.New("id is required")
	}
	snapshot, err := s.service.Snapshot(ctx)
	if err != nil {
		return core.Plugin{}, err
	}
	for _, plugin := range snapshot.Plugins {
		if plugin.ID == pluginID {
			if plugin.BuiltIn {
				return core.Plugin{}, errors.New("built-in plugin cannot be replaced")
			}
			return s.service.RegisterPlugin(ctx, mergeMCPPluginPatch(plugin, patch))
		}
	}
	return core.Plugin{}, eventstore.ErrNotFound
}

func mergeMCPPluginPatch(current core.Plugin, patch mcpPluginPatch) core.Plugin {
	updated := current
	if patch.Name != nil {
		updated.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.Kind != nil {
		updated.Kind = strings.TrimSpace(*patch.Kind)
	}
	if patch.Protocol != nil {
		updated.Protocol = strings.TrimSpace(*patch.Protocol)
	}
	if patch.Enabled != nil {
		updated.Enabled = *patch.Enabled
	}
	if patch.Command != nil {
		updated.Command = *patch.Command
	}
	if patch.Endpoint != nil {
		updated.Endpoint = strings.TrimSpace(*patch.Endpoint)
	}
	if patch.Capabilities != nil {
		updated.Capabilities = *patch.Capabilities
	}
	if patch.Config != nil {
		updated.Config = *patch.Config
	}
	updated.Status = ""
	updated.Error = ""
	updated.ID = current.ID
	updated.BuiltIn = current.BuiltIn
	return updated
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
			Name:        "aged_task_detail",
			Title:       "Get task detail",
			Description: "Return a UI-parity task detail projection with related workers, execution nodes, PRs, recent events, apply policy, and available actions.",
			InputSchema: objectSchema(map[string]any{"taskId": stringSchema("Task id.")}, []string{"taskId"}),
		},
		{
			Name:        "aged_worker_detail",
			Title:       "Get worker detail",
			Description: "Return a worker detail projection with command, prompt, execution node, completion, recent worker events, and available actions.",
			InputSchema: objectSchema(map[string]any{"workerId": stringSchema("Worker id.")}, []string{"workerId"}),
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
			Name:        "aged_list_targets",
			Title:       "List execution targets",
			Description: "Return configured execution targets with live availability, running worker count, health, and resource signals.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "aged_create_target",
			Title:       "Create execution target",
			Description: "Register a local or SSH execution target using the same behavior as POST /api/targets. Registration persists the target and starts a health probe.",
			InputSchema: objectSchema(targetMCPProperties(), []string{"id"}),
		},
		{
			Name:        "aged_update_target",
			Title:       "Update execution target",
			Description: "Patch an existing execution target. Omitted fields are preserved; pass empty strings/maps or false only when intentionally setting or clearing that value. Numeric zero clears optional fields such as port and memoryGB; maxWorkers and cpuWeight normalize to at least 1. The update is persisted and starts a health probe.",
			InputSchema: objectSchema(targetMCPProperties(), []string{"id"}),
		},
		{
			Name:        "aged_delete_target",
			Title:       "Delete execution target",
			Description: "Delete an execution target. The daemon refuses to delete the only target or a target with running workers.",
			InputSchema: objectSchema(map[string]any{"targetId": stringSchema("Target id to delete.")}, []string{"targetId"}),
		},
		{
			Name:        "aged_target_health",
			Title:       "Refresh target health",
			Description: "Run or refresh the target health probe when supported and return the current target state.",
			InputSchema: objectSchema(map[string]any{"targetId": stringSchema("Target id to refresh.")}, []string{"targetId"}),
		},
		{
			Name:        "aged_list_plugins",
			Title:       "List plugins",
			Description: "Return built-in, configured, and dynamically registered plugins with status and driver lifecycle state.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "aged_create_plugin",
			Title:       "Register plugin",
			Description: "Register a plugin using the same behavior as POST /api/plugins. Command plugins may be probed and enabled driver plugins may start.",
			InputSchema: objectSchema(pluginMCPProperties(), []string{"id"}),
		},
		{
			Name:        "aged_update_plugin",
			Title:       "Update plugin",
			Description: "Patch an existing dynamically registered plugin. Omitted fields are preserved; pass empty strings/arrays/maps or false only when intentionally setting that value. Built-in plugins cannot be replaced.",
			InputSchema: objectSchema(pluginMCPProperties(), []string{"id"}),
		},
		{
			Name:        "aged_delete_plugin",
			Title:       "Delete plugin",
			Description: "Delete a dynamically registered plugin. Built-in plugins cannot be deleted.",
			InputSchema: objectSchema(map[string]any{"pluginId": stringSchema("Plugin id to delete.")}, []string{"pluginId"}),
		},
		{
			Name:        "aged_create_project",
			Title:       "Create aged project",
			Description: "Add a project/repository to the daemon.",
			InputSchema: objectSchema(map[string]any{
				"id":            stringSchema("Project id."),
				"name":          stringSchema("Display name."),
				"localPath":     stringSchema("Absolute path to local checkout on the daemon host."),
				"repo":          stringSchema("GitHub repo for the local checkout, such as fork-owner/name."),
				"upstreamRepo":  stringSchema("Optional upstream GitHub repo used as PR and issue target."),
				"headRepoOwner": stringSchema("Optional GitHub owner for fork PR heads."),
				"pushRemote":    stringSchema("Optional VCS remote to push PR branches/bookmarks to."),
				"vcs":           stringSchema("VCS kind: auto, jj, or git."),
				"defaultBase":   stringSchema("Default PR base branch."),
				"workspaceRoot": stringSchema("Optional workspace root override."),
				"targetLabels":  objectUntypedSchema("Optional target label policy."),
				"pullRequestPolicy": objectSchema(map[string]any{
					"branchPrefix": stringSchema("Optional PR branch prefix."),
					"draft":        map[string]any{"type": "boolean", "description": "Create PRs as draft by default."},
					"allowMerge":   map[string]any{"type": "boolean", "description": "Allow PR merge actions."},
					"autoMerge":    map[string]any{"type": "boolean", "description": "Enable auto-merge when available."},
				}, nil),
			}, []string{"id", "name", "localPath"}),
		},
		{
			Name:        "aged_update_project",
			Title:       "Update aged project",
			Description: "Patch an existing project/repository. Omitted fields are preserved; pass empty strings, empty maps, or false only when intentionally setting or clearing that value. Project normalization restores defaults for fields such as name, vcs, defaultBase, and pullRequestPolicy.branchPrefix when left empty.",
			InputSchema: objectSchema(map[string]any{
				"id":            stringSchema("Project id to update."),
				"name":          stringSchema("Display name."),
				"localPath":     stringSchema("Absolute path to local checkout on the daemon host."),
				"repo":          stringSchema("GitHub repo for the local checkout, such as fork-owner/name."),
				"upstreamRepo":  stringSchema("Optional upstream GitHub repo used as PR and issue target."),
				"headRepoOwner": stringSchema("Optional GitHub owner for fork PR heads."),
				"pushRemote":    stringSchema("Optional VCS remote to push PR branches/bookmarks to."),
				"vcs":           stringSchema("VCS kind: auto, jj, or git."),
				"defaultBase":   stringSchema("Default PR base branch."),
				"workspaceRoot": stringSchema("Optional workspace root override."),
				"targetLabels":  objectUntypedSchema("Optional target label policy."),
				"pullRequestPolicy": objectSchema(map[string]any{
					"branchPrefix": stringSchema("Optional PR branch prefix."),
					"draft":        map[string]any{"type": "boolean", "description": "Create PRs as draft by default."},
					"allowMerge":   map[string]any{"type": "boolean", "description": "Allow PR merge actions."},
					"autoMerge":    map[string]any{"type": "boolean", "description": "Enable auto-merge when available."},
				}, nil),
			}, []string{"id"}),
		},
		{
			Name:        "aged_delete_project",
			Title:       "Delete aged project",
			Description: "Delete an existing project. The daemon refuses to delete the last project or projects with nonterminal tasks.",
			InputSchema: objectSchema(map[string]any{"projectId": stringSchema("Project id to delete.")}, []string{"projectId"}),
		},
		{
			Name:        "aged_project_health",
			Title:       "Check project health",
			Description: "Run the dashboard project health/status check for path, VCS, GitHub auth/repo readiness, default base, and target policy.",
			InputSchema: objectSchema(map[string]any{"projectId": stringSchema("Project id to check.")}, []string{"projectId"}),
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
			Name:        "aged_watch_prs",
			Title:       "Watch existing PRs",
			Description: "Import existing GitHub PRs into an aged task so the GitHub monitor can babysit them as a long-running objective.",
			InputSchema: objectSchema(map[string]any{
				"taskId":     stringSchema("Task id that should own the watched PRs."),
				"repo":       stringSchema("GitHub repo, such as owner/name."),
				"number":     map[string]any{"type": "integer", "description": "Optional PR number for a single PR."},
				"url":        stringSchema("Optional PR URL. Can be used instead of repo plus number."),
				"state":      stringSchema("PR state for list mode: open, closed, merged, or all."),
				"author":     stringSchema("Optional author filter for list mode."),
				"headBranch": stringSchema("Optional head branch filter for list mode."),
				"limit":      map[string]any{"type": "integer", "description": "Optional max PR count for list mode."},
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
			Description: "Retry a failed or canceled task from its persisted plan.",
			InputSchema: objectSchema(map[string]any{"taskId": stringSchema("Task id.")}, []string{"taskId"}),
		},
		{
			Name:        "aged_clear_task",
			Title:       "Clear task",
			Description: "Hide a terminal task from active snapshots while keeping event history.",
			InputSchema: objectSchema(map[string]any{"taskId": stringSchema("Task id.")}, []string{"taskId"}),
		},
		{
			Name:        "aged_clear_finished_tasks",
			Title:       "Clear finished tasks",
			Description: "Hide all terminal tasks from active snapshots while keeping event history.",
			InputSchema: objectSchema(nil, nil),
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
		{
			Name:        "aged_cancel_worker",
			Title:       "Cancel worker",
			Description: "Cancel an active worker.",
			InputSchema: objectSchema(map[string]any{"workerId": stringSchema("Worker id.")}, []string{"workerId"}),
		},
		{
			Name:        "aged_review_worker_changes",
			Title:       "Review worker changes",
			Description: "Return a worker's workspace metadata and diff/changed-file summary.",
			InputSchema: objectSchema(map[string]any{"workerId": stringSchema("Worker id.")}, []string{"workerId"}),
		},
		{
			Name:        "aged_apply_worker_changes",
			Title:       "Apply worker changes",
			Description: "Manually apply a worker's changes to the source checkout.",
			InputSchema: objectSchema(map[string]any{"workerId": stringSchema("Worker id.")}, []string{"workerId"}),
		},
		{
			Name:        "aged_apply_task_result",
			Title:       "Apply task result",
			Description: "Apply the selected final task candidate locally.",
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

func targetMCPProperties() map[string]any {
	return map[string]any{
		"id":                    stringSchema("Target id."),
		"kind":                  stringSchema("Target kind: local or ssh."),
		"host":                  stringSchema("SSH host."),
		"user":                  stringSchema("Optional SSH user."),
		"port":                  map[string]any{"type": "integer", "description": "Optional SSH port."},
		"identityFile":          stringSchema("Optional SSH identity file path."),
		"insecureIgnoreHostKey": map[string]any{"type": "boolean", "description": "Skip SSH host key verification."},
		"workDir":               stringSchema("Checkout path on the target."),
		"workRoot":              stringSchema("Run directory root on the target."),
		"labels":                map[string]any{"type": "object", "description": "Target labels used for placement.", "additionalProperties": map[string]any{"type": "string"}},
		"capacity": objectSchema(map[string]any{
			"maxWorkers": map[string]any{"type": "integer", "description": "Maximum concurrent workers. Values less than 1 normalize to 1."},
			"cpuWeight":  map[string]any{"type": "number", "description": "Relative scheduling weight. Values less than or equal to 0 normalize to 1."},
			"memoryGB":   map[string]any{"type": "number", "description": "Approximate memory capacity. Use 0 to clear the optional memory capacity."},
		}, nil),
	}
}

func pluginMCPProperties() map[string]any {
	return map[string]any{
		"id":           stringSchema("Plugin id."),
		"name":         stringSchema("Display name."),
		"kind":         stringSchema("Plugin kind, such as driver, runner, integration, or external."),
		"protocol":     stringSchema("Plugin protocol, such as aged-plugin-v1 or aged-runner-v1."),
		"enabled":      map[string]any{"type": "boolean", "description": "Whether the plugin is enabled."},
		"command":      map[string]any{"type": "array", "description": "Command argv for command plugins.", "items": map[string]any{"type": "string"}},
		"endpoint":     stringSchema("Optional plugin endpoint."),
		"capabilities": map[string]any{"type": "array", "description": "Plugin capabilities.", "items": map[string]any{"type": "string"}},
		"config":       map[string]any{"type": "object", "description": "Plugin config key/value pairs.", "additionalProperties": map[string]any{"type": "string"}},
	}
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
