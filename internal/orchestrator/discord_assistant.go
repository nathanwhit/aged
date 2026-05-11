package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"

	"aged/internal/core"
)

func discordAssistantPrompt(content string) string {
	return fmt.Sprintf(`You are the natural-language Discord interface for aged, a durable autonomous development orchestrator.

Answer the user's question using the provided aged index context and, when useful, by inspecting files in the current read-only project checkout. Do not edit files from chat; create a task instead when code changes are needed.

The context is an index, not the full daemon state. It intentionally contains compact recent IDs, statuses, and summaries. When the user needs deeper task logs, worker prompts, worker diffs, project health, target health, or PR refresh data, return the appropriate structured action such as "show_task", "show_worker", "review_worker_changes", "project_health", "target_health", or "refresh_pr". The Discord driver will run that detail fetch and send the result. Do not guess details that are not present in the index.

Return exactly one JSON object with this schema and no Markdown fence:

{
  "action": "answer | list_projects | list_targets | list_plugins | show_task | show_worker | create_project | update_project | delete_project | project_health | create_target | update_target | delete_target | target_health | create_plugin | update_plugin | delete_plugin | propose_task | create_task | retry_task | steer_task | cancel_task | cancel_worker | clear_task | clear_finished_tasks | publish_pr | watch_prs | refresh_pr | babysit_pr | review_worker_changes | apply_task_result | apply_worker_changes",
  "reply": "short Discord-ready message to send to the user",
  "taskId": "task id to inspect when action is show_task",
  "workerId": "worker id for worker actions",
  "pullRequestId": "aged pull request id for PR actions",
  "projectId": "configured project id for project actions",
  "targetId": "configured target id for target actions",
  "pluginId": "configured plugin id for plugin actions",
  "message": "steering or feedback message when action is steer_task",
  "confirmed": false,
  "project": {
    "id": "short stable project id",
    "name": "human project name",
    "localPath": "/absolute/path/to/local/checkout",
    "repo": "optional owner/repo",
    "upstreamRepo": "optional upstream owner/repo",
    "headRepoOwner": "optional fork owner",
    "pushRemote": "optional VCS push remote",
    "vcs": "optional auto | jj | git",
    "defaultBase": "optional default branch",
    "workspaceRoot": "optional workspace root override",
    "targetLabels": {},
    "remoteCheckouts": {"target-id": "optional project checkout path override on that SSH target"},
    "pullRequestPolicy": {
      "branchPrefix": "optional PR branch prefix",
      "draft": false,
      "allowMerge": false,
      "autoMerge": false
    }
  },
  "target": {
    "id": "short stable target id",
    "kind": "local | ssh",
    "host": "ssh host for ssh targets",
    "user": "optional ssh user",
    "port": 22,
    "identityFile": "optional ssh identity file path",
    "insecureIgnoreHostKey": false,
    "checkoutRoot": "root directory for derived per-project checkouts on the target",
    "workDir": "compatibility alias for checkoutRoot",
    "workRoot": "worker run root on the target",
    "labels": {},
    "capacity": {
      "maxWorkers": 1,
      "cpuWeight": 1,
      "memoryGB": 0
    }
  },
  "plugin": {
    "id": "stable plugin id",
    "name": "human plugin name",
    "kind": "driver | runner | integration | external",
    "protocol": "optional protocol such as aged-plugin-v1 or aged-runner-v1",
    "enabled": false,
    "command": ["optional", "command", "argv"],
    "endpoint": "optional endpoint",
    "capabilities": ["optional capabilities"],
    "config": {}
  },
  "proposedTask": {
    "projectId": "one configured project id, or omit when the default project is correct",
    "title": "optional short task title",
    "prompt": "specific prompt to create as an aged task if the user replies do it",
    "completionMode": "github | local"
  },
  "publishPr": {
    "workerId": "optional worker id",
    "repo": "optional owner/repo override",
    "base": "optional base branch",
    "branch": "optional branch name",
    "title": "optional pull request title",
    "body": "optional pull request body",
    "draft": false
  },
  "watchPrs": {
    "repo": "optional owner/repo",
    "number": 0,
    "url": "optional pull request URL",
    "state": "open",
    "author": "optional author filter",
    "headBranch": "optional head branch filter",
    "limit": 0
  }
}

Use "answer" for questions and discussion. Use "list_projects", "list_targets", or "list_plugins" when the user asks what is configured. Use "project_health" when the user asks for health/status/readiness of a configured project; set projectId to an exact id from the project list, or omit it only when the selected project is clearly intended. Use "target_health" when the user asks for health/status/resources/readiness of a configured execution target; set targetId to an exact id from the target list. Use "update_project" when the user asks to edit a configured project; set projectId to the exact existing project id and include changed project fields in "project" while preserving unrelated fields by omission. Empty project name, vcs, defaultBase, and pullRequestPolicy.branchPrefix are normalized back to service defaults. Use "update_target" or "update_plugin" similarly for configured targets and plugins; set targetId/pluginId exactly and include only changed fields when possible. For update actions, omit unchanged fields inside the selected project/target/plugin object; include empty strings, empty arrays/maps, false booleans, or numeric zero only when the user explicitly wants to clear or set that value. For target capacity, maxWorkers and cpuWeight must be positive and zero normalizes to one; memoryGB zero clears optional memory capacity. Use "delete_project", "delete_target", or "delete_plugin" when the user asks to remove a configured item; set the exact id and set confirmed true only when the user explicitly confirms deletion. Use "show_task" when the user asks for status/details/logs/workers/PRs/actions for one identifiable task; set taskId to the exact id from the snapshot. Use "show_worker" or "review_worker_changes" when the user asks for one identifiable worker's details or diff; set workerId exactly. Use task control actions when the user asks to retry, steer, cancel, clear, publish a PR, watch PRs, refresh a PR, babysit a PR, or apply results. For "steer_task", set message to the exact feedback or answer that should be sent to the task. For PR actions, use the exact aged pullRequestId from the snapshot when one exists; for "watch_prs", set taskId and fill watchPrs from the user's repo/number/url filters. For "publish_pr", set taskId and optional publishPr fields. For "publish_pr", "apply_task_result", and "apply_worker_changes", set confirmed true only when the user explicitly confirms publishing or applying changes; otherwise leave confirmed false so the bot can ask for confirmation. Use "create_project" when the user clearly asks to add/register a project and provides at least an id or name plus a local checkout path; otherwise ask a follow-up for the missing fields. Use "create_target" when the user clearly asks to add/register a target and provides an id, plus a host for ssh targets. Use "create_plugin" when the user clearly asks to add/register a plugin and provides an id. Use "propose_task" when a task is plausible but the user has not clearly decided to run it. Use "create_task" when the conversation clearly asks aged to start doing work, even if the user does not literally say "create a task". Omit proposedTask.completionMode to use the default GitHub PR completion; set it to "local" only when the user explicitly asks for local-only/no-PR completion. Set unrelated top-level object fields to null or empty values. If the user asks for work in a repo/project and multiple projects could match, ask a concise follow-up in "reply", set "action" to "answer", and set "proposedTask" to null. Only use ids that appear in the provided project, target, and plugin lists for update/delete/health actions.

User message:
%s`, content)
}

func parseDiscordAssistantResponse(message string) DiscordAssistantDecision {
	if decision, ok := parseDiscordAssistantJSON(message); ok {
		return decision
	}
	marker := "AGED_TASK_PROMPT:"
	index := strings.LastIndex(message, marker)
	if index < 0 {
		return DiscordAssistantDecision{Action: "answer", Reply: strings.TrimSpace(message)}
	}
	reply := strings.TrimSpace(message[:index])
	prompt := strings.TrimSpace(message[index+len(marker):])
	if reply == "" {
		reply = "I can run that. Reply `do it` to create the task."
	} else {
		reply += "\n\nReply `do it` to create the task."
	}
	return DiscordAssistantDecision{
		Action:   "propose_task",
		Reply:    reply,
		Proposal: DiscordTaskProposal{Prompt: prompt},
	}
}

func parseDiscordAssistantJSON(message string) (DiscordAssistantDecision, bool) {
	raw := strings.TrimSpace(message)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.HasPrefix(raw, "{") {
		return DiscordAssistantDecision{}, false
	}
	var payload struct {
		Action        string                          `json:"action"`
		Reply         string                          `json:"reply"`
		TaskID        string                          `json:"taskId"`
		WorkerID      string                          `json:"workerId"`
		PullRequestID string                          `json:"pullRequestId"`
		ProjectID     string                          `json:"projectId"`
		TargetID      string                          `json:"targetId"`
		PluginID      string                          `json:"pluginId"`
		Message       string                          `json:"message"`
		Confirmed     bool                            `json:"confirmed"`
		Project       json.RawMessage                 `json:"project"`
		Target        json.RawMessage                 `json:"target"`
		Plugin        json.RawMessage                 `json:"plugin"`
		ProposedTask  *DiscordTaskProposal            `json:"proposedTask"`
		PublishPR     *core.PublishPullRequestRequest `json:"publishPr"`
		WatchPRs      *core.WatchPullRequestsRequest  `json:"watchPrs"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return DiscordAssistantDecision{}, false
	}
	reply := strings.TrimSpace(payload.Reply)
	if reply == "" {
		reply = "I can run that. Reply `do it` to create the task."
	}
	action := strings.TrimSpace(strings.ToLower(payload.Action))
	if action == "" {
		action = "answer"
	}
	var proposal DiscordTaskProposal
	if payload.ProposedTask != nil {
		proposal = *payload.ProposedTask
		proposal.ProjectID = strings.TrimSpace(proposal.ProjectID)
		proposal.Title = strings.TrimSpace(proposal.Title)
		proposal.Prompt = strings.TrimSpace(proposal.Prompt)
		proposal.CompletionMode = strings.ToLower(strings.TrimSpace(proposal.CompletionMode))
		if action == "answer" && proposal.Prompt != "" {
			action = "propose_task"
		}
		if action == "propose_task" && proposal.Prompt != "" {
			reply += "\n\nReply `do it` to create the task."
		}
	}
	switch action {
	case "answer", "list_projects", "list_targets", "list_plugins", "show_task", "show_worker",
		"create_project", "update_project", "delete_project", "project_health",
		"create_target", "update_target", "delete_target", "target_health",
		"create_plugin", "update_plugin", "delete_plugin", "propose_task", "create_task",
		"retry_task", "steer_task", "cancel_task", "cancel_worker", "clear_task", "clear_finished_tasks",
		"publish_pr", "watch_prs", "refresh_pr", "babysit_pr", "review_worker_changes", "apply_task_result", "apply_worker_changes":
	default:
		action = "answer"
	}
	var project core.Project
	var projectPatch discordProjectPatch
	if hasDiscordObject(payload.Project) {
		_ = json.Unmarshal(payload.Project, &project)
		_ = json.Unmarshal(payload.Project, &projectPatch)
	}
	var target core.TargetConfig
	var targetPatch discordTargetPatch
	if hasDiscordObject(payload.Target) {
		_ = json.Unmarshal(payload.Target, &target)
		_ = json.Unmarshal(payload.Target, &targetPatch)
	}
	var plugin core.Plugin
	var pluginPatch discordPluginPatch
	if hasDiscordObject(payload.Plugin) {
		_ = json.Unmarshal(payload.Plugin, &plugin)
		_ = json.Unmarshal(payload.Plugin, &pluginPatch)
	}
	var publishPR core.PublishPullRequestRequest
	if payload.PublishPR != nil {
		publishPR = *payload.PublishPR
	}
	var watchPRs core.WatchPullRequestsRequest
	if payload.WatchPRs != nil {
		watchPRs = *payload.WatchPRs
	}
	return DiscordAssistantDecision{
		Action:        action,
		Reply:         reply,
		TaskID:        strings.TrimSpace(payload.TaskID),
		WorkerID:      strings.TrimSpace(payload.WorkerID),
		PullRequestID: strings.TrimSpace(payload.PullRequestID),
		ProjectID:     strings.TrimSpace(payload.ProjectID),
		TargetID:      strings.TrimSpace(payload.TargetID),
		PluginID:      strings.TrimSpace(payload.PluginID),
		Message:       strings.TrimSpace(payload.Message),
		Confirmed:     payload.Confirmed,
		Proposal:      proposal,
		Project:       project,
		ProjectPatch:  projectPatch,
		Target:        target,
		TargetPatch:   targetPatch,
		Plugin:        plugin,
		PluginPatch:   pluginPatch,
		PublishPR:     publishPR,
		WatchPRs:      watchPRs,
	}, true
}

func hasDiscordObject(raw json.RawMessage) bool {
	value := strings.TrimSpace(string(raw))
	return value != "" && value != "null"
}
