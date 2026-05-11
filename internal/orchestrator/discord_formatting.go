package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"aged/internal/core"
)

func discordTaskDetail(detail TaskDetail) string {
	var builder strings.Builder
	task := detail.Task
	builder.WriteString(fmt.Sprintf("Task `%s`: %s\n", task.ID, task.Title))
	builder.WriteString(fmt.Sprintf("Status: `%s`", task.Status))
	if task.ObjectiveStatus != "" {
		builder.WriteString(fmt.Sprintf(" / `%s`", task.ObjectiveStatus))
	}
	if task.ObjectivePhase != "" {
		builder.WriteString(" (" + task.ObjectivePhase + ")")
	}
	builder.WriteString("\n")
	if detail.Project != nil && detail.Project.ID != "" {
		builder.WriteString(fmt.Sprintf("Project: `%s`", detail.Project.ID))
		if detail.Project.Repo != "" {
			builder.WriteString(" (" + detail.Project.Repo + ")")
		}
		builder.WriteString("\n")
	} else if task.ProjectID != "" {
		builder.WriteString(fmt.Sprintf("Project: `%s`\n", task.ProjectID))
	}
	if task.Prompt != "" {
		builder.WriteString("Prompt: " + truncateText(task.Prompt, 500) + "\n")
	}
	if len(detail.Workers) > 0 {
		builder.WriteString(fmt.Sprintf("Workers: %d\n", len(detail.Workers)))
		for _, worker := range detail.Workers {
			builder.WriteString(fmt.Sprintf("- `%s` %s `%s`", shortDiscordID(worker.Worker.ID), worker.Worker.Kind, worker.Worker.Status))
			if worker.ExecutionNode != nil && worker.ExecutionNode.Role != "" {
				builder.WriteString(" " + worker.ExecutionNode.Role)
			}
			if len(worker.ChangedFiles) > 0 {
				builder.WriteString(fmt.Sprintf(", %d changed files", len(worker.ChangedFiles)))
			}
			if worker.Applied {
				builder.WriteString(", applied")
			}
			if worker.LatestEvent != nil {
				builder.WriteString(": " + truncateText(discordEventSummary(*worker.LatestEvent), 180))
			}
			builder.WriteString("\n")
		}
	}
	if len(detail.PullRequests) > 0 {
		builder.WriteString("Pull requests:\n")
		for _, pr := range detail.PullRequests {
			label := pr.URL
			if label == "" {
				label = pr.ID
			}
			builder.WriteString(fmt.Sprintf("- `%s` %s", pr.ID, label))
			if pr.State != "" {
				builder.WriteString(" " + pr.State)
			}
			if pr.ChecksStatus != "" || pr.ReviewStatus != "" || pr.MergeStatus != "" {
				builder.WriteString(fmt.Sprintf(" checks=%s review=%s merge=%s", nonEmpty(pr.ChecksStatus, "unknown"), nonEmpty(pr.ReviewStatus, "unknown"), nonEmpty(pr.MergeStatus, "unknown")))
			}
			builder.WriteString("\n")
		}
	}
	if len(detail.RecentEvents) > 0 {
		builder.WriteString("Recent events:\n")
		start := len(detail.RecentEvents) - 5
		if start < 0 {
			start = 0
		}
		for _, event := range detail.RecentEvents[start:] {
			builder.WriteString(fmt.Sprintf("- `%s` %s\n", event.Type, truncateText(discordEventSummary(event), 180)))
		}
	}
	if len(detail.AvailableActions) > 0 {
		builder.WriteString("Available actions: ")
		var actions []string
		for _, action := range detail.AvailableActions {
			actions = append(actions, "`"+action.Name+"`")
		}
		builder.WriteString(strings.Join(actions, ", "))
	}
	return strings.TrimSpace(builder.String())
}

func discordWorkerDetail(detail WorkerDetail) string {
	var builder strings.Builder
	worker := detail.Worker.Worker
	builder.WriteString(fmt.Sprintf("Worker `%s` for task `%s`\n", worker.ID, detail.Task.ID))
	builder.WriteString(fmt.Sprintf("Status: `%s`", worker.Status))
	if worker.Kind != "" {
		builder.WriteString(" kind=" + worker.Kind)
	}
	if detail.Worker.ExecutionNode != nil && detail.Worker.ExecutionNode.Role != "" {
		builder.WriteString(" role=" + detail.Worker.ExecutionNode.Role)
	}
	builder.WriteString("\n")
	if len(worker.Command) > 0 {
		builder.WriteString("Command: `" + truncateText(strings.Join(worker.Command, " "), 500) + "`\n")
	}
	if worker.Prompt != "" {
		builder.WriteString("Prompt: " + truncateText(worker.Prompt, 700) + "\n")
	}
	if len(detail.Worker.ChangedFiles) > 0 {
		builder.WriteString(fmt.Sprintf("Changed files: %d\n", len(detail.Worker.ChangedFiles)))
		for _, file := range detail.Worker.ChangedFiles {
			builder.WriteString(fmt.Sprintf("- `%s` %s\n", file.Status, file.Path))
		}
	}
	if detail.Worker.Applied {
		builder.WriteString("Applied: yes\n")
	}
	if len(detail.RecentEvents) > 0 {
		builder.WriteString("Recent events:\n")
		start := len(detail.RecentEvents) - 5
		if start < 0 {
			start = 0
		}
		for _, event := range detail.RecentEvents[start:] {
			builder.WriteString(fmt.Sprintf("- `%s` %s\n", event.Type, truncateText(discordEventSummary(event), 180)))
		}
	}
	if len(detail.Worker.AvailableActions) > 0 {
		builder.WriteString("Available actions: ")
		var actions []string
		for _, action := range detail.Worker.AvailableActions {
			actions = append(actions, "`"+action.Name+"`")
		}
		builder.WriteString(strings.Join(actions, ", "))
	}
	return strings.TrimSpace(builder.String())
}

func discordWorkerChangesReview(review WorkerChangesReview) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Worker `%s` changes\n", review.WorkerID))
	if review.Workspace.CWD != "" {
		builder.WriteString("Workspace: `" + review.Workspace.CWD + "`\n")
	}
	if review.Changes.Error != "" {
		builder.WriteString("Error: " + review.Changes.Error + "\n")
	}
	if len(review.Changes.ChangedFiles) > 0 {
		builder.WriteString(fmt.Sprintf("Changed files: %d\n", len(review.Changes.ChangedFiles)))
		for _, file := range review.Changes.ChangedFiles {
			builder.WriteString(fmt.Sprintf("- `%s` %s\n", file.Status, file.Path))
		}
	}
	if review.Changes.DiffStat != "" {
		builder.WriteString("Diff stat:\n```text\n" + truncateText(review.Changes.DiffStat, 700) + "\n```\n")
	}
	if review.Changes.Diff != "" {
		builder.WriteString("Diff:\n```diff\n" + truncateText(review.Changes.Diff, 1000) + "\n```")
	}
	return strings.TrimSpace(builder.String())
}

func discordPullRequestSummary(pr core.PullRequest) string {
	label := pr.URL
	if label == "" {
		label = pr.ID
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("- `%s` %s", pr.ID, label))
	if pr.Repo != "" && pr.Number > 0 {
		builder.WriteString(fmt.Sprintf(" (%s#%d)", pr.Repo, pr.Number))
	}
	if pr.State != "" {
		builder.WriteString(" state=" + pr.State)
	}
	if pr.ChecksStatus != "" || pr.ReviewStatus != "" || pr.MergeStatus != "" {
		builder.WriteString(fmt.Sprintf(" checks=%s review=%s merge=%s", nonEmpty(pr.ChecksStatus, "unknown"), nonEmpty(pr.ReviewStatus, "unknown"), nonEmpty(pr.MergeStatus, "unknown")))
	}
	return builder.String()
}

func discordPullRequestList(title string, prs []core.PullRequest) string {
	var builder strings.Builder
	builder.WriteString(title)
	for _, pr := range prs {
		builder.WriteString("\n")
		builder.WriteString(discordPullRequestSummary(pr))
	}
	return strings.TrimSpace(builder.String())
}

const (
	discordAssistantTaskLimit    = 50
	discordAssistantWorkerLimit  = 80
	discordAssistantNodeLimit    = 80
	discordAssistantPRLimit      = 50
	discordAssistantEventLimit   = 30
	discordAssistantPromptLimit  = 600
	discordAssistantSummaryLimit = 320
)

func discordAssistantIndex(channelID string, userID string, selectedProject core.Project, snapshot core.Snapshot) map[string]any {
	return map[string]any{
		"channelId":       channelID,
		"userId":          userID,
		"selectedProject": selectedProject,
		"detailActions": map[string]string{
			"show_task":             "Fetch detailed task status, prompt, workers, pull requests, recent events, and available actions by taskId.",
			"show_worker":           "Fetch detailed worker command, prompt, changed files, recent events, and available actions by workerId.",
			"review_worker_changes": "Fetch worker workspace and diff summary by workerId.",
			"project_health":        "Fetch project checkout/readiness details by projectId.",
			"target_health":         "Fetch target reachability/resource details by targetId.",
			"refresh_pr":            "Refresh and fetch pull request status by pullRequestId.",
		},
		"counts": map[string]int{
			"projects":        len(snapshot.Projects),
			"tasks":           len(snapshot.Tasks),
			"workers":         len(snapshot.Workers),
			"executionNodes":  len(snapshot.ExecutionNodes),
			"targets":         len(snapshot.Targets),
			"plugins":         len(snapshot.Plugins),
			"pullRequests":    len(snapshot.PullRequests),
			"events":          len(snapshot.Events),
			"includedTasks":   min(len(snapshot.Tasks), discordAssistantTaskLimit),
			"includedWorkers": min(len(snapshot.Workers), discordAssistantWorkerLimit),
			"includedEvents":  min(len(snapshot.Events), discordAssistantEventLimit),
		},
		"projects":       snapshot.Projects,
		"targets":        compactDiscordTargets(snapshot.Targets),
		"plugins":        compactDiscordPlugins(snapshot.Plugins),
		"tasks":          compactDiscordTasks(snapshot.Tasks, discordAssistantTaskLimit),
		"workers":        compactDiscordWorkers(snapshot.Workers, discordAssistantWorkerLimit),
		"executionNodes": compactDiscordExecutionNodes(snapshot.ExecutionNodes, discordAssistantNodeLimit),
		"pullRequests":   compactDiscordPullRequests(snapshot.PullRequests, discordAssistantPRLimit),
		"recentEvents":   compactDiscordEventSummaries(snapshot.Events, discordAssistantEventLimit),
	}
}

func compactDiscordTasks(tasks []core.Task, limit int) []map[string]any {
	if limit > 0 && len(tasks) > limit {
		tasks = tasks[len(tasks)-limit:]
	}
	out := make([]map[string]any, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, map[string]any{
			"id":                     task.ID,
			"shortId":                shortDiscordID(task.ID),
			"projectId":              task.ProjectID,
			"title":                  task.Title,
			"prompt":                 truncateText(task.Prompt, discordAssistantPromptLimit),
			"status":                 task.Status,
			"error":                  truncateText(task.Error, discordAssistantSummaryLimit),
			"objectiveStatus":        task.ObjectiveStatus,
			"objectivePhase":         task.ObjectivePhase,
			"finalCandidateWorkerId": task.FinalCandidateWorkerID,
			"appliedWorkerId":        task.AppliedWorkerID,
			"updatedAt":              task.UpdatedAt,
		})
	}
	return out
}

func compactDiscordWorkers(workers []core.Worker, limit int) []map[string]any {
	if limit > 0 && len(workers) > limit {
		workers = workers[len(workers)-limit:]
	}
	out := make([]map[string]any, 0, len(workers))
	for _, worker := range workers {
		prompt := strings.TrimSpace(worker.Prompt)
		out = append(out, map[string]any{
			"id":              worker.ID,
			"shortId":         shortDiscordID(worker.ID),
			"taskId":          worker.TaskID,
			"kind":            worker.Kind,
			"status":          worker.Status,
			"command":         worker.Command,
			"prompt":          truncateText(prompt, discordAssistantPromptLimit),
			"promptTruncated": len(prompt) > discordAssistantPromptLimit,
			"promptPath":      worker.PromptPath,
			"promptError":     truncateText(worker.PromptError, discordAssistantSummaryLimit),
			"createdAt":       worker.CreatedAt,
			"updatedAt":       worker.UpdatedAt,
		})
	}
	return out
}

func compactDiscordExecutionNodes(nodes []core.ExecutionNode, limit int) []map[string]any {
	if limit > 0 && len(nodes) > limit {
		nodes = nodes[len(nodes)-limit:]
	}
	out := make([]map[string]any, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, map[string]any{
			"id":           node.ID,
			"taskId":       node.TaskID,
			"workerId":     node.WorkerID,
			"workerKind":   node.WorkerKind,
			"status":       node.Status,
			"parentNodeId": node.ParentNodeID,
			"spawnId":      node.SpawnID,
			"role":         node.Role,
			"reason":       truncateText(node.Reason, discordAssistantSummaryLimit),
			"targetId":     node.TargetID,
			"targetKind":   node.TargetKind,
			"updatedAt":    node.UpdatedAt,
		})
	}
	return out
}

func compactDiscordPullRequests(prs []core.PullRequest, limit int) []map[string]any {
	if limit > 0 && len(prs) > limit {
		prs = prs[len(prs)-limit:]
	}
	out := make([]map[string]any, 0, len(prs))
	for _, pr := range prs {
		out = append(out, map[string]any{
			"id":               pr.ID,
			"shortId":          shortDiscordID(pr.ID),
			"taskId":           pr.TaskID,
			"repo":             pr.Repo,
			"number":           pr.Number,
			"url":              pr.URL,
			"branch":           pr.Branch,
			"base":             pr.Base,
			"title":            pr.Title,
			"state":            pr.State,
			"draft":            pr.Draft,
			"checksStatus":     pr.ChecksStatus,
			"checksConclusion": pr.ChecksConclusion,
			"mergeStatus":      pr.MergeStatus,
			"mergeable":        pr.Mergeable,
			"reviewStatus":     pr.ReviewStatus,
			"babysitterTaskId": pr.BabysitterTaskID,
			"updatedAt":        pr.UpdatedAt,
		})
	}
	return out
}

func compactDiscordTargets(targets []core.TargetState) []map[string]any {
	out := make([]map[string]any, 0, len(targets))
	for _, target := range targets {
		out = append(out, map[string]any{
			"id":        target.ID,
			"kind":      target.Kind,
			"host":      target.Host,
			"labels":    target.Labels,
			"running":   target.Running,
			"available": target.Available,
			"health": map[string]any{
				"status":    target.Health.Status,
				"error":     truncateText(target.Health.Error, discordAssistantSummaryLimit),
				"reachable": target.Health.Reachable,
			},
		})
	}
	return out
}

func compactDiscordPlugins(plugins []core.Plugin) []map[string]any {
	out := make([]map[string]any, 0, len(plugins))
	for _, plugin := range plugins {
		out = append(out, map[string]any{
			"id":           plugin.ID,
			"name":         plugin.Name,
			"kind":         plugin.Kind,
			"protocol":     plugin.Protocol,
			"enabled":      plugin.Enabled,
			"builtIn":      plugin.BuiltIn,
			"status":       plugin.Status,
			"error":        truncateText(plugin.Error, discordAssistantSummaryLimit),
			"capabilities": plugin.Capabilities,
		})
	}
	return out
}

func compactDiscordEventSummaries(events []core.Event, limit int) []map[string]any {
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	out := make([]map[string]any, 0, len(events))
	for _, event := range events {
		out = append(out, map[string]any{
			"id":               event.ID,
			"at":               event.At,
			"type":             event.Type,
			"taskId":           event.TaskID,
			"workerId":         event.WorkerID,
			"summary":          truncateText(discordEventSummary(event), discordAssistantSummaryLimit),
			"payloadTruncated": len(event.Payload) > 0,
		})
	}
	return out
}

func discordApplyResult(prefix string, result WorkerApplyResult) string {
	var builder strings.Builder
	builder.WriteString(prefix)
	builder.WriteString(fmt.Sprintf("\nWorker: `%s`", result.WorkerID))
	if result.Method != "" {
		builder.WriteString(" method=" + result.Method)
	}
	if result.SourceRoot != "" {
		builder.WriteString("\nSource: `" + result.SourceRoot + "`")
	}
	if len(result.AppliedFiles) > 0 {
		builder.WriteString(fmt.Sprintf("\nApplied files: %d", len(result.AppliedFiles)))
		for _, file := range result.AppliedFiles {
			builder.WriteString(fmt.Sprintf("\n- `%s` %s", file.Status, file.Path))
		}
	}
	if len(result.SkippedFiles) > 0 {
		builder.WriteString(fmt.Sprintf("\nSkipped files: %d", len(result.SkippedFiles)))
	}
	return strings.TrimSpace(builder.String())
}

func discordEventSummary(event core.Event) string {
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return strings.TrimSpace(string(event.Payload))
	}
	for _, key := range []string{"summary", "text", "error", "status", "reason", "question", "message", "title"} {
		if value, ok := payload[key]; ok {
			if text := strings.TrimSpace(fmt.Sprint(value)); text != "" {
				return text
			}
		}
	}
	if raw, err := json.Marshal(payload); err == nil {
		return string(raw)
	}
	return ""
}

func compactDiscordEvents(events []core.Event, limit int) []core.Event {
	if limit <= 0 || len(events) <= limit {
		return events
	}
	return events[len(events)-limit:]
}

func discordProjectList(projects []core.Project) string {
	if len(projects) == 0 {
		return "No projects are configured."
	}
	var builder strings.Builder
	builder.WriteString("Configured projects:\n")
	for _, project := range projects {
		builder.WriteString("- `")
		builder.WriteString(project.ID)
		builder.WriteString("`")
		if strings.TrimSpace(project.Name) != "" && project.Name != project.ID {
			builder.WriteString(" - ")
			builder.WriteString(project.Name)
		}
		if strings.TrimSpace(project.Repo) != "" {
			builder.WriteString(" (")
			builder.WriteString(project.Repo)
			builder.WriteString(")")
		}
		if strings.TrimSpace(project.LocalPath) != "" {
			builder.WriteString("\n  ")
			builder.WriteString(project.LocalPath)
		}
		builder.WriteString("\n")
	}
	return strings.TrimSpace(builder.String())
}

func discordTargetList(targets []core.TargetState) string {
	if len(targets) == 0 {
		return "No execution targets are configured."
	}
	var builder strings.Builder
	builder.WriteString("Configured targets:\n")
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("- `%s` %s", target.ID, nonEmpty(target.Kind, "local")))
		if target.Host != "" {
			builder.WriteString(" " + target.Host)
		}
		builder.WriteString(fmt.Sprintf(" running=%d available=%t", target.Running, target.Available))
		if target.Health.Status != "" {
			builder.WriteString(" health=`" + target.Health.Status + "`")
		}
		if len(target.Labels) > 0 {
			builder.WriteString(" labels=" + compactStringMap(target.Labels))
		}
		builder.WriteString("\n")
	}
	return strings.TrimSpace(builder.String())
}

func discordPluginList(plugins []core.Plugin) string {
	if len(plugins) == 0 {
		return "No plugins are configured."
	}
	var builder strings.Builder
	builder.WriteString("Configured plugins:\n")
	for _, plugin := range plugins {
		builder.WriteString(fmt.Sprintf("- `%s` %s enabled=%t", plugin.ID, nonEmpty(plugin.Kind, "external"), plugin.Enabled))
		if plugin.Name != "" && plugin.Name != plugin.ID {
			builder.WriteString(" - " + plugin.Name)
		}
		if plugin.BuiltIn {
			builder.WriteString(" built-in")
		}
		if plugin.Status != "" {
			builder.WriteString(" status=`" + plugin.Status + "`")
		}
		if plugin.Error != "" {
			builder.WriteString(" error=" + truncateText(plugin.Error, 120))
		}
		builder.WriteString("\n")
	}
	return strings.TrimSpace(builder.String())
}

func discordProjectHealth(health core.ProjectHealth) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Project `%s` health: ", health.ProjectID))
	if health.OK {
		builder.WriteString("healthy")
	} else {
		builder.WriteString("needs attention")
	}
	builder.WriteString(fmt.Sprintf("\n- path: `%s`", nonEmpty(health.PathStatus, "unknown")))
	builder.WriteString(fmt.Sprintf("\n- vcs: `%s`", nonEmpty(health.VCSStatus, "unknown")))
	if health.DetectedVCS != "" {
		builder.WriteString(" detected `" + health.DetectedVCS + "`")
	}
	if health.GitHubStatus != "" {
		builder.WriteString(fmt.Sprintf("\n- github: `%s`", health.GitHubStatus))
		if health.DetectedRepo != "" {
			builder.WriteString(" detected `" + health.DetectedRepo + "`")
		}
	}
	if health.DefaultBaseStatus != "" {
		builder.WriteString(fmt.Sprintf("\n- base: `%s`", health.DefaultBaseStatus))
		if health.DetectedBase != "" {
			builder.WriteString(" detected `" + health.DetectedBase + "`")
		}
	}
	if health.TargetStatus != "" {
		builder.WriteString(fmt.Sprintf("\n- target: `%s`", health.TargetStatus))
	}
	for _, err := range health.Errors {
		builder.WriteString("\n- " + truncateText(err, 220))
	}
	return strings.TrimSpace(builder.String())
}

func discordTargetHealth(target core.TargetState) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Target `%s` health: `%s`", target.ID, nonEmpty(target.Health.Status, "unknown")))
	builder.WriteString(fmt.Sprintf("\n- kind: `%s`", nonEmpty(target.Kind, "local")))
	builder.WriteString(fmt.Sprintf("\n- running: `%d` available: `%t`", target.Running, target.Available))
	if target.Host != "" {
		builder.WriteString("\n- host: `" + target.Host + "`")
	}
	if !target.Health.CheckedAt.IsZero() {
		builder.WriteString("\n- checked: `" + target.Health.CheckedAt.Format(time.RFC3339) + "`")
	}
	builder.WriteString(fmt.Sprintf("\n- reachable: `%t` tmux: `%t` repo: `%t`", target.Health.Reachable, target.Health.Tmux, target.Health.RepoPresent))
	if target.Resources.CPUCount > 0 {
		builder.WriteString(fmt.Sprintf("\n- cpu/load: `%d` cpu, load1 `%.2f`", target.Resources.CPUCount, target.Resources.Load1))
	}
	if target.Resources.MemoryTotalMB > 0 || target.Resources.MemoryAvailableMB > 0 {
		builder.WriteString(fmt.Sprintf("\n- memory: `%d` MB available / `%d` MB total", target.Resources.MemoryAvailableMB, target.Resources.MemoryTotalMB))
	}
	if target.Resources.DiskAvailableMB > 0 || target.Resources.DiskUsedPercent > 0 {
		builder.WriteString(fmt.Sprintf("\n- disk: `%d` MB available, `%.1f%%` used", target.Resources.DiskAvailableMB, target.Resources.DiskUsedPercent))
	}
	if len(target.Health.Tools) > 0 {
		builder.WriteString("\n- tools: " + compactBoolMap(target.Health.Tools))
	}
	if target.Health.Error != "" {
		builder.WriteString("\n- " + truncateText(target.Health.Error, 220))
	}
	return strings.TrimSpace(builder.String())
}
