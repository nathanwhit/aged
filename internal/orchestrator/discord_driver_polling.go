package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"aged/internal/core"
)

func (d *DiscordDriver) Run(ctx context.Context) {
	if d == nil || !d.config.Enabled {
		return
	}
	d.runOnceLogged(ctx)
	ticker := time.NewTicker(time.Duration(d.config.IntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runOnceLogged(ctx)
		}
	}
}

func (d *DiscordDriver) runOnceLogged(ctx context.Context) {
	if err := d.RunOnce(ctx); err != nil {
		slog.Warn("discord driver poll failed", "error", err)
	}
}

func (d *DiscordDriver) RunOnce(ctx context.Context) error {
	if d == nil || d.service == nil || d.client == nil || !d.config.Enabled {
		return nil
	}
	if err := d.ensureBotID(ctx); err != nil {
		return err
	}
	var errs []string
	for _, channel := range d.config.Channels {
		if strings.TrimSpace(channel.ID) == "" {
			continue
		}
		if err := d.pollChannel(ctx, channel); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", channel.ID, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (d *DiscordDriver) ensureBotID(ctx context.Context) error {
	d.mu.Lock()
	if d.botID != "" {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()
	me, err := d.client.Me(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.botID = me.ID
	d.mu.Unlock()
	return nil
}

func (d *DiscordDriver) pollChannel(ctx context.Context, channel DiscordChannelConfig) error {
	d.mu.Lock()
	afterID := d.lastSeen[channel.ID]
	d.mu.Unlock()
	messages, err := d.client.ListMessages(ctx, channel.ID, afterID, d.config.MessageLimit)
	if err != nil {
		return err
	}
	slices.Reverse(messages)
	if len(messages) == 0 {
		d.markInitialized(channel.ID)
		return nil
	}
	if !d.isInitialized(channel.ID) && !d.config.ProcessHistory {
		d.setLastSeen(channel.ID, messages[len(messages)-1].ID)
		d.markInitialized(channel.ID)
		return nil
	}
	for _, message := range messages {
		if message.ID == "" {
			continue
		}
		d.setLastSeen(channel.ID, message.ID)
		if err := d.handleMessage(ctx, channel, message); err != nil {
			return err
		}
	}
	d.markInitialized(channel.ID)
	return nil
}

func (d *DiscordDriver) handleMessage(ctx context.Context, channel DiscordChannelConfig, message DiscordMessage) error {
	if message.Author.Bot || message.Author.ID == d.botID {
		return nil
	}
	if len(channel.AllowedUserIDs) > 0 && !slices.Contains(channel.AllowedUserIDs, message.Author.ID) {
		return nil
	}
	content := strings.TrimSpace(message.Content)
	if content == "" {
		return nil
	}
	if channel.RequireMention {
		mention := "<@" + d.botID + ">"
		nickMention := "<@!" + d.botID + ">"
		if !strings.Contains(content, mention) && !strings.Contains(content, nickMention) {
			return nil
		}
		content = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(content, mention, ""), nickMention, ""))
	}
	switch {
	case strings.HasPrefix(strings.ToLower(content), strings.ToLower(channel.TaskPrefix)):
		prompt := strings.TrimSpace(content[len(channel.TaskPrefix):])
		snapshot, _ := d.service.Snapshot(ctx)
		project := d.selectDiscordProject(channel, message.Author.ID, prompt, snapshot.Projects)
		return d.createDiscordTask(ctx, channel, message, DiscordTaskProposal{
			ProjectID: project.ID,
			Prompt:    prompt,
		})
	case isDiscordDoIt(content):
		proposal := d.savedTaskProposal(ctx, channel.ID, message.Author.ID)
		if strings.TrimSpace(proposal.Prompt) == "" {
			return d.client.SendMessage(ctx, channel.ID, "I do not have a proposed task to run yet. Ask about the work first, or use `task: <prompt>`.")
		}
		return d.createDiscordTask(ctx, channel, message, proposal)
	default:
		return d.answerDiscordMessage(ctx, channel, message, content)
	}
}

func (d *DiscordDriver) answerDiscordMessage(ctx context.Context, channel DiscordChannelConfig, message DiscordMessage, content string) error {
	snapshot, _ := d.service.Snapshot(ctx)
	project := d.selectDiscordProject(channel, message.Author.ID, content, snapshot.Projects)
	conversationID := discordConversationID(channel.ID, message.Author.ID, project.ID)
	response, err := d.service.Ask(ctx, core.AssistantRequest{
		ConversationID: conversationID,
		Message:        discordAssistantPrompt(content),
		WorkDir:        project.LocalPath,
		Context: core.MustJSON(map[string]any{
			"source": "discord",
			"index":  discordAssistantIndex(channel.ID, message.Author.ID, project, snapshot),
		}),
	})
	if err != nil {
		d.saveTaskProposal(channel.ID, message.Author.ID, DiscordTaskProposal{
			ProjectID: channelDefaultProjectID(channel),
			Prompt:    content,
		})
		return d.client.SendMessage(ctx, channel.ID, "I can hand this to aged as a task, but the interactive assistant is not configured well enough to answer conversationally right now. Reply `do it` to create a task from your message, or use `task: <prompt>`.")
	}
	decision := parseDiscordAssistantResponse(response.Message)
	if resolved, prompt := resolveDiscordDecision(snapshot, decision, content); prompt != "" {
		return d.client.SendMessage(ctx, channel.ID, prompt)
	} else {
		decision = resolved
	}
	switch decision.Action {
	case "list_projects":
		return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordProjectList(snapshot.Projects)))
	case "list_targets":
		return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordTargetList(snapshot.Targets)))
	case "create_target":
		return d.createDiscordTarget(ctx, channel, decision.Target)
	case "update_target":
		return d.updateDiscordTarget(ctx, channel, decision.TargetID, decision.Target, decision.TargetPatch)
	case "delete_target":
		return d.deleteDiscordTarget(ctx, channel, decision.TargetID, decision.Target.ID, decision.Confirmed)
	case "target_health":
		return d.sendDiscordTargetHealth(ctx, channel, nonEmpty(decision.TargetID, decision.Target.ID))
	case "list_plugins":
		return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordPluginList(snapshot.Plugins)))
	case "create_plugin":
		return d.createDiscordPlugin(ctx, channel, decision.Plugin)
	case "update_plugin":
		return d.updateDiscordPlugin(ctx, channel, decision.PluginID, decision.Plugin, decision.PluginPatch)
	case "delete_plugin":
		return d.deleteDiscordPlugin(ctx, channel, decision.PluginID, decision.Plugin.ID, decision.Confirmed)
	case "create_project":
		return d.createDiscordProject(ctx, channel, message, decision.Project)
	case "update_project":
		return d.updateDiscordProject(ctx, channel, decision.ProjectID, decision.Project, decision.ProjectPatch)
	case "delete_project":
		return d.deleteDiscordProject(ctx, channel, decision.ProjectID, decision.Project.ID, decision.Confirmed)
	case "project_health":
		return d.sendDiscordProjectHealth(ctx, channel, nonEmpty(decision.ProjectID, decision.Project.ID, project.ID))
	case "show_task":
		return d.sendDiscordTaskDetail(ctx, channel, decision.TaskID)
	case "show_worker":
		return d.sendDiscordWorkerDetail(ctx, channel, decision.WorkerID)
	case "retry_task":
		return d.retryDiscordTask(ctx, channel, decision.TaskID)
	case "steer_task":
		return d.steerDiscordTask(ctx, channel, decision.TaskID, decision.Message)
	case "cancel_task":
		return d.cancelDiscordTask(ctx, channel, decision.TaskID)
	case "cancel_worker":
		return d.cancelDiscordWorker(ctx, channel, decision.WorkerID)
	case "clear_task":
		return d.clearDiscordTask(ctx, channel, decision.TaskID, decision.Confirmed)
	case "clear_finished_tasks":
		return d.clearFinishedDiscordTasks(ctx, channel, decision.Confirmed)
	case "publish_pr":
		return d.publishDiscordPullRequest(ctx, channel, decision.TaskID, decision.PublishPR, decision.Confirmed)
	case "watch_prs":
		return d.watchDiscordPullRequests(ctx, channel, decision.TaskID, decision.WatchPRs)
	case "refresh_pr":
		return d.refreshDiscordPullRequest(ctx, channel, decision.PullRequestID)
	case "babysit_pr":
		return d.babysitDiscordPullRequest(ctx, channel, decision.PullRequestID)
	case "review_worker_changes":
		return d.reviewDiscordWorkerChanges(ctx, channel, decision.WorkerID)
	case "apply_task_result":
		return d.applyDiscordTaskResult(ctx, channel, decision.TaskID, decision.Confirmed)
	case "apply_worker_changes":
		return d.applyDiscordWorkerChanges(ctx, channel, decision.WorkerID, decision.Confirmed)
	}
	if strings.TrimSpace(decision.Proposal.Prompt) != "" {
		if strings.TrimSpace(decision.Proposal.ProjectID) == "" {
			decision.Proposal.ProjectID = project.ID
		}
		d.saveLastProject(channel.ID, message.Author.ID, decision.Proposal.ProjectID)
		if decision.Action == "create_task" {
			return d.createDiscordTask(ctx, channel, message, decision.Proposal)
		}
		d.saveTaskProposal(channel.ID, message.Author.ID, decision.Proposal)
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(decision.Reply))
}

func (d *DiscordDriver) sendDiscordTaskDetail(ctx context.Context, channel DiscordChannelConfig, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should I inspect? Send the task id from the dashboard or task list.")
	}
	detail, err := d.service.TaskDetail(ctx, taskID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task detail error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordTaskDetail(detail)))
}

func (d *DiscordDriver) sendDiscordWorkerDetail(ctx context.Context, channel DiscordChannelConfig, workerID string) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which worker should I inspect? Send the worker id from the task detail.")
	}
	detail, err := d.service.WorkerDetail(ctx, workerID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Worker detail error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordWorkerDetail(detail)))
}

func (d *DiscordDriver) retryDiscordTask(ctx context.Context, channel DiscordChannelConfig, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should I retry?")
	}
	task, err := d.service.RetryTask(ctx, taskID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task retry error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Retrying task `%s`: %s", task.ID, task.Title))
}

func (d *DiscordDriver) steerDiscordTask(ctx context.Context, channel DiscordChannelConfig, taskID string, steering string) error {
	taskID = strings.TrimSpace(taskID)
	steering = strings.TrimSpace(steering)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should I steer?")
	}
	if steering == "" {
		return d.client.SendMessage(ctx, channel.ID, "What steering message should I send?")
	}
	if err := d.requireDiscordTask(ctx, taskID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task steer error: "+err.Error())
	}
	if err := d.service.SteerTask(ctx, taskID, core.SteeringRequest{Message: steering}); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task steer error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Sent steering to task `%s`.", taskID))
}

func (d *DiscordDriver) cancelDiscordTask(ctx context.Context, channel DiscordChannelConfig, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should I cancel?")
	}
	if err := d.requireDiscordTask(ctx, taskID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task cancel error: "+err.Error())
	}
	if err := d.service.CancelTask(ctx, taskID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task cancel error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Canceled task `%s`.", taskID))
}

func (d *DiscordDriver) cancelDiscordWorker(ctx context.Context, channel DiscordChannelConfig, workerID string) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which worker should I cancel?")
	}
	if err := d.service.CancelWorker(ctx, workerID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Worker cancel error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Canceled worker `%s`.", workerID))
}

func (d *DiscordDriver) clearDiscordTask(ctx context.Context, channel DiscordChannelConfig, taskID string, confirmed bool) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should I clear?")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, "Clearing hides the task from active snapshots. Repeat the request with an explicit confirmation if you want me to clear it.")
	}
	if err := d.service.ClearTask(ctx, taskID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task clear error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Cleared task `%s` from active snapshots.", taskID))
}

func (d *DiscordDriver) clearFinishedDiscordTasks(ctx context.Context, channel DiscordChannelConfig, confirmed bool) error {
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, "Clearing hides all finished tasks from active snapshots. Repeat the request with an explicit confirmation if you want me to clear them.")
	}
	result, err := d.service.ClearTerminalTasks(ctx)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Clear finished tasks error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Cleared %d finished task(s).", len(result.Cleared)))
}

func (d *DiscordDriver) publishDiscordPullRequest(ctx context.Context, channel DiscordChannelConfig, taskID string, req core.PublishPullRequestRequest, confirmed bool) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should I publish as a PR?")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, "Publishing a PR may apply worker changes, push a branch, and create external GitHub state. Repeat the request with an explicit confirmation if you want me to publish it.")
	}
	pr, err := d.service.PublishTaskPullRequest(ctx, taskID, req)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Publish PR error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, "Published pull request:\n"+discordPullRequestSummary(pr))
}

func (d *DiscordDriver) watchDiscordPullRequests(ctx context.Context, channel DiscordChannelConfig, taskID string, req core.WatchPullRequestsRequest) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task should watch those PRs?")
	}
	prs, err := d.service.WatchPullRequests(ctx, taskID, req)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Watch PRs error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordPullRequestList("Watching pull requests:", prs)))
}

func (d *DiscordDriver) refreshDiscordPullRequest(ctx context.Context, channel DiscordChannelConfig, pullRequestID string) error {
	pullRequestID = strings.TrimSpace(pullRequestID)
	if pullRequestID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which pull request should I refresh?")
	}
	pr, err := d.service.RefreshPullRequest(ctx, pullRequestID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Refresh PR error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, "Refreshed pull request:\n"+discordPullRequestSummary(pr))
}

func (d *DiscordDriver) babysitDiscordPullRequest(ctx context.Context, channel DiscordChannelConfig, pullRequestID string) error {
	pullRequestID = strings.TrimSpace(pullRequestID)
	if pullRequestID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which pull request should I babysit?")
	}
	task, err := d.service.StartPullRequestBabysitter(ctx, pullRequestID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Babysit PR error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Babysitting PR `%s` with task `%s`: %s", pullRequestID, task.ID, task.Title))
}

func (d *DiscordDriver) reviewDiscordWorkerChanges(ctx context.Context, channel DiscordChannelConfig, workerID string) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which worker should I review?")
	}
	review, err := d.service.ReviewWorkerChanges(ctx, workerID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Worker review error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordWorkerChangesReview(review)))
}

func (d *DiscordDriver) applyDiscordTaskResult(ctx context.Context, channel DiscordChannelConfig, taskID string, confirmed bool) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which task result should I apply?")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, "Applying a task result mutates the local source checkout. Repeat the request with an explicit confirmation if you want me to apply it.")
	}
	result, err := d.service.ApplyTaskResult(ctx, taskID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task apply error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, discordApplyResult("Applied task result.", result))
}

func (d *DiscordDriver) applyDiscordWorkerChanges(ctx context.Context, channel DiscordChannelConfig, workerID string, confirmed bool) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which worker changes should I apply?")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, "Applying worker changes mutates the local source checkout. Repeat the request with an explicit confirmation if you want me to apply them.")
	}
	result, err := d.service.ApplyWorkerChanges(ctx, workerID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Worker apply error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, discordApplyResult("Applied worker changes.", result))
}

func (d *DiscordDriver) requireDiscordTask(ctx context.Context, taskID string) error {
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, ok := findTask(snapshot, taskID); !ok {
		return fmt.Errorf("task not found: %s", taskID)
	}
	return nil
}

func (d *DiscordDriver) createDiscordProject(ctx context.Context, channel DiscordChannelConfig, message DiscordMessage, project core.Project) error {
	project.ID = strings.TrimSpace(project.ID)
	project.Name = strings.TrimSpace(project.Name)
	project.LocalPath = strings.TrimSpace(project.LocalPath)
	project.Repo = strings.TrimSpace(project.Repo)
	project.VCS = strings.TrimSpace(project.VCS)
	project.DefaultBase = strings.TrimSpace(project.DefaultBase)
	if project.ID == "" || project.LocalPath == "" {
		return d.client.SendMessage(ctx, channel.ID, "Project create error: project id and localPath are required.")
	}
	created, err := d.service.CreateProject(ctx, project)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Project create error: "+err.Error())
	}
	d.saveLastProject(channel.ID, message.Author.ID, created.ID)
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Created project `%s` (%s)\n%s", created.ID, nonEmpty(created.Name, created.ID), created.LocalPath))
}

func (d *DiscordDriver) updateDiscordProject(ctx context.Context, channel DiscordChannelConfig, projectID string, patch core.Project, fields discordProjectPatch) error {
	projectID = strings.TrimSpace(nonEmpty(projectID, patch.ID))
	if projectID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which project should I update? Send the project id from the project list.")
	}
	current, err := d.discordProjectByID(ctx, projectID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Project update error: "+err.Error())
	}
	updated, err := d.service.UpdateProject(ctx, projectID, mergeDiscordProjectPatch(current, patch, fields))
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Project update error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Updated project `%s` (%s)\n%s", updated.ID, nonEmpty(updated.Name, updated.ID), updated.LocalPath))
}

func (d *DiscordDriver) deleteDiscordProject(ctx context.Context, channel DiscordChannelConfig, projectID string, fallbackID string, confirmed bool) error {
	projectID = strings.TrimSpace(nonEmpty(projectID, fallbackID))
	if projectID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which project should I delete? Send the project id from the project list.")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Deleting project `%s` removes it from aged configuration. Repeat the request with an explicit confirmation if you want me to delete it.", projectID))
	}
	if err := d.service.DeleteProject(ctx, projectID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Project delete error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Deleted project `%s`.", projectID))
}

func (d *DiscordDriver) sendDiscordProjectHealth(ctx context.Context, channel DiscordChannelConfig, projectID string) error {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which project should I check? Send the project id from the project list.")
	}
	health, err := d.service.ProjectHealth(ctx, projectID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Project health error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordProjectHealth(health)))
}

func (d *DiscordDriver) createDiscordTarget(ctx context.Context, channel DiscordChannelConfig, target core.TargetConfig) error {
	target.ID = strings.TrimSpace(target.ID)
	target.Kind = strings.TrimSpace(target.Kind)
	target.Host = strings.TrimSpace(target.Host)
	target.User = strings.TrimSpace(target.User)
	target.IdentityFile = strings.TrimSpace(target.IdentityFile)
	target.CheckoutRoot = strings.TrimSpace(target.CheckoutRoot)
	target.WorkDir = strings.TrimSpace(target.WorkDir)
	target.WorkRoot = strings.TrimSpace(target.WorkRoot)
	if target.ID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Target create error: target id is required.")
	}
	created, err := d.service.RegisterTarget(ctx, target)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Target create error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Created target `%s` (%s).", created.ID, created.Kind))
}

func (d *DiscordDriver) updateDiscordTarget(ctx context.Context, channel DiscordChannelConfig, targetID string, patch core.TargetConfig, fields discordTargetPatch) error {
	targetID = strings.TrimSpace(nonEmpty(targetID, patch.ID))
	if targetID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which target should I update? Send the target id from the target list.")
	}
	current, err := d.discordTargetByID(ctx, targetID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Target update error: "+err.Error())
	}
	updated, err := d.service.RegisterTarget(ctx, mergeDiscordTargetPatch(current.TargetConfig, patch, fields))
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Target update error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Updated target `%s` (%s).", updated.ID, updated.Kind))
}

func (d *DiscordDriver) deleteDiscordTarget(ctx context.Context, channel DiscordChannelConfig, targetID string, fallbackID string, confirmed bool) error {
	targetID = strings.TrimSpace(nonEmpty(targetID, fallbackID))
	if targetID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which target should I delete? Send the target id from the target list.")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Deleting target `%s` removes it from aged execution placement. Repeat the request with an explicit confirmation if you want me to delete it.", targetID))
	}
	if err := d.service.DeleteTarget(ctx, targetID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Target delete error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Deleted target `%s`.", targetID))
}

func (d *DiscordDriver) sendDiscordTargetHealth(ctx context.Context, channel DiscordChannelConfig, targetID string) error {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which target should I check? Send the target id from the target list.")
	}
	d.service.RefreshTargetHealthFor(ctx, targetID)
	target, err := d.discordTargetByID(ctx, targetID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Target health error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordTargetHealth(target)))
}

func (d *DiscordDriver) createDiscordPlugin(ctx context.Context, channel DiscordChannelConfig, plugin core.Plugin) error {
	plugin.ID = strings.TrimSpace(plugin.ID)
	plugin.Name = strings.TrimSpace(plugin.Name)
	plugin.Kind = strings.TrimSpace(plugin.Kind)
	plugin.Protocol = strings.TrimSpace(plugin.Protocol)
	plugin.Endpoint = strings.TrimSpace(plugin.Endpoint)
	if plugin.ID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Plugin create error: plugin id is required.")
	}
	created, err := d.service.RegisterPlugin(ctx, plugin)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Plugin create error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Created plugin `%s` (%s).", created.ID, created.Kind))
}

func (d *DiscordDriver) updateDiscordPlugin(ctx context.Context, channel DiscordChannelConfig, pluginID string, patch core.Plugin, fields discordPluginPatch) error {
	pluginID = strings.TrimSpace(nonEmpty(pluginID, patch.ID))
	if pluginID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which plugin should I update? Send the plugin id from the plugin list.")
	}
	current, err := d.discordPluginByID(ctx, pluginID)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Plugin update error: "+err.Error())
	}
	updated, err := d.service.RegisterPlugin(ctx, mergeDiscordPluginPatch(current, patch, fields))
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Plugin update error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Updated plugin `%s` (%s).", updated.ID, updated.Kind))
}

func (d *DiscordDriver) deleteDiscordPlugin(ctx context.Context, channel DiscordChannelConfig, pluginID string, fallbackID string, confirmed bool) error {
	pluginID = strings.TrimSpace(nonEmpty(pluginID, fallbackID))
	if pluginID == "" {
		return d.client.SendMessage(ctx, channel.ID, "Which plugin should I delete? Send the plugin id from the plugin list.")
	}
	if !confirmed {
		return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Deleting plugin `%s` removes it from aged configuration. Repeat the request with an explicit confirmation if you want me to delete it.", pluginID))
	}
	if err := d.service.DeletePlugin(ctx, pluginID); err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Plugin delete error: "+err.Error())
	}
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Deleted plugin `%s`.", pluginID))
}

func (d *DiscordDriver) discordProjectByID(ctx context.Context, projectID string) (core.Project, error) {
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return core.Project{}, err
	}
	if project, ok := projectByID(snapshot.Projects, projectID); ok {
		return project, nil
	}
	return core.Project{}, fmt.Errorf("project not found: %s", projectID)
}

func (d *DiscordDriver) discordTargetByID(ctx context.Context, targetID string) (core.TargetState, error) {
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return core.TargetState{}, err
	}
	for _, target := range snapshot.Targets {
		if target.ID == targetID {
			return target, nil
		}
	}
	return core.TargetState{}, fmt.Errorf("target not found: %s", targetID)
}

func (d *DiscordDriver) discordPluginByID(ctx context.Context, pluginID string) (core.Plugin, error) {
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return core.Plugin{}, err
	}
	for _, plugin := range snapshot.Plugins {
		if plugin.ID == pluginID {
			return plugin, nil
		}
	}
	return core.Plugin{}, fmt.Errorf("plugin not found: %s", pluginID)
}

func (d *DiscordDriver) createDiscordTask(ctx context.Context, channel DiscordChannelConfig, message DiscordMessage, proposal DiscordTaskProposal) error {
	proposal.Prompt = strings.TrimSpace(proposal.Prompt)
	proposal.ProjectID = strings.TrimSpace(proposal.ProjectID)
	proposal.Title = strings.TrimSpace(proposal.Title)
	proposal.CompletionMode = strings.ToLower(strings.TrimSpace(proposal.CompletionMode))
	if proposal.Prompt == "" {
		return d.client.SendMessage(ctx, channel.ID, "Task prompt is empty.")
	}
	metadata := map[string]any{
		"channelId": channel.ID,
		"messageId": message.ID,
		"userId":    message.Author.ID,
	}
	if proposal.CompletionMode == "local" || proposal.CompletionMode == "github" {
		metadata["completionMode"] = proposal.CompletionMode
	}
	req, err := NormalizeCreateTaskRequest(core.CreateTaskRequest{
		ProjectID:  proposal.ProjectID,
		Title:      proposal.Title,
		Prompt:     proposal.Prompt,
		Source:     "discord",
		ExternalID: "discord:" + message.ID,
		Metadata:   core.MustJSON(metadata),
	})
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task create error: "+err.Error())
	}
	task, err := d.service.CreateTask(ctx, req)
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task create error: "+err.Error())
	}
	d.clearTaskProposal(channel.ID, message.Author.ID)
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Created aged task `%s`: %s", task.ID, task.Title))
}
