package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"aged/internal/core"
)

type DiscordDriverConfig struct {
	Enabled          bool                   `json:"enabled"`
	Token            string                 `json:"token,omitempty"`
	IntervalSeconds  int                    `json:"intervalSeconds,omitempty"`
	MessageLimit     int                    `json:"messageLimit,omitempty"`
	ProcessHistory   bool                   `json:"processHistory,omitempty"`
	AssistantProject string                 `json:"assistantProjectId,omitempty"`
	Channels         []DiscordChannelConfig `json:"channels"`
}

type DiscordChannelConfig struct {
	ID               string   `json:"id"`
	ProjectID        string   `json:"projectId,omitempty"`
	DefaultProjectID string   `json:"defaultProjectId,omitempty"`
	AllowedUserIDs   []string `json:"allowedUserIds,omitempty"`
	RequireMention   bool     `json:"requireMention,omitempty"`
	TaskPrefix       string   `json:"taskPrefix,omitempty"`
}

type DiscordUser struct {
	ID       string `json:"id"`
	Username string `json:"username,omitempty"`
	Bot      bool   `json:"bot,omitempty"`
}

type DiscordMessage struct {
	ID        string      `json:"id"`
	ChannelID string      `json:"channel_id,omitempty"`
	Content   string      `json:"content"`
	Author    DiscordUser `json:"author"`
}

type DiscordClient interface {
	Me(ctx context.Context) (DiscordUser, error)
	ListMessages(ctx context.Context, channelID string, afterID string, limit int) ([]DiscordMessage, error)
	SendMessage(ctx context.Context, channelID string, content string) error
}

type DiscordDriver struct {
	service *Service
	client  DiscordClient
	config  DiscordDriverConfig

	mu           sync.Mutex
	botID        string
	lastSeen     map[string]string
	lastProposal map[string]DiscordTaskProposal
	lastProject  map[string]string
	initialized  map[string]bool
}

type DiscordTaskProposal struct {
	ProjectID string `json:"projectId,omitempty"`
	Title     string `json:"title,omitempty"`
	Prompt    string `json:"prompt"`
}

type DiscordAssistantDecision struct {
	Action        string
	Reply         string
	TaskID        string
	WorkerID      string
	PullRequestID string
	ProjectID     string
	TargetID      string
	PluginID      string
	Message       string
	Confirmed     bool
	Proposal      DiscordTaskProposal
	Project       core.Project
	ProjectPatch  discordProjectPatch
	Target        core.TargetConfig
	TargetPatch   discordTargetPatch
	Plugin        core.Plugin
	PluginPatch   discordPluginPatch
	PublishPR     core.PublishPullRequestRequest
	WatchPRs      core.WatchPullRequestsRequest
}

type discordProjectPatch struct {
	ID                *string                        `json:"id"`
	Name              *string                        `json:"name"`
	LocalPath         *string                        `json:"localPath"`
	Repo              *string                        `json:"repo"`
	UpstreamRepo      *string                        `json:"upstreamRepo"`
	HeadRepoOwner     *string                        `json:"headRepoOwner"`
	PushRemote        *string                        `json:"pushRemote"`
	VCS               *string                        `json:"vcs"`
	DefaultBase       *string                        `json:"defaultBase"`
	WorkspaceRoot     *string                        `json:"workspaceRoot"`
	TargetLabels      *map[string]string             `json:"targetLabels"`
	PullRequestPolicy *discordPullRequestPolicyPatch `json:"pullRequestPolicy"`
}

type discordPullRequestPolicyPatch struct {
	BranchPrefix *string `json:"branchPrefix"`
	Draft        *bool   `json:"draft"`
	AllowMerge   *bool   `json:"allowMerge"`
	AutoMerge    *bool   `json:"autoMerge"`
}

type discordTargetPatch struct {
	ID                    *string                     `json:"id"`
	Kind                  *string                     `json:"kind"`
	Host                  *string                     `json:"host"`
	User                  *string                     `json:"user"`
	Port                  *int                        `json:"port"`
	IdentityFile          *string                     `json:"identityFile"`
	InsecureIgnoreHostKey *bool                       `json:"insecureIgnoreHostKey"`
	WorkDir               *string                     `json:"workDir"`
	WorkRoot              *string                     `json:"workRoot"`
	Labels                *map[string]string          `json:"labels"`
	Capacity              *discordTargetCapacityPatch `json:"capacity"`
}

type discordTargetCapacityPatch struct {
	MaxWorkers *int     `json:"maxWorkers"`
	CPUWeight  *float64 `json:"cpuWeight"`
	MemoryGB   *float64 `json:"memoryGB"`
}

type discordPluginPatch struct {
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

func LoadDiscordDriverConfig(value string) (DiscordDriverConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return DiscordDriverConfig{}, nil
	}
	var data []byte
	if strings.HasPrefix(value, "{") {
		data = []byte(value)
	} else {
		var err error
		data, err = os.ReadFile(value)
		if err != nil {
			return DiscordDriverConfig{}, err
		}
	}
	var config DiscordDriverConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return DiscordDriverConfig{}, err
	}
	return normalizeDiscordDriverConfig(config), nil
}

func normalizeDiscordDriverConfig(config DiscordDriverConfig) DiscordDriverConfig {
	if config.IntervalSeconds <= 0 {
		config.IntervalSeconds = 5
	}
	if config.MessageLimit <= 0 {
		config.MessageLimit = 20
	}
	if strings.TrimSpace(config.Token) == "" {
		config.Token = os.Getenv("DISCORD_BOT_TOKEN")
	}
	for index := range config.Channels {
		if strings.TrimSpace(config.Channels[index].TaskPrefix) == "" {
			config.Channels[index].TaskPrefix = "task:"
		}
		if strings.TrimSpace(config.Channels[index].DefaultProjectID) == "" {
			config.Channels[index].DefaultProjectID = config.Channels[index].ProjectID
		}
	}
	return config
}

func NewDiscordDriver(service *Service, config DiscordDriverConfig, client DiscordClient) *DiscordDriver {
	config = normalizeDiscordDriverConfig(config)
	if client == nil && strings.TrimSpace(config.Token) != "" {
		client = NewDiscordRESTClient(config.Token)
	}
	return &DiscordDriver{
		service:      service,
		client:       client,
		config:       config,
		lastSeen:     map[string]string{},
		lastProposal: map[string]DiscordTaskProposal{},
		lastProject:  map[string]string{},
		initialized:  map[string]bool{},
	}
}

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
			"source":          "discord",
			"channelId":       channel.ID,
			"userId":          message.Author.ID,
			"selectedProject": project,
			"projects":        snapshot.Projects,
			"tasks":           snapshot.Tasks,
			"workers":         snapshot.Workers,
			"executionNodes":  snapshot.ExecutionNodes,
			"targets":         snapshot.Targets,
			"plugins":         snapshot.Plugins,
			"pullRequests":    snapshot.PullRequests,
			"recentEvents":    compactDiscordEvents(snapshot.Events, 80),
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
	if proposal.Prompt == "" {
		return d.client.SendMessage(ctx, channel.ID, "Task prompt is empty.")
	}
	task, err := d.service.CreateTask(ctx, core.CreateTaskRequest{
		ProjectID:  proposal.ProjectID,
		Title:      proposal.Title,
		Prompt:     proposal.Prompt,
		Source:     "discord",
		ExternalID: "discord:" + message.ID,
		Metadata: core.MustJSON(map[string]any{
			"channelId": channel.ID,
			"messageId": message.ID,
			"userId":    message.Author.ID,
		}),
	})
	if err != nil {
		return d.client.SendMessage(ctx, channel.ID, "Task create error: "+err.Error())
	}
	d.clearTaskProposal(channel.ID, message.Author.ID)
	return d.client.SendMessage(ctx, channel.ID, fmt.Sprintf("Created aged task `%s`: %s", task.ID, task.Title))
}

func discordAssistantPrompt(content string) string {
	return fmt.Sprintf(`You are the natural-language Discord interface for aged, a durable autonomous development orchestrator.

Answer the user's question using the provided aged snapshot context and, when useful, by inspecting files in the current read-only project checkout. Do not edit files from chat; create a task instead when code changes are needed.

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
    "workDir": "checkout path on the target",
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
    "prompt": "specific prompt to create as an aged task if the user replies do it"
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

Use "answer" for questions and discussion. Use "list_projects", "list_targets", or "list_plugins" when the user asks what is configured. Use "project_health" when the user asks for health/status/readiness of a configured project; set projectId to an exact id from the project list, or omit it only when the selected project is clearly intended. Use "target_health" when the user asks for health/status/resources/readiness of a configured execution target; set targetId to an exact id from the target list. Use "update_project" when the user asks to edit a configured project; set projectId to the exact existing project id and include changed project fields in "project" while preserving unrelated fields by omission. Empty project name, vcs, defaultBase, and pullRequestPolicy.branchPrefix are normalized back to service defaults. Use "update_target" or "update_plugin" similarly for configured targets and plugins; set targetId/pluginId exactly and include only changed fields when possible. For update actions, omit unchanged fields inside the selected project/target/plugin object; include empty strings, empty arrays/maps, false booleans, or numeric zero only when the user explicitly wants to clear or set that value. For target capacity, maxWorkers and cpuWeight must be positive and zero normalizes to one; memoryGB zero clears optional memory capacity. Use "delete_project", "delete_target", or "delete_plugin" when the user asks to remove a configured item; set the exact id and set confirmed true only when the user explicitly confirms deletion. Use "show_task" when the user asks for status/details/logs/workers/PRs/actions for one identifiable task; set taskId to the exact id from the snapshot. Use "show_worker" or "review_worker_changes" when the user asks for one identifiable worker's details or diff; set workerId exactly. Use task control actions when the user asks to retry, steer, cancel, clear, publish a PR, watch PRs, refresh a PR, babysit a PR, or apply results. For "steer_task", set message to the exact feedback or answer that should be sent to the task. For PR actions, use the exact aged pullRequestId from the snapshot when one exists; for "watch_prs", set taskId and fill watchPrs from the user's repo/number/url filters. For "publish_pr", set taskId and optional publishPr fields. For "publish_pr", "apply_task_result", and "apply_worker_changes", set confirmed true only when the user explicitly confirms publishing or applying changes; otherwise leave confirmed false so the bot can ask for confirmation. Use "create_project" when the user clearly asks to add/register a project and provides at least an id or name plus a local checkout path; otherwise ask a follow-up for the missing fields. Use "create_target" when the user clearly asks to add/register a target and provides an id, plus a host for ssh targets. Use "create_plugin" when the user clearly asks to add/register a plugin and provides an id. Use "propose_task" when a task is plausible but the user has not clearly decided to run it. Use "create_task" when the conversation clearly asks aged to start doing work, even if the user does not literally say "create a task". Set unrelated top-level object fields to null or empty values. If the user asks for work in a repo/project and multiple projects could match, ask a concise follow-up in "reply", set "action" to "answer", and set "proposedTask" to null. Only use ids that appear in the provided project, target, and plugin lists for update/delete/health actions.

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

func resolveDiscordDecision(snapshot core.Snapshot, decision DiscordAssistantDecision, content string) (DiscordAssistantDecision, string) {
	if discordActionNeedsTaskID(decision.Action) {
		taskID, prompt := resolveDiscordTaskID(snapshot, decision.TaskID, content)
		if prompt != "" {
			return decision, prompt
		}
		decision.TaskID = taskID
	}
	if discordActionNeedsWorkerID(decision.Action) {
		workerID, prompt := resolveDiscordWorkerID(snapshot, decision.WorkerID, content, decision.TaskID)
		if prompt != "" {
			return decision, prompt
		}
		decision.WorkerID = workerID
	}
	if decision.Action == "publish_pr" {
		workerID, prompt := resolveDiscordWorkerID(snapshot, decision.PublishPR.WorkerID, content, decision.TaskID)
		if prompt != "" {
			return decision, prompt
		}
		decision.PublishPR.WorkerID = workerID
	}
	if discordActionNeedsPullRequestID(decision.Action) {
		pullRequestID, prompt := resolveDiscordPullRequestID(snapshot, decision.PullRequestID, content)
		if prompt != "" {
			return decision, prompt
		}
		decision.PullRequestID = pullRequestID
	}
	if decision.Action == "watch_prs" {
		decision.WatchPRs = resolveDiscordWatchPullRequestReference(decision.WatchPRs, content)
	}
	return decision, ""
}

func discordActionNeedsTaskID(action string) bool {
	switch action {
	case "show_task", "retry_task", "steer_task", "cancel_task", "clear_task", "publish_pr", "watch_prs", "apply_task_result":
		return true
	default:
		return false
	}
}

func discordActionNeedsWorkerID(action string) bool {
	switch action {
	case "show_worker", "cancel_worker", "review_worker_changes", "apply_worker_changes":
		return true
	default:
		return false
	}
}

func discordActionNeedsPullRequestID(action string) bool {
	switch action {
	case "refresh_pr", "babysit_pr":
		return true
	default:
		return false
	}
}

func resolveDiscordTaskID(snapshot core.Snapshot, explicit string, content string) (string, string) {
	if ref := strings.TrimSpace(explicit); ref != "" {
		if id, prompt, ok := matchDiscordTaskReference(snapshot, ref); ok {
			return id, prompt
		}
		return ref, ""
	}
	lower := strings.ToLower(content)
	for _, phrase := range []string{"latest task", "newest task", "last task"} {
		if strings.Contains(lower, phrase) {
			return latestDiscordTaskID(snapshot)
		}
	}
	if strings.Contains(lower, "running task") {
		return singleDiscordTaskWithStatus(snapshot, core.TaskRunning, "running")
	}
	if strings.Contains(lower, "failed task") {
		return singleDiscordTaskWithStatus(snapshot, core.TaskFailed, "failed")
	}
	for _, token := range discordReferenceTokens(content) {
		if id, prompt, ok := matchDiscordTaskReference(snapshot, token); ok {
			return id, prompt
		}
	}
	return "", ""
}

func matchDiscordTaskReference(snapshot core.Snapshot, ref string) (string, string, bool) {
	ref = cleanDiscordReference(ref)
	if ref == "" {
		return "", "", false
	}
	switch strings.ToLower(ref) {
	case "latest task", "newest task", "last task", "latest", "newest":
		id, prompt := latestDiscordTaskID(snapshot)
		return id, prompt, true
	case "running task", "running":
		id, prompt := singleDiscordTaskWithStatus(snapshot, core.TaskRunning, "running")
		return id, prompt, true
	case "failed task", "failed":
		id, prompt := singleDiscordTaskWithStatus(snapshot, core.TaskFailed, "failed")
		return id, prompt, true
	}
	for _, task := range snapshot.Tasks {
		if task.ID == ref {
			return task.ID, "", true
		}
	}
	if len(ref) < 4 {
		return "", "", false
	}
	var matches []core.Task
	for _, task := range snapshot.Tasks {
		if strings.HasPrefix(task.ID, ref) {
			matches = append(matches, task)
		}
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple tasks match `%s`: %s. Send the full task id.", ref, compactDiscordTaskIDs(matches)), true
	}
}

func latestDiscordTaskID(snapshot core.Snapshot) (string, string) {
	if len(snapshot.Tasks) == 0 {
		return "", "I do not see any tasks right now."
	}
	latest := snapshot.Tasks[0]
	for _, task := range snapshot.Tasks[1:] {
		if latest.CreatedAt.Before(task.CreatedAt) || (latest.CreatedAt.Equal(task.CreatedAt) && latest.ID < task.ID) {
			latest = task
		}
	}
	return latest.ID, ""
}

func singleDiscordTaskWithStatus(snapshot core.Snapshot, status core.TaskStatus, label string) (string, string) {
	var matches []core.Task
	for _, task := range snapshot.Tasks {
		if task.Status == status {
			matches = append(matches, task)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Sprintf("I do not see a %s task right now.", label)
	case 1:
		return matches[0].ID, ""
	default:
		return "", fmt.Sprintf("Multiple %s tasks match: %s. Send the full task id.", label, compactDiscordTaskIDs(matches))
	}
}

func resolveDiscordWorkerID(snapshot core.Snapshot, explicit string, content string, taskID string) (string, string) {
	if ref := strings.TrimSpace(explicit); ref != "" {
		if id, prompt, ok := matchDiscordWorkerReference(snapshot, ref, taskID); ok {
			return id, prompt
		}
		return ref, ""
	}
	for _, token := range discordReferenceTokens(content) {
		if id, prompt, ok := matchDiscordWorkerReference(snapshot, token, taskID); ok {
			return id, prompt
		}
	}
	return "", ""
}

func matchDiscordWorkerReference(snapshot core.Snapshot, ref string, taskID string) (string, string, bool) {
	ref = cleanDiscordReference(ref)
	if ref == "" {
		return "", "", false
	}
	for _, worker := range snapshot.Workers {
		if worker.ID == ref {
			return worker.ID, "", true
		}
	}
	if len(ref) < 4 {
		return "", "", false
	}
	matches := discordWorkerPrefixMatches(snapshot.Workers, ref, "")
	if len(matches) > 1 && strings.TrimSpace(taskID) != "" {
		if scoped := discordWorkerPrefixMatches(snapshot.Workers, ref, taskID); len(scoped) > 0 {
			matches = scoped
		}
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple workers match `%s`: %s. Send the full worker id.", ref, compactDiscordWorkerIDs(matches)), true
	}
}

func discordWorkerPrefixMatches(workers []core.Worker, prefix string, taskID string) []core.Worker {
	var matches []core.Worker
	for _, worker := range workers {
		if taskID != "" && worker.TaskID != taskID {
			continue
		}
		if strings.HasPrefix(worker.ID, prefix) {
			matches = append(matches, worker)
		}
	}
	return matches
}

func resolveDiscordPullRequestID(snapshot core.Snapshot, explicit string, content string) (string, string) {
	if ref := strings.TrimSpace(explicit); ref != "" {
		if id, prompt, ok := matchDiscordPullRequestReference(snapshot, ref, true); ok {
			return id, prompt
		}
		return ref, ""
	}
	tokens := discordReferenceTokens(content)
	for _, token := range tokens {
		if id, prompt, ok := matchDiscordPullRequestReference(snapshot, token, false); ok {
			return id, prompt
		}
	}
	for i, token := range tokens {
		if i == 0 || !isDiscordPullRequestWord(tokens[i-1]) {
			continue
		}
		if id, prompt, ok := matchDiscordPullRequestReference(snapshot, token, true); ok {
			return id, prompt
		}
	}
	return "", ""
}

func matchDiscordPullRequestReference(snapshot core.Snapshot, ref string, allowBareNumber bool) (string, string, bool) {
	ref = cleanDiscordReference(ref)
	if ref == "" {
		return "", "", false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == ref || (pr.URL != "" && pr.URL == ref) {
			return pr.ID, "", true
		}
	}
	if repo, number := parsePullRequestURL(ref); repo != "" && number > 0 {
		return matchDiscordPullRequestNumber(snapshot, repo, number, ref)
	}
	if repo, number := parseDiscordRepoNumberReference(ref); repo != "" && number > 0 {
		return matchDiscordPullRequestNumber(snapshot, repo, number, ref)
	}
	if allowBareNumber {
		if number, ok := parseDiscordPullRequestNumber(ref); ok {
			return matchDiscordPullRequestNumber(snapshot, "", number, ref)
		}
	}
	if len(ref) < 4 {
		return "", "", false
	}
	var matches []core.PullRequest
	for _, pr := range snapshot.PullRequests {
		if strings.HasPrefix(pr.ID, ref) {
			matches = append(matches, pr)
		}
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple pull requests match `%s`: %s. Send the full aged pull request id.", ref, compactDiscordPullRequestIDs(matches)), true
	}
}

func matchDiscordPullRequestNumber(snapshot core.Snapshot, repo string, number int, ref string) (string, string, bool) {
	var matches []core.PullRequest
	for _, pr := range snapshot.PullRequests {
		if pr.Number != number {
			continue
		}
		if repo != "" && pr.Repo != repo {
			continue
		}
		matches = append(matches, pr)
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple pull requests match `%s`: %s. Send the full aged pull request id.", ref, compactDiscordPullRequestIDs(matches)), true
	}
}

func resolveDiscordWatchPullRequestReference(req core.WatchPullRequestsRequest, content string) core.WatchPullRequestsRequest {
	if strings.TrimSpace(req.URL) != "" || req.Number > 0 {
		return req
	}
	tokens := discordReferenceTokens(content)
	for _, token := range tokens {
		clean := cleanDiscordReference(token)
		if repo, number := parsePullRequestURL(clean); repo != "" && number > 0 {
			req.URL = clean
			if strings.TrimSpace(req.Repo) == "" {
				req.Repo = repo
			}
			req.Number = number
			return req
		}
		if repo, number := parseDiscordRepoNumberReference(clean); repo != "" && number > 0 {
			if strings.TrimSpace(req.Repo) == "" {
				req.Repo = repo
			}
			req.Number = number
			return req
		}
	}
	for i, token := range tokens {
		if i == 0 || !isDiscordPullRequestWord(tokens[i-1]) {
			continue
		}
		if number, ok := parseDiscordPullRequestNumber(token); ok {
			req.Number = number
			return req
		}
	}
	return req
}

func parseDiscordRepoNumberReference(ref string) (string, int) {
	index := strings.LastIndex(ref, "#")
	if index <= 0 || index == len(ref)-1 {
		return "", 0
	}
	repo := strings.TrimSpace(ref[:index])
	if !strings.Contains(repo, "/") {
		return "", 0
	}
	number, err := strconv.Atoi(strings.TrimSpace(ref[index+1:]))
	if err != nil {
		return "", 0
	}
	return repo, number
}

func parseDiscordPullRequestNumber(ref string) (int, bool) {
	ref = strings.TrimSpace(strings.ToLower(ref))
	ref = strings.TrimPrefix(ref, "pr#")
	ref = strings.TrimPrefix(ref, "#")
	ref = strings.TrimPrefix(ref, "pr-")
	number, err := strconv.Atoi(ref)
	if err != nil || number <= 0 {
		return 0, false
	}
	return number, true
}

func isDiscordPullRequestWord(value string) bool {
	switch strings.ToLower(cleanDiscordReference(value)) {
	case "pr", "prs", "pull", "pull-request", "pull-request-id", "pullrequest":
		return true
	default:
		return false
	}
}

func discordReferenceTokens(content string) []string {
	var tokens []string
	for _, field := range strings.Fields(content) {
		token := cleanDiscordReference(field)
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

func cleanDiscordReference(value string) string {
	return strings.Trim(strings.TrimSpace(value), " \t\r\n`'\"<>[](){}.,;:!?")
}

func compactDiscordTaskIDs(tasks []core.Task) string {
	var ids []string
	for _, task := range tasks {
		ids = append(ids, "`"+shortDiscordID(task.ID)+"`")
	}
	return strings.Join(ids, ", ")
}

func compactDiscordWorkerIDs(workers []core.Worker) string {
	var ids []string
	for _, worker := range workers {
		ids = append(ids, "`"+shortDiscordID(worker.ID)+"`")
	}
	return strings.Join(ids, ", ")
}

func compactDiscordPullRequestIDs(prs []core.PullRequest) string {
	var ids []string
	for _, pr := range prs {
		ids = append(ids, "`"+shortDiscordID(pr.ID)+"`")
	}
	return strings.Join(ids, ", ")
}

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

func mergeDiscordProjectPatch(current core.Project, _ core.Project, patch discordProjectPatch) core.Project {
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

func mergeDiscordTargetPatch(current core.TargetConfig, _ core.TargetConfig, patch discordTargetPatch) core.TargetConfig {
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

func mergeDiscordPluginPatch(current core.Plugin, _ core.Plugin, patch discordPluginPatch) core.Plugin {
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

func compactStringMap(values map[string]string) string {
	var parts []string
	for key, value := range values {
		parts = append(parts, key+"="+value)
	}
	slices.Sort(parts)
	return "`" + strings.Join(parts, ",") + "`"
}

func compactBoolMap(values map[string]bool) string {
	var parts []string
	for key, value := range values {
		parts = append(parts, fmt.Sprintf("%s=%t", key, value))
	}
	slices.Sort(parts)
	return "`" + strings.Join(parts, ",") + "`"
}

func shortDiscordID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func truncateText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return strings.TrimSpace(value[:limit-3]) + "..."
}

func isDiscordDoIt(content string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.Trim(content, ".! ")))
	switch normalized {
	case "do it", "let's do it", "lets do it", "go", "run it", "make it happen", "ship it":
		return true
	default:
		return false
	}
}

func discordConversationID(channelID string, userID string, projectID string) string {
	if strings.TrimSpace(projectID) == "" {
		return "discord:" + channelID + ":" + userID
	}
	return "discord:" + channelID + ":" + userID + ":" + projectID
}

func discordMemoryKey(channelID string, userID string) string {
	return channelID + ":" + userID
}

func (d *DiscordDriver) selectDiscordProject(channel DiscordChannelConfig, userID string, content string, projects []core.Project) core.Project {
	if len(projects) == 0 {
		return core.Project{ID: channelDefaultProjectID(channel)}
	}
	if project, ok := matchDiscordProject(content, projects); ok {
		d.saveLastProject(channel.ID, userID, project.ID)
		return project
	}
	if projectID := d.savedLastProject(channel.ID, userID); projectID != "" {
		if project, ok := projectByID(projects, projectID); ok {
			return project
		}
	}
	if project, ok := projectByID(projects, channelDefaultProjectID(channel)); ok {
		return project
	}
	for _, project := range projects {
		if strings.TrimSpace(project.ID) == "default" {
			return project
		}
	}
	return projects[0]
}

func matchDiscordProject(content string, projects []core.Project) (core.Project, bool) {
	content = strings.ToLower(content)
	words := discordProjectWords(content)
	var matches []core.Project
	for _, project := range projects {
		for _, token := range discordProjectTokens(project) {
			if token != "" && (strings.Contains(token, "/") && strings.Contains(content, token) || words[token]) {
				matches = append(matches, project)
				break
			}
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}
	return core.Project{}, false
}

func discordProjectWords(content string) map[string]bool {
	words := map[string]bool{}
	for _, word := range strings.FieldsFunc(content, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) {
		if word != "" {
			words[word] = true
		}
	}
	return words
}

func discordProjectTokens(project core.Project) []string {
	values := []string{project.ID, project.Name, project.Repo}
	if project.Repo != "" && strings.Contains(project.Repo, "/") {
		parts := strings.Split(project.Repo, "/")
		values = append(values, parts[len(parts)-1])
	}
	tokens := []string{}
	for _, value := range values {
		value = strings.TrimSpace(strings.ToLower(value))
		if len(value) >= 2 {
			tokens = append(tokens, value)
		}
	}
	return tokens
}

func projectByID(projects []core.Project, id string) (core.Project, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return core.Project{}, false
	}
	for _, project := range projects {
		if project.ID == id {
			return project, true
		}
	}
	return core.Project{}, false
}

func (d *DiscordDriver) savedTaskProposal(ctx context.Context, channelID string, userID string) DiscordTaskProposal {
	d.mu.Lock()
	proposal := d.lastProposal[discordMemoryKey(channelID, userID)]
	d.mu.Unlock()
	if strings.TrimSpace(proposal.Prompt) != "" {
		return proposal
	}
	return d.latestAssistantProposal(ctx, channelID, userID)
}

func (d *DiscordDriver) saveTaskProposal(channelID string, userID string, proposal DiscordTaskProposal) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastProposal[discordMemoryKey(channelID, userID)] = proposal
}

func (d *DiscordDriver) clearTaskProposal(channelID string, userID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.lastProposal, discordMemoryKey(channelID, userID))
}

func (d *DiscordDriver) latestAssistantProposal(ctx context.Context, channelID string, userID string) DiscordTaskProposal {
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return DiscordTaskProposal{}
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventAssistantAnswered {
			continue
		}
		var payload struct {
			ConversationID string `json:"conversationId"`
			Message        string `json:"message"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || !strings.HasPrefix(payload.ConversationID, discordConversationID(channelID, userID, "")) {
			continue
		}
		return parseDiscordAssistantResponse(payload.Message).Proposal
	}
	return DiscordTaskProposal{}
}

func (d *DiscordDriver) savedLastProject(channelID string, userID string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastProject[discordMemoryKey(channelID, userID)]
}

func (d *DiscordDriver) saveLastProject(channelID string, userID string, projectID string) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastProject[discordMemoryKey(channelID, userID)] = projectID
}

func channelDefaultProjectID(channel DiscordChannelConfig) string {
	return strings.TrimSpace(nonEmpty(channel.DefaultProjectID, channel.ProjectID))
}

func (d *DiscordDriver) isInitialized(channelID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.initialized[channelID]
}

func (d *DiscordDriver) markInitialized(channelID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.initialized[channelID] = true
}

func (d *DiscordDriver) setLastSeen(channelID string, messageID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSeen[channelID] = messageID
}

func truncateDiscordMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= 1900 {
		return message
	}
	return message[:1890] + "\n..."
}

type DiscordRESTClient struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func NewDiscordRESTClient(token string) DiscordRESTClient {
	return DiscordRESTClient{
		token:      token,
		baseURL:    "https://discord.com/api/v10",
		httpClient: http.DefaultClient,
	}
}

func (c DiscordRESTClient) Me(ctx context.Context) (DiscordUser, error) {
	var user DiscordUser
	if err := c.do(ctx, http.MethodGet, "/users/@me", nil, &user); err != nil {
		return DiscordUser{}, err
	}
	return user, nil
}

func (c DiscordRESTClient) ListMessages(ctx context.Context, channelID string, afterID string, limit int) ([]DiscordMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	path := "/channels/" + url.PathEscape(channelID) + "/messages?limit=" + url.QueryEscape(fmt.Sprintf("%d", limit))
	if strings.TrimSpace(afterID) != "" {
		path += "&after=" + url.QueryEscape(afterID)
	}
	var messages []DiscordMessage
	if err := c.do(ctx, http.MethodGet, path, nil, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

func (c DiscordRESTClient) SendMessage(ctx context.Context, channelID string, content string) error {
	body := map[string]string{"content": truncateDiscordMessage(content)}
	return c.do(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/messages", body, nil)
}

func (c DiscordRESTClient) do(ctx context.Context, method string, path string, body any, out any) error {
	if strings.TrimSpace(c.token) == "" {
		return errors.New("discord bot token is required")
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.baseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := c.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("discord api %s %s: status %d: %s", method, path, res.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return err
		}
	}
	return nil
}
