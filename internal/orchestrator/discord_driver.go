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
	Action   string
	Reply    string
	Proposal DiscordTaskProposal
	Project  core.Project
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
			"pullRequests":    snapshot.PullRequests,
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
	switch decision.Action {
	case "list_projects":
		return d.client.SendMessage(ctx, channel.ID, truncateDiscordMessage(discordProjectList(snapshot.Projects)))
	case "create_project":
		return d.createDiscordProject(ctx, channel, message, decision.Project)
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
  "action": "answer | list_projects | create_project | propose_task | create_task",
  "reply": "short Discord-ready message to send to the user",
  "project": {
    "id": "short stable project id",
    "name": "human project name",
    "localPath": "/absolute/path/to/local/checkout",
    "repo": "optional owner/repo",
    "vcs": "optional auto | jj | git",
    "defaultBase": "optional default branch",
    "workspaceRoot": "optional workspace root override",
    "targetLabels": {}
  },
  "proposedTask": {
    "projectId": "one configured project id, or omit when the default project is correct",
    "title": "optional short task title",
    "prompt": "specific prompt to create as an aged task if the user replies do it"
  }
}

Use "answer" for questions and discussion. Use "list_projects" when the user asks what projects are configured. Use "create_project" when the user clearly asks to add/register a project and provides at least an id or name plus a local checkout path; otherwise ask a follow-up for the missing fields. Use "propose_task" when a task is plausible but the user has not clearly decided to run it. Use "create_task" when the conversation clearly asks aged to start doing work, even if the user does not literally say "create a task". Set "project" to null unless creating a project. Set "proposedTask" to null when no task should be proposed or created. If the user asks for work in a repo/project and multiple projects could match, ask a concise follow-up in "reply", set "action" to "answer", and set "proposedTask" to null. Only use a projectId that appears in the provided project list for tasks.

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
		Action       string               `json:"action"`
		Reply        string               `json:"reply"`
		Project      *core.Project        `json:"project"`
		ProposedTask *DiscordTaskProposal `json:"proposedTask"`
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
	case "answer", "list_projects", "create_project", "propose_task", "create_task":
	default:
		action = "answer"
	}
	var project core.Project
	if payload.Project != nil {
		project = *payload.Project
	}
	return DiscordAssistantDecision{Action: action, Reply: reply, Proposal: proposal, Project: project}, true
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
