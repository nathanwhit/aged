package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"aged/internal/core"
)

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
