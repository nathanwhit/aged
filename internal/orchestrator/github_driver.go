package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"aged/internal/core"
)

type GitHubDriverConfig struct {
	Enabled         bool                          `json:"enabled"`
	IntervalSeconds int                           `json:"intervalSeconds,omitempty"`
	IssueLimit      int                           `json:"issueLimit,omitempty"`
	Issues          []GitHubIssueSourceConfig     `json:"issues,omitempty"`
	Mentions        GitHubMentionDriverConfig     `json:"mentions,omitempty"`
	PullRequests    GitHubPullRequestDriverConfig `json:"pullRequests,omitempty"`
}

type GitHubIssueSourceConfig struct {
	Repo        string   `json:"repo"`
	Labels      []string `json:"labels,omitempty"`
	ProjectID   string   `json:"projectId,omitempty"`
	Enabled     *bool    `json:"enabled,omitempty"`
	IssueLimit  int      `json:"issueLimit,omitempty"`
	AutoPublish *bool    `json:"autoPublish,omitempty"`
}

type GitHubPullRequestDriverConfig struct {
	Enabled     *bool    `json:"enabled,omitempty"`
	Repos       []string `json:"repos,omitempty"`
	AutoPublish *bool    `json:"autoPublish,omitempty"`
	AutoBabysit *bool    `json:"autoBabysit,omitempty"`
	Draft       bool     `json:"draft,omitempty"`
}

type GitHubMentionDriverConfig struct {
	Enabled *bool    `json:"enabled,omitempty"`
	Repos   []string `json:"repos,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
	Limit   int      `json:"limit,omitempty"`
}

type GitHubIssue struct {
	Repo      string
	Number    int
	Title     string
	Body      string
	URL       string
	Labels    []string
	UpdatedAt string
}

type GitHubMention struct {
	ID          string
	Repo        string
	SubjectType string
	Number      int
	Title       string
	URL         string
	Reason      string
	Body        string
	Author      string
	CommentURL  string
	UpdatedAt   string
}

type GitHubClient interface {
	ListIssues(ctx context.Context, repo string, labels []string, limit int) ([]GitHubIssue, error)
	ListMentions(ctx context.Context, limit int) ([]GitHubMention, error)
}

type GitHubDriver struct {
	service *Service
	client  GitHubClient
	config  GitHubDriverConfig
}

func LoadGitHubDriverConfig(value string) (GitHubDriverConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return GitHubDriverConfig{}, nil
	}
	var data []byte
	if strings.HasPrefix(value, "{") {
		data = []byte(value)
	} else {
		var err error
		data, err = os.ReadFile(value)
		if err != nil {
			return GitHubDriverConfig{}, err
		}
	}
	var config GitHubDriverConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return GitHubDriverConfig{}, err
	}
	return normalizeGitHubDriverConfig(config), nil
}

func normalizeGitHubDriverConfig(config GitHubDriverConfig) GitHubDriverConfig {
	if config.IntervalSeconds <= 0 {
		config.IntervalSeconds = 60
	}
	if config.IssueLimit <= 0 {
		config.IssueLimit = 20
	}
	return config
}

func NewGitHubDriver(service *Service, config GitHubDriverConfig, client GitHubClient) *GitHubDriver {
	if client == nil {
		client = ghGitHubClient{}
	}
	return &GitHubDriver{
		service: service,
		client:  client,
		config:  normalizeGitHubDriverConfig(config),
	}
}

func (d *GitHubDriver) Run(ctx context.Context) {
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

func (d *GitHubDriver) runOnceLogged(ctx context.Context) {
	if err := d.RunOnce(ctx); err != nil {
		slog.Warn("github driver poll failed", "error", err)
	}
}

func (d *GitHubDriver) RunOnce(ctx context.Context) error {
	if d == nil || d.service == nil || d.client == nil || !d.config.Enabled {
		return nil
	}
	var errs []string
	if err := d.pollIssues(ctx); err != nil {
		errs = append(errs, err.Error())
	}
	if err := d.pollMentions(ctx); err != nil {
		errs = append(errs, err.Error())
	}
	if err := d.publishCompletedIssueTasks(ctx); err != nil {
		errs = append(errs, err.Error())
	}
	if err := d.monitorPullRequests(ctx); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (d *GitHubDriver) pollIssues(ctx context.Context) error {
	var errs []string
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return err
	}
	resolvedIssues := githubIssueExternalIDsWithMergedPullRequests(snapshot)
	for _, source := range d.config.Issues {
		if source.Enabled != nil && !*source.Enabled {
			continue
		}
		repo := strings.TrimSpace(source.Repo)
		if repo == "" {
			continue
		}
		limit := source.IssueLimit
		if limit <= 0 {
			limit = d.config.IssueLimit
		}
		issues, err := d.client.ListIssues(ctx, repo, source.Labels, limit)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s issues: %v", repo, err))
			continue
		}
		for _, issue := range issues {
			if issue.Repo == "" {
				issue.Repo = repo
			}
			if resolvedIssues[fmt.Sprintf("%s#%d", issue.Repo, issue.Number)] {
				continue
			}
			autoPublish := boolDefault(source.AutoPublish, boolDefault(d.config.PullRequests.AutoPublish, true))
			if _, err := d.service.CreateTask(ctx, githubIssueTaskRequest(issue, source.ProjectID, autoPublish)); err != nil {
				errs = append(errs, fmt.Sprintf("%s#%d task: %v", issue.Repo, issue.Number, err))
			}
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (d *GitHubDriver) pollMentions(ctx context.Context) error {
	if !boolDefault(d.config.Mentions.Enabled, false) {
		return nil
	}
	limit := d.config.Mentions.Limit
	if limit <= 0 {
		limit = d.config.IssueLimit
	}
	mentions, err := d.client.ListMentions(ctx, limit)
	if err != nil {
		return fmt.Errorf("mentions: %w", err)
	}
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return err
	}
	seen := githubMentionExternalIDs(snapshot)
	reasons := githubMentionReasons(d.config.Mentions.Reasons)
	var errs []string
	for _, mention := range mentions {
		mention.Repo = strings.TrimSpace(mention.Repo)
		mention.ID = strings.TrimSpace(mention.ID)
		if mention.Repo == "" || mention.ID == "" {
			continue
		}
		if !reasons[strings.ToLower(strings.TrimSpace(mention.Reason))] {
			continue
		}
		if !d.monitorsMentionRepo(mention.Repo) {
			continue
		}
		if seen[mention.ID] {
			continue
		}
		if _, err := d.service.CreateTask(ctx, githubMentionTaskRequest(mention)); err != nil {
			errs = append(errs, fmt.Sprintf("%s mention %s task: %v", mention.Repo, mention.ID, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func githubMentionReasons(values []string) map[string]bool {
	if len(values) == 0 {
		values = []string{"mention", "team_mention", "review_requested"}
	}
	out := map[string]bool{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func githubMentionExternalIDs(snapshot core.Snapshot) map[string]bool {
	seen := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type != core.EventTaskCreated {
			continue
		}
		var payload struct {
			Metadata map[string]any `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if stringMetadataValue(payload.Metadata["source"]) != "github-mention" {
			continue
		}
		externalID := stringMetadataValue(payload.Metadata["externalId"])
		if externalID != "" {
			seen[externalID] = true
		}
	}
	return seen
}

// githubIssueExternalIDsWithMergedPullRequests returns the set of github-issue
// external IDs whose prior aged task already saw its pull request merge. The
// scan walks the full event history so cleared tasks still count, preventing
// the driver from recreating tasks for issues that have already been resolved
// by a merged PR but remain open on GitHub.
func githubIssueExternalIDsWithMergedPullRequests(snapshot core.Snapshot) map[string]bool {
	externalIDByTask := map[string]string{}
	for _, event := range snapshot.Events {
		if event.Type != core.EventTaskCreated {
			continue
		}
		var payload struct {
			Metadata map[string]any `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if stringMetadataValue(payload.Metadata["source"]) != "github-issue" {
			continue
		}
		externalID := stringMetadataValue(payload.Metadata["externalId"])
		if externalID != "" {
			externalIDByTask[event.TaskID] = externalID
		}
	}
	resolved := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type != core.EventTaskMilestone {
			continue
		}
		externalID, ok := externalIDByTask[event.TaskID]
		if !ok {
			continue
		}
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Name == "pr_merged" {
			resolved[externalID] = true
		}
	}
	return resolved
}

func (d *GitHubDriver) publishCompletedIssueTasks(ctx context.Context) error {
	if !boolDefault(d.config.PullRequests.AutoPublish, true) {
		return nil
	}
	snapshot, err := d.service.Snapshot(ctx)
	if err != nil {
		return err
	}
	publishedByTask := map[string]bool{}
	for _, pr := range snapshot.PullRequests {
		publishedByTask[pr.TaskID] = true
	}
	var errs []string
	for _, task := range snapshot.Tasks {
		if task.Status != core.TaskSucceeded || publishedByTask[task.ID] {
			continue
		}
		if taskCompletionModeFromTask(task) != "github" {
			continue
		}
		source, _ := taskExternalRef(task)
		if source != "github-issue" {
			continue
		}
		repo := taskMetadataString(task, "repo")
		if !d.monitorsPullRequestRepo(repo) {
			continue
		}
		_, err := d.service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{
			Repo:  repo,
			Draft: d.config.PullRequests.Draft,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s publish pr: %v", task.ID, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (d *GitHubDriver) monitorPullRequests(ctx context.Context) error {
	if !boolDefault(d.config.PullRequests.Enabled, true) {
		return nil
	}
	return d.service.monitorPullRequests(ctx, pullRequestMonitorOptions{
		AutoBabysit: boolDefault(d.config.PullRequests.AutoBabysit, true),
		IncludeRepo: d.monitorsPullRequestRepo,
	})
}

func (d *GitHubDriver) monitorsPullRequestRepo(repo string) bool {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return true
	}
	if len(d.config.PullRequests.Repos) == 0 {
		for _, source := range d.config.Issues {
			if strings.EqualFold(strings.TrimSpace(source.Repo), repo) {
				return true
			}
		}
		return len(d.config.Issues) == 0
	}
	for _, allowed := range d.config.PullRequests.Repos {
		if strings.EqualFold(strings.TrimSpace(allowed), repo) {
			return true
		}
	}
	return false
}

func (d *GitHubDriver) monitorsMentionRepo(repo string) bool {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return true
	}
	for _, allowed := range d.config.Mentions.Repos {
		if strings.EqualFold(strings.TrimSpace(allowed), repo) {
			return true
		}
	}
	if len(d.config.Mentions.Repos) > 0 {
		return false
	}
	return d.monitorsPullRequestRepo(repo)
}

func githubIssueTaskRequest(issue GitHubIssue, projectID string, githubCompletion bool) core.CreateTaskRequest {
	labels := issue.Labels
	slices.Sort(labels)
	title := fmt.Sprintf("GitHub issue %s#%d: %s", issue.Repo, issue.Number, strings.TrimSpace(issue.Title))
	metadata := map[string]any{
		"repo":      issue.Repo,
		"number":    issue.Number,
		"url":       issue.URL,
		"labels":    labels,
		"updatedAt": issue.UpdatedAt,
	}
	if githubCompletion {
		metadata["completionMode"] = "github"
	} else {
		metadata["completionMode"] = "local"
	}
	return core.CreateTaskRequest{
		ProjectID:  projectID,
		Title:      title,
		Prompt:     githubIssuePrompt(issue),
		Source:     "github-issue",
		ExternalID: fmt.Sprintf("%s#%d", issue.Repo, issue.Number),
		Metadata:   core.MustJSON(metadata),
	}
}

func githubIssuePrompt(issue GitHubIssue) string {
	body := strings.TrimSpace(issue.Body)
	if body == "" {
		body = "(no issue body)"
	}
	return fmt.Sprintf(`Work on GitHub issue %s#%d.

URL: %s
Title: %s
Labels: %s

Issue body:
%s

Implement the requested change in the current repository. Do not open the pull request yourself; the orchestrator will publish the PR after the task succeeds. Report changed files, commands run, and any blockers.
`, issue.Repo, issue.Number, issue.URL, issue.Title, strings.Join(issue.Labels, ", "), body)
}

func githubMentionTaskRequest(mention GitHubMention) core.CreateTaskRequest {
	title := fmt.Sprintf("GitHub mention %s#%d: %s", mention.Repo, mention.Number, strings.TrimSpace(mention.Title))
	if mention.Number <= 0 {
		title = fmt.Sprintf("GitHub mention %s: %s", mention.Repo, strings.TrimSpace(mention.Title))
	}
	metadata := map[string]any{
		"repo":           mention.Repo,
		"number":         mention.Number,
		"url":            mention.URL,
		"reason":         mention.Reason,
		"subjectType":    mention.SubjectType,
		"updatedAt":      mention.UpdatedAt,
		"completionMode": "local",
	}
	if mention.CommentURL != "" {
		metadata["commentUrl"] = mention.CommentURL
	}
	if mention.Author != "" {
		metadata["author"] = mention.Author
	}
	return core.CreateTaskRequest{
		Title:      title,
		Prompt:     githubMentionPrompt(mention),
		Source:     "github-mention",
		ExternalID: mention.ID,
		Metadata:   core.MustJSON(metadata),
	}
}

func githubMentionPrompt(mention GitHubMention) string {
	body := strings.TrimSpace(mention.Body)
	if body == "" {
		body = "(no mention body available)"
	}
	subject := strings.TrimSpace(mention.SubjectType)
	if subject == "" {
		subject = "GitHub item"
	}
	return fmt.Sprintf(`Handle this GitHub notification for %s.

Repository: %s
URL: %s
Notification reason: %s
Subject: %s #%d
Title: %s
Author: %s
Comment URL: %s

Mention body:
%s

Inspect the linked GitHub context and decide the appropriate response. If this is a pull request review request or mention, review the PR and leave a concise GitHub comment or review when useful. Use gh pr review for whole-PR review comments, approval, or change requests; when precise inline code comments are warranted, use gh api to create a pull request review with line or range comments. If code changes are clearly requested and appropriate for the current repository, make them in the current checkout and report what changed. Do not open a new pull request unless explicitly asked.
`, subject, mention.Repo, mention.URL, mention.Reason, subject, mention.Number, mention.Title, mention.Author, mention.CommentURL, body)
}

func pullRequestNeedsBabysitter(pr core.PullRequest) bool {
	if !strings.EqualFold(pr.State, "OPEN") {
		return false
	}
	checks := strings.ToLower(strings.TrimSpace(pr.ChecksStatus))
	review := strings.ToUpper(strings.TrimSpace(pr.ReviewStatus))
	merge := strings.ToUpper(strings.TrimSpace(pr.MergeStatus))
	return pullRequestHasUntriggeredFeedback(pr) ||
		pullRequestAutoMergeError(pr) != "" ||
		pullRequestMergeNeedsWork(pr) ||
		checks == "failing" ||
		review == "CHANGES_REQUESTED" ||
		review == "COMMENTED" ||
		merge == "DIRTY" ||
		merge == "BLOCKED" ||
		merge == "CONFLICTING"
}

func boolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func taskMetadataString(task core.Task, key string) string {
	if len(task.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return ""
	}
	return stringMetadataValue(metadata[key])
}

type ghGitHubClient struct{}

func (ghGitHubClient) ListIssues(ctx context.Context, repo string, labels []string, limit int) ([]GitHubIssue, error) {
	if limit <= 0 {
		limit = 20
	}
	args := []string{"issue", "list", "--repo", repo, "--state", "open", "--limit", strconv.Itoa(limit), "--json", "number,title,body,url,labels,updatedAt"}
	for _, label := range labels {
		if strings.TrimSpace(label) != "" {
			args = append(args, "--label", strings.TrimSpace(label))
		}
	}
	out, err := runCommand(ctx, "", "gh", args...)
	if err != nil {
		return nil, wrapGitHubCommandError("list GitHub issues", err)
	}
	var payload []struct {
		Number    int    `json:"number"`
		Title     string `json:"title"`
		Body      string `json:"body"`
		URL       string `json:"url"`
		UpdatedAt string `json:"updatedAt"`
		Labels    []struct {
			Name string `json:"name"`
		} `json:"labels"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return nil, err
	}
	issues := make([]GitHubIssue, 0, len(payload))
	for _, item := range payload {
		labels := make([]string, 0, len(item.Labels))
		for _, label := range item.Labels {
			if strings.TrimSpace(label.Name) != "" {
				labels = append(labels, label.Name)
			}
		}
		issues = append(issues, GitHubIssue{
			Repo:      repo,
			Number:    item.Number,
			Title:     item.Title,
			Body:      item.Body,
			URL:       item.URL,
			Labels:    labels,
			UpdatedAt: item.UpdatedAt,
		})
	}
	return issues, nil
}

func (ghGitHubClient) ListMentions(ctx context.Context, limit int) ([]GitHubMention, error) {
	if limit <= 0 {
		limit = 20
	}
	out, err := runCommand(ctx, "", "gh", "api", "--method", "GET", "notifications", "-F", "all=false", "-F", "participating=false", "-F", "per_page="+strconv.Itoa(limit))
	if err != nil {
		return nil, wrapGitHubCommandError("list GitHub notifications", err)
	}
	var payload []struct {
		ID         string `json:"id"`
		Reason     string `json:"reason"`
		UpdatedAt  string `json:"updated_at"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		Subject struct {
			Title            string `json:"title"`
			Type             string `json:"type"`
			URL              string `json:"url"`
			LatestCommentURL string `json:"latest_comment_url"`
		} `json:"subject"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return nil, err
	}
	mentions := make([]GitHubMention, 0, len(payload))
	for _, item := range payload {
		repo := strings.TrimSpace(item.Repository.FullName)
		number := githubNotificationSubjectNumber(item.Subject.URL)
		url := githubNotificationHTMLURL(repo, item.Subject.Type, number)
		body, author, commentURL := ghGitHubMentionComment(ctx, item.Subject.LatestCommentURL)
		mentions = append(mentions, GitHubMention{
			ID:          item.ID,
			Repo:        repo,
			SubjectType: item.Subject.Type,
			Number:      number,
			Title:       item.Subject.Title,
			URL:         url,
			Reason:      item.Reason,
			Body:        body,
			Author:      author,
			CommentURL:  commentURL,
			UpdatedAt:   item.UpdatedAt,
		})
	}
	return mentions, nil
}

func ghGitHubMentionComment(ctx context.Context, apiURL string) (string, string, string) {
	apiURL = strings.TrimSpace(apiURL)
	if apiURL == "" {
		return "", "", ""
	}
	out, err := runCommand(ctx, "", "gh", "api", apiURL)
	if err != nil {
		return "", "", ""
	}
	var payload struct {
		Body    string `json:"body"`
		HTMLURL string `json:"html_url"`
		User    struct {
			Login string `json:"login"`
		} `json:"user"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return "", "", ""
	}
	return payload.Body, payload.User.Login, payload.HTMLURL
}

func githubNotificationSubjectNumber(apiURL string) int {
	apiURL = strings.TrimSpace(apiURL)
	if apiURL == "" {
		return 0
	}
	parts := strings.Split(strings.TrimRight(apiURL, "/"), "/")
	if len(parts) == 0 {
		return 0
	}
	number, _ := strconv.Atoi(parts[len(parts)-1])
	return number
}

func githubNotificationHTMLURL(repo string, subjectType string, number int) string {
	repo = strings.TrimSpace(repo)
	if repo == "" || number <= 0 {
		return ""
	}
	path := "issues"
	if strings.EqualFold(subjectType, "PullRequest") {
		path = "pull"
	}
	return fmt.Sprintf("https://github.com/%s/%s/%d", repo, path, number)
}
