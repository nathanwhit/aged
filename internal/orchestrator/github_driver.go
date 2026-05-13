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

type GitHubIssue struct {
	Repo      string
	Number    int
	Title     string
	Body      string
	URL       string
	Labels    []string
	UpdatedAt string
}

type GitHubClient interface {
	ListIssues(ctx context.Context, repo string, labels []string, limit int) ([]GitHubIssue, error)
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

func pullRequestNeedsBabysitter(pr core.PullRequest) bool {
	if !strings.EqualFold(pr.State, "OPEN") {
		return false
	}
	checks := strings.ToLower(strings.TrimSpace(pr.ChecksStatus))
	review := strings.ToUpper(strings.TrimSpace(pr.ReviewStatus))
	merge := strings.ToUpper(strings.TrimSpace(pr.MergeStatus))
	return pullRequestHasUntriggeredFeedback(pr) ||
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
