package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"aged/internal/core"

	"github.com/google/uuid"
)

type PullRequestPublishSpec struct {
	TaskID   string
	WorkerID string
	WorkDir  string
	Repo     string
	Base     string
	Branch   string
	Title    string
	Body     string
	Draft    bool
	Metadata map[string]any
}

type PullRequestPublisher interface {
	Publish(ctx context.Context, spec PullRequestPublishSpec) (core.PullRequest, error)
	Inspect(ctx context.Context, pr core.PullRequest) (core.PullRequest, error)
}

type commandExecutor func(ctx context.Context, dir string, name string, args ...string) (string, error)

type LocalPullRequestPublisher struct {
	exec commandExecutor
}

func NewLocalPullRequestPublisher() LocalPullRequestPublisher {
	return LocalPullRequestPublisher{exec: runCommand}
}

func (p LocalPullRequestPublisher) Publish(ctx context.Context, spec PullRequestPublishSpec) (core.PullRequest, error) {
	if strings.TrimSpace(spec.WorkDir) == "" {
		return core.PullRequest{}, errors.New("publish requires a workdir")
	}
	exec := p.exec
	if exec == nil {
		exec = runCommand
	}
	repo := strings.TrimSpace(spec.Repo)
	if repo == "" {
		resolved, err := exec(ctx, spec.WorkDir, "gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner")
		if err != nil {
			return core.PullRequest{}, fmt.Errorf("resolve GitHub repo: %w", err)
		}
		repo = strings.TrimSpace(resolved)
	}
	if repo == "" {
		return core.PullRequest{}, errors.New("publish requires repo")
	}
	base := strings.TrimSpace(spec.Base)
	if base == "" {
		base = "main"
	}
	branch := strings.TrimSpace(spec.Branch)
	if branch == "" {
		branch = defaultPRBranch(spec)
	}
	title := strings.TrimSpace(spec.Title)
	if title == "" {
		title = "aged task " + shortID(spec.TaskID)
	}
	body := strings.TrimSpace(spec.Body)
	if body == "" {
		body = defaultPRBody(spec)
	}
	if err := p.pushBranch(ctx, exec, spec.WorkDir, branch); err != nil {
		return core.PullRequest{}, err
	}

	args := []string{"pr", "create", "--repo", repo, "--base", base, "--head", branch, "--title", title, "--body", body}
	if spec.Draft {
		args = append(args, "--draft")
	}
	out, err := exec(ctx, spec.WorkDir, "gh", args...)
	if err != nil {
		existing, existingErr := p.findExistingPullRequest(ctx, exec, spec.WorkDir, repo, branch)
		if existingErr != nil {
			return core.PullRequest{}, fmt.Errorf("create GitHub pull request: %w; find existing pull request: %w", err, existingErr)
		}
		existing.ID = newPullRequestID()
		existing.TaskID = spec.TaskID
		existing.Repo = repo
		if existing.Branch == "" {
			existing.Branch = branch
		}
		if existing.Base == "" {
			existing.Base = base
		}
		if existing.Title == "" {
			existing.Title = title
		}
		existing.Metadata = core.MustJSON(spec.Metadata)
		return existing, nil
	}
	prURL := firstURL(out)
	if prURL == "" {
		prURL = strings.TrimSpace(out)
	}
	pr := core.PullRequest{
		ID:       newPullRequestID(),
		TaskID:   spec.TaskID,
		Repo:     repo,
		URL:      prURL,
		Branch:   branch,
		Base:     base,
		Title:    title,
		State:    "OPEN",
		Draft:    spec.Draft,
		Metadata: core.MustJSON(spec.Metadata),
	}
	inspected, err := p.Inspect(ctx, pr)
	if err != nil {
		return pr, nil
	}
	inspected.ID = pr.ID
	inspected.TaskID = spec.TaskID
	if len(inspected.Metadata) == 0 {
		inspected.Metadata = pr.Metadata
	}
	return inspected, nil
}

func (p LocalPullRequestPublisher) findExistingPullRequest(ctx context.Context, exec commandExecutor, dir string, repo string, branch string) (core.PullRequest, error) {
	out, err := exec(ctx, dir, "gh", "pr", "list", "--repo", repo, "--head", branch, "--state", "all", "--json", "number,url,state,title,isDraft,headRefName,baseRefName")
	if err != nil {
		return core.PullRequest{}, err
	}
	var prs []struct {
		Number      int    `json:"number"`
		URL         string `json:"url"`
		State       string `json:"state"`
		Title       string `json:"title"`
		IsDraft     bool   `json:"isDraft"`
		HeadRefName string `json:"headRefName"`
		BaseRefName string `json:"baseRefName"`
	}
	if err := json.Unmarshal([]byte(out), &prs); err != nil {
		return core.PullRequest{}, err
	}
	if len(prs) == 0 {
		return core.PullRequest{}, errors.New("no existing pull request found for branch")
	}
	return core.PullRequest{
		Number: prs[0].Number,
		URL:    prs[0].URL,
		State:  prs[0].State,
		Title:  prs[0].Title,
		Draft:  prs[0].IsDraft,
		Branch: prs[0].HeadRefName,
		Base:   prs[0].BaseRefName,
	}, nil
}

func (p LocalPullRequestPublisher) pushBranch(ctx context.Context, exec commandExecutor, dir string, branch string) error {
	if _, err := exec(ctx, dir, "jj", "root"); err == nil {
		if _, err := exec(ctx, dir, "jj", "bookmark", "create", branch, "--revision", "@"); err != nil {
			if _, setErr := exec(ctx, dir, "jj", "bookmark", "set", branch, "--revision", "@"); setErr != nil {
				return fmt.Errorf("create jj bookmark: %w; set existing bookmark: %w", err, setErr)
			}
		}
		if _, err := exec(ctx, dir, "jj", "git", "push", "--bookmark", branch); err != nil {
			return fmt.Errorf("push jj bookmark: %w", err)
		}
		return nil
	}
	if _, err := exec(ctx, dir, "git", "rev-parse", "--show-toplevel"); err == nil {
		if _, err := exec(ctx, dir, "git", "branch", "-f", branch, "HEAD"); err != nil {
			return fmt.Errorf("create git branch: %w", err)
		}
		if _, err := exec(ctx, dir, "git", "push", "-u", "origin", branch); err != nil {
			return fmt.Errorf("push git branch: %w", err)
		}
		return nil
	}
	return errors.New("publish requires a jj or git repository")
}

func (p LocalPullRequestPublisher) Inspect(ctx context.Context, pr core.PullRequest) (core.PullRequest, error) {
	exec := p.exec
	if exec == nil {
		exec = runCommand
	}
	ref := pr.URL
	if ref == "" && pr.Number > 0 {
		ref = strconv.Itoa(pr.Number)
	}
	if ref == "" {
		return core.PullRequest{}, errors.New("inspect requires pull request url or number")
	}
	out, err := exec(ctx, "", "gh", "pr", "view", ref, "--repo", pr.Repo, "--json", "number,url,state,title,isDraft,headRefName,baseRefName,mergeStateStatus,statusCheckRollup,reviewDecision")
	if err != nil {
		return core.PullRequest{}, fmt.Errorf("inspect GitHub pull request: %w", err)
	}
	var payload struct {
		Number            int             `json:"number"`
		URL               string          `json:"url"`
		State             string          `json:"state"`
		Title             string          `json:"title"`
		IsDraft           bool            `json:"isDraft"`
		HeadRefName       string          `json:"headRefName"`
		BaseRefName       string          `json:"baseRefName"`
		MergeStateStatus  string          `json:"mergeStateStatus"`
		ReviewDecision    string          `json:"reviewDecision"`
		StatusCheckRollup json.RawMessage `json:"statusCheckRollup"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return core.PullRequest{}, fmt.Errorf("decode GitHub pull request: %w", err)
	}
	checked := pr
	checked.Number = payload.Number
	checked.URL = payload.URL
	checked.State = payload.State
	checked.Title = payload.Title
	checked.Draft = payload.IsDraft
	checked.Branch = payload.HeadRefName
	checked.Base = payload.BaseRefName
	checked.MergeStatus = payload.MergeStateStatus
	checked.ReviewStatus = payload.ReviewDecision
	checked.ChecksStatus = summarizeStatusCheckRollup(payload.StatusCheckRollup)
	return checked, nil
}

func defaultPRBranch(spec PullRequestPublishSpec) string {
	suffix := spec.TaskID
	if spec.WorkerID != "" {
		suffix = spec.WorkerID
	}
	return "codex/aged-" + shortID(suffix)
}

func defaultPRBody(spec PullRequestPublishSpec) string {
	var builder strings.Builder
	builder.WriteString("Created by aged.\n\n")
	if spec.TaskID != "" {
		builder.WriteString("- Task: `" + spec.TaskID + "`\n")
	}
	if spec.WorkerID != "" {
		builder.WriteString("- Worker: `" + spec.WorkerID + "`\n")
	}
	return builder.String()
}

var urlPattern = regexp.MustCompile(`https?://\S+`)

func firstURL(value string) string {
	match := urlPattern.FindString(value)
	if match == "" {
		return ""
	}
	trimmed := strings.TrimRight(match, ".,;)")
	if _, err := url.Parse(trimmed); err != nil {
		return ""
	}
	return trimmed
}

func summarizeStatusCheckRollup(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var checks []struct {
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
	}
	if err := json.Unmarshal(raw, &checks); err != nil {
		return "unknown"
	}
	if len(checks) == 0 {
		return "none"
	}
	pending := 0
	failing := 0
	success := 0
	for _, check := range checks {
		status := strings.ToUpper(check.Status)
		conclusion := strings.ToUpper(check.Conclusion)
		switch {
		case conclusion == "FAILURE" || conclusion == "CANCELLED" || conclusion == "TIMED_OUT" || conclusion == "ACTION_REQUIRED":
			failing++
		case conclusion == "SUCCESS" || conclusion == "NEUTRAL" || conclusion == "SKIPPED":
			success++
		case status != "COMPLETED":
			pending++
		default:
			pending++
		}
	}
	switch {
	case failing > 0:
		return "failing"
	case pending > 0:
		return "pending"
	case success == len(checks):
		return "passing"
	default:
		return "unknown"
	}
}

func newPullRequestID() string {
	return "pr_" + strings.ReplaceAll(uuid.NewString(), "-", "")
}
