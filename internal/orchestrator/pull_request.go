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
	"time"

	"aged/internal/core"

	"github.com/google/uuid"
)

type PullRequestPublishSpec struct {
	TaskID        string
	WorkerID      string
	WorkDir       string
	Repo          string
	Base          string
	Branch        string
	HeadRepoOwner string
	PushRemote    string
	BranchPrefix  string
	Title         string
	Body          string
	Draft         bool
	Metadata      map[string]any
}

type PullRequestPublisher interface {
	Publish(ctx context.Context, spec PullRequestPublishSpec) (core.PullRequest, error)
	Inspect(ctx context.Context, pr core.PullRequest) (core.PullRequest, error)
}

type PullRequestListSpec struct {
	TaskID     string
	Repo       string
	Number     int
	URL        string
	State      string
	Author     string
	HeadBranch string
	Limit      int
	Metadata   map[string]any
}

type PullRequestLister interface {
	List(ctx context.Context, spec PullRequestListSpec) ([]core.PullRequest, error)
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
	if err := p.pushBranch(ctx, exec, spec.WorkDir, branch, spec.PushRemote); err != nil {
		return core.PullRequest{}, err
	}

	head := prHeadRef(spec.HeadRepoOwner, branch)
	args := []string{"pr", "create", "--repo", repo, "--base", base, "--head", head, "--title", title, "--body", body}
	if spec.Draft {
		args = append(args, "--draft")
	}
	out, err := exec(ctx, spec.WorkDir, "gh", args...)
	if err != nil {
		existing, existingErr := p.findExistingPullRequest(ctx, exec, spec.WorkDir, repo, head)
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
	headOwner, headBranch, hasHeadOwner := strings.Cut(branch, ":")
	jsonFields := "number,url,state,title,isDraft,headRefName,baseRefName,headRepositoryOwner"
	args := []string{"pr", "list", "--repo", repo, "--state", "all", "--json", jsonFields}
	if hasHeadOwner {
		args = append(args, "--search", "head:"+headOwner+":"+headBranch)
	} else {
		args = append(args, "--head", branch)
	}
	out, err := exec(ctx, dir, "gh", args...)
	if err != nil {
		return core.PullRequest{}, err
	}
	var prs []struct {
		Number              int    `json:"number"`
		URL                 string `json:"url"`
		State               string `json:"state"`
		Title               string `json:"title"`
		IsDraft             bool   `json:"isDraft"`
		HeadRefName         string `json:"headRefName"`
		BaseRefName         string `json:"baseRefName"`
		HeadRepositoryOwner struct {
			Login string `json:"login"`
		} `json:"headRepositoryOwner"`
	}
	if err := json.Unmarshal([]byte(out), &prs); err != nil {
		return core.PullRequest{}, err
	}
	for _, pr := range prs {
		if hasHeadOwner && (!strings.EqualFold(pr.HeadRepositoryOwner.Login, headOwner) || pr.HeadRefName != headBranch) {
			continue
		}
		return core.PullRequest{
			Number: pr.Number,
			URL:    pr.URL,
			State:  pr.State,
			Title:  pr.Title,
			Draft:  pr.IsDraft,
			Branch: pr.HeadRefName,
			Base:   pr.BaseRefName,
		}, nil
	}
	return core.PullRequest{}, errors.New("no existing pull request found for branch")
}

func (p LocalPullRequestPublisher) pushBranch(ctx context.Context, exec commandExecutor, dir string, branch string, remote string) error {
	remote = strings.TrimSpace(remote)
	if _, err := exec(ctx, dir, "jj", "root"); err == nil {
		if _, err := exec(ctx, dir, "jj", "bookmark", "create", branch, "--revision", "@"); err != nil {
			if _, setErr := exec(ctx, dir, "jj", "bookmark", "set", branch, "--revision", "@"); setErr != nil {
				return fmt.Errorf("create jj bookmark: %w; set existing bookmark: %w", err, setErr)
			}
		}
		args := []string{"git", "push", "--bookmark", branch}
		if remote != "" {
			args = append(args, "--remote", remote)
		}
		if _, err := exec(ctx, dir, "jj", args...); err != nil {
			return fmt.Errorf("push jj bookmark: %w", err)
		}
		return nil
	}
	if _, err := exec(ctx, dir, "git", "rev-parse", "--show-toplevel"); err == nil {
		if _, err := exec(ctx, dir, "git", "branch", "-f", branch, "HEAD"); err != nil {
			return fmt.Errorf("create git branch: %w", err)
		}
		if remote == "" {
			remote = "origin"
		}
		if _, err := exec(ctx, dir, "git", "push", "-u", remote, branch); err != nil {
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
	out, err := exec(ctx, "", "gh", "pr", "view", ref, "--repo", pr.Repo, "--json", "number,url,state,title,isDraft,headRefName,baseRefName,mergeStateStatus,statusCheckRollup,reviewDecision,comments")
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
		Comments          []prComment     `json:"comments"`
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
	checked.Metadata = pullRequestMetadataWithComments(pr.Metadata, payload.Comments, &checked)
	return checked, nil
}

func (p LocalPullRequestPublisher) List(ctx context.Context, spec PullRequestListSpec) ([]core.PullRequest, error) {
	exec := p.exec
	if exec == nil {
		exec = runCommand
	}
	repo := strings.TrimSpace(spec.Repo)
	number := spec.Number
	if repo == "" && strings.TrimSpace(spec.URL) != "" {
		parsedRepo, parsedNumber := parsePullRequestURL(spec.URL)
		repo = parsedRepo
		if number == 0 {
			number = parsedNumber
		}
	}
	if repo == "" {
		return nil, errors.New("watch pull requests requires repo or url")
	}
	if number > 0 || strings.TrimSpace(spec.URL) != "" {
		pr := core.PullRequest{
			ID:       newPullRequestID(),
			TaskID:   spec.TaskID,
			Repo:     repo,
			Number:   number,
			URL:      strings.TrimSpace(spec.URL),
			Metadata: core.MustJSON(spec.Metadata),
		}
		inspected, err := p.Inspect(ctx, pr)
		if err != nil {
			return nil, err
		}
		inspected.ID = pr.ID
		inspected.TaskID = spec.TaskID
		if len(inspected.Metadata) == 0 {
			inspected.Metadata = pr.Metadata
		}
		return []core.PullRequest{inspected}, nil
	}
	limit := spec.Limit
	if limit <= 0 {
		limit = 20
	}
	state := strings.ToLower(strings.TrimSpace(spec.State))
	if state == "" {
		state = "open"
	}
	jsonFields := "number,url,state,title,isDraft,headRefName,baseRefName,mergeStateStatus,statusCheckRollup,reviewDecision"
	args := []string{"pr", "list", "--repo", repo, "--state", state, "--limit", strconv.Itoa(limit), "--json", jsonFields}
	if strings.TrimSpace(spec.Author) != "" {
		args = append(args, "--author", strings.TrimSpace(spec.Author))
	}
	if strings.TrimSpace(spec.HeadBranch) != "" {
		args = append(args, "--head", strings.TrimSpace(spec.HeadBranch))
	}
	out, err := exec(ctx, "", "gh", args...)
	if err != nil {
		return nil, fmt.Errorf("list GitHub pull requests: %w", err)
	}
	var payload []struct {
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
		return nil, fmt.Errorf("decode GitHub pull request list: %w", err)
	}
	prs := make([]core.PullRequest, 0, len(payload))
	for _, item := range payload {
		prs = append(prs, core.PullRequest{
			ID:           newPullRequestID(),
			TaskID:       spec.TaskID,
			Repo:         repo,
			Number:       item.Number,
			URL:          item.URL,
			Branch:       item.HeadRefName,
			Base:         item.BaseRefName,
			Title:        item.Title,
			State:        item.State,
			Draft:        item.IsDraft,
			ChecksStatus: summarizeStatusCheckRollup(item.StatusCheckRollup),
			MergeStatus:  item.MergeStateStatus,
			ReviewStatus: item.ReviewDecision,
			Metadata:     core.MustJSON(spec.Metadata),
		})
	}
	return prs, nil
}

type prComment struct {
	ID              string `json:"id"`
	Body            string `json:"body"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
	ViewerDidAuthor bool   `json:"viewerDidAuthor"`
	Author          struct {
		Login string `json:"login"`
	} `json:"author"`
}

func pullRequestMetadataWithComments(raw json.RawMessage, comments []prComment, checked *core.PullRequest) json.RawMessage {
	metadata := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &metadata)
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	baselineEstablished, _ := metadata["conversationCommentBaselineEstablished"].(bool)
	previousSignature, _ := metadata["latestConversationCommentSignature"].(string)
	triggeredSignature, _ := metadata["latestConversationCommentTriggeredSignature"].(string)
	latest, ok := latestExternalConversationComment(comments)
	if ok {
		signature := latest.Signature()
		shouldTrigger := baselineEstablished && signature != previousSignature
		if !shouldTrigger && triggeredSignature != signature && commentAfterPullRequestWatch(latest, checked.CreatedAt) {
			shouldTrigger = true
		}
		if shouldTrigger {
			checked.ReviewStatus = "COMMENTED"
			metadata["latestConversationCommentTriggeredSignature"] = signature
		}
		metadata["latestConversationCommentSignature"] = signature
		metadata["latestConversationCommentId"] = latest.ID
		metadata["latestConversationCommentAuthor"] = latest.Author.Login
		metadata["latestConversationCommentCreatedAt"] = latest.CreatedAt
		metadata["latestConversationCommentUpdatedAt"] = latest.UpdatedAt
		metadata["latestConversationCommentBody"] = truncatePRCommentBody(latest.Body)
	}
	metadata["conversationCommentBaselineEstablished"] = true
	return core.MustJSON(metadata)
}

func commentAfterPullRequestWatch(comment prComment, watchedAt time.Time) bool {
	if watchedAt.IsZero() {
		return false
	}
	commentAt := strings.TrimSpace(comment.UpdatedAt)
	if commentAt == "" {
		commentAt = strings.TrimSpace(comment.CreatedAt)
	}
	if commentAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339, commentAt)
	if err != nil {
		return false
	}
	return parsed.After(watchedAt)
}

func latestExternalConversationComment(comments []prComment) (prComment, bool) {
	var latest prComment
	for _, comment := range comments {
		if comment.ID == "" || comment.ViewerDidAuthor {
			continue
		}
		if latest.ID == "" || comment.Signature() > latest.Signature() {
			latest = comment
		}
	}
	return latest, latest.ID != ""
}

func (c prComment) Signature() string {
	updated := strings.TrimSpace(c.UpdatedAt)
	if updated == "" {
		updated = strings.TrimSpace(c.CreatedAt)
	}
	return updated + ":" + strings.TrimSpace(c.ID)
}

func truncatePRCommentBody(body string) string {
	body = strings.TrimSpace(body)
	const limit = 2000
	if len(body) <= limit {
		return body
	}
	return body[:limit] + "\n[truncated]"
}

func defaultPRBranch(spec PullRequestPublishSpec) string {
	suffix := spec.TaskID
	if spec.WorkerID != "" {
		suffix = spec.WorkerID
	}
	prefix := strings.TrimSpace(spec.BranchPrefix)
	if prefix == "" {
		prefix = "codex/aged-"
	}
	return prefix + shortID(suffix)
}

func parsePullRequestURL(value string) (string, int) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", 0
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "pull" {
		return "", 0
	}
	number, _ := strconv.Atoi(parts[3])
	return parts[0] + "/" + parts[1], number
}

func prHeadRef(owner string, branch string) string {
	owner = strings.TrimSpace(owner)
	branch = strings.TrimSpace(branch)
	if owner == "" || strings.Contains(branch, ":") {
		return branch
	}
	return owner + ":" + branch
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
