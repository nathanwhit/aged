package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
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
	ResetWorkDir  bool
	Metadata      map[string]any
}

type PullRequestPublisher interface {
	Publish(ctx context.Context, spec PullRequestPublishSpec) (core.PullRequest, error)
	Update(ctx context.Context, pr core.PullRequest, spec PullRequestPublishSpec) (core.PullRequest, error)
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
			return core.PullRequest{}, wrapGitHubCommandError("resolve GitHub repo", err)
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
	bodyFile, err := writePullRequestBodyFile(body)
	if err != nil {
		return core.PullRequest{}, err
	}
	defer os.Remove(bodyFile)

	if err := p.pushBranch(ctx, exec, spec, branch, base); err != nil {
		return core.PullRequest{}, err
	}

	head := prHeadRef(spec.HeadRepoOwner, branch)
	args := []string{"pr", "create", "--repo", repo, "--base", base, "--head", head, "--title", title, "--body-file", bodyFile}
	if spec.Draft {
		args = append(args, "--draft")
	}
	out, err := exec(ctx, spec.WorkDir, "gh", args...)
	if err != nil {
		existing, existingErr := p.findExistingPullRequest(ctx, exec, spec.WorkDir, repo, head)
		if existingErr != nil {
			return core.PullRequest{}, fmt.Errorf("%w; %w", wrapGitHubCommandError("create GitHub pull request", err), wrapGitHubCommandError("find existing pull request", existingErr))
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

func (p LocalPullRequestPublisher) Update(ctx context.Context, pr core.PullRequest, spec PullRequestPublishSpec) (core.PullRequest, error) {
	if strings.TrimSpace(spec.WorkDir) == "" {
		return core.PullRequest{}, errors.New("update pull request requires a workdir")
	}
	exec := p.exec
	if exec == nil {
		exec = runCommand
	}
	repo := nonEmpty(spec.Repo, pr.Repo)
	if repo == "" {
		resolved, err := exec(ctx, spec.WorkDir, "gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner")
		if err != nil {
			return core.PullRequest{}, wrapGitHubCommandError("resolve GitHub repo", err)
		}
		repo = strings.TrimSpace(resolved)
	}
	if repo == "" {
		return core.PullRequest{}, errors.New("update pull request requires repo")
	}
	base := nonEmpty(spec.Base, pr.Base, "main")
	branch := nonEmpty(spec.Branch, pr.Branch)
	if branch == "" {
		return core.PullRequest{}, errors.New("update pull request requires branch")
	}
	spec.Repo = repo
	spec.Base = base
	spec.Branch = branch
	if err := p.pushBranch(ctx, exec, spec, branch, base); err != nil {
		return core.PullRequest{}, err
	}
	if strings.TrimSpace(spec.Title) != "" || strings.TrimSpace(spec.Body) != "" {
		ref := pr.URL
		if ref == "" && pr.Number > 0 {
			ref = strconv.Itoa(pr.Number)
		}
		if ref == "" {
			return core.PullRequest{}, errors.New("update pull request metadata requires pull request url or number")
		}
		args := []string{"pr", "edit", ref, "--repo", repo}
		if title := strings.TrimSpace(spec.Title); title != "" {
			args = append(args, "--title", title)
		}
		var bodyFile string
		if body := strings.TrimSpace(spec.Body); body != "" {
			var err error
			bodyFile, err = writePullRequestBodyFile(body)
			if err != nil {
				return core.PullRequest{}, err
			}
			defer os.Remove(bodyFile)
			args = append(args, "--body-file", bodyFile)
		}
		if _, err := exec(ctx, spec.WorkDir, "gh", args...); err != nil {
			return core.PullRequest{}, wrapGitHubCommandError("edit GitHub pull request", err)
		}
	}
	updated := pr
	updated.Repo = repo
	updated.Branch = branch
	updated.Base = base
	if strings.TrimSpace(spec.Title) != "" {
		updated.Title = strings.TrimSpace(spec.Title)
	}
	inspected, err := p.Inspect(ctx, updated)
	if err != nil {
		updated.Metadata = core.MustJSON(spec.Metadata)
		return updated, nil
	}
	inspected.ID = pr.ID
	inspected.TaskID = pr.TaskID
	if len(inspected.Metadata) == 0 {
		inspected.Metadata = core.MustJSON(spec.Metadata)
	}
	return inspected, nil
}

func writePullRequestBodyFile(body string) (string, error) {
	file, err := os.CreateTemp("", "aged-pr-body-*.md")
	if err != nil {
		return "", fmt.Errorf("create pull request body file: %w", err)
	}
	path := file.Name()
	if _, err := file.WriteString(body); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("write pull request body file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close pull request body file: %w", err)
	}
	return path, nil
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
		return core.PullRequest{}, wrapGitHubCommandError("find existing pull request", err)
	}
	var prs []githubPullRequestPayload
	if err := json.Unmarshal([]byte(out), &prs); err != nil {
		return core.PullRequest{}, err
	}
	for _, pr := range prs {
		if hasHeadOwner && (!strings.EqualFold(pr.HeadRepositoryOwner.Login, headOwner) || pr.HeadRefName != headBranch) {
			continue
		}
		return pr.pullRequest(core.PullRequest{}), nil
	}
	return core.PullRequest{}, errors.New("no existing pull request found for branch")
}

func (p LocalPullRequestPublisher) pushBranch(ctx context.Context, exec commandExecutor, spec PullRequestPublishSpec, branch string, base string) error {
	dir := spec.WorkDir
	remote := spec.PushRemote
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
		if err := materializeGitPullRequestChanges(ctx, exec, dir, branch, spec); err != nil {
			return err
		}
		if err := ensureGitPullRequestHasChanges(ctx, exec, dir, base); err != nil {
			return err
		}
		if _, err := exec(ctx, dir, "git", "branch", "-f", branch, "HEAD"); err != nil {
			return fmt.Errorf("create git branch: %w", err)
		}
		if remote == "" {
			remote = "origin"
		}
		if _, err := exec(ctx, dir, "git", "push", "-u", remote, branch); err != nil {
			return fmt.Errorf("push git branch: %w", err)
		}
		if spec.ResetWorkDir {
			if err := resetGitPullRequestWorkDir(ctx, exec, dir, base); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("publish requires a jj or git repository")
}

func resetGitPullRequestWorkDir(ctx context.Context, exec commandExecutor, dir string, base string) error {
	baseRef := gitPublishBaseRef(ctx, exec, dir, base)
	if baseRef == "" {
		return nil
	}
	if _, err := exec(ctx, dir, "git", "reset", "--hard", baseRef); err != nil {
		return fmt.Errorf("reset git publish workdir to base: %w", err)
	}
	return nil
}

func materializeGitPullRequestChanges(ctx context.Context, exec commandExecutor, dir string, branch string, spec PullRequestPublishSpec) error {
	status, err := exec(ctx, dir, "git", "status", "--porcelain=v1")
	if err != nil {
		return fmt.Errorf("read git status before publish: %w", err)
	}
	if strings.TrimSpace(status) == "" {
		return nil
	}
	if _, err := exec(ctx, dir, "git", "add", "-A"); err != nil {
		return fmt.Errorf("stage git changes before publish: %w", err)
	}
	changedFiles, err := gitIndexChangedFilesWithExec(ctx, exec, dir)
	if err != nil {
		return fmt.Errorf("list staged git changes before publish: %w", err)
	}
	if len(changedFiles) == 0 {
		return nil
	}
	fallback := "Publish aged worker changes"
	if strings.TrimSpace(branch) != "" {
		fallback = "Publish " + strings.TrimSpace(branch)
	}
	message := changeCommitMessage(changeCommitMessageContext{
		Fallback:     fallback,
		PullTitle:    spec.Title,
		Metadata:     spec.Metadata,
		ChangedFiles: changedFiles,
	})
	if _, err := exec(ctx, dir, "git", "-c", "commit.gpgsign=false", "commit", "-m", message); err != nil {
		return fmt.Errorf("commit git changes before publish: %w", err)
	}
	return nil
}

func gitIndexChangedFilesWithExec(ctx context.Context, exec commandExecutor, dir string) ([]string, error) {
	out, err := exec(ctx, dir, "git", "diff", "--cached", "--name-only", "-z", "HEAD", "--")
	if err != nil {
		return nil, err
	}
	return splitNULFields(out), nil
}

func ensureGitPullRequestHasChanges(ctx context.Context, exec commandExecutor, dir string, base string) error {
	baseRef := gitPublishBaseRef(ctx, exec, dir, base)
	if baseRef == "" {
		return nil
	}
	diff, err := exec(ctx, dir, "git", "diff", "--name-only", baseRef+"...HEAD", "--")
	if err != nil {
		return fmt.Errorf("inspect git changes against base before publish: %w", err)
	}
	if strings.TrimSpace(diff) == "" {
		return errors.New("refusing to publish pull request with no changes against base")
	}
	return nil
}

func gitPublishBaseRef(ctx context.Context, exec commandExecutor, dir string, base string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "main"
	}
	candidates := []string{
		"refs/remotes/upstream/" + base,
		"refs/remotes/origin/" + base,
		"refs/heads/" + base,
		base,
	}
	if strings.HasPrefix(base, "refs/") {
		candidates = append([]string{base}, candidates...)
	}
	for _, candidate := range candidates {
		if _, err := exec(ctx, dir, "git", "rev-parse", "--verify", "--quiet", candidate+"^{commit}"); err == nil {
			return candidate
		}
	}
	return ""
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
	out, err := exec(ctx, "", "gh", "pr", "view", ref, "--repo", pr.Repo, "--json", "number,url,state,title,isDraft,headRefName,baseRefName,mergeStateStatus,mergeable,statusCheckRollup,reviewDecision,comments,reviews")
	if err != nil {
		return core.PullRequest{}, wrapGitHubCommandError("inspect GitHub pull request", err)
	}
	var payload struct {
		githubPullRequestPayload
		Comments []prComment `json:"comments"`
		Reviews  []prReview  `json:"reviews"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return core.PullRequest{}, fmt.Errorf("decode GitHub pull request: %w", err)
	}
	checked := payload.pullRequest(pr)
	threadFeedback, err := p.pullRequestReviewThreadFeedback(ctx, exec, checked)
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.Metadata = pullRequestMetadataWithFeedback(pr.Metadata, pullRequestFeedback(payload.Comments, payload.Reviews, threadFeedback), &checked)
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
	jsonFields := "number,url,state,title,isDraft,headRefName,baseRefName,mergeStateStatus,mergeable,statusCheckRollup,reviewDecision"
	args := []string{"pr", "list", "--repo", repo, "--state", state, "--limit", strconv.Itoa(limit), "--json", jsonFields}
	if strings.TrimSpace(spec.Author) != "" {
		args = append(args, "--author", strings.TrimSpace(spec.Author))
	}
	if strings.TrimSpace(spec.HeadBranch) != "" {
		args = append(args, "--head", strings.TrimSpace(spec.HeadBranch))
	}
	out, err := exec(ctx, "", "gh", args...)
	if err != nil {
		return nil, wrapGitHubCommandError("list GitHub pull requests", err)
	}
	var payload []githubPullRequestPayload
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return nil, fmt.Errorf("decode GitHub pull request list: %w", err)
	}
	prs := make([]core.PullRequest, 0, len(payload))
	for _, item := range payload {
		prs = append(prs, item.pullRequest(core.PullRequest{
			ID:       newPullRequestID(),
			TaskID:   spec.TaskID,
			Repo:     repo,
			Metadata: core.MustJSON(spec.Metadata),
		}))
	}
	return prs, nil
}

type githubPullRequestPayload struct {
	Number              int             `json:"number"`
	URL                 string          `json:"url"`
	State               string          `json:"state"`
	Title               string          `json:"title"`
	IsDraft             bool            `json:"isDraft"`
	HeadRefName         string          `json:"headRefName"`
	BaseRefName         string          `json:"baseRefName"`
	MergeStateStatus    string          `json:"mergeStateStatus"`
	Mergeable           string          `json:"mergeable"`
	ReviewDecision      string          `json:"reviewDecision"`
	StatusCheckRollup   json.RawMessage `json:"statusCheckRollup"`
	HeadRepositoryOwner struct {
		Login string `json:"login"`
	} `json:"headRepositoryOwner"`
}

func (payload githubPullRequestPayload) pullRequest(pr core.PullRequest) core.PullRequest {
	pr.Number = payload.Number
	pr.URL = payload.URL
	pr.State = payload.State
	pr.Title = payload.Title
	pr.Draft = payload.IsDraft
	pr.Branch = payload.HeadRefName
	pr.Base = payload.BaseRefName
	pr.Mergeable = payload.Mergeable
	pr.MergeStatus = pullRequestMergeStatus(payload.MergeStateStatus, payload.Mergeable)
	pr.ReviewStatus = payload.ReviewDecision
	checks := summarizeStatusCheckRollup(payload.StatusCheckRollup)
	pr.ChecksStatus = checks.Status
	pr.ChecksConclusion = checks.Conclusion
	return pr
}

type prComment struct {
	ID              string `json:"id"`
	Body            string `json:"body"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
	ViewerDidAuthor bool   `json:"viewerDidAuthor"`
	Author          prAuthor
}

type prReview struct {
	ID          string   `json:"id"`
	Body        string   `json:"body"`
	SubmittedAt string   `json:"submittedAt"`
	State       string   `json:"state"`
	Author      prAuthor `json:"author"`
}

type prAuthor struct {
	Login string `json:"login"`
}

type prFeedback struct {
	ID              string
	Body            string
	CreatedAt       string
	UpdatedAt       string
	ViewerDidAuthor bool
	Author          prAuthor
	Source          string
	Path            string
	Line            int
	URL             string
}

func pullRequestFeedback(comments []prComment, reviews []prReview, threadFeedback []prFeedback) []prFeedback {
	feedback := make([]prFeedback, 0, len(comments)+len(reviews)+len(threadFeedback))
	for _, comment := range comments {
		feedback = append(feedback, prFeedback{
			ID:              comment.ID,
			Body:            comment.Body,
			CreatedAt:       comment.CreatedAt,
			UpdatedAt:       comment.UpdatedAt,
			ViewerDidAuthor: comment.ViewerDidAuthor,
			Author:          comment.Author,
			Source:          "conversation",
		})
	}
	for _, review := range reviews {
		if strings.TrimSpace(review.Body) == "" {
			continue
		}
		feedback = append(feedback, prFeedback{
			ID:        review.ID,
			Body:      review.Body,
			CreatedAt: review.SubmittedAt,
			UpdatedAt: review.SubmittedAt,
			Author:    review.Author,
			Source:    "review",
		})
	}
	feedback = append(feedback, threadFeedback...)
	return feedback
}

func (p LocalPullRequestPublisher) pullRequestReviewThreadFeedback(ctx context.Context, exec commandExecutor, pr core.PullRequest) ([]prFeedback, error) {
	repo := strings.TrimSpace(pr.Repo)
	if repo == "" {
		repo, _ = parsePullRequestURL(pr.URL)
	}
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || strings.TrimSpace(owner) == "" || strings.TrimSpace(name) == "" || pr.Number == 0 {
		return nil, nil
	}
	const query = `query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        nodes {
          isResolved
          path
          line
          startLine
          comments(first: 100) {
            nodes {
              id
              body
              createdAt
              updatedAt
              viewerDidAuthor
              url
              author { login }
            }
          }
        }
      }
    }
  }
}`
	out, err := exec(ctx, "", "gh", "api", "graphql", "-f", "query="+query, "-f", "owner="+owner, "-f", "name="+name, "-F", fmt.Sprintf("number=%d", pr.Number))
	if err != nil {
		return nil, fmt.Errorf("inspect GitHub pull request review threads: %w", err)
	}
	var payload struct {
		Data struct {
			Repository struct {
				PullRequest struct {
					ReviewThreads struct {
						Nodes []struct {
							IsResolved bool   `json:"isResolved"`
							Path       string `json:"path"`
							Line       int    `json:"line"`
							StartLine  int    `json:"startLine"`
							Comments   struct {
								Nodes []prFeedback `json:"nodes"`
							} `json:"comments"`
						} `json:"nodes"`
					} `json:"reviewThreads"`
				} `json:"pullRequest"`
			} `json:"repository"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return nil, fmt.Errorf("decode GitHub pull request review threads: %w", err)
	}
	var feedback []prFeedback
	for _, thread := range payload.Data.Repository.PullRequest.ReviewThreads.Nodes {
		if thread.IsResolved {
			continue
		}
		line := thread.Line
		if line == 0 {
			line = thread.StartLine
		}
		for _, comment := range thread.Comments.Nodes {
			comment.Source = "review_thread"
			comment.Path = thread.Path
			comment.Line = line
			feedback = append(feedback, comment)
		}
	}
	return feedback, nil
}

func pullRequestMetadataWithFeedback(raw json.RawMessage, feedback []prFeedback, checked *core.PullRequest) json.RawMessage {
	metadata := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &metadata)
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	baselineEstablished, _ := metadata["pullRequestFeedbackBaselineEstablished"].(bool)
	if !baselineEstablished {
		baselineEstablished, _ = metadata["conversationCommentBaselineEstablished"].(bool)
	}
	previousSignature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackSignature"]))
	if previousSignature == "" {
		previousSignature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentSignature"]))
	}
	triggeredSignature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackTriggeredSignature"]))
	if triggeredSignature == "" {
		triggeredSignature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentTriggeredSignature"]))
	}
	latest, ok := latestExternalPullRequestFeedback(feedback)
	if ok {
		signature := latest.Signature()
		shouldTrigger := baselineEstablished && triggeredSignature != signature
		if !shouldTrigger && triggeredSignature != signature && feedbackAfterPullRequestWatch(latest, checked.CreatedAt) {
			shouldTrigger = true
		}
		if shouldTrigger {
			checked.ReviewStatus = "COMMENTED"
		} else if !baselineEstablished && previousSignature == "" && triggeredSignature == "" {
			metadata["latestPullRequestFeedbackTriggeredSignature"] = signature
			metadata["latestConversationCommentTriggeredSignature"] = signature
		}
		metadata["latestPullRequestFeedbackSignature"] = signature
		metadata["latestPullRequestFeedbackId"] = latest.ID
		metadata["latestPullRequestFeedbackAuthor"] = latest.Author.Login
		metadata["latestPullRequestFeedbackCreatedAt"] = latest.CreatedAt
		metadata["latestPullRequestFeedbackUpdatedAt"] = latest.UpdatedAt
		metadata["latestPullRequestFeedbackBody"] = truncatePRCommentBody(latest.Body)
		metadata["latestPullRequestFeedbackSource"] = latest.Source
		metadata["latestPullRequestFeedbackPath"] = latest.Path
		metadata["latestPullRequestFeedbackLine"] = latest.Line
		metadata["latestPullRequestFeedbackURL"] = latest.URL
		metadata["latestConversationCommentSignature"] = signature
		metadata["latestConversationCommentId"] = latest.ID
		metadata["latestConversationCommentAuthor"] = latest.Author.Login
		metadata["latestConversationCommentCreatedAt"] = latest.CreatedAt
		metadata["latestConversationCommentUpdatedAt"] = latest.UpdatedAt
		metadata["latestConversationCommentBody"] = truncatePRCommentBody(latest.Body)
	}
	metadata["pullRequestFeedbackBaselineEstablished"] = true
	metadata["conversationCommentBaselineEstablished"] = true
	return core.MustJSON(metadata)
}

func pullRequestMetadataMarkFeedbackTriggered(raw json.RawMessage) (json.RawMessage, bool) {
	metadata := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &metadata)
	}
	if metadata == nil {
		return raw, false
	}
	signature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackSignature"]))
	if signature == "" {
		signature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentSignature"]))
	}
	if signature == "" {
		return raw, false
	}
	triggeredSignature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackTriggeredSignature"]))
	if triggeredSignature == "" {
		triggeredSignature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentTriggeredSignature"]))
	}
	if triggeredSignature == signature {
		return raw, false
	}
	metadata["latestPullRequestFeedbackTriggeredSignature"] = signature
	metadata["latestConversationCommentTriggeredSignature"] = signature
	return core.MustJSON(metadata), true
}

func pullRequestHasUntriggeredFeedback(pr core.PullRequest) bool {
	if len(pr.Metadata) == 0 {
		return false
	}
	var metadata map[string]any
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return false
	}
	signature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackSignature"]))
	if signature == "" {
		signature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentSignature"]))
	}
	if signature == "" {
		return false
	}
	triggeredSignature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackTriggeredSignature"]))
	if triggeredSignature == "" {
		triggeredSignature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentTriggeredSignature"]))
	}
	return triggeredSignature != signature
}

func feedbackAfterPullRequestWatch(feedback prFeedback, watchedAt time.Time) bool {
	if watchedAt.IsZero() {
		return false
	}
	feedbackAt := strings.TrimSpace(feedback.UpdatedAt)
	if feedbackAt == "" {
		feedbackAt = strings.TrimSpace(feedback.CreatedAt)
	}
	if feedbackAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339, feedbackAt)
	if err != nil {
		return false
	}
	return parsed.After(watchedAt)
}

func latestExternalPullRequestFeedback(feedback []prFeedback) (prFeedback, bool) {
	var latest prFeedback
	for _, item := range feedback {
		if item.ID == "" || strings.TrimSpace(item.Body) == "" || item.ViewerDidAuthor {
			continue
		}
		if latest.ID == "" || item.Signature() > latest.Signature() {
			latest = item
		}
	}
	return latest, latest.ID != ""
}

func (f prFeedback) Signature() string {
	updated := strings.TrimSpace(f.UpdatedAt)
	if updated == "" {
		updated = strings.TrimSpace(f.CreatedAt)
	}
	return updated + ":" + strings.TrimSpace(f.Source) + ":" + strings.TrimSpace(f.ID)
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
	return ""
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

type statusCheckRollupSummary struct {
	Status     string
	Conclusion string
}

func summarizeStatusCheckRollup(raw json.RawMessage) statusCheckRollupSummary {
	if len(raw) == 0 || string(raw) == "null" {
		return statusCheckRollupSummary{}
	}
	var checks []struct {
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		State      string `json:"state"`
	}
	if err := json.Unmarshal(raw, &checks); err != nil {
		return statusCheckRollupSummary{Status: "unknown", Conclusion: "unknown"}
	}
	if len(checks) == 0 {
		return statusCheckRollupSummary{Status: "none", Conclusion: "none"}
	}
	pending := 0
	failing := 0
	success := 0
	for _, check := range checks {
		status := strings.ToUpper(check.Status)
		conclusion := strings.ToUpper(nonEmpty(check.Conclusion, check.State))
		switch {
		case conclusion == "FAILURE" || conclusion == "ERROR" || conclusion == "CANCELLED" || conclusion == "TIMED_OUT" || conclusion == "ACTION_REQUIRED":
			failing++
		case conclusion == "SUCCESS" || conclusion == "NEUTRAL" || conclusion == "SKIPPED":
			success++
		case status != "" && status != "COMPLETED":
			pending++
		default:
			pending++
		}
	}
	switch {
	case failing > 0:
		return statusCheckRollupSummary{Status: "failing", Conclusion: "failure"}
	case pending > 0:
		return statusCheckRollupSummary{Status: "pending", Conclusion: "pending"}
	case success == len(checks):
		return statusCheckRollupSummary{Status: "passing", Conclusion: "success"}
	default:
		return statusCheckRollupSummary{Status: "unknown", Conclusion: "unknown"}
	}
}

func pullRequestMergeStatus(mergeStateStatus string, mergeable string) string {
	if strings.TrimSpace(mergeStateStatus) != "" {
		return mergeStateStatus
	}
	return mergeable
}

func normalizePullRequestStatusFields(pr core.PullRequest) core.PullRequest {
	if strings.TrimSpace(pr.ChecksStatus) == "" {
		pr.ChecksStatus = checksStatusFromConclusion(pr.ChecksConclusion)
	}
	if strings.TrimSpace(pr.ChecksConclusion) == "" {
		pr.ChecksConclusion = checksConclusionFromStatus(pr.ChecksStatus)
	}
	if strings.TrimSpace(pr.MergeStatus) == "" {
		pr.MergeStatus = pr.Mergeable
	}
	if strings.TrimSpace(pr.Mergeable) == "" {
		pr.Mergeable = mergeableFromStatus(pr.MergeStatus)
	}
	return pr
}

func checksStatusFromConclusion(conclusion string) string {
	switch strings.ToUpper(strings.TrimSpace(conclusion)) {
	case "SUCCESS", "NEUTRAL", "SKIPPED":
		return "passing"
	case "FAILURE", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE":
		return "failing"
	case "PENDING", "EXPECTED":
		return "pending"
	case "NONE":
		return "none"
	case "UNKNOWN":
		return "unknown"
	default:
		return ""
	}
}

func checksConclusionFromStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "passing", "success":
		return "success"
	case "failing", "failure":
		return "failure"
	case "pending":
		return "pending"
	case "none":
		return "none"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func mergeableFromStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "MERGEABLE", "CONFLICTING", "UNKNOWN":
		return strings.ToUpper(strings.TrimSpace(status))
	default:
		return ""
	}
}

func newPullRequestID() string {
	return "pr_" + strings.ReplaceAll(uuid.NewString(), "-", "")
}
