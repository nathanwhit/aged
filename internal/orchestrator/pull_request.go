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
	TaskID         string
	WorkerID       string
	WorkDir        string
	Repo           string
	Base           string
	Branch         string
	HeadRepoOwner  string
	PushRemote     string
	BranchPrefix   string
	Title          string
	Body           string
	Draft          bool
	Patch          string
	PatchFromBase  bool
	PatchBaseRef   string
	ResetWorkDir   bool
	ForceWithLease bool
	MetadataOnly   bool
	UpdateExisting bool
	Metadata       map[string]any
}

type PullRequestMergeSpec struct {
	WorkDir string
	Repo    string
	Number  int
	URL     string
	Method  string
	Auto    bool
}

type PullRequestPublisher interface {
	Publish(ctx context.Context, spec PullRequestPublishSpec) (core.PullRequest, error)
	Update(ctx context.Context, pr core.PullRequest, spec PullRequestPublishSpec) (core.PullRequest, error)
	Inspect(ctx context.Context, pr core.PullRequest) (core.PullRequest, error)
}

type PullRequestMerger interface {
	Merge(ctx context.Context, pr core.PullRequest, spec PullRequestMergeSpec) (core.PullRequest, error)
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
	spec.UpdateExisting = true
	if !spec.MetadataOnly {
		if err := p.pushBranch(ctx, exec, spec, branch, base); err != nil {
			return core.PullRequest{}, err
		}
	}
	if strings.TrimSpace(spec.Title) != "" || strings.TrimSpace(spec.Body) != "" {
		if err := updatePullRequestMetadata(ctx, exec, spec.WorkDir, repo, pr, spec); err != nil {
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

func updatePullRequestMetadata(ctx context.Context, exec commandExecutor, dir string, repo string, pr core.PullRequest, spec PullRequestPublishSpec) error {
	number := pr.Number
	if number <= 0 {
		parsedRepo, parsedNumber := parsePullRequestURL(pr.URL)
		number = parsedNumber
		if strings.TrimSpace(repo) == "" {
			repo = parsedRepo
		}
	}
	if number <= 0 {
		return errors.New("update pull request metadata requires pull request url or number")
	}
	if strings.TrimSpace(repo) == "" {
		return errors.New("update pull request metadata requires repo")
	}
	payload := map[string]string{}
	if title := strings.TrimSpace(spec.Title); title != "" {
		payload["title"] = title
	}
	if body := strings.TrimSpace(spec.Body); body != "" {
		payload["body"] = body
	}
	if len(payload) == 0 {
		return nil
	}
	file, err := os.CreateTemp("", "aged-pr-edit-*.json")
	if err != nil {
		return fmt.Errorf("create pull request edit payload: %w", err)
	}
	path := file.Name()
	defer os.Remove(path)
	if err := json.NewEncoder(file).Encode(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("write pull request edit payload: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close pull request edit payload: %w", err)
	}
	_, err = exec(ctx, dir, "gh", "api", "--method", "PATCH", "repos/"+repo+"/pulls/"+strconv.Itoa(number), "--input", path)
	return err
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
	if spec.PatchFromBase {
		if _, err := exec(ctx, dir, "git", "rev-parse", "--show-toplevel"); err == nil {
			return p.pushGitPatchBranch(ctx, exec, dir, branch, base, remote, spec)
		}
		return errors.New("publish patch requires a git repository")
	}
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
		if remote == "" {
			remote = "origin"
		}
		if err := pushGitPublishBranch(ctx, exec, dir, branch, remote, spec.ForceWithLease); err != nil {
			return err
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

func (p LocalPullRequestPublisher) pushGitPatchBranch(ctx context.Context, exec commandExecutor, dir string, branch string, base string, remote string, spec PullRequestPublishSpec) error {
	refreshGitPublishBaseRefs(ctx, exec, dir)
	applyBaseRef := gitPublishPatchApplyBaseRef(ctx, exec, dir, base, spec)
	if applyBaseRef == "" {
		return fmt.Errorf("prepare git patch worktree: base %q is not available", nonEmpty(spec.PatchBaseRef, base, "main"))
	}
	worktree, err := os.MkdirTemp("", "aged-pr-worktree-*")
	if err != nil {
		return fmt.Errorf("create git patch worktree tempdir: %w", err)
	}
	if err := os.Remove(worktree); err != nil {
		_ = os.RemoveAll(worktree)
		return fmt.Errorf("prepare git patch worktree tempdir: %w", err)
	}
	cleanup := func() {
		_, _ = exec(ctx, dir, "git", "worktree", "remove", "--force", worktree)
		_ = os.RemoveAll(worktree)
	}
	defer cleanup()
	if _, err := exec(ctx, dir, "git", "worktree", "add", "--detach", worktree, applyBaseRef); err != nil {
		return fmt.Errorf("create git patch worktree: %w", err)
	}
	if strings.TrimSpace(spec.Patch) != "" {
		if err := applyGitPatchToWorkspace(ctx, worktree, spec.Patch); err != nil {
			return fmt.Errorf("apply worker patch to git patch worktree: %w", err)
		}
	}
	if err := materializeGitPullRequestChanges(ctx, exec, worktree, branch, spec); err != nil {
		return err
	}
	if err := ensureGitPullRequestHasChanges(ctx, exec, worktree, base); err != nil {
		return err
	}
	if remote == "" {
		remote = "origin"
	}
	if err := pushGitPublishBranch(ctx, exec, worktree, branch, remote, spec.ForceWithLease); err != nil {
		return err
	}
	return nil
}

func gitPublishPatchApplyBaseRef(ctx context.Context, exec commandExecutor, dir string, base string, spec PullRequestPublishSpec) string {
	if spec.UpdateExisting {
		patchBaseRef := strings.TrimSpace(spec.PatchBaseRef)
		if patchBaseRef != "" {
			if _, err := exec(ctx, dir, "git", "rev-parse", "--verify", "--quiet", patchBaseRef+"^{commit}"); err == nil {
				return patchBaseRef
			}
		}
	}
	return gitPublishBaseRef(ctx, exec, dir, base)
}

// pushGitPublishBranch updates the local branch ref to HEAD and pushes it to
// the remote. When the local branch is currently checked out by another git
// worktree (e.g. the user's source checkout or a long-lived aged worktree)
// `git branch -f` refuses with "cannot force update the branch ... used by
// worktree ...". In that case the local ref is left alone and the remote
// branch is force-updated directly via a refspec push, which does not require
// the local branch to be free.
func pushGitPublishBranch(ctx context.Context, exec commandExecutor, dir string, branch string, remote string, forceWithLease bool) error {
	leaseArg := ""
	if forceWithLease {
		var err error
		leaseArg, err = gitForceWithLeaseArg(ctx, exec, dir, remote, branch)
		if err != nil {
			return fmt.Errorf("prepare git branch lease: %w", err)
		}
	}
	if _, err := exec(ctx, dir, "git", "branch", "-f", branch, "HEAD"); err != nil {
		if !isBranchInUseByWorktreeError(err) {
			return fmt.Errorf("create git branch: %w", err)
		}
		args := []string{"push"}
		if forceWithLease {
			args = append(args, leaseArg)
		} else {
			args = append(args, "--force")
		}
		args = append(args, remote, "HEAD:refs/heads/"+branch)
		if _, pushErr := exec(ctx, dir, "git", args...); pushErr != nil {
			return fmt.Errorf("push git branch: %w", pushErr)
		}
		return nil
	}
	args := []string{"push", "-u"}
	if forceWithLease {
		args = append(args, leaseArg)
	}
	args = append(args, remote, branch)
	if _, err := exec(ctx, dir, "git", args...); err != nil {
		return fmt.Errorf("push git branch: %w", err)
	}
	return nil
}

func gitForceWithLeaseArg(ctx context.Context, exec commandExecutor, dir string, remote string, branch string) (string, error) {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return "", errors.New("branch is required")
	}
	out, err := exec(ctx, dir, "git", "ls-remote", "--heads", remote, branch)
	if err != nil {
		return "", fmt.Errorf("read remote branch head: %w", err)
	}
	oid := ""
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[1] != "refs/heads/"+branch {
			continue
		}
		oid = fields[0]
		break
	}
	if oid == "" {
		return "", fmt.Errorf("remote branch %q not found on %s", branch, remote)
	}
	return "--force-with-lease=refs/heads/" + branch + ":" + oid, nil
}

func isBranchInUseByWorktreeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "cannot force update the branch") && strings.Contains(msg, "used by worktree")
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

func refreshGitPublishBaseRefs(ctx context.Context, exec commandExecutor, dir string) {
	for _, remote := range []string{"upstream", "origin"} {
		if _, err := exec(ctx, dir, "git", "remote", "get-url", remote); err != nil {
			continue
		}
		_, _ = exec(ctx, dir, "git", "fetch", remote, "--prune")
	}
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
	checked.Metadata = pullRequestMetadataWithFeedback(checked.Metadata, pullRequestFeedback(payload.Comments, payload.Reviews, threadFeedback), &checked)
	return checked, nil
}

func (p LocalPullRequestPublisher) Merge(ctx context.Context, pr core.PullRequest, spec PullRequestMergeSpec) (core.PullRequest, error) {
	exec := p.exec
	if exec == nil {
		exec = runCommand
	}
	repo := strings.TrimSpace(nonEmpty(spec.Repo, pr.Repo))
	if repo == "" {
		return core.PullRequest{}, errors.New("merge requires repo")
	}
	target := strings.TrimSpace(spec.URL)
	if target == "" {
		if spec.Number > 0 {
			target = strconv.Itoa(spec.Number)
		} else if pr.Number > 0 {
			target = strconv.Itoa(pr.Number)
		}
	}
	if target == "" {
		return core.PullRequest{}, errors.New("merge requires pull request number or URL")
	}
	method := strings.ToLower(strings.TrimSpace(spec.Method))
	if method == "" {
		method = "squash"
	}
	args := []string{"pr", "merge", "--repo", repo, target}
	switch method {
	case "merge":
		args = append(args, "--merge")
	case "rebase":
		args = append(args, "--rebase")
	default:
		args = append(args, "--squash")
	}
	if spec.Auto {
		args = append(args, "--auto")
	}
	if _, err := exec(ctx, strings.TrimSpace(spec.WorkDir), "gh", args...); err != nil {
		return core.PullRequest{}, wrapGitHubCommandError("merge GitHub pull request", err)
	}
	merged := pr
	merged.State = "MERGED"
	if merged.Repo == "" {
		merged.Repo = repo
	}
	if merged.Number == 0 && spec.Number > 0 {
		merged.Number = spec.Number
	}
	inspected, err := p.Inspect(ctx, merged)
	if err == nil {
		return inspected, nil
	}
	return merged, nil
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
	pr.Metadata = pullRequestMetadataWithFailingChecks(pr.Metadata, checks.FailingChecks)
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
	signature := pullRequestFeedbackSignatureFromMetadata(metadata)
	if signature == "" {
		return raw, false
	}
	triggeredSignature := pullRequestTriggeredFeedbackSignatureFromMetadata(metadata)
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
	signature := pullRequestFeedbackSignatureFromMetadata(metadata)
	if signature == "" {
		return false
	}
	triggeredSignature := pullRequestTriggeredFeedbackSignatureFromMetadata(metadata)
	return triggeredSignature != signature
}

func pullRequestFeedbackSignature(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return ""
	}
	return pullRequestFeedbackSignatureFromMetadata(metadata)
}

func pullRequestFeedbackSignatureFromMetadata(metadata map[string]any) string {
	signature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackSignature"]))
	if signature == "" {
		signature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentSignature"]))
	}
	return signature
}

func pullRequestTriggeredFeedbackSignatureFromMetadata(metadata map[string]any) string {
	signature := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackTriggeredSignature"]))
	if signature == "" {
		signature = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentTriggeredSignature"]))
	}
	return signature
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
	if parsed.Scheme != "" || parsed.Host != "" {
		if !isSupportedGitHubPullRequestHost(parsed.Hostname()) {
			return "", 0
		}
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "pull" {
		return "", 0
	}
	number, err := strconv.Atoi(parts[3])
	if err != nil || number <= 0 {
		return "", 0
	}
	return parts[0] + "/" + parts[1], number
}

func isSupportedGitHubPullRequestHost(host string) bool {
	return strings.EqualFold(strings.TrimSpace(host), "github.com")
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
	Status        string
	Conclusion    string
	FailingChecks []pullRequestCheckFailure
}

func summarizeStatusCheckRollup(raw json.RawMessage) statusCheckRollupSummary {
	if len(raw) == 0 || string(raw) == "null" {
		return statusCheckRollupSummary{}
	}
	var checks []struct {
		Name        string `json:"name"`
		Workflow    string `json:"workflowName"`
		Context     string `json:"context"`
		Status      string `json:"status"`
		Conclusion  string `json:"conclusion"`
		State       string `json:"state"`
		DetailsURL  string `json:"detailsUrl"`
		TargetURL   string `json:"targetUrl"`
		URL         string `json:"url"`
		Description string `json:"description"`
		Summary     string `json:"summary"`
		Text        string `json:"text"`
		Output      struct {
			Title   string `json:"title"`
			Summary string `json:"summary"`
			Text    string `json:"text"`
		} `json:"output"`
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
	var failingChecks []pullRequestCheckFailure
	for _, check := range checks {
		status := strings.ToUpper(check.Status)
		conclusion := strings.ToUpper(nonEmpty(check.Conclusion, check.State))
		switch {
		case conclusion == "FAILURE" || conclusion == "ERROR" || conclusion == "CANCELLED" || conclusion == "TIMED_OUT" || conclusion == "ACTION_REQUIRED":
			failing++
			failingChecks = append(failingChecks, pullRequestCheckFailure{
				Name:       checkFailureName(check.Name, check.Workflow, check.Context),
				Status:     nonEmpty(check.Status, check.State),
				Conclusion: nonEmpty(check.Conclusion, check.State),
				URL:        nonEmpty(check.DetailsURL, check.TargetURL, check.URL),
				Summary:    checkFailureSummary(check.Output.Title, check.Output.Summary, check.Output.Text, check.Description, check.Summary, check.Text),
			})
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
		return statusCheckRollupSummary{Status: "failing", Conclusion: "failure", FailingChecks: failingChecks}
	case pending > 0:
		return statusCheckRollupSummary{Status: "pending", Conclusion: "pending"}
	case success == len(checks):
		return statusCheckRollupSummary{Status: "passing", Conclusion: "success"}
	default:
		return statusCheckRollupSummary{Status: "unknown", Conclusion: "unknown"}
	}
}

type pullRequestCheckFailure struct {
	Name       string `json:"name,omitempty"`
	Status     string `json:"status,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
	URL        string `json:"url,omitempty"`
	Summary    string `json:"summary,omitempty"`
}

func checkFailureName(name string, workflow string, context string) string {
	name = strings.TrimSpace(name)
	workflow = strings.TrimSpace(workflow)
	context = strings.TrimSpace(context)
	if name != "" && workflow != "" && !strings.Contains(name, workflow) {
		return workflow + " / " + name
	}
	return nonEmpty(name, context, workflow)
}

func checkFailureSummary(values ...string) string {
	var parts []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		parts = append(parts, value)
	}
	return truncatePRCheckSummary(strings.Join(parts, "\n\n"))
}

func truncatePRCheckSummary(summary string) string {
	summary = strings.TrimSpace(summary)
	const limit = 1000
	if len(summary) <= limit {
		return summary
	}
	return summary[:limit] + "\n[truncated]"
}

func pullRequestMetadataWithFailingChecks(raw json.RawMessage, checks []pullRequestCheckFailure) json.RawMessage {
	metadata := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &metadata)
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	if len(checks) == 0 {
		delete(metadata, "latestFailingCheckName")
		delete(metadata, "latestFailingCheckStatus")
		delete(metadata, "latestFailingCheckConclusion")
		delete(metadata, "latestFailingCheckURL")
		delete(metadata, "latestFailingCheckSummary")
		delete(metadata, "latestFailingChecks")
		return core.MustJSON(metadata)
	}
	const limit = 3
	if len(checks) > limit {
		checks = checks[:limit]
	}
	metadata["latestFailingChecks"] = checks
	first := checks[0]
	metadata["latestFailingCheckName"] = first.Name
	metadata["latestFailingCheckStatus"] = first.Status
	metadata["latestFailingCheckConclusion"] = first.Conclusion
	metadata["latestFailingCheckURL"] = first.URL
	metadata["latestFailingCheckSummary"] = first.Summary
	return core.MustJSON(metadata)
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
