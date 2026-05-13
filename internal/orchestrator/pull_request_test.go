package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
)

func TestSanitizeGitNetworkEnvDropsCertificateOverrides(t *testing.T) {
	got := sanitizeGitNetworkEnv([]string{
		"PATH=/opt/homebrew/bin:/usr/bin",
		"HOME=/Users/test",
		"SSL_CERT_FILE=/bad.pem",
		"SSL_CERT_DIR=/bad-certs",
		"GIT_SSL_CAINFO=/bad-git.pem",
		"REQUESTS_CA_BUNDLE=/bad-requests.pem",
		"CURL_CA_BUNDLE=/bad-curl.pem",
		"NODE_EXTRA_CA_CERTS=/bad-node.pem",
		"NIX_SSL_CERT_FILE=/bad-nix.pem",
	})
	if !slices.Contains(got, "PATH=/opt/homebrew/bin:/usr/bin") || !slices.Contains(got, "HOME=/Users/test") {
		t.Fatalf("sanitized env dropped required entries: %v", got)
	}
	for _, entry := range got {
		if strings.Contains(entry, "CERT") || strings.Contains(entry, "CA_BUNDLE") || strings.Contains(entry, "SSL_CERT") {
			t.Fatalf("sanitized env kept certificate override %q in %v", entry, got)
		}
	}
}

func TestCommandEnvSanitizesGitHubNetworkCommands(t *testing.T) {
	for _, name := range []string{"gh", "/usr/bin/git", "/opt/homebrew/bin/jj"} {
		if env := commandEnv(name); env == nil {
			t.Fatalf("%s env = nil, want sanitized environment", name)
		}
	}
	if env := commandEnv("go"); env != nil {
		t.Fatalf("go env = %v, want nil", env)
	}
}

func TestParsePullRequestURL(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantRepo   string
		wantNumber int
	}{
		{
			name:       "github URL",
			value:      "https://github.com/owner/repo/pull/7",
			wantRepo:   "owner/repo",
			wantNumber: 7,
		},
		{
			name:       "github URL with trailing path",
			value:      "https://github.com/owner/repo/pull/7/files",
			wantRepo:   "owner/repo",
			wantNumber: 7,
		},
		{
			name:       "relative reference",
			value:      "owner/repo/pull/7",
			wantRepo:   "owner/repo",
			wantNumber: 7,
		},
		{
			name:  "non github host",
			value: "https://example.com/owner/repo/pull/7",
		},
		{
			name:  "www github host",
			value: "https://www.github.com/owner/repo/pull/7",
		},
		{
			name:  "scheme without host",
			value: "https:owner/repo/pull/7",
		},
		{
			name:  "non numeric pull number",
			value: "https://github.com/owner/repo/pull/not-a-number",
		},
		{
			name:  "missing pull number",
			value: "https://github.com/owner/repo/pull",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotRepo, gotNumber := parsePullRequestURL(test.value)
			if gotRepo != test.wantRepo || gotNumber != test.wantNumber {
				t.Fatalf("parsePullRequestURL(%q) = %q, %d; want %q, %d", test.value, gotRepo, gotNumber, test.wantRepo, test.wantNumber)
			}
		})
	}
}

func TestFindExistingPullRequestUsesSearchForForkHead(t *testing.T) {
	var gotArgs []string
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			if name != "gh" {
				t.Fatalf("command = %q, want gh", name)
			}
			gotArgs = append([]string(nil), args...)
			return `[{"number":7,"url":"https://github.com/upstream/repo/pull/7","state":"OPEN","title":"Fix","isDraft":false,"headRefName":"feature","baseRefName":"main","headRepositoryOwner":{"login":"fork-owner"}}]`, nil
		},
	}

	pr, err := publisher.findExistingPullRequest(context.Background(), publisher.exec, "", "upstream/repo", "fork-owner:feature")
	if err != nil {
		t.Fatal(err)
	}
	if pr.Number != 7 || pr.Branch != "feature" {
		t.Fatalf("pr = %+v", pr)
	}
	for i, arg := range gotArgs {
		if arg == "--head" {
			t.Fatalf("used unsupported --head for owner-qualified branch: %v", gotArgs)
		}
		if arg == "--search" {
			if i+1 >= len(gotArgs) || gotArgs[i+1] != "head:fork-owner:feature" {
				t.Fatalf("search args = %v, want head:fork-owner:feature", gotArgs)
			}
			return
		}
	}
	t.Fatalf("missing --search in args %v", gotArgs)
}

func TestFindExistingPullRequestKeepsHeadForLocalBranch(t *testing.T) {
	var gotArgs []string
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			gotArgs = append([]string(nil), args...)
			return `[{"number":8,"url":"https://github.com/owner/repo/pull/8","state":"OPEN","title":"Fix","isDraft":false,"headRefName":"feature","baseRefName":"main"}]`, nil
		},
	}

	if _, err := publisher.findExistingPullRequest(context.Background(), publisher.exec, "", "owner/repo", "feature"); err != nil {
		t.Fatal(err)
	}
	for i, arg := range gotArgs {
		if arg == "--head" && i+1 < len(gotArgs) && gotArgs[i+1] == "feature" {
			return
		}
	}
	t.Fatalf("missing --head feature in args %v", gotArgs)
}

func TestPublishForkPullRequestUsesUpstreamRepoQualifiedHeadAndPushRemote(t *testing.T) {
	var createdBody string
	stub := newPullRequestCommandStub(t, "upstream/repo", 9, "Fix", "feature", "trunk")
	stub.reviewDecision = "REVIEW_REQUIRED"
	stub.createdBody = &createdBody
	stub.before = func(_ context.Context, _ string, name string, args ...string) (string, bool, error) {
		if name == "jj" && len(args) >= 1 && (args[0] == "root" || args[0] == "bookmark" || args[0] == "git") {
			return "", true, nil
		}
		return "", false, nil
	}
	stub.fallback = func(_ context.Context, _ string, name string, args ...string) (string, error) {
		t.Fatalf("unexpected command %s %v", name, args)
		return "", nil
	}

	pr, err := (LocalPullRequestPublisher{exec: stub.exec}).Publish(context.Background(), PullRequestPublishSpec{
		TaskID:        "task-1",
		WorkDir:       "/repo",
		Repo:          "upstream/repo",
		Base:          "trunk",
		Branch:        "feature",
		HeadRepoOwner: "fork-owner",
		PushRemote:    "fork",
		Title:         "Fix",
		Body:          "Body",
	})
	if err != nil {
		t.Fatal(err)
	}
	if pr.Repo != "upstream/repo" || pr.Branch != "feature" || pr.Base != "trunk" {
		t.Fatalf("pr = %+v", pr)
	}
	assertCommandContains(t, stub.calls, []string{"jj", "git", "push", "--bookmark", "feature", "--remote", "fork"})
	assertCommandContains(t, stub.calls, []string{"gh", "pr", "create", "--repo", "upstream/repo", "--base", "trunk", "--head", "fork-owner:feature"})
	if createdBody != "Body" {
		t.Fatalf("body file contents = %q", createdBody)
	}
	for _, call := range stub.calls {
		if containsSubsequence(call, []string{"gh", "pr", "create", "--body", "Body"}) {
			t.Fatalf("gh pr create used --body instead of --body-file: %v", call)
		}
	}
}

func TestPublishGitPullRequestCommitsDirtyWorkspaceBeforePush(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	runTestGit(t, repo, "checkout", "--detach", "main")
	if err := os.MkdirAll(filepath.Join(repo, ".github", "workflows"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".github", "workflows", "ci.yml"), []byte("name: CI\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stub := newPullRequestCommandStub(t, "owner/repo", 10, "CI", "feature", "main")

	if _, err := (LocalPullRequestPublisher{exec: stub.exec}).Publish(ctx, PullRequestPublishSpec{
		TaskID:  "task-1",
		WorkDir: repo,
		Repo:    "owner/repo",
		Base:    "main",
		Branch:  "feature",
		Title:   "CI",
		Body:    "Body",
	}); err != nil {
		t.Fatal(err)
	}

	if status := strings.TrimSpace(runTestGit(t, repo, "status", "--porcelain=v1")); status != "" {
		t.Fatalf("status = %q, want clean committed workspace", status)
	}
	if contents := runTestGit(t, repo, "show", "feature:.github/workflows/ci.yml"); contents != "name: CI\n" {
		t.Fatalf("published branch missing workflow: %q", contents)
	}
	assertCommandContains(t, stub.calls, []string{"git", "add", "-A"})
	assertCommandContains(t, stub.calls, []string{"git", "-c", "commit.gpgsign=false", "commit", "-m", "Update GitHub workflows"})
	for _, call := range stub.calls {
		if slices.Contains(call, "user.name=aged") || slices.Contains(call, "user.email=aged@example.invalid") {
			t.Fatalf("publish commit should not override git author config: %v", call)
		}
	}
	assertCommandContains(t, stub.calls, []string{"git", "push", "-u", "origin", "feature"})
}

func TestPublishGitPatchBranchStartsFromBaseAndPreservesDirtyWorkspace(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	patch := newFilePatch("tests/unit_node/fs_test.ts", "worker change\n")

	runTestGit(t, repo, "checkout", "-b", "manual-investigation")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("manual committed work\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "unrelated.txt")
	runTestGit(t, repo, "commit", "-m", "manual investigation")
	if err := os.WriteFile(filepath.Join(repo, "manual-dirty.txt"), []byte("manual dirty work\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stub := newPullRequestCommandStub(t, "owner/repo", 11, "CI", "feature", "main")

	if _, err := (LocalPullRequestPublisher{exec: stub.exec}).Publish(ctx, PullRequestPublishSpec{
		TaskID:        "task-1",
		WorkDir:       repo,
		Repo:          "owner/repo",
		Base:          "main",
		Branch:        "feature",
		Title:         "CI",
		Body:          "Body",
		Patch:         patch,
		PatchFromBase: true,
	}); err != nil {
		t.Fatal(err)
	}

	if contents := runTestGit(t, repo, "show", "feature:tests/unit_node/fs_test.ts"); contents != "worker change\n" {
		t.Fatalf("published branch missing worker change: %q", contents)
	}
	if _, err := runCommand(ctx, repo, "git", "cat-file", "-e", "feature:unrelated.txt"); err == nil {
		t.Fatal("published branch included unrelated committed source checkout work")
	}
	if _, err := runCommand(ctx, repo, "git", "cat-file", "-e", "feature:manual-dirty.txt"); err == nil {
		t.Fatal("published branch included dirty source checkout work")
	}
	if branch := strings.TrimSpace(runTestGit(t, repo, "branch", "--show-current")); branch != "manual-investigation" {
		t.Fatalf("source checkout branch = %q, want manual-investigation", branch)
	}
	if _, err := os.Stat(filepath.Join(repo, "manual-dirty.txt")); err != nil {
		t.Fatalf("dirty source checkout file was not preserved: %v", err)
	}
	assertCommandContains(t, stub.calls, []string{"git", "worktree", "add", "--detach"})
	assertCommandContains(t, stub.calls, []string{"git", "push", "-u", "origin", "feature"})
}

func TestPublishGitBranchFallsBackToRefspecPushWhenLocalBranchInUseByWorktree(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")

	// Create and check out the publish branch in the source repo. This is the
	// state that triggers "fatal: cannot force update the branch ... used by
	// worktree ..." when another worktree tries to update the same branch.
	runTestGit(t, repo, "checkout", "-b", "feature")

	// Simulate the aged worker workspace: a detached worktree that has the
	// changes to publish.
	worktree := filepath.Join(t.TempDir(), "aged-worker")
	runTestGit(t, repo, "worktree", "add", "--detach", worktree, "main")
	if err := os.WriteFile(filepath.Join(worktree, "fix.txt"), []byte("worker change\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stub := newPullRequestCommandStub(t, "owner/repo", 12, "Fix", "feature", "main")
	stub.before = func(ctx context.Context, dir string, name string, args ...string) (string, bool, error) {
		if name == "git" && len(args) > 0 && args[0] == "push" {
			out, err := runCommand(ctx, dir, name, args...)
			return out, true, err
		}
		return "", false, nil
	}

	if _, err := (LocalPullRequestPublisher{exec: stub.exec}).Publish(ctx, PullRequestPublishSpec{
		TaskID:  "task-1",
		WorkDir: worktree,
		Repo:    "owner/repo",
		Base:    "main",
		Branch:  "feature",
		Title:   "Fix",
		Body:    "Body",
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// `git branch -f` should have been attempted and a refspec push used as
	// the fallback because the branch is checked out by the source worktree.
	assertCommandContains(t, stub.calls, []string{"git", "branch", "-f", "feature", "HEAD"})
	assertCommandContains(t, stub.calls, []string{"git", "push", "--force", "origin", "HEAD:refs/heads/feature"})

	// The remote branch must now point at the worker's commit.
	remoteHead := strings.TrimSpace(runTestGit(t, remote, "rev-parse", "refs/heads/feature"))
	worktreeHead := strings.TrimSpace(runTestGit(t, worktree, "rev-parse", "HEAD"))
	if remoteHead != worktreeHead {
		t.Fatalf("remote feature = %q, worktree HEAD = %q", remoteHead, worktreeHead)
	}
	// The source checkout must still be on `feature` (we didn't move it).
	if current := strings.TrimSpace(runTestGit(t, repo, "branch", "--show-current")); current != "feature" {
		t.Fatalf("source checkout branch = %q, want feature", current)
	}
}

func TestIsBranchInUseByWorktreeErrorMatchesGitMessage(t *testing.T) {
	if !isBranchInUseByWorktreeError(errors.New("fatal: cannot force update the branch 'feature' used by worktree at '/tmp/wt'")) {
		t.Fatal("expected branch-in-use error to be recognized")
	}
	if isBranchInUseByWorktreeError(errors.New("fatal: some other error")) {
		t.Fatal("unexpected error matched as branch-in-use")
	}
	if isBranchInUseByWorktreeError(nil) {
		t.Fatal("nil should not match branch-in-use")
	}
}

func TestInspectPullRequestPopulatesStatusAliasesFromGitHubPayload(t *testing.T) {
	var jsonFields string
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			if name == "gh" && len(args) >= 2 && args[0] == "api" && args[1] == "graphql" {
				return `{}`, nil
			}
			if name != "gh" || !containsSubsequence(args, []string{"pr", "view"}) {
				t.Fatalf("unexpected command %s %v", name, args)
			}
			jsonFields = argAfter(args, "--json")
			return `{"number":22,"url":"https://github.com/owner/repo/pull/22","state":"OPEN","title":"CI","isDraft":false,"headRefName":"feature","baseRefName":"main","mergeStateStatus":"","mergeable":"MERGEABLE","statusCheckRollup":[{"__typename":"CheckRun","status":"COMPLETED","conclusion":"SUCCESS"},{"__typename":"StatusContext","state":"SUCCESS"}],"reviewDecision":"APPROVED","comments":[]}`, nil
		},
	}

	pr, err := publisher.Inspect(context.Background(), core.PullRequest{
		ID:     "pr-1",
		TaskID: "task-1",
		Repo:   "owner/repo",
		Number: 22,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonFields, "mergeable") {
		t.Fatalf("json fields = %q, want mergeable", jsonFields)
	}
	if pr.ChecksStatus != "passing" || pr.ChecksConclusion != "success" {
		t.Fatalf("checks = status %q conclusion %q", pr.ChecksStatus, pr.ChecksConclusion)
	}
	if pr.MergeStatus != "MERGEABLE" || pr.Mergeable != "MERGEABLE" {
		t.Fatalf("merge = status %q mergeable %q", pr.MergeStatus, pr.Mergeable)
	}
}

func TestMaterializeGitPullRequestChangesIgnoresDirtySubmoduleOnlyStatus(t *testing.T) {
	ctx := context.Background()
	var calls [][]string
	exec := func(_ context.Context, _ string, name string, args ...string) (string, error) {
		call := append([]string{name}, args...)
		calls = append(calls, call)
		switch {
		case name == "git" && containsSubsequence(args, []string{"status", "--porcelain=v1"}):
			return " m tests/bench/testdata/lsp_benchdata\n", nil
		case name == "git" && containsSubsequence(args, []string{"add", "-A"}):
			return "", nil
		case name == "git" && containsSubsequence(args, []string{"diff", "--cached", "--name-only", "-z", "HEAD", "--"}):
			return "", nil
		case name == "git" && containsSubsequence(args, []string{"commit"}):
			t.Fatalf("commit should not run when add -A staged no files")
		default:
			t.Fatalf("unexpected command %s %v", name, args)
		}
		return "", nil
	}

	err := materializeGitPullRequestChanges(ctx, exec, "/repo", "feature", PullRequestPublishSpec{})
	if err != nil {
		t.Fatal(err)
	}
	assertCommandContains(t, calls, []string{"git", "add", "-A"})
}

func TestPublishGitPullRequestRejectsEmptyBranch(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")

	publisher := LocalPullRequestPublisher{
		exec: func(ctx context.Context, dir string, name string, args ...string) (string, error) {
			if name == "git" && len(args) > 0 && args[0] == "push" {
				t.Fatalf("push should not run for empty PR")
			}
			if name == "gh" {
				t.Fatalf("gh should not run for empty PR")
			}
			return runCommand(ctx, dir, name, args...)
		},
	}

	_, err := publisher.Publish(ctx, PullRequestPublishSpec{
		TaskID:  "task-1",
		WorkDir: repo,
		Repo:    "owner/repo",
		Base:    "main",
		Branch:  "feature",
		Title:   "Noop",
		Body:    "Body",
	})
	if err == nil || !strings.Contains(err.Error(), "no changes against base") {
		t.Fatalf("err = %v, want no changes error", err)
	}
}

func TestDefaultPullRequestTitlePrefersExplicitTitle(t *testing.T) {
	title := defaultPullRequestTitle("  Reduce repeated Codex infrastructure warning noise in worker output.  ", core.Task{
		ID:    "task-1",
		Title: "Task title",
	}, "Fix fallback title selection.", WorkspaceChanges{})

	if title != "Reduce repeated Codex infrastructure warning noise in worker output" {
		t.Fatalf("title = %q", title)
	}
}

func TestDefaultPullRequestTitleUsesTaskIntentBeforeWorkerReportProse(t *testing.T) {
	title := defaultPullRequestTitle("", core.Task{
		ID:       "task-1",
		Title:    "The reviewer’s gap was valid. The prior fix rejected missing SSH",
		Metadata: core.MustJSON(map[string]any{"intent": "Avoid selecting SSH targets whose checkout path is invalid"}),
	}, "**Findings**\nThe reviewer’s gap was valid. The prior fix rejected missing SSH checkouts.\n\n**Commands Run**\ngo test ./internal/orchestrator", WorkspaceChanges{
		ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/targets.go"}},
	})

	if title != "Avoid selecting SSH targets whose checkout path is invalid" {
		t.Fatalf("title = %q", title)
	}
}

func TestDefaultPullRequestTitleSkipsReportProseForChangedFiles(t *testing.T) {
	title := defaultPullRequestTitle("", core.Task{
		ID:    "task-1",
		Title: "Codex worker output was parsed line-by-line and every stderr line",
	}, "**Findings**\nCodex worker output was parsed line-by-line and every stderr line created warning noise.\n\n**Commands Run**\ngo test ./internal/orchestrator", WorkspaceChanges{
		ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/workers.go"}},
	})

	if title != "Update internal orchestrator" {
		t.Fatalf("title = %q", title)
	}
}

func TestInspectPullRequestFlagsNewConversationCommentOnce(t *testing.T) {
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			if name != "gh" {
				t.Fatalf("command = %q, want gh", name)
			}
			return `{"number":2,"url":"https://github.com/owner/repo/pull/2","state":"OPEN","title":"Fix","isDraft":false,"headRefName":"feature","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":"","comments":[{"id":"IC_1","body":"Can you summarize the approach here?","createdAt":"2026-05-01T04:31:10Z","updatedAt":"2026-05-01T04:31:10Z","viewerDidAuthor":false,"author":{"login":"reviewer"}}]}`, nil
		},
	}

	baseline, err := publisher.Inspect(context.Background(), core.PullRequest{
		Repo:      "owner/repo",
		URL:       "https://github.com/owner/repo/pull/2",
		CreatedAt: time.Date(2026, 5, 1, 4, 32, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if baseline.ReviewStatus == "COMMENTED" {
		t.Fatalf("baseline review status = %q", baseline.ReviewStatus)
	}

	var metadata map[string]any
	if err := json.Unmarshal(baseline.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	delete(metadata, "latestConversationCommentSignature")
	delete(metadata, "latestConversationCommentId")
	delete(metadata, "latestConversationCommentAuthor")
	delete(metadata, "latestConversationCommentCreatedAt")
	delete(metadata, "latestConversationCommentUpdatedAt")
	delete(metadata, "latestConversationCommentBody")
	delete(metadata, "latestConversationCommentTriggeredSignature")
	delete(metadata, "latestPullRequestFeedbackSignature")
	delete(metadata, "latestPullRequestFeedbackId")
	delete(metadata, "latestPullRequestFeedbackAuthor")
	delete(metadata, "latestPullRequestFeedbackCreatedAt")
	delete(metadata, "latestPullRequestFeedbackUpdatedAt")
	delete(metadata, "latestPullRequestFeedbackBody")
	delete(metadata, "latestPullRequestFeedbackSource")
	delete(metadata, "latestPullRequestFeedbackPath")
	delete(metadata, "latestPullRequestFeedbackLine")
	delete(metadata, "latestPullRequestFeedbackURL")
	delete(metadata, "latestPullRequestFeedbackTriggeredSignature")

	withNewComment, err := publisher.Inspect(context.Background(), core.PullRequest{
		Repo:     "owner/repo",
		URL:      "https://github.com/owner/repo/pull/2",
		Metadata: core.MustJSON(metadata),
	})
	if err != nil {
		t.Fatal(err)
	}
	if withNewComment.ReviewStatus != "COMMENTED" {
		t.Fatalf("review status = %q, want COMMENTED", withNewComment.ReviewStatus)
	}
	if !strings.Contains(string(withNewComment.Metadata), "Can you summarize") {
		t.Fatalf("metadata missing comment body: %s", withNewComment.Metadata)
	}

	markedMetadata, changed := pullRequestMetadataMarkFeedbackTriggered(withNewComment.Metadata)
	if !changed {
		t.Fatal("new comment metadata was not marked handled")
	}
	withNewComment.Metadata = markedMetadata
	again, err := publisher.Inspect(context.Background(), withNewComment)
	if err != nil {
		t.Fatal(err)
	}
	if again.ReviewStatus == "COMMENTED" {
		t.Fatalf("already-seen comment triggered again")
	}
}

func TestInspectPullRequestFlagsNewReviewBody(t *testing.T) {
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			if name != "gh" {
				t.Fatalf("command = %q, want gh", name)
			}
			if len(args) >= 2 && args[0] == "api" && args[1] == "graphql" {
				return `{}`, nil
			}
			return "{\"number\":2,\"url\":\"https://github.com/owner/repo/pull/2\",\"state\":\"OPEN\",\"title\":\"Fix\",\"isDraft\":false,\"headRefName\":\"feature\",\"baseRefName\":\"main\",\"mergeStateStatus\":\"CLEAN\",\"statusCheckRollup\":[],\"reviewDecision\":\"\",\"comments\":[],\"reviews\":[{\"id\":\"PRR_1\",\"body\":\"Your title is wrong; it needs to start with `fix:`.\",\"submittedAt\":\"2026-05-01T04:35:10Z\",\"state\":\"COMMENTED\",\"author\":{\"login\":\"reviewer\"}}]}", nil
		},
	}

	checked, err := publisher.Inspect(context.Background(), core.PullRequest{
		Repo: "owner/repo",
		URL:  "https://github.com/owner/repo/pull/2",
		Metadata: core.MustJSON(map[string]any{
			"pullRequestFeedbackBaselineEstablished": true,
			"latestPullRequestFeedbackSignature":     "2026-05-01T04:31:10Z:conversation:IC_1",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked.ReviewStatus != "COMMENTED" {
		t.Fatalf("review status = %q, want COMMENTED", checked.ReviewStatus)
	}
	if !strings.Contains(string(checked.Metadata), "needs to start") || !strings.Contains(string(checked.Metadata), `"latestPullRequestFeedbackSource":"review"`) {
		t.Fatalf("metadata missing review body feedback: %s", checked.Metadata)
	}
}

func TestInspectPullRequestFlagsNewInlineReviewThreadComment(t *testing.T) {
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			if name != "gh" {
				t.Fatalf("command = %q, want gh", name)
			}
			if len(args) >= 2 && args[0] == "api" && args[1] == "graphql" {
				return `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":false,"path":"cli/tools/installer/global.rs","line":42,"startLine":0,"comments":{"nodes":[{"id":"PRRC_1","body":"This branch should not touch the installer output path.","createdAt":"2026-05-01T04:36:10Z","updatedAt":"2026-05-01T04:36:10Z","viewerDidAuthor":false,"url":"https://github.com/owner/repo/pull/2#discussion_r1","author":{"login":"reviewer"}}]}}]}}}}}`, nil
			}
			return `{"number":2,"url":"https://github.com/owner/repo/pull/2","state":"OPEN","title":"Fix","isDraft":false,"headRefName":"feature","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":"","comments":[],"reviews":[]}`, nil
		},
	}

	checked, err := publisher.Inspect(context.Background(), core.PullRequest{
		Repo: "owner/repo",
		URL:  "https://github.com/owner/repo/pull/2",
		Metadata: core.MustJSON(map[string]any{
			"pullRequestFeedbackBaselineEstablished": true,
			"latestPullRequestFeedbackSignature":     "2026-05-01T04:31:10Z:conversation:IC_1",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked.ReviewStatus != "COMMENTED" {
		t.Fatalf("review status = %q, want COMMENTED", checked.ReviewStatus)
	}
	if !strings.Contains(string(checked.Metadata), "installer output path") || !strings.Contains(string(checked.Metadata), "cli/tools/installer/global.rs") {
		t.Fatalf("metadata missing inline review feedback: %s", checked.Metadata)
	}
	promptContext := pullRequestCommentPromptContext(checked)
	if !strings.Contains(promptContext, "cli/tools/installer/global.rs:42") || !strings.Contains(promptContext, "installer output path") {
		t.Fatalf("prompt context missing inline review location:\n%s", promptContext)
	}
}

func TestInspectPullRequestFlagsCommentAfterWatchOnUpgrade(t *testing.T) {
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			return `{"number":2,"url":"https://github.com/owner/repo/pull/2","state":"OPEN","title":"Fix","isDraft":false,"headRefName":"feature","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":"","comments":[{"id":"IC_1","body":"Can you summarize the approach here?","createdAt":"2026-05-01T04:31:10Z","updatedAt":"2026-05-01T04:31:10Z","viewerDidAuthor":false,"author":{"login":"reviewer"}}]}`, nil
		},
	}
	metadata := core.MustJSON(map[string]any{
		"conversationCommentBaselineEstablished": true,
		"latestConversationCommentSignature":     "2026-05-01T04:31:10Z:IC_1",
	})

	checked, err := publisher.Inspect(context.Background(), core.PullRequest{
		Repo:      "owner/repo",
		URL:       "https://github.com/owner/repo/pull/2",
		CreatedAt: time.Date(2026, 5, 1, 4, 30, 0, 0, time.UTC),
		Metadata:  metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked.ReviewStatus != "COMMENTED" {
		t.Fatalf("review status = %q, want COMMENTED", checked.ReviewStatus)
	}

	markedMetadata, changed := pullRequestMetadataMarkFeedbackTriggered(checked.Metadata)
	if !changed {
		t.Fatal("upgrade comment metadata was not marked handled")
	}
	checked.Metadata = markedMetadata
	again, err := publisher.Inspect(context.Background(), checked)
	if err != nil {
		t.Fatal(err)
	}
	if again.ReviewStatus == "COMMENTED" {
		t.Fatal("upgrade comment trigger repeated")
	}
}

type pullRequestCommandStub struct {
	t              *testing.T
	repo           string
	number         int
	title          string
	branch         string
	base           string
	reviewDecision string
	createdBody    *string
	calls          [][]string
	before         func(context.Context, string, string, ...string) (string, bool, error)
	fallback       func(context.Context, string, string, ...string) (string, error)
}

func newPullRequestCommandStub(t *testing.T, repo string, number int, title string, branch string, base string) *pullRequestCommandStub {
	return &pullRequestCommandStub{t: t, repo: repo, number: number, title: title, branch: branch, base: base, fallback: runCommand}
}

func (s *pullRequestCommandStub) exec(ctx context.Context, dir string, name string, args ...string) (string, error) {
	s.calls = append(s.calls, append([]string{name}, args...))
	if s.before != nil {
		if out, ok, err := s.before(ctx, dir, name, args...); ok {
			return out, err
		}
	}
	switch {
	case name == "git" && len(args) > 0 && args[0] == "push":
		return "", nil
	case name == "gh" && containsSubsequence(args, []string{"pr", "create"}):
		if s.createdBody != nil {
			bodyFile := argAfter(args, "--body-file")
			if bodyFile == "" {
				s.t.Fatalf("missing --body-file in gh pr create args: %v", args)
			}
			body, err := os.ReadFile(bodyFile)
			if err != nil {
				s.t.Fatal(err)
			}
			*s.createdBody = string(body)
		}
		return fmt.Sprintf("https://github.com/%s/pull/%d", s.repo, s.number), nil
	case name == "gh" && containsSubsequence(args, []string{"pr", "view"}):
		return fmt.Sprintf(`{"number":%d,"url":"https://github.com/%s/pull/%d","state":"OPEN","title":%q,"isDraft":false,"headRefName":%q,"baseRefName":%q,"mergeStateStatus":"UNKNOWN","statusCheckRollup":[],"reviewDecision":%q}`,
			s.number, s.repo, s.number, s.title, s.branch, s.base, s.reviewDecision), nil
	case name == "gh" && len(args) >= 2 && args[0] == "api" && args[1] == "graphql":
		return `{}`, nil
	default:
		return s.fallback(ctx, dir, name, args...)
	}
}

func assertCommandContains(t *testing.T, calls [][]string, want []string) {
	t.Helper()
	for _, call := range calls {
		if containsSubsequence(call, want) {
			return
		}
	}
	t.Fatalf("missing command containing %v in calls %v", want, calls)
}

func argAfter(args []string, flag string) string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func containsSubsequence(values []string, want []string) bool {
	if len(want) > len(values) {
		return false
	}
	for start := 0; start <= len(values)-len(want); start++ {
		matched := true
		for i := range want {
			if values[start+i] != want[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
