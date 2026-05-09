package orchestrator

import (
	"context"
	"encoding/json"
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
	var calls [][]string
	var createdBody string
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
			call := append([]string{name}, args...)
			calls = append(calls, call)
			switch {
			case name == "jj" && len(args) >= 1 && args[0] == "root":
				return "", nil
			case name == "jj" && len(args) >= 2 && args[0] == "bookmark":
				return "", nil
			case name == "jj" && len(args) >= 2 && args[0] == "git":
				return "", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "create":
				bodyFile := argAfter(args, "--body-file")
				if bodyFile == "" {
					t.Fatalf("missing --body-file in gh pr create args: %v", args)
				}
				body, err := os.ReadFile(bodyFile)
				if err != nil {
					t.Fatal(err)
				}
				createdBody = string(body)
				return "https://github.com/upstream/repo/pull/9", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "view":
				return `{"number":9,"url":"https://github.com/upstream/repo/pull/9","state":"OPEN","title":"Fix","isDraft":false,"headRefName":"feature","baseRefName":"trunk","mergeStateStatus":"UNKNOWN","statusCheckRollup":[],"reviewDecision":"REVIEW_REQUIRED"}`, nil
			default:
				t.Fatalf("unexpected command %s %v", name, args)
				return "", nil
			}
		},
	}

	pr, err := publisher.Publish(context.Background(), PullRequestPublishSpec{
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
	assertCommandContains(t, calls, []string{"jj", "git", "push", "--bookmark", "feature", "--remote", "fork"})
	assertCommandContains(t, calls, []string{"gh", "pr", "create", "--repo", "upstream/repo", "--base", "trunk", "--head", "fork-owner:feature"})
	if createdBody != "Body" {
		t.Fatalf("body file contents = %q", createdBody)
	}
	for _, call := range calls {
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

	var calls [][]string
	publisher := LocalPullRequestPublisher{
		exec: func(ctx context.Context, dir string, name string, args ...string) (string, error) {
			call := append([]string{name}, args...)
			calls = append(calls, call)
			switch {
			case name == "git" && len(args) > 0 && args[0] == "push":
				return "", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "create":
				return "https://github.com/owner/repo/pull/10", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "view":
				return `{"number":10,"url":"https://github.com/owner/repo/pull/10","state":"OPEN","title":"CI","isDraft":false,"headRefName":"feature","baseRefName":"main","mergeStateStatus":"UNKNOWN","statusCheckRollup":[],"reviewDecision":""}`, nil
			default:
				return runCommand(ctx, dir, name, args...)
			}
		},
	}

	if _, err := publisher.Publish(ctx, PullRequestPublishSpec{
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
	assertCommandContains(t, calls, []string{"git", "add", "-A"})
	assertCommandContains(t, calls, []string{"git", "-c", "user.name=aged", "-c", "user.email=aged@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "Update GitHub workflows"})
	assertCommandContains(t, calls, []string{"git", "push", "-u", "origin", "feature"})
}

func TestInspectPullRequestPopulatesStatusAliasesFromGitHubPayload(t *testing.T) {
	var jsonFields string
	publisher := LocalPullRequestPublisher{
		exec: func(_ context.Context, _ string, name string, args ...string) (string, error) {
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

func TestGeneratedPullRequestBodyNormalizesWorkerReportSummary(t *testing.T) {
	body := generatedPullRequestBody(core.Task{
		ID:    "task-1",
		Title: "Publish aged worker changes",
	}, "worker-1", "**Summary**\n- Improve generated PR body summaries.\n\n**Validation**\n- go test ./internal/orchestrator", WorkspaceChanges{
		ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/pull_request_workflow.go"}},
	})

	if !strings.Contains(body, "- Improve generated PR body summaries") {
		t.Fatalf("body summary was not normalized:\n%s", body)
	}
	if strings.Contains(body, "**Summary**") || strings.Contains(body, "**Validation**") {
		t.Fatalf("body leaked report headings into summary:\n%s", body)
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

	again, err := publisher.Inspect(context.Background(), withNewComment)
	if err != nil {
		t.Fatal(err)
	}
	if again.ReviewStatus == "COMMENTED" {
		t.Fatalf("already-seen comment triggered again")
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

	again, err := publisher.Inspect(context.Background(), checked)
	if err != nil {
		t.Fatal(err)
	}
	if again.ReviewStatus == "COMMENTED" {
		t.Fatal("upgrade comment trigger repeated")
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
