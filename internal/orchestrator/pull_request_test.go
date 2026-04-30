package orchestrator

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestSanitizeGitHubCLIEnvDropsCertificateOverrides(t *testing.T) {
	got := sanitizeGitHubCLIEnv([]string{
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

func TestCommandEnvOnlyCustomizesGitHubCLI(t *testing.T) {
	if env := commandEnv("jj"); env != nil {
		t.Fatalf("jj env = %v, want nil", env)
	}
	if env := commandEnv("/opt/homebrew/bin/gh"); env == nil {
		t.Fatalf("gh env = nil, want sanitized environment")
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
