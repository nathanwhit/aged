package orchestrator

import (
	"context"
	"fmt"
	"strings"
)

func githubAuthStatus(ctx context.Context, dir string) (string, error) {
	if _, err := runCommand(ctx, dir, "gh", "auth", "status", "--hostname", "github.com"); err != nil {
		return "auth_not_ready", wrapGitHubCommandError("check GitHub auth status", err)
	}
	if _, err := runCommand(ctx, dir, "gh", "api", "graphql", "-f", "query=query{viewer{login}}"); err != nil {
		if isGitHubBadCredentials(err) {
			return "auth_bad_credentials", wrapGitHubCommandError("validate GitHub GraphQL credentials", err)
		}
		return "auth_not_ready", wrapGitHubCommandError("validate GitHub GraphQL credentials", err)
	}
	return "auth_ok", nil
}

func wrapGitHubCommandError(action string, err error) error {
	if err == nil {
		return nil
	}
	if isGitHubBadCredentials(err) {
		return fmt.Errorf("%s: GitHub credentials rejected (401 Bad credentials): %w", action, err)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func isGitHubBadCredentials(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "bad credentials") ||
		strings.Contains(text, "status\": \"401\"") ||
		strings.Contains(text, "\"status\":401") ||
		strings.Contains(text, "http 401") ||
		strings.Contains(text, "status code 401")
}
