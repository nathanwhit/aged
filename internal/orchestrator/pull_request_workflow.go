package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"aged/internal/core"
)

func (s *Service) defaultPullRequestBody(ctx context.Context, snapshot core.Snapshot, task core.Task, workerID string, sourceRoot string) string {
	changes := WorkspaceChanges{}
	if strings.TrimSpace(workerID) != "" {
		if completed, err := s.completedWorkspaceChanges(ctx, workerID); err == nil {
			changes = completed
		}
	}
	summary := workerCompletionSummaryFromSnapshot(snapshot, workerID)
	generated := generatedPullRequestBody(task, workerID, summary, changes)
	template, templatePath := pullRequestTemplate(sourceRoot)
	if strings.TrimSpace(template) == "" {
		return generated
	}
	var builder strings.Builder
	builder.WriteString(strings.TrimSpace(template))
	builder.WriteString("\n\n---\n\n")
	builder.WriteString("## Aged context\n\n")
	builder.WriteString("Repository PR template detected at `")
	builder.WriteString(templatePath)
	builder.WriteString("`.\n\n")
	builder.WriteString(generated)
	return builder.String()
}

func generatedPullRequestBody(task core.Task, workerID string, summary string, changes WorkspaceChanges) string {
	var builder strings.Builder
	builder.WriteString("## Summary\n")
	builder.WriteString("- ")
	builder.WriteString(nonEmpty(strings.TrimSpace(summary), strings.TrimSpace(task.Title), "Aged task result"))
	builder.WriteString("\n\n")
	if len(changes.ChangedFiles) > 0 {
		builder.WriteString("## Changed files\n")
		for _, file := range changes.ChangedFiles {
			path := strings.TrimSpace(file.Path)
			if path == "" {
				continue
			}
			status := strings.TrimSpace(file.Status)
			if status != "" {
				builder.WriteString("- `")
				builder.WriteString(path)
				builder.WriteString("` (")
				builder.WriteString(status)
				builder.WriteString(")\n")
			} else {
				builder.WriteString("- `")
				builder.WriteString(path)
				builder.WriteString("`\n")
			}
		}
		builder.WriteString("\n")
	}
	if strings.TrimSpace(changes.DiffStat) != "" {
		builder.WriteString("## Diffstat\n")
		builder.WriteString("```text\n")
		builder.WriteString(strings.TrimSpace(changes.DiffStat))
		builder.WriteString("\n```\n\n")
	}
	builder.WriteString("## Validation\n")
	builder.WriteString("- Worker completed successfully before PR publication.\n\n")
	builder.WriteString("## Aged task\n")
	builder.WriteString("- Task: `")
	builder.WriteString(task.ID)
	builder.WriteString("`\n")
	if strings.TrimSpace(workerID) != "" {
		builder.WriteString("- Worker: `")
		builder.WriteString(workerID)
		builder.WriteString("`\n")
	}
	return builder.String()
}

func workerCompletionSummaryFromSnapshot(snapshot core.Snapshot, workerID string) string {
	if strings.TrimSpace(workerID) == "" {
		return ""
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.WorkerID != workerID || event.Type != core.EventWorkerCompleted {
			continue
		}
		var payload struct {
			Summary string `json:"summary"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			return strings.TrimSpace(payload.Summary)
		}
	}
	return ""
}

func pullRequestTemplate(root string) (string, string) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", ""
	}
	candidates := []string{
		filepath.Join(root, ".github", "pull_request_template.md"),
		filepath.Join(root, ".github", "PULL_REQUEST_TEMPLATE.md"),
		filepath.Join(root, "PULL_REQUEST_TEMPLATE.md"),
		filepath.Join(root, "pull_request_template.md"),
	}
	if entries, err := os.ReadDir(filepath.Join(root, ".github", "PULL_REQUEST_TEMPLATE")); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasSuffix(strings.ToLower(name), ".md") {
				candidates = append(candidates, filepath.Join(root, ".github", "PULL_REQUEST_TEMPLATE", name))
			}
		}
	}
	for _, candidate := range candidates {
		body, err := os.ReadFile(candidate)
		if err != nil || strings.TrimSpace(string(body)) == "" {
			continue
		}
		rel, relErr := filepath.Rel(root, candidate)
		if relErr != nil {
			rel = candidate
		}
		return string(body), filepath.ToSlash(rel)
	}
	return "", ""
}

func objectiveForPullRequest(pr core.PullRequest) (core.ObjectiveStatus, string) {
	switch strings.ToUpper(strings.TrimSpace(pr.State)) {
	case "MERGED":
		return core.ObjectiveSatisfied, "merged"
	case "CLOSED":
		return core.ObjectiveAbandoned, "pr_closed"
	}
	if strings.EqualFold(pr.ChecksStatus, "failing") || strings.EqualFold(pr.ReviewStatus, "CHANGES_REQUESTED") {
		return core.ObjectiveActive, "pr_needs_work"
	}
	if strings.EqualFold(pr.ChecksStatus, "success") && (pr.ReviewStatus == "" || strings.EqualFold(pr.ReviewStatus, "APPROVED")) {
		return core.ObjectiveWaitingExternal, "ready_to_merge"
	}
	return core.ObjectiveWaitingExternal, "pr_open"
}

func pullRequestObjectiveSummary(pr core.PullRequest, phase string) string {
	switch phase {
	case "merged":
		return "Pull request merged."
	case "pr_closed":
		return "Pull request closed without merge."
	case "pr_needs_work":
		return "Pull request needs follow-up work from checks or review."
	case "ready_to_merge":
		return "Pull request is ready for merge."
	default:
		return "Pull request is open; waiting on external GitHub state."
	}
}

func pullRequestBabysitterPrompt(pr core.PullRequest) string {
	return fmt.Sprintf(`Monitor GitHub pull request %s#%d until it is ready to merge.

Pull request URL: %s
Branch: %s
Base: %s

Repeatedly inspect CI status, review comments, and mergeability. If checks fail or review comments request changes, diagnose the issue, make the required code changes in the repo, and report what changed. If the PR is green and no action is needed, report that it is ready. Do not merge unless the user explicitly asks for merge.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base)
}

func pullRequestFollowUpPrompt(pr core.PullRequest) string {
	comment := pullRequestCommentPromptContext(pr)
	if comment != "" {
		comment = "\nLatest PR conversation comment:\n" + comment + "\n"
	}
	return fmt.Sprintf(`GitHub pull request %s#%d needs follow-up work on the existing task.

Pull request URL: %s
Branch: %s
Base: %s
State: %s
Checks: %s
Merge status: %s
Review status: %s
%s

Inspect the current PR state, CI failures, review comments, and mergeability. Schedule the next bounded worker turn needed to fix the PR or report that it is ready. Keep this as the same long-running task objective; do not start a separate babysitter task.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base, pr.State, pr.ChecksStatus, pr.MergeStatus, pr.ReviewStatus, comment)
}

func pullRequestCommentPromptContext(pr core.PullRequest) string {
	var metadata map[string]any
	if len(pr.Metadata) == 0 {
		return ""
	}
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return ""
	}
	body := strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentBody"]))
	if body == "" {
		return ""
	}
	author := strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentAuthor"]))
	createdAt := strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentCreatedAt"]))
	prefix := ""
	if author != "" {
		prefix = "@" + author
	}
	if createdAt != "" {
		if prefix != "" {
			prefix += " "
		}
		prefix += "(" + createdAt + ")"
	}
	if prefix == "" {
		return body
	}
	return prefix + ":\n" + body
}
