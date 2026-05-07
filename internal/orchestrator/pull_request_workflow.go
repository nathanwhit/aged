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

type pullRequestText struct {
	Title         string
	CommitMessage string
	Summary       string
	Validation    []string
}

func defaultPullRequestBody(task core.Task, text pullRequestText, changes WorkspaceChanges, sourceRoot string) string {
	generated := generatedPullRequestBody(task, text, changes)
	template, _ := pullRequestTemplate(sourceRoot)
	if strings.TrimSpace(template) == "" {
		return generated
	}
	var builder strings.Builder
	builder.WriteString(strings.TrimSpace(template))
	builder.WriteString("\n\n---\n\n")
	builder.WriteString(generated)
	return builder.String()
}

func describePullRequestPublishChanges(ctx context.Context, workDir string, base string) (WorkspaceChanges, error) {
	changes := WorkspaceChanges{Root: workDir, CWD: workDir, VCSType: "git"}
	if strings.TrimSpace(workDir) == "" {
		return changes, nil
	}
	if _, err := runCommand(ctx, workDir, "git", "rev-parse", "--show-toplevel"); err != nil {
		return changes, nil
	}
	baseRef := gitPublishBaseRef(ctx, runCommand, workDir, base)
	if baseRef == "" {
		return changes, fmt.Errorf("inspect pull request changes: base ref %q was not found", nonEmpty(strings.TrimSpace(base), "main"))
	}
	diffStat, err := runCommand(ctx, workDir, "git", "diff", "--stat", baseRef+"...HEAD", "--")
	if err != nil {
		return changes, fmt.Errorf("inspect pull request diffstat: %w", err)
	}
	nameStatus, err := runCommand(ctx, workDir, "git", "diff", "--name-status", "-z", baseRef+"...HEAD", "--")
	if err != nil {
		return changes, fmt.Errorf("inspect pull request changed files: %w", err)
	}
	diff, err := runCommand(ctx, workDir, "git", "diff", "--binary", baseRef+"...HEAD", "--")
	if err != nil {
		return changes, fmt.Errorf("inspect pull request diff: %w", err)
	}
	changes.DiffStat = strings.TrimSpace(diffStat)
	changes.ChangedFiles = parseGitNameStatus(nameStatus)
	changes.Diff = strings.TrimSpace(diff)
	changes.Dirty = changes.DiffStat != "" || len(changes.ChangedFiles) > 0
	return changes, nil
}

func (s *Service) generatePullRequestText(ctx context.Context, task core.Task, summary string, changes WorkspaceChanges, sourceRoot string) pullRequestText {
	fallback := fallbackPullRequestText(summary, changes)
	assistant := s.assistant
	if assistant == nil {
		var ok bool
		assistant, ok = s.brain.(AssistantProvider)
		if !ok {
			return fallback
		}
	}
	response, err := assistant.Ask(ctx, core.AssistantRequest{
		WorkDir: sourceRoot,
		Message: pullRequestTextPrompt(task, summary, changes),
	})
	if err != nil {
		return fallback
	}
	text := parsePullRequestText(response.Message)
	if text.Title == "" {
		text.Title = fallback.Title
	}
	if text.CommitMessage == "" {
		text.CommitMessage = fallback.CommitMessage
	}
	if text.Summary == "" {
		text.Summary = fallback.Summary
	}
	if len(text.Validation) == 0 {
		text.Validation = fallback.Validation
	}
	return text
}

func fallbackPullRequestText(summary string, changes WorkspaceChanges) pullRequestText {
	title := pullRequestTitle(summary, changes)
	return pullRequestText{
		Title:         title,
		CommitMessage: title,
		Summary:       title,
		Validation:    []string{"Not reported."},
	}
}

func generatedPullRequestBody(_ core.Task, text pullRequestText, changes WorkspaceChanges) string {
	var builder strings.Builder
	builder.WriteString("## Summary\n")
	builder.WriteString("- ")
	builder.WriteString(nonEmpty(strings.TrimSpace(text.Summary), text.Title, "Update project files"))
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
	validation := text.Validation
	if len(validation) == 0 {
		validation = []string{"Not reported."}
	}
	for _, item := range validation {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		builder.WriteString("- ")
		builder.WriteString(item)
		builder.WriteString("\n")
	}
	return builder.String()
}

func pullRequestTextPrompt(task core.Task, summary string, changes WorkspaceChanges) string {
	var builder strings.Builder
	builder.WriteString("Generate reviewer-facing pull request metadata for a code change.\n")
	builder.WriteString("Return only compact JSON with keys: title, commitMessage, summary, validation.\n")
	builder.WriteString("Rules:\n")
	builder.WriteString("- Use only the worker summary, changed files, and diffstat/diff excerpt below.\n")
	builder.WriteString("- Do not mention aged, worker IDs, task IDs, automation, or publication mechanics.\n")
	builder.WriteString("- Do not use the task prompt as the title unless the actual changes support it.\n")
	builder.WriteString("- validation must be an array of commands or checks explicitly present in the worker summary; if none are present, use [\"Not reported.\"].\n\n")
	builder.WriteString("Task title, for weak context only:\n")
	builder.WriteString(strings.TrimSpace(task.Title))
	builder.WriteString("\n\nWorker summary:\n")
	builder.WriteString(strings.TrimSpace(summary))
	builder.WriteString("\n\nChanged files:\n")
	for _, file := range changes.ChangedFiles {
		if path := strings.TrimSpace(file.Path); path != "" {
			builder.WriteString("- ")
			builder.WriteString(path)
			if status := strings.TrimSpace(file.Status); status != "" {
				builder.WriteString(" (")
				builder.WriteString(status)
				builder.WriteString(")")
			}
			builder.WriteString("\n")
		}
	}
	if stat := strings.TrimSpace(changes.DiffStat); stat != "" {
		builder.WriteString("\nDiffstat:\n")
		builder.WriteString(stat)
		builder.WriteString("\n")
	}
	if diff := strings.TrimSpace(changes.Diff); diff != "" {
		const limit = 6000
		if len(diff) > limit {
			diff = diff[:limit] + "\n[truncated]"
		}
		builder.WriteString("\nDiff excerpt:\n")
		builder.WriteString(diff)
		builder.WriteString("\n")
	}
	return builder.String()
}

func parsePullRequestText(value string) pullRequestText {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```json")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	value = strings.TrimSpace(value)
	var payload struct {
		Title         string   `json:"title"`
		CommitMessage string   `json:"commitMessage"`
		Summary       string   `json:"summary"`
		Validation    []string `json:"validation"`
	}
	if err := json.Unmarshal([]byte(value), &payload); err != nil {
		return pullRequestText{}
	}
	return pullRequestText{
		Title:         normalizeGeneratedTitle(payload.Title),
		CommitMessage: normalizeCommitMessageTitle(payload.CommitMessage),
		Summary:       normalizePullRequestSummary(payload.Summary),
		Validation:    normalizePullRequestValidation(payload.Validation),
	}
}

func normalizePullRequestSummary(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimLeft(value, "-* \t")
	value = strings.Join(strings.Fields(value), " ")
	return strings.TrimSpace(value)
}

func normalizePullRequestValidation(values []string) []string {
	var out []string
	for _, value := range values {
		value = normalizePullRequestSummary(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func pullRequestSummaryLine(_ core.Task, summary string, changes WorkspaceChanges) string {
	return pullRequestTitle(summary, changes)
}

func pullRequestTitle(summary string, changes WorkspaceChanges) string {
	changedFiles := make([]string, 0, len(changes.ChangedFiles))
	for _, file := range changes.ChangedFiles {
		if path := strings.TrimSpace(file.Path); path != "" {
			changedFiles = append(changedFiles, path)
		}
	}
	return changeCommitMessage(changeCommitMessageContext{
		Fallback:      "Update project files",
		WorkerSummary: summary,
		ChangedFiles:  changedFiles,
	})
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
