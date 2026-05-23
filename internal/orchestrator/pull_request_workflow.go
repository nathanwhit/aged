package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"aged/internal/core"
)

func (s *Service) pullRequestWorkspaceChanges(ctx context.Context, workerID string) WorkspaceChanges {
	if strings.TrimSpace(workerID) == "" {
		return WorkspaceChanges{}
	}
	changes, err := s.completedWorkspaceChanges(ctx, workerID)
	if err != nil {
		return WorkspaceChanges{}
	}
	return changes
}

func defaultPullRequestTitle(explicit string, task core.Task, summary string, changes WorkspaceChanges) string {
	return defaultPullRequestTitleForPublication(explicit, task, summary, changes, false)
}

func defaultPullRequestTitleForPublication(explicit string, task core.Task, summary string, changes WorkspaceChanges, intermediate bool) string {
	if title := normalizePullRequestTitle(explicit, allowReportProseTitle); title != "" {
		return title
	}
	if !intermediate {
		for _, candidate := range taskTitleIntentCandidates(task) {
			if title := normalizePullRequestTitle(candidate, rejectReportProseTitle); title != "" {
				return title
			}
		}
	}
	changedFiles := make([]string, 0, len(changes.ChangedFiles))
	for _, file := range changes.ChangedFiles {
		if path := strings.TrimSpace(file.Path); path != "" {
			changedFiles = append(changedFiles, path)
		}
	}
	title := changeCommitMessage(changeCommitMessageContext{
		Fallback:      pullRequestTitleFallback(task, intermediate),
		WorkerSummary: summary,
		ChangedFiles:  changedFiles,
	})
	if normalized := normalizePullRequestTitle(title, rejectReportProseTitle); normalized != "" {
		return normalized
	}
	if title := commitMessageFromChangedFiles(changedFiles); title != "" {
		return title
	}
	if !intermediate {
		if title := normalizePullRequestTitle(task.Title, rejectReportProseTitle); title != "" {
			return title
		}
	}
	return "aged task " + shortID(task.ID)
}

func pullRequestTitleFallback(task core.Task, intermediate bool) string {
	if intermediate {
		return "Update task output"
	}
	if title := strings.TrimSpace(task.Title); title != "" {
		return title
	}
	return "Task result"
}

type reportProsePolicy bool

const (
	allowReportProseTitle  reportProsePolicy = false
	rejectReportProseTitle reportProsePolicy = true
)

func normalizePullRequestTitle(value string, policy reportProsePolicy) string {
	title := normalizeCommitMessageTitle(value)
	if title == "" || isGenericCommitMessageTitle(title) {
		return ""
	}
	if policy == rejectReportProseTitle && isWorkerReportProseTitle(title) {
		return ""
	}
	return title
}

func taskTitleIntentCandidates(task core.Task) []string {
	candidates := []string{task.Title}
	var metadata map[string]any
	if len(task.Metadata) > 0 && string(task.Metadata) != "null" {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	for _, key := range []string{"pullRequestTitle", "prTitle", "taskTitle", "intent", "objective", "title"} {
		if value := stringMetadataValue(metadata[key]); value != "" {
			candidates = append(candidates, value)
		}
	}
	return candidates
}

func isWorkerReportProseTitle(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(value), " "))
	normalized = strings.Trim(normalized, " .:-_#*`")
	if normalized == "" {
		return false
	}
	if strings.Count(value, ".") > 0 && strings.Contains(value, ". ") {
		return true
	}
	for _, prefix := range []string{"the ", "this ", "that ", "these ", "those ", "we ", "i ", "codex ", "worker "} {
		if strings.HasPrefix(normalized, prefix) && containsReportProseVerb(normalized) {
			return true
		}
	}
	return false
}

func containsReportProseVerb(value string) bool {
	for _, verb := range []string{" was ", " were ", " is ", " are ", " had ", " has ", " did ", " does "} {
		if strings.Contains(value, verb) {
			return true
		}
	}
	return false
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

func objectiveForPullRequest(pr core.PullRequest) (core.ObjectiveStatus, string) {
	switch strings.ToUpper(strings.TrimSpace(pr.State)) {
	case "MERGED":
		return core.ObjectiveSatisfied, "merged"
	case "CLOSED":
		return core.ObjectiveAbandoned, "pr_closed"
	}
	if strings.EqualFold(pr.ChecksStatus, "failing") || strings.EqualFold(pr.ReviewStatus, "CHANGES_REQUESTED") || pullRequestMergeNeedsWork(pr) || pullRequestAutoMergeError(pr) != "" || pullRequestHasUntriggeredFeedback(pr) {
		return core.ObjectiveActive, "pr_needs_work"
	}
	if pullRequestChecksPassing(pr) && (pr.ReviewStatus == "" || strings.EqualFold(pr.ReviewStatus, "APPROVED")) {
		return core.ObjectiveWaitingExternal, "ready_to_merge"
	}
	return core.ObjectiveWaitingExternal, "pr_open"
}

func pullRequestChecksPassing(pr core.PullRequest) bool {
	checks := strings.ToLower(strings.TrimSpace(pr.ChecksStatus))
	conclusion := strings.ToLower(strings.TrimSpace(pr.ChecksConclusion))
	return checks == "passing" || checks == "success" || conclusion == "success"
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

Repeatedly inspect CI status, review comments, and mergeability. If checks fail or review comments request changes, diagnose the issue, make the required code changes in the repo, and report what changed. Decide whether a GitHub PR comment is warranted; if so, leave a concise comment on the pull request and include the comment outcome in the report. If you use aged-publish-pr, treat its output as queued until aged reports the published PR URL; do not comment that code changes were pushed or published based only on a queued callback. If the PR is green and no action is needed, report that it is ready. Do not merge unless the user explicitly asks for merge.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base)
}

func pullRequestFollowUpPrompt(pr core.PullRequest) string {
	comment := pullRequestCommentPromptContext(pr)
	if comment != "" {
		comment = "\nLatest PR feedback:\n" + comment + "\n"
	}
	checkFailure := pullRequestCheckFailurePromptContext(pr)
	if checkFailure != "" {
		checkFailure = "\nFailing check context:\n" + checkFailure + "\n"
	}
	autoMergeError := pullRequestAutoMergeError(pr)
	if autoMergeError != "" {
		autoMergeError = "\nAuto-merge failed:\n" + autoMergeError + "\n"
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
%s
%s

Inspect the current PR state, CI failures, review comments, and mergeability. Schedule the next bounded worker turn needed to fix the PR or report that it is ready. The worker should decide whether a GitHub PR comment is warranted, such as answering reviewer feedback, explaining that no code change is needed, or summarizing a completed fix; if so, it should leave a concise comment on the pull request and report what it posted. If the worker uses aged-publish-pr, it must treat the helper output as queued until aged reports the published PR URL and must not comment that code changes were pushed or published based only on a queued callback. Keep this as the same long-running task objective; do not start a separate babysitter task.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base, pr.State, pr.ChecksStatus, pr.MergeStatus, pr.ReviewStatus, comment, checkFailure, autoMergeError)
}

func pullRequestCheckFailurePromptContext(pr core.PullRequest) string {
	if len(pr.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return ""
	}
	name := strings.TrimSpace(stringMetadataValue(metadata["latestFailingCheckName"]))
	status := strings.TrimSpace(stringMetadataValue(metadata["latestFailingCheckStatus"]))
	conclusion := strings.TrimSpace(stringMetadataValue(metadata["latestFailingCheckConclusion"]))
	url := strings.TrimSpace(stringMetadataValue(metadata["latestFailingCheckURL"]))
	summary := strings.TrimSpace(stringMetadataValue(metadata["latestFailingCheckSummary"]))
	if name == "" && status == "" && conclusion == "" && url == "" && summary == "" {
		return ""
	}
	var b strings.Builder
	if name != "" {
		b.WriteString(name)
	}
	state := nonEmpty(conclusion, status)
	if state != "" {
		if b.Len() > 0 {
			b.WriteString(" ")
		}
		b.WriteString("(")
		b.WriteString(state)
		b.WriteString(")")
	}
	if url != "" {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("URL: ")
		b.WriteString(url)
	}
	if summary != "" {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString(summary)
	}
	return strings.TrimSpace(b.String())
}

func pullRequestCommentPromptContext(pr core.PullRequest) string {
	var metadata map[string]any
	if len(pr.Metadata) == 0 {
		return ""
	}
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return ""
	}
	body := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackBody"]))
	if body == "" {
		body = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentBody"]))
	}
	if body == "" {
		return ""
	}
	author := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackAuthor"]))
	if author == "" {
		author = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentAuthor"]))
	}
	createdAt := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackCreatedAt"]))
	if createdAt == "" {
		createdAt = strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentCreatedAt"]))
	}
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
	source := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackSource"]))
	if source != "" && source != "conversation" {
		if prefix != "" {
			prefix += " "
		}
		prefix += "[" + strings.ReplaceAll(source, "_", " ") + "]"
	}
	location := pullRequestFeedbackLocation(metadata)
	if location != "" {
		if prefix != "" {
			prefix += " "
		}
		prefix += location
	}
	if prefix == "" {
		return body
	}
	return prefix + ":\n" + body
}

func pullRequestFeedbackLocation(metadata map[string]any) string {
	path := strings.TrimSpace(stringMetadataValue(metadata["latestPullRequestFeedbackPath"]))
	if path == "" {
		return ""
	}
	line := intMetadata(metadata, "latestPullRequestFeedbackLine")
	if line > 0 {
		return path + ":" + strconv.Itoa(line)
	}
	return path
}
