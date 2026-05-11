package orchestrator

import (
	"fmt"
	"strconv"
	"strings"

	"aged/internal/core"
)

func resolveDiscordDecision(snapshot core.Snapshot, decision DiscordAssistantDecision, content string) (DiscordAssistantDecision, string) {
	if discordActionNeedsTaskID(decision.Action) {
		taskID, prompt := resolveDiscordTaskID(snapshot, decision.TaskID, content)
		if prompt != "" {
			return decision, prompt
		}
		decision.TaskID = taskID
	}
	if discordActionNeedsWorkerID(decision.Action) {
		workerID, prompt := resolveDiscordWorkerID(snapshot, decision.WorkerID, content, decision.TaskID)
		if prompt != "" {
			return decision, prompt
		}
		decision.WorkerID = workerID
	}
	if decision.Action == "publish_pr" {
		workerID, prompt := resolveDiscordWorkerID(snapshot, decision.PublishPR.WorkerID, content, decision.TaskID)
		if prompt != "" {
			return decision, prompt
		}
		decision.PublishPR.WorkerID = workerID
	}
	if discordActionNeedsPullRequestID(decision.Action) {
		pullRequestID, prompt := resolveDiscordPullRequestID(snapshot, decision.PullRequestID, content)
		if prompt != "" {
			return decision, prompt
		}
		decision.PullRequestID = pullRequestID
	}
	if decision.Action == "watch_prs" {
		decision.WatchPRs = resolveDiscordWatchPullRequestReference(decision.WatchPRs, content)
	}
	return decision, ""
}

func discordActionNeedsTaskID(action string) bool {
	switch action {
	case "show_task", "retry_task", "steer_task", "cancel_task", "clear_task", "publish_pr", "watch_prs", "apply_task_result":
		return true
	default:
		return false
	}
}

func discordActionNeedsWorkerID(action string) bool {
	switch action {
	case "show_worker", "cancel_worker", "review_worker_changes", "apply_worker_changes":
		return true
	default:
		return false
	}
}

func discordActionNeedsPullRequestID(action string) bool {
	switch action {
	case "refresh_pr", "babysit_pr":
		return true
	default:
		return false
	}
}

func resolveDiscordTaskID(snapshot core.Snapshot, explicit string, content string) (string, string) {
	if ref := strings.TrimSpace(explicit); ref != "" {
		if id, prompt, ok := matchDiscordTaskReference(snapshot, ref); ok {
			return id, prompt
		}
		return ref, ""
	}
	lower := strings.ToLower(content)
	for _, phrase := range []string{"latest task", "newest task", "last task"} {
		if strings.Contains(lower, phrase) {
			return latestDiscordTaskID(snapshot)
		}
	}
	if strings.Contains(lower, "running task") {
		return singleDiscordTaskWithStatus(snapshot, core.TaskRunning, "running")
	}
	if strings.Contains(lower, "failed task") {
		return singleDiscordTaskWithStatus(snapshot, core.TaskFailed, "failed")
	}
	for _, token := range discordReferenceTokens(content) {
		if id, prompt, ok := matchDiscordTaskReference(snapshot, token); ok {
			return id, prompt
		}
	}
	return "", ""
}

func matchDiscordTaskReference(snapshot core.Snapshot, ref string) (string, string, bool) {
	ref = cleanDiscordReference(ref)
	if ref == "" {
		return "", "", false
	}
	switch strings.ToLower(ref) {
	case "latest task", "newest task", "last task", "latest", "newest":
		id, prompt := latestDiscordTaskID(snapshot)
		return id, prompt, true
	case "running task", "running":
		id, prompt := singleDiscordTaskWithStatus(snapshot, core.TaskRunning, "running")
		return id, prompt, true
	case "failed task", "failed":
		id, prompt := singleDiscordTaskWithStatus(snapshot, core.TaskFailed, "failed")
		return id, prompt, true
	}
	for _, task := range snapshot.Tasks {
		if task.ID == ref {
			return task.ID, "", true
		}
	}
	if len(ref) < 4 {
		return "", "", false
	}
	var matches []core.Task
	for _, task := range snapshot.Tasks {
		if strings.HasPrefix(task.ID, ref) {
			matches = append(matches, task)
		}
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple tasks match `%s`: %s. Send the full task id.", ref, compactDiscordTaskIDs(matches)), true
	}
}

func latestDiscordTaskID(snapshot core.Snapshot) (string, string) {
	if len(snapshot.Tasks) == 0 {
		return "", "I do not see any tasks right now."
	}
	latest := snapshot.Tasks[0]
	for _, task := range snapshot.Tasks[1:] {
		if latest.CreatedAt.Before(task.CreatedAt) || (latest.CreatedAt.Equal(task.CreatedAt) && latest.ID < task.ID) {
			latest = task
		}
	}
	return latest.ID, ""
}

func singleDiscordTaskWithStatus(snapshot core.Snapshot, status core.TaskStatus, label string) (string, string) {
	var matches []core.Task
	for _, task := range snapshot.Tasks {
		if task.Status == status {
			matches = append(matches, task)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Sprintf("I do not see a %s task right now.", label)
	case 1:
		return matches[0].ID, ""
	default:
		return "", fmt.Sprintf("Multiple %s tasks match: %s. Send the full task id.", label, compactDiscordTaskIDs(matches))
	}
}

func resolveDiscordWorkerID(snapshot core.Snapshot, explicit string, content string, taskID string) (string, string) {
	if ref := strings.TrimSpace(explicit); ref != "" {
		if id, prompt, ok := matchDiscordWorkerReference(snapshot, ref, taskID); ok {
			return id, prompt
		}
		return ref, ""
	}
	for _, token := range discordReferenceTokens(content) {
		if id, prompt, ok := matchDiscordWorkerReference(snapshot, token, taskID); ok {
			return id, prompt
		}
	}
	return "", ""
}

func matchDiscordWorkerReference(snapshot core.Snapshot, ref string, taskID string) (string, string, bool) {
	ref = cleanDiscordReference(ref)
	if ref == "" {
		return "", "", false
	}
	for _, worker := range snapshot.Workers {
		if worker.ID == ref {
			return worker.ID, "", true
		}
	}
	if len(ref) < 4 {
		return "", "", false
	}
	matches := discordWorkerPrefixMatches(snapshot.Workers, ref, "")
	if len(matches) > 1 && strings.TrimSpace(taskID) != "" {
		if scoped := discordWorkerPrefixMatches(snapshot.Workers, ref, taskID); len(scoped) > 0 {
			matches = scoped
		}
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple workers match `%s`: %s. Send the full worker id.", ref, compactDiscordWorkerIDs(matches)), true
	}
}

func discordWorkerPrefixMatches(workers []core.Worker, prefix string, taskID string) []core.Worker {
	var matches []core.Worker
	for _, worker := range workers {
		if taskID != "" && worker.TaskID != taskID {
			continue
		}
		if strings.HasPrefix(worker.ID, prefix) {
			matches = append(matches, worker)
		}
	}
	return matches
}

func resolveDiscordPullRequestID(snapshot core.Snapshot, explicit string, content string) (string, string) {
	if ref := strings.TrimSpace(explicit); ref != "" {
		if id, prompt, ok := matchDiscordPullRequestReference(snapshot, ref, true); ok {
			return id, prompt
		}
		return ref, ""
	}
	tokens := discordReferenceTokens(content)
	for _, token := range tokens {
		if id, prompt, ok := matchDiscordPullRequestReference(snapshot, token, false); ok {
			return id, prompt
		}
	}
	for i, token := range tokens {
		if i == 0 || !isDiscordPullRequestWord(tokens[i-1]) {
			continue
		}
		if id, prompt, ok := matchDiscordPullRequestReference(snapshot, token, true); ok {
			return id, prompt
		}
	}
	return "", ""
}

func matchDiscordPullRequestReference(snapshot core.Snapshot, ref string, allowBareNumber bool) (string, string, bool) {
	ref = cleanDiscordReference(ref)
	if ref == "" {
		return "", "", false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == ref || (pr.URL != "" && pr.URL == ref) {
			return pr.ID, "", true
		}
	}
	if repo, number := parsePullRequestURL(ref); repo != "" && number > 0 {
		return matchDiscordPullRequestNumber(snapshot, repo, number, ref)
	}
	if repo, number := parseDiscordRepoNumberReference(ref); repo != "" && number > 0 {
		return matchDiscordPullRequestNumber(snapshot, repo, number, ref)
	}
	if allowBareNumber {
		if number, ok := parseDiscordPullRequestNumber(ref); ok {
			return matchDiscordPullRequestNumber(snapshot, "", number, ref)
		}
	}
	if len(ref) < 4 {
		return "", "", false
	}
	var matches []core.PullRequest
	for _, pr := range snapshot.PullRequests {
		if strings.HasPrefix(pr.ID, ref) {
			matches = append(matches, pr)
		}
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple pull requests match `%s`: %s. Send the full aged pull request id.", ref, compactDiscordPullRequestIDs(matches)), true
	}
}

func matchDiscordPullRequestNumber(snapshot core.Snapshot, repo string, number int, ref string) (string, string, bool) {
	var matches []core.PullRequest
	for _, pr := range snapshot.PullRequests {
		if pr.Number != number {
			continue
		}
		if repo != "" && pr.Repo != repo {
			continue
		}
		matches = append(matches, pr)
	}
	switch len(matches) {
	case 0:
		return "", "", false
	case 1:
		return matches[0].ID, "", true
	default:
		return "", fmt.Sprintf("Multiple pull requests match `%s`: %s. Send the full aged pull request id.", ref, compactDiscordPullRequestIDs(matches)), true
	}
}

func resolveDiscordWatchPullRequestReference(req core.WatchPullRequestsRequest, content string) core.WatchPullRequestsRequest {
	if strings.TrimSpace(req.URL) != "" || req.Number > 0 {
		return req
	}
	tokens := discordReferenceTokens(content)
	for _, token := range tokens {
		clean := cleanDiscordReference(token)
		if repo, number := parsePullRequestURL(clean); repo != "" && number > 0 {
			req.URL = clean
			if strings.TrimSpace(req.Repo) == "" {
				req.Repo = repo
			}
			req.Number = number
			return req
		}
		if repo, number := parseDiscordRepoNumberReference(clean); repo != "" && number > 0 {
			if strings.TrimSpace(req.Repo) == "" {
				req.Repo = repo
			}
			req.Number = number
			return req
		}
	}
	for i, token := range tokens {
		if i == 0 || !isDiscordPullRequestWord(tokens[i-1]) {
			continue
		}
		if number, ok := parseDiscordPullRequestNumber(token); ok {
			req.Number = number
			return req
		}
	}
	return req
}

func parseDiscordRepoNumberReference(ref string) (string, int) {
	index := strings.LastIndex(ref, "#")
	if index <= 0 || index == len(ref)-1 {
		return "", 0
	}
	repo := strings.TrimSpace(ref[:index])
	if !strings.Contains(repo, "/") {
		return "", 0
	}
	number, err := strconv.Atoi(strings.TrimSpace(ref[index+1:]))
	if err != nil {
		return "", 0
	}
	return repo, number
}

func parseDiscordPullRequestNumber(ref string) (int, bool) {
	ref = strings.TrimSpace(strings.ToLower(ref))
	ref = strings.TrimPrefix(ref, "pr#")
	ref = strings.TrimPrefix(ref, "#")
	ref = strings.TrimPrefix(ref, "pr-")
	number, err := strconv.Atoi(ref)
	if err != nil || number <= 0 {
		return 0, false
	}
	return number, true
}

func isDiscordPullRequestWord(value string) bool {
	switch strings.ToLower(cleanDiscordReference(value)) {
	case "pr", "prs", "pull", "pull-request", "pull-request-id", "pullrequest":
		return true
	default:
		return false
	}
}

func discordReferenceTokens(content string) []string {
	var tokens []string
	for _, field := range strings.Fields(content) {
		token := cleanDiscordReference(field)
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

func cleanDiscordReference(value string) string {
	return strings.Trim(strings.TrimSpace(value), " \t\r\n`'\"<>[](){}.,;:!?")
}

func compactDiscordTaskIDs(tasks []core.Task) string {
	var ids []string
	for _, task := range tasks {
		ids = append(ids, "`"+shortDiscordID(task.ID)+"`")
	}
	return strings.Join(ids, ", ")
}

func compactDiscordWorkerIDs(workers []core.Worker) string {
	var ids []string
	for _, worker := range workers {
		ids = append(ids, "`"+shortDiscordID(worker.ID)+"`")
	}
	return strings.Join(ids, ", ")
}

func compactDiscordPullRequestIDs(prs []core.PullRequest) string {
	var ids []string
	for _, pr := range prs {
		ids = append(ids, "`"+shortDiscordID(pr.ID)+"`")
	}
	return strings.Join(ids, ", ")
}
