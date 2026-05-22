package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"

	"github.com/google/uuid"
)

var (
	githubPullRequestURLRE      = regexp.MustCompile(`https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[0-9]+`)
	pullRequestClosingKeywordRE = regexp.MustCompile(`(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b`)
	errTerminalPullRequest      = errors.New("pull request is already terminal")
)

func (s *Service) SetPullRequestPublisher(publisher PullRequestPublisher) {
	s.prPublisher = publisher
}

func (s *Service) StartPullRequestMonitor(ctx context.Context, interval time.Duration) {
	if s == nil {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		s.monitorPullRequestsLogged(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.monitorPullRequestsLogged(ctx)
			}
		}
	}()
}

func (s *Service) monitorPullRequestsLogged(ctx context.Context) {
	if err := s.MonitorPullRequestsOnce(ctx); err != nil {
		slog.Warn("pull request monitor failed", "error", err)
	}
}

func (s *Service) MonitorPullRequestsOnce(ctx context.Context) error {
	return s.monitorPullRequests(ctx, pullRequestMonitorOptions{AutoBabysit: true})
}

type pullRequestMonitorOptions struct {
	AutoBabysit bool
	IncludeRepo func(string) bool
}

func (s *Service) monitorPullRequests(ctx context.Context, options pullRequestMonitorOptions) error {
	if s == nil {
		return nil
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	var errs []string
	for _, pr := range snapshot.PullRequests {
		if options.IncludeRepo != nil && !options.IncludeRepo(pr.Repo) {
			continue
		}
		if s.pullRequestMonitoringDisabled(snapshot, pr) {
			continue
		}
		if isTerminalPullRequestState(pr.State) {
			if err := s.ReconcilePullRequestTerminalTasks(ctx, pr.ID); err != nil {
				errs = append(errs, fmt.Sprintf("%s reconcile terminal pr: %v", pr.ID, err))
			}
			continue
		}
		checked, err := s.RefreshPullRequest(ctx, pr.ID)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s refresh pr: %v", pr.ID, err))
			continue
		}
		if s.pullRequestAutoMergeEnabled(snapshot, checked) && pullRequestReadyForAutoMerge(checked) {
			merged, err := s.mergePullRequest(ctx, snapshot, checked)
			if err != nil {
				failed, recordErr := s.recordPullRequestAutoMergeFailure(ctx, snapshot, checked, err)
				if recordErr != nil {
					errs = append(errs, fmt.Sprintf("%s record merge failure: %v", checked.ID, recordErr))
					continue
				}
				if err := s.ContinueTaskForPullRequest(ctx, failed.ID); err != nil {
					errs = append(errs, fmt.Sprintf("%s continue after merge failure: %v", failed.ID, err))
				}
				continue
			}
			checked = merged
		}
		if options.AutoBabysit && pullRequestNeedsBabysitter(checked) {
			if err := s.ContinueTaskForPullRequest(ctx, pr.ID); err != nil {
				errs = append(errs, fmt.Sprintf("%s continue pr task: %v", pr.ID, err))
			}
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (s *Service) pullRequestMonitoringDisabled(snapshot core.Snapshot, pr core.PullRequest) bool {
	project, ok := s.projectForPullRequestPolicy(snapshot, pr)
	if !ok {
		return false
	}
	return project.PullRequestPolicy.MonitorPullRequests != nil && !*project.PullRequestPolicy.MonitorPullRequests
}

func (s *Service) pullRequestAutoMergeEnabled(snapshot core.Snapshot, pr core.PullRequest) bool {
	project, ok := s.projectForPullRequestPolicy(snapshot, pr)
	if !ok {
		return false
	}
	return project.PullRequestPolicy.AllowMerge && project.PullRequestPolicy.AutoMerge
}

func (s *Service) PublishTaskPullRequest(ctx context.Context, taskID string, req core.PublishPullRequestRequest) (core.PullRequest, error) {
	return s.publishTaskPullRequest(ctx, taskID, req, nil)
}

func (s *Service) publishTaskPullRequest(ctx context.Context, taskID string, req core.PublishPullRequestRequest, beforeRecordPublished func(core.PullRequest) error) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	project, err := s.projectForTask(task)
	if err != nil {
		return core.PullRequest{}, err
	}
	sourceRoot := project.LocalPath
	workerID, err := resolvePullRequestWorkerID(snapshot, task, req.WorkerID)
	if err != nil {
		return core.PullRequest{}, err
	}
	var publishWorkspace PreparedWorkspace
	if workerID != "" {
		sourceRoot, publishWorkspace, err = s.pullRequestPublishSourceRoot(ctx, snapshot, workerID, project)
		if err != nil {
			return core.PullRequest{}, err
		}
	}
	repo := pullRequestTargetRepo(req, project, task)
	base := nonEmpty(req.Base, project.DefaultBase)
	draft := req.Draft || project.PullRequestPolicy.Draft
	metadata := map[string]any{
		"workerId":             workerID,
		"taskTitle":            task.Title,
		"workDir":              sourceRoot,
		"projectId":            project.ID,
		"branchPrefix":         project.PullRequestPolicy.BranchPrefix,
		"mergeAllowed":         project.PullRequestPolicy.AllowMerge,
		"autoMerge":            project.PullRequestPolicy.AutoMerge,
		"mergeMethod":          project.PullRequestPolicy.MergeMethod,
		"pullRequestPolicy":    project.PullRequestPolicy,
		"continueAfterPublish": req.ContinueAfterPublish,
		"publicationPhase":     pullRequestPublicationPhase(req.ContinueAfterPublish),
	}
	changes := s.pullRequestWorkspaceChanges(ctx, workerID)
	publishPatch, patchFromBase, patchBaseRef := pullRequestPublishPatch(publishWorkspace, changes)
	summary := workerCompletionSummaryFromSnapshot(snapshot, workerID)
	title := defaultPullRequestTitleForPublication(req.Title, task, summary, changes, req.ContinueAfterPublish)
	body := pullRequestBodyWithIssueClosingReference(strings.TrimSpace(req.Body), task, repo)
	pr, adopted, err := s.workerCreatedPullRequest(ctx, snapshot, task, workerID, repo, metadata)
	if err != nil {
		return core.PullRequest{}, err
	}
	if !adopted {
		pr, err = s.prPublisher.Publish(ctx, PullRequestPublishSpec{
			TaskID:        taskID,
			WorkerID:      workerID,
			WorkDir:       sourceRoot,
			Repo:          repo,
			Base:          base,
			Branch:        req.Branch,
			HeadRepoOwner: pullRequestHeadRepoOwner(project),
			PushRemote:    project.PushRemote,
			BranchPrefix:  project.PullRequestPolicy.BranchPrefix,
			Title:         title,
			Body:          body,
			Draft:         draft,
			Patch:         publishPatch,
			PatchFromBase: patchFromBase,
			PatchBaseRef:  patchBaseRef,
			ResetWorkDir:  !patchFromBase && shouldResetPullRequestWorkDirAfterPublish(publishWorkspace),
			Metadata:      metadata,
		})
		if err != nil {
			return core.PullRequest{}, err
		}
	}
	if pr.ID == "" {
		pr.ID = uuid.NewString()
	}
	pr.TaskID = taskID
	pr = normalizePullRequestStatusFields(pr)
	if err := s.recordTaskMilestone(ctx, taskID, "pr_opened", "pr_opened", "Pull request opened.", map[string]any{
		"pullRequestId": pr.ID,
		"url":           pr.URL,
		"repo":          pr.Repo,
		"number":        pr.Number,
		"branch":        pr.Branch,
	}); err != nil {
		return core.PullRequest{}, err
	}
	if beforeRecordPublished != nil {
		if err := beforeRecordPublished(pr); err != nil {
			return core.PullRequest{}, err
		}
	}
	if err := s.recordPullRequestPublished(ctx, pr); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.recordPullRequestArtifact(ctx, pr); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.recordPullRequestBabysitter(ctx, pr); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "pr_opened", "Pull request opened; objective continues until the PR reaches its terminal condition."); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskWaiting); err != nil {
		return core.PullRequest{}, err
	}
	return pr, nil
}

func (s *Service) UpdateTaskPullRequest(ctx context.Context, taskID string, pr core.PullRequest, req core.PublishPullRequestRequest) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	if pr.ID == "" {
		return core.PullRequest{}, errors.New("update pull request requires tracked pull request")
	}
	project, err := s.projectForTask(task)
	if err != nil {
		return core.PullRequest{}, err
	}
	sourceRoot := project.LocalPath
	workerID, err := resolvePullRequestWorkerID(snapshot, task, req.WorkerID)
	if err != nil {
		return core.PullRequest{}, err
	}
	var publishWorkspace PreparedWorkspace
	if workerID != "" {
		sourceRoot, publishWorkspace, err = s.pullRequestPublishSourceRoot(ctx, snapshot, workerID, project)
		if err != nil {
			return core.PullRequest{}, err
		}
	}
	changes := s.pullRequestWorkspaceChanges(ctx, workerID)
	publishPatch, patchFromBase, patchBaseRef := pullRequestPublishPatch(publishWorkspace, changes)
	metadata := map[string]any{}
	if len(pr.Metadata) > 0 {
		_ = json.Unmarshal(pr.Metadata, &metadata)
	}
	metadata["workerId"] = workerID
	metadata["taskTitle"] = task.Title
	metadata["workDir"] = sourceRoot
	metadata["projectId"] = project.ID
	metadata["updatedExistingPullRequest"] = true
	metadata["pullRequestId"] = pr.ID
	repo := nonEmpty(req.Repo, pr.Repo, pullRequestTargetRepo(req, project, task))
	base := nonEmpty(req.Base, pr.Base, project.DefaultBase)
	branch := nonEmpty(req.Branch, pr.Branch)
	body := pullRequestBodyWithIssueClosingReference(strings.TrimSpace(req.Body), task, repo)
	updated, err := s.prPublisher.Update(ctx, pr, PullRequestPublishSpec{
		TaskID:         taskID,
		WorkerID:       workerID,
		WorkDir:        sourceRoot,
		Repo:           repo,
		Base:           base,
		Branch:         branch,
		HeadRepoOwner:  pullRequestHeadRepoOwner(project),
		PushRemote:     project.PushRemote,
		BranchPrefix:   project.PullRequestPolicy.BranchPrefix,
		Title:          strings.TrimSpace(req.Title),
		Body:           body,
		Draft:          req.Draft,
		Patch:          publishPatch,
		PatchFromBase:  patchFromBase,
		PatchBaseRef:   patchBaseRef,
		ResetWorkDir:   !patchFromBase && shouldResetPullRequestWorkDirAfterPublish(publishWorkspace),
		ForceWithLease: true,
		Metadata:       metadata,
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	if updated.ID == "" {
		updated.ID = pr.ID
	}
	updated.TaskID = taskID
	updated = normalizePullRequestStatusFields(updated)
	if err := s.recordTaskMilestone(ctx, taskID, "pr_updated", "pr_updated", "Pull request branch updated.", map[string]any{
		"pullRequestId": updated.ID,
		"url":           updated.URL,
		"repo":          updated.Repo,
		"number":        updated.Number,
		"branch":        updated.Branch,
		"workerId":      workerID,
	}); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.recordPullRequestUpdated(ctx, updated); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.recordPullRequestArtifact(ctx, updated); err != nil {
		return core.PullRequest{}, err
	}
	return updated, nil
}

type githubIssueClosingReference struct {
	Repo   string
	Number int
}

func (ref githubIssueClosingReference) String() string {
	return fmt.Sprintf("%s#%d", ref.Repo, ref.Number)
}

func pullRequestBodyWithIssueClosingReference(body string, task core.Task, publishRepo string) string {
	ref, ok := githubIssueClosingReferenceForTask(task)
	if !ok || pullRequestBodyAlreadyClosesIssue(body, ref, publishRepo) {
		return body
	}
	closingLine := "Closes " + ref.String()
	if body == "" {
		return closingLine
	}
	return body + "\n\n" + closingLine
}

func githubIssueClosingReferenceForTask(task core.Task) (githubIssueClosingReference, bool) {
	source, externalID := taskExternalRef(task)
	if !strings.EqualFold(source, "github-issue") {
		return githubIssueClosingReference{}, false
	}
	if ref, ok := parseGitHubIssueExternalID(externalID); ok {
		return ref, true
	}
	if len(task.Metadata) == 0 {
		return githubIssueClosingReference{}, false
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return githubIssueClosingReference{}, false
	}
	repo := strings.TrimSpace(stringMetadataValue(metadata["repo"]))
	number := intMetadata(metadata, "number")
	if repo == "" || number <= 0 {
		return githubIssueClosingReference{}, false
	}
	return githubIssueClosingReference{Repo: repo, Number: number}, true
}

func parseGitHubIssueExternalID(externalID string) (githubIssueClosingReference, bool) {
	repo, numberText, ok := strings.Cut(strings.TrimSpace(externalID), "#")
	if !ok {
		return githubIssueClosingReference{}, false
	}
	repo = strings.TrimSpace(repo)
	number, err := strconv.Atoi(strings.TrimSpace(numberText))
	if err != nil || repo == "" || !strings.Contains(repo, "/") || number <= 0 {
		return githubIssueClosingReference{}, false
	}
	return githubIssueClosingReference{Repo: repo, Number: number}, true
}

func pullRequestBodyAlreadyClosesIssue(body string, ref githubIssueClosingReference, publishRepo string) bool {
	if strings.TrimSpace(body) == "" {
		return false
	}
	numberText := strconv.Itoa(ref.Number)
	sameRepo := publishRepo == "" || strings.EqualFold(strings.TrimSpace(publishRepo), ref.Repo)
	for _, line := range strings.Split(body, "\n") {
		if !pullRequestClosingKeywordRE.MatchString(line) {
			continue
		}
		if lineReferencesQualifiedIssue(line, ref.Repo, numberText) || lineReferencesIssueURL(line, ref.Repo, numberText) {
			return true
		}
		if sameRepo && lineReferencesIssueNumber(line, numberText) {
			return true
		}
	}
	return false
}

func lineReferencesQualifiedIssue(line string, repo string, numberText string) bool {
	pattern := fmt.Sprintf(`(?i)(?:^|[^A-Za-z0-9_.-])%s#%s(?:[^0-9]|$)`, regexp.QuoteMeta(repo), regexp.QuoteMeta(numberText))
	return regexp.MustCompile(pattern).MatchString(line)
}

func lineReferencesIssueURL(line string, repo string, numberText string) bool {
	pattern := fmt.Sprintf(`(?i)github\.com/%s/issues/%s(?:[^0-9]|$)`, regexp.QuoteMeta(repo), regexp.QuoteMeta(numberText))
	return regexp.MustCompile(pattern).MatchString(line)
}

func lineReferencesIssueNumber(line string, numberText string) bool {
	pattern := fmt.Sprintf(`(?:^|[^A-Za-z0-9_])#%s(?:[^0-9]|$)`, regexp.QuoteMeta(numberText))
	return regexp.MustCompile(pattern).MatchString(line)
}

func (s *Service) pullRequestPublishSourceRoot(ctx context.Context, snapshot core.Snapshot, workerID string, project core.Project) (string, PreparedWorkspace, error) {
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return "", PreparedWorkspace{}, err
	}
	if workspace.VCSType == "ssh" {
		return project.LocalPath, workspace, nil
	}
	sourceRoot, err := s.pullRequestSourceRootForWorker(ctx, snapshot, workerID, project)
	if err != nil {
		return "", PreparedWorkspace{}, err
	}
	return sourceRoot, workspace, nil
}

func pullRequestPublishPatch(workspace PreparedWorkspace, changes WorkspaceChanges) (string, bool, string) {
	if workspace.VCSType != "ssh" {
		return "", false, ""
	}
	if strings.TrimSpace(changes.PublishDiff) != "" {
		return changes.PublishDiff, true, strings.TrimSpace(changes.PublishBase)
	}
	return changes.Diff, true, ""
}

func shouldResetPullRequestWorkDirAfterPublish(workspace PreparedWorkspace) bool {
	return workspace.VCSType == "ssh" || workspace.Mode == string(WorkspaceModeShared)
}

func (s *Service) workerCreatedPullRequest(ctx context.Context, snapshot core.Snapshot, task core.Task, workerID string, targetRepo string, metadata map[string]any) (core.PullRequest, bool, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return core.PullRequest{}, false, nil
	}
	for _, value := range workerPullRequestURLs(snapshot, task.ID, workerID) {
		repo, number := parsePullRequestURL(value)
		if repo == "" || number == 0 {
			continue
		}
		if strings.TrimSpace(targetRepo) != "" && !strings.EqualFold(repo, targetRepo) {
			continue
		}
		pr := core.PullRequest{
			ID:       watchedPullRequestID(core.PullRequest{Repo: repo, Number: number, URL: value}),
			TaskID:   task.ID,
			Repo:     repo,
			Number:   number,
			URL:      value,
			Metadata: core.MustJSON(workerCreatedPullRequestMetadata(metadata, value)),
		}
		inspected, err := s.prPublisher.Inspect(ctx, pr)
		if err != nil {
			return core.PullRequest{}, false, fmt.Errorf("inspect worker-created pull request %s: %w", value, err)
		}
		inspected.ID = pr.ID
		inspected.TaskID = task.ID
		if inspected.Repo == "" {
			inspected.Repo = repo
		}
		if inspected.Number == 0 {
			inspected.Number = number
		}
		if inspected.URL == "" {
			inspected.URL = value
		}
		if len(inspected.Metadata) == 0 {
			inspected.Metadata = pr.Metadata
		}
		if isTerminalPullRequestState(inspected.State) {
			continue
		}
		return inspected, true, nil
	}
	return core.PullRequest{}, false, nil
}

func workerCreatedPullRequestMetadata(metadata map[string]any, url string) map[string]any {
	out := maps.Clone(metadata)
	if out == nil {
		out = map[string]any{}
	}
	out["workerCreated"] = true
	out["adoptedFromWorkerOutput"] = true
	out["workerOutputURL"] = url
	return out
}

func workerPullRequestURLs(snapshot core.Snapshot, taskID string, workerID string) []string {
	seen := map[string]bool{}
	var urls []string
	add := func(text string) {
		for _, value := range githubPullRequestURLRE.FindAllString(text, -1) {
			if seen[value] {
				continue
			}
			seen[value] = true
			urls = append(urls, value)
		}
	}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.WorkerID != workerID {
			continue
		}
		switch event.Type {
		case core.EventWorkerOutput:
			var payload worker.Event
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				continue
			}
			add(payload.Text)
			if len(payload.Raw) > 0 {
				add(string(payload.Raw))
			}
		case core.EventWorkerCompleted:
			var payload struct {
				Summary string `json:"summary"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				continue
			}
			add(payload.Summary)
			add(payload.Error)
		}
	}
	return urls
}

func (s *Service) pullRequestSourceRootForWorker(ctx context.Context, snapshot core.Snapshot, workerID string, project core.Project) (string, error) {
	if appliedRoot := appliedWorkerSourceRoot(snapshot, workerID); appliedRoot != "" {
		return appliedRoot, nil
	}
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return "", err
	}
	if workspace.VCSType != "ssh" {
		if workspace.CWD != "" {
			return workspace.CWD, nil
		}
		return project.LocalPath, nil
	}
	result, err := s.ApplyWorkerChanges(ctx, workerID)
	if err != nil {
		if appliedRoot := appliedWorkerSourceRootFromStore(ctx, s.store, workerID); appliedRoot != "" {
			return appliedRoot, nil
		}
		return "", err
	}
	return nonEmpty(result.SourceRoot, project.LocalPath), nil
}

func (s *Service) WatchPullRequests(ctx context.Context, taskID string, req core.WatchPullRequestsRequest) ([]core.PullRequest, error) {
	if s.prPublisher == nil {
		return nil, errors.New("pull request publisher is not configured")
	}
	lister, ok := s.prPublisher.(PullRequestLister)
	if !ok {
		return nil, errors.New("pull request publisher cannot list existing pull requests")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	project, err := s.projectForTask(task)
	if err != nil {
		return nil, err
	}
	repo := strings.TrimSpace(req.Repo)
	if repo == "" {
		repo = project.UpstreamRepo
	}
	if repo == "" {
		repo = project.Repo
	}
	if repo == "" && strings.TrimSpace(req.URL) != "" {
		parsedRepo, _ := parsePullRequestURL(req.URL)
		repo = parsedRepo
	}
	metadata := map[string]any{
		"watch": true,
		"repo":  repo,
		"state": nonEmpty(req.State, "open"),
	}
	if req.Number > 0 {
		metadata["number"] = req.Number
	}
	if req.URL != "" {
		metadata["url"] = req.URL
	}
	if req.Author != "" {
		metadata["author"] = req.Author
	}
	if req.HeadBranch != "" {
		metadata["headBranch"] = req.HeadBranch
	}
	prs, err := lister.List(ctx, PullRequestListSpec{
		TaskID:     taskID,
		Repo:       repo,
		Number:     req.Number,
		URL:        req.URL,
		State:      req.State,
		Author:     req.Author,
		HeadBranch: req.HeadBranch,
		Limit:      req.Limit,
		Metadata:   metadata,
	})
	if err != nil {
		return nil, err
	}
	if len(prs) == 0 {
		return nil, errors.New("no pull requests matched watch request")
	}
	if err := s.recordTaskMilestone(ctx, taskID, "pull_requests_watched", "waiting_external", fmt.Sprintf("Watching %d existing pull request(s).", len(prs)), map[string]any{
		"count": len(prs),
		"repo":  repo,
	}); err != nil {
		return nil, err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "watching_pull_requests", fmt.Sprintf("Watching %d pull request(s) for GitHub state changes.", len(prs))); err != nil {
		return nil, err
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskWaiting); err != nil {
		return nil, err
	}
	for index, pr := range prs {
		pr.ID = existingPullRequestID(snapshot, taskID, pr)
		if pr.ID == "" {
			pr.ID = watchedPullRequestID(pr)
		}
		pr.TaskID = taskID
		if pr.Repo == "" {
			pr.Repo = repo
		}
		if len(pr.Metadata) == 0 {
			pr.Metadata = core.MustJSON(metadata)
		}
		pr = normalizePullRequestStatusFields(pr)
		if err := s.recordPullRequestPublished(ctx, pr); err != nil {
			return nil, err
		}
		if err := s.recordPullRequestArtifact(ctx, pr); err != nil {
			return nil, err
		}
		prs[index] = pr
	}
	return prs, nil
}

func (s *Service) watchRequestTargetsTerminalPullRequest(ctx context.Context, taskID string, req core.WatchPullRequestsRequest) bool {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false
	}
	repo := strings.ToLower(strings.TrimSpace(req.Repo))
	url := strings.TrimSpace(req.URL)
	branch := strings.TrimSpace(req.HeadBranch)
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID || !isTerminalPullRequestState(pr.State) {
			continue
		}
		if url != "" && strings.EqualFold(pr.URL, url) {
			return true
		}
		if repo != "" && req.Number > 0 && strings.EqualFold(pr.Repo, repo) && pr.Number == req.Number {
			return true
		}
		if branch != "" && pr.Branch == branch && (repo == "" || strings.EqualFold(pr.Repo, repo)) {
			return true
		}
	}
	return false
}

func existingPullRequestID(snapshot core.Snapshot, taskID string, pr core.PullRequest) string {
	for _, existing := range snapshot.PullRequests {
		if existing.TaskID != taskID {
			continue
		}
		if samePullRequestIdentity(existing, pr) {
			return existing.ID
		}
	}
	return ""
}

func samePullRequestIdentity(a core.PullRequest, b core.PullRequest) bool {
	aRepo := strings.TrimSpace(a.Repo)
	bRepo := strings.TrimSpace(b.Repo)
	if aRepo != "" && bRepo != "" && strings.EqualFold(aRepo, bRepo) && a.Number > 0 && a.Number == b.Number {
		return true
	}
	aURL := strings.TrimSpace(a.URL)
	bURL := strings.TrimSpace(b.URL)
	if aURL != "" && bURL != "" && strings.EqualFold(aURL, bURL) {
		return true
	}
	if aURL != "" && bRepo != "" && b.Number > 0 {
		repo, number := parsePullRequestURL(aURL)
		return strings.EqualFold(repo, bRepo) && number == b.Number
	}
	if bURL != "" && aRepo != "" && a.Number > 0 {
		repo, number := parsePullRequestURL(bURL)
		return strings.EqualFold(repo, aRepo) && number == a.Number
	}
	return false
}

func watchedPullRequestID(pr core.PullRequest) string {
	repo := strings.TrimSpace(pr.Repo)
	if repo != "" && pr.Number > 0 {
		return "github:" + repo + "#" + fmt.Sprint(pr.Number)
	}
	if strings.TrimSpace(pr.URL) != "" {
		repo, number := parsePullRequestURL(pr.URL)
		if repo != "" && number > 0 {
			return "github:" + repo + "#" + fmt.Sprint(number)
		}
	}
	if strings.TrimSpace(pr.ID) != "" {
		return pr.ID
	}
	return newPullRequestID()
}

func (s *Service) RefreshPullRequest(ctx context.Context, prID string) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	checked, err := s.prPublisher.Inspect(ctx, pr)
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.ID = pr.ID
	checked.TaskID = pr.TaskID
	checked = normalizePullRequestStatusFields(checked)
	return s.recordPullRequestStatus(ctx, snapshot, checked)
}

func (s *Service) mergePullRequest(ctx context.Context, snapshot core.Snapshot, pr core.PullRequest) (core.PullRequest, error) {
	merger, ok := s.prPublisher.(PullRequestMerger)
	if !ok {
		return core.PullRequest{}, errors.New("pull request publisher cannot merge pull requests")
	}
	merged, err := merger.Merge(ctx, pr, PullRequestMergeSpec{
		WorkDir: pullRequestMetadataString(pr, "workDir"),
		Repo:    pr.Repo,
		Number:  pr.Number,
		URL:     pr.URL,
		Method:  s.pullRequestMergeMethod(snapshot, pr),
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	if merged.ID == "" {
		merged.ID = pr.ID
	}
	if merged.TaskID == "" {
		merged.TaskID = pr.TaskID
	}
	if merged.Repo == "" {
		merged.Repo = pr.Repo
	}
	if merged.Number == 0 {
		merged.Number = pr.Number
	}
	if merged.URL == "" {
		merged.URL = pr.URL
	}
	if merged.Branch == "" {
		merged.Branch = pr.Branch
	}
	if merged.Base == "" {
		merged.Base = pr.Base
	}
	if merged.Title == "" {
		merged.Title = pr.Title
	}
	if len(merged.Metadata) == 0 {
		merged.Metadata = pr.Metadata
	}
	merged = normalizePullRequestStatusFields(merged)
	return s.recordPullRequestStatus(ctx, snapshot, merged)
}

func (s *Service) pullRequestMergeMethod(snapshot core.Snapshot, pr core.PullRequest) string {
	project, ok := s.projectForPullRequestPolicy(snapshot, pr)
	if !ok {
		return "squash"
	}
	return normalizePullRequestMergeMethod(project.PullRequestPolicy.MergeMethod)
}

func (s *Service) projectForPullRequestPolicy(snapshot core.Snapshot, pr core.PullRequest) (core.Project, bool) {
	if len(pr.Metadata) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(pr.Metadata, &metadata); err == nil {
			if projectID := strings.TrimSpace(stringMetadataValue(metadata["projectId"])); projectID != "" {
				if s.projects == nil {
					return core.Project{}, false
				}
				return s.projects.Get(projectID)
			}
		}
	}
	if repo := pullRequestRepoForPolicy(pr); repo != "" {
		if s.projects == nil {
			return core.Project{}, false
		}
		return s.projects.FindByIssueRepo(repo)
	}
	task, ok := findTask(snapshot, pr.TaskID)
	if !ok {
		return core.Project{}, false
	}
	project, err := s.projectForTask(task)
	if err != nil {
		return core.Project{}, false
	}
	return project, true
}

func pullRequestRepoForPolicy(pr core.PullRequest) string {
	if repo := strings.TrimSpace(pr.Repo); repo != "" {
		return repo
	}
	if strings.TrimSpace(pr.URL) != "" {
		repo, _ := parsePullRequestURL(pr.URL)
		return repo
	}
	return ""
}

func (s *Service) recordPullRequestAutoMergeFailure(ctx context.Context, snapshot core.Snapshot, pr core.PullRequest, mergeErr error) (core.PullRequest, error) {
	failed := pr
	failed.Metadata = pullRequestMetadataWithAutoMergeFailure(pr.Metadata, mergeErr)
	return s.recordPullRequestStatus(ctx, snapshot, failed)
}

func (s *Service) recordPullRequestStatus(ctx context.Context, snapshot core.Snapshot, checked core.PullRequest) (core.PullRequest, error) {
	event, err := s.append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: checked.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":               checked.ID,
			"state":            checked.State,
			"draft":            checked.Draft,
			"checksStatus":     checked.ChecksStatus,
			"checksConclusion": checked.ChecksConclusion,
			"mergeStatus":      checked.MergeStatus,
			"mergeable":        checked.Mergeable,
			"reviewStatus":     checked.ReviewStatus,
			"metadata":         checked.Metadata,
		}),
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.UpdatedAt = event.At
	if err := s.recordPullRequestArtifact(ctx, checked); err != nil {
		return core.PullRequest{}, err
	}
	status, phase := objectiveForPullRequest(checked)
	if phase != "" {
		if err := s.updateTaskObjective(ctx, checked.TaskID, status, phase, pullRequestObjectiveSummary(checked, phase)); err != nil {
			return core.PullRequest{}, err
		}
	}
	if strings.EqualFold(checked.State, "MERGED") {
		if err := s.recordTaskMilestone(ctx, checked.TaskID, "pr_merged", "merged", "Pull request merged.", map[string]any{
			"pullRequestId": checked.ID,
			"url":           checked.URL,
			"repo":          checked.Repo,
			"number":        checked.Number,
		}); err != nil {
			return core.PullRequest{}, err
		}
		if !pullRequestTerminalizesTask(snapshot, checked) {
			if err := s.continueTaskAfterIntermediatePullRequest(ctx, snapshot, checked, "intermediate_pr_merged", "Intermediate pull request merged; objective continues."); err != nil {
				return core.PullRequest{}, err
			}
			return checked, nil
		}
		if err := s.setTaskStatus(ctx, checked.TaskID, core.TaskSucceeded); err != nil {
			return core.PullRequest{}, err
		}
		if err := s.completeRelatedPullRequestTasks(ctx, snapshot, checked, core.TaskSucceeded, core.ObjectiveSatisfied, "pr_merged", "merged", "Pull request merged."); err != nil {
			return core.PullRequest{}, err
		}
	} else if strings.EqualFold(checked.State, "CLOSED") {
		if err := s.recordTaskMilestone(ctx, checked.TaskID, "pr_closed", "pr_closed", "Pull request closed without merge.", map[string]any{
			"pullRequestId": checked.ID,
			"url":           checked.URL,
			"repo":          checked.Repo,
			"number":        checked.Number,
		}); err != nil {
			return core.PullRequest{}, err
		}
		if !pullRequestTerminalizesTask(snapshot, checked) {
			if err := s.continueTaskAfterIntermediatePullRequest(ctx, snapshot, checked, "intermediate_pr_closed", "Intermediate pull request closed; objective continues."); err != nil {
				return core.PullRequest{}, err
			}
			return checked, nil
		}
		if err := s.setTaskStatus(ctx, checked.TaskID, core.TaskCanceled); err != nil {
			return core.PullRequest{}, err
		}
		if err := s.completeRelatedPullRequestTasks(ctx, snapshot, checked, core.TaskCanceled, core.ObjectiveAbandoned, "pr_closed", "pr_closed", "Pull request closed without merge."); err != nil {
			return core.PullRequest{}, err
		}
	}
	return checked, nil
}

func (s *Service) continueTaskAfterIntermediatePullRequest(ctx context.Context, snapshot core.Snapshot, pr core.PullRequest, phase string, summary string) error {
	if err := s.updateTaskObjective(ctx, pr.TaskID, core.ObjectiveActive, phase, summary); err != nil {
		return err
	}
	if _, ok := s.brain.(ReplanProvider); !ok {
		return nil
	}
	task, ok := findTask(snapshot, pr.TaskID)
	if !ok || taskHasActiveWorkers(snapshot, pr.TaskID) || intermediatePullRequestContinuationRecorded(snapshot, pr) {
		return nil
	}
	switch task.Status {
	case core.TaskWaiting, core.TaskRunning, core.TaskFailed:
	default:
		return nil
	}
	initial, results, err := retryGraphStateForTask(snapshot, pr.TaskID)
	if err != nil {
		return nil
	}
	if err := s.recordTaskAction(ctx, pr.TaskID, map[string]any{
		"kind":          "intermediate_pull_request_terminal_replan",
		"status":        "started",
		"reason":        summary,
		"phase":         phase,
		"pullRequestId": pr.ID,
		"url":           pr.URL,
		"repo":          pr.Repo,
		"number":        pr.Number,
	}); err != nil {
		return err
	}
	if err := s.setTaskStatus(ctx, pr.TaskID, core.TaskPlanning); err != nil {
		return err
	}
	task.Status = core.TaskPlanning
	task.Error = ""
	task.ObjectiveStatus = core.ObjectiveActive
	task.ObjectivePhase = phase
	s.startTaskRoutine(pr.TaskID, func(taskCtx context.Context) {
		s.retryGraphTask(taskCtx, task, initial, results)
	})
	return nil
}

func intermediatePullRequestContinuationRecorded(snapshot core.Snapshot, pr core.PullRequest) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != pr.TaskID || event.Type != core.EventTaskAction {
			continue
		}
		var payload struct {
			Kind          string `json:"kind"`
			PullRequestID string `json:"pullRequestId"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == "intermediate_pull_request_terminal_replan" && payload.PullRequestID == pr.ID {
			return true
		}
	}
	return false
}

func pullRequestReadyForAutoMerge(pr core.PullRequest) bool {
	if !strings.EqualFold(pr.State, "OPEN") || pr.Draft {
		return false
	}
	if !pullRequestChecksPassing(pr) {
		return false
	}
	review := strings.ToUpper(strings.TrimSpace(pr.ReviewStatus))
	if review != "" && review != "APPROVED" {
		return false
	}
	return !pullRequestMergeNeedsWork(pr)
}

func pullRequestMergeNeedsWork(pr core.PullRequest) bool {
	merge := strings.ToUpper(strings.TrimSpace(pr.MergeStatus))
	mergeable := strings.ToUpper(strings.TrimSpace(pr.Mergeable))
	for _, value := range []string{merge, mergeable} {
		switch value {
		case "DIRTY", "CONFLICTING", "BEHIND":
			return true
		case "BLOCKED":
			if pullRequestBlockedByActionableState(pr) {
				return true
			}
		}
	}
	return false
}

func pullRequestBlockedByActionableState(pr core.PullRequest) bool {
	checks := strings.ToLower(strings.TrimSpace(pr.ChecksStatus))
	review := strings.ToUpper(strings.TrimSpace(pr.ReviewStatus))
	return checks == "failing" || checks == "failure" || review == "CHANGES_REQUESTED" || review == "COMMENTED"
}

func pullRequestMetadataString(pr core.PullRequest, key string) string {
	if len(pr.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return ""
	}
	return stringMetadataValue(metadata[key])
}

func pullRequestMetadataWithAutoMergeFailure(raw json.RawMessage, mergeErr error) json.RawMessage {
	metadata := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &metadata)
	}
	metadata["autoMergeError"] = strings.TrimSpace(mergeErr.Error())
	metadata["autoMergeFailedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	return core.MustJSON(metadata)
}

func pullRequestAutoMergeError(pr core.PullRequest) string {
	if len(pr.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return ""
	}
	return strings.TrimSpace(stringMetadataValue(metadata["autoMergeError"]))
}

func (s *Service) ReconcilePullRequestTerminalTasks(ctx context.Context, prID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	if !pullRequestTerminalizesTask(snapshot, pr) {
		return nil
	}
	switch {
	case strings.EqualFold(pr.State, "MERGED"):
		return s.completeRelatedPullRequestTasks(ctx, snapshot, pr, core.TaskSucceeded, core.ObjectiveSatisfied, "pr_merged", "merged", "Pull request merged.")
	case strings.EqualFold(pr.State, "CLOSED"):
		return s.completeRelatedPullRequestTasks(ctx, snapshot, pr, core.TaskCanceled, core.ObjectiveAbandoned, "pr_closed", "pr_closed", "Pull request closed without merge.")
	default:
		return nil
	}
}

func isTerminalPullRequestState(state string) bool {
	return strings.EqualFold(state, "MERGED") || strings.EqualFold(state, "CLOSED")
}

func pullRequestPublicationPhase(continueAfterPublish bool) string {
	if continueAfterPublish {
		return "intermediate"
	}
	return "completion"
}

func pullRequestContinuesTask(pr core.PullRequest) bool {
	if len(pr.Metadata) == 0 {
		return false
	}
	var metadata map[string]any
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return false
	}
	if boolMetadata(metadata, "continueAfterPublish") {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(stringMetadataValue(metadata["publicationPhase"])), "intermediate")
}

func pullRequestTerminalizesTask(snapshot core.Snapshot, pr core.PullRequest) bool {
	if pullRequestContinuesTask(pr) {
		return false
	}
	if taskHasActiveWorkers(snapshot, pr.TaskID) {
		return false
	}
	if task, ok := findTask(snapshot, pr.TaskID); ok {
		switch task.Status {
		case core.TaskRunning, core.TaskPlanning:
			return false
		}
	}
	return true
}

func (s *Service) completeRelatedPullRequestTasks(ctx context.Context, snapshot core.Snapshot, pr core.PullRequest, taskStatus core.TaskStatus, objectiveStatus core.ObjectiveStatus, milestone string, phase string, summary string) error {
	for _, task := range snapshot.Tasks {
		if task.ID == pr.TaskID || isTerminalTaskStatus(task.Status) || !taskWatchesPullRequest(task, pr) {
			continue
		}
		if err := s.updateTaskObjective(ctx, task.ID, objectiveStatus, phase, summary); err != nil {
			return err
		}
		if err := s.recordTaskMilestone(ctx, task.ID, milestone, phase, summary, map[string]any{
			"pullRequestId": pr.ID,
			"url":           pr.URL,
			"repo":          pr.Repo,
			"number":        pr.Number,
		}); err != nil {
			return err
		}
		if err := s.setTaskStatus(ctx, task.ID, taskStatus); err != nil {
			return err
		}
	}
	return nil
}

func taskWatchesPullRequest(task core.Task, pr core.PullRequest) bool {
	if len(task.Metadata) == 0 {
		return false
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return false
	}
	if pullRequestID := strings.TrimSpace(stringMetadataValue(metadata["pullRequestId"])); pullRequestID != "" && pullRequestID == pr.ID {
		return true
	}
	repo := strings.TrimSpace(stringMetadataValue(metadata["repo"]))
	number := intMetadata(metadata, "number")
	return repo != "" && pr.Repo != "" && strings.EqualFold(repo, pr.Repo) && number > 0 && number == pr.Number
}

func (s *Service) StartPullRequestBabysitter(ctx context.Context, prID string) (core.Task, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	task, ok := findTask(snapshot, pr.TaskID)
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) && !strings.EqualFold(pr.State, "OPEN") {
		return task, nil
	}
	if !pullRequestHasBabysitter(snapshot, pr.ID) {
		if err := s.recordPullRequestBabysitter(ctx, pr); err != nil {
			return core.Task{}, err
		}
	}
	if !isTerminalTaskStatus(task.Status) {
		if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingExternal, "pr_open", "Pull request is open; waiting on external GitHub state."); err != nil {
			return core.Task{}, err
		}
		if task.Status != core.TaskWaiting {
			if err := s.setTaskStatus(ctx, task.ID, core.TaskWaiting); err != nil {
				return core.Task{}, err
			}
		}
	}
	return task, nil
}

func (s *Service) recordPullRequestBabysitter(ctx context.Context, pr core.PullRequest) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventPRBabysitter,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":               pr.ID,
			"babysitterTaskId": pr.TaskID,
		}),
	})
	return err
}

func pullRequestHasBabysitter(snapshot core.Snapshot, prID string) bool {
	for _, event := range snapshot.Events {
		if event.Type != core.EventPRBabysitter {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
			return true
		}
	}
	return false
}

func (s *Service) ContinueTaskForPullRequest(ctx context.Context, prID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	task, ok := findTask(snapshot, pr.TaskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) {
		return nil
	}
	if pullRequestFeedbackAlreadyPending(snapshot, pr.TaskID, pr.ID) {
		return nil
	}
	if pullRequestFollowUpStartedAfterLatestStatus(snapshot, pr.ID) {
		return nil
	}
	attempt := pullRequestFollowUpAttempt(snapshot, pr.ID) + 1
	prompt := pullRequestFollowUpPrompt(pr)
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":                pr.ID,
			"attempt":           attempt,
			"reason":            "pull_request_needs_work",
			"status":            "queued",
			"repo":              pr.Repo,
			"number":            pr.Number,
			"url":               pr.URL,
			"branch":            pr.Branch,
			"base":              pr.Base,
			"state":             pr.State,
			"checksStatus":      pr.ChecksStatus,
			"mergeStatus":       pr.MergeStatus,
			"reviewStatus":      pr.ReviewStatus,
			"feedbackSignature": pullRequestFeedbackSignature(pr.Metadata),
			"prompt":            prompt,
		}),
	}); err != nil {
		return err
	}
	if err := s.recordTaskMilestone(ctx, pr.TaskID, fmt.Sprintf("pr_followup_%d", attempt), "pr_needs_work", "Pull request needs follow-up work.", map[string]any{
		"pullRequestId": pr.ID,
		"url":           pr.URL,
		"repo":          pr.Repo,
		"number":        pr.Number,
		"attempt":       attempt,
	}); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, pr.TaskID, core.ObjectiveActive, "pr_needs_work", "Pull request needs follow-up work from checks or review."); err != nil {
		return err
	}
	if err := s.markPullRequestFeedbackTriggered(ctx, pr); err != nil {
		return err
	}
	if task.Status == core.TaskWaiting {
		s.startTaskRoutine(pr.TaskID, func(taskCtx context.Context) {
			s.resumePullRequestFeedbackQueue(taskCtx, pr.TaskID)
		})
	}
	return nil
}

func (s *Service) markPullRequestFeedbackTriggered(ctx context.Context, pr core.PullRequest) error {
	metadata, changed := pullRequestMetadataMarkFeedbackTriggered(pr.Metadata)
	if !changed {
		return nil
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":       pr.ID,
			"metadata": metadata,
		}),
	})
	return err
}

func activePullRequestBabysitter(snapshot core.Snapshot, pr core.PullRequest) core.Task {
	if pr.BabysitterTaskID == "" {
		return core.Task{}
	}
	task, ok := findTask(snapshot, pr.BabysitterTaskID)
	if !ok || isTerminalTaskStatus(task.Status) {
		return core.Task{}
	}
	return task
}

func pullRequestBabysitterAttempt(snapshot core.Snapshot, prID string) int {
	attempt := 0
	for _, event := range snapshot.Events {
		if event.Type != core.EventPRBabysitter {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
			attempt++
		}
	}
	return attempt
}

func pullRequestFollowUpAttempt(snapshot core.Snapshot, prID string) int {
	attempt := 0
	for _, event := range snapshot.Events {
		if event.Type != core.EventPRFollowUp {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
			attempt++
		}
	}
	return attempt
}

func pullRequestFollowUpStartedAfterLatestStatus(snapshot core.Snapshot, prID string) bool {
	latestStatusEvent := int64(0)
	latestFollowUpEvent := int64(0)
	for _, event := range snapshot.Events {
		var payload struct {
			ID string `json:"id"`
		}
		switch event.Type {
		case core.EventPRStatusChecked:
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
				latestStatusEvent = event.ID
			}
		case core.EventPRFollowUp:
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
				latestFollowUpEvent = event.ID
			}
		}
	}
	return latestFollowUpEvent > 0 && latestFollowUpEvent >= latestStatusEvent
}

func (s *Service) recordPullRequestArtifact(ctx context.Context, pr core.PullRequest) error {
	name := pr.Title
	if name == "" {
		name = fmt.Sprintf("%s#%d", pr.Repo, pr.Number)
	}
	return s.recordTaskArtifact(ctx, pr.TaskID, pr.ID, "github_pull_request", name, pr.URL, pr.Branch, map[string]any{
		"repo":             pr.Repo,
		"number":           pr.Number,
		"state":            pr.State,
		"draft":            pr.Draft,
		"checksStatus":     pr.ChecksStatus,
		"checksConclusion": pr.ChecksConclusion,
		"mergeStatus":      pr.MergeStatus,
		"mergeable":        pr.Mergeable,
		"reviewStatus":     pr.ReviewStatus,
	})
}

func (s *Service) recordPullRequestPublished(ctx context.Context, pr core.PullRequest) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":               pr.ID,
			"repo":             pr.Repo,
			"number":           pr.Number,
			"url":              pr.URL,
			"branch":           pr.Branch,
			"base":             pr.Base,
			"title":            pr.Title,
			"state":            pr.State,
			"draft":            pr.Draft,
			"checksStatus":     pr.ChecksStatus,
			"checksConclusion": pr.ChecksConclusion,
			"mergeStatus":      pr.MergeStatus,
			"mergeable":        pr.Mergeable,
			"reviewStatus":     pr.ReviewStatus,
			"metadata":         pr.Metadata,
		}),
	})
	return err
}

func (s *Service) recordPullRequestUpdated(ctx context.Context, pr core.PullRequest) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventPRUpdated,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":               pr.ID,
			"repo":             pr.Repo,
			"number":           pr.Number,
			"url":              pr.URL,
			"branch":           pr.Branch,
			"base":             pr.Base,
			"title":            pr.Title,
			"state":            pr.State,
			"draft":            pr.Draft,
			"checksStatus":     pr.ChecksStatus,
			"checksConclusion": pr.ChecksConclusion,
			"mergeStatus":      pr.MergeStatus,
			"mergeable":        pr.Mergeable,
			"reviewStatus":     pr.ReviewStatus,
			"metadata":         pr.Metadata,
		}),
	})
	return err
}

func (s *Service) findPullRequest(ctx context.Context, prID string) (core.PullRequest, bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false, err
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == prID {
			return pr, true, nil
		}
	}
	return core.PullRequest{}, false, nil
}

func (s *Service) retryWaitingPublishPullRequestAction(ctx context.Context, task core.Task, snapshot core.Snapshot) bool {
	action, workerID, ok := latestWaitingPublishPullRequestAction(snapshot, task.ID)
	if !ok {
		return false
	}
	if !latestApprovalNeededMatches(snapshot, task.ID, workerID, "ssh_signing_agent_failed") {
		return false
	}
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "retrying", "Retrying pull request publication after user remediation."); err != nil {
		return true
	}
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return true
	}
	req := publishPullRequestRequestFromAction(action)
	req.WorkerID = workerID
	recordCompletedAction := func(pr core.PullRequest) error {
		return s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":          action.Kind,
			"when":          nonEmpty(action.When, "after_success"),
			"reason":        action.Reason,
			"inputs":        action.Inputs,
			"workerId":      workerID,
			"pullRequestId": pr.ID,
			"url":           pr.URL,
		})
	}
	_, err := s.publishTaskPullRequest(ctx, task.ID, req, recordCompletedAction)
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, workerID, err) {
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":     action.Kind,
				"when":     nonEmpty(action.When, "after_success"),
				"reason":   action.Reason,
				"inputs":   action.Inputs,
				"workerId": workerID,
				"status":   "waiting",
				"error":    err.Error(),
			})
			return true
		}
		_ = s.failTask(ctx, task.ID, err)
		return true
	}
	return true
}

func (s *Service) retryFailedPublishPullRequestAction(ctx context.Context, task core.Task, snapshot core.Snapshot) bool {
	action, workerID, ok := latestFailedPublishPullRequestAction(snapshot, task.ID)
	if !ok {
		return false
	}
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "retrying", "Retrying failed pull request publication."); err != nil {
		return true
	}
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return true
	}
	req := publishPullRequestRequestFromAction(action)
	req.WorkerID = workerID
	recordCompletedAction := func(pr core.PullRequest) error {
		return s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":          action.Kind,
			"when":          nonEmpty(action.When, "after_success"),
			"reason":        action.Reason,
			"inputs":        action.Inputs,
			"workerId":      workerID,
			"pullRequestId": pr.ID,
			"url":           pr.URL,
		})
	}
	_, err := s.publishTaskPullRequest(ctx, task.ID, req, recordCompletedAction)
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, workerID, err) {
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":     action.Kind,
				"when":     nonEmpty(action.When, "after_success"),
				"reason":   action.Reason,
				"inputs":   action.Inputs,
				"workerId": workerID,
				"status":   "waiting",
				"error":    err.Error(),
			})
			return true
		}
		_ = s.failTask(ctx, task.ID, err)
		return true
	}
	return true
}

func latestWaitingPublishPullRequestAction(snapshot core.Snapshot, taskID string) (PlanAction, string, bool) {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind     string         `json:"kind"`
			When     string         `json:"when"`
			Reason   string         `json:"reason"`
			Inputs   map[string]any `json:"inputs"`
			WorkerID string         `json:"workerId"`
			Status   string         `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return PlanAction{}, "", false
		}
		if payload.Kind != "publish_pull_request" || payload.Status != "waiting" || strings.TrimSpace(payload.WorkerID) == "" {
			return PlanAction{}, "", false
		}
		return PlanAction{
			Kind:     payload.Kind,
			When:     payload.When,
			Reason:   payload.Reason,
			WorkerID: payload.WorkerID,
			Inputs:   payload.Inputs,
		}, payload.WorkerID, true
	}
	return PlanAction{}, "", false
}

func latestFailedPublishPullRequestAction(snapshot core.Snapshot, taskID string) (PlanAction, string, bool) {
	if !latestTaskFailureMatches(snapshot, taskID, func(errorText string) bool {
		return strings.Contains(errorText, errPullRequestWorkerNotPublishable.Error()) ||
			strings.Contains(errorText, "publish_pull_request action has no successful worker with candidate changes")
	}) {
		return PlanAction{}, "", false
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind     string         `json:"kind"`
			When     string         `json:"when"`
			Reason   string         `json:"reason"`
			Inputs   map[string]any `json:"inputs"`
			WorkerID string         `json:"workerId"`
			Status   string         `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return PlanAction{}, "", false
		}
		if payload.Kind != "publish_pull_request" {
			return PlanAction{}, "", false
		}
		if payload.Status != "started" || strings.TrimSpace(payload.WorkerID) == "" {
			return PlanAction{}, "", false
		}
		return PlanAction{
			Kind:     payload.Kind,
			When:     payload.When,
			Reason:   payload.Reason,
			WorkerID: payload.WorkerID,
			Inputs:   payload.Inputs,
		}, payload.WorkerID, true
	}
	return PlanAction{}, "", false
}

func resumingPullRequestFollowUp(snapshot core.Snapshot, taskID string) bool {
	latestFollowUp := int64(0)
	latestWaitingStatus := int64(0)
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventPRFollowUp:
			latestFollowUp = event.ID
		case core.EventTaskStatus:
			var payload struct {
				Status core.TaskStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.Status == core.TaskWaiting {
				latestWaitingStatus = event.ID
			}
		}
	}
	return latestFollowUp > 0 && latestFollowUp > latestWaitingStatus
}

func latestPullRequestFollowUp(snapshot core.Snapshot, taskID string) (core.PullRequest, bool) {
	latestFollowUp := int64(0)
	latestPullRequestID := ""
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventPRFollowUp {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
			continue
		}
		if event.ID >= latestFollowUp {
			latestFollowUp = event.ID
			latestPullRequestID = strings.TrimSpace(payload.ID)
		}
	}
	if latestPullRequestID == "" {
		return core.PullRequest{}, false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == latestPullRequestID {
			return pr, true
		}
	}
	return core.PullRequest{}, false
}

func latestPullRequestFollowUpIsQueued(snapshot core.Snapshot, taskID string) bool {
	latestFollowUp := int64(0)
	queued := false
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventPRFollowUp {
			continue
		}
		var payload struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if event.ID >= latestFollowUp {
			latestFollowUp = event.ID
			queued = payload.Status == "queued"
		}
	}
	return queued
}

func pendingPullRequestFeedback(snapshot core.Snapshot, taskID string) []PullRequestFeedbackItem {
	latestPlanned := int64(0)
	pullRequests := map[string]core.PullRequest{}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID {
			pullRequests[pr.ID] = pr
		}
	}
	for _, event := range snapshot.Events {
		if event.TaskID == taskID && event.Type == core.EventTaskPlanned && event.ID > latestPlanned {
			latestPlanned = event.ID
		}
	}
	var items []PullRequestFeedbackItem
	itemByPullRequest := map[string]int{}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventPRFollowUp || event.ID <= latestPlanned {
			continue
		}
		var payload struct {
			ID                string `json:"id"`
			Attempt           int    `json:"attempt"`
			Reason            string `json:"reason"`
			Repo              string `json:"repo"`
			Number            int    `json:"number"`
			URL               string `json:"url"`
			Branch            string `json:"branch"`
			Base              string `json:"base"`
			State             string `json:"state"`
			ChecksStatus      string `json:"checksStatus"`
			MergeStatus       string `json:"mergeStatus"`
			ReviewStatus      string `json:"reviewStatus"`
			FeedbackSignature string `json:"feedbackSignature"`
			Prompt            string `json:"prompt"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
			continue
		}
		if pr, ok := pullRequests[payload.ID]; ok {
			payload.Repo = nonEmpty(payload.Repo, pr.Repo)
			payload.Number = firstNonZero(payload.Number, pr.Number)
			payload.URL = nonEmpty(payload.URL, pr.URL)
			payload.Branch = nonEmpty(payload.Branch, pr.Branch)
			payload.Base = nonEmpty(payload.Base, pr.Base)
			payload.State = nonEmpty(payload.State, pr.State)
			payload.ChecksStatus = nonEmpty(payload.ChecksStatus, pr.ChecksStatus)
			payload.MergeStatus = nonEmpty(payload.MergeStatus, pr.MergeStatus)
			payload.ReviewStatus = nonEmpty(payload.ReviewStatus, pr.ReviewStatus)
			if payload.Prompt == "" {
				payload.Prompt = pullRequestFollowUpPrompt(pr)
			}
		}
		item := PullRequestFeedbackItem{
			EventID:           event.ID,
			PullRequestID:     payload.ID,
			Attempt:           payload.Attempt,
			Reason:            payload.Reason,
			Repo:              payload.Repo,
			Number:            payload.Number,
			URL:               payload.URL,
			Branch:            payload.Branch,
			Base:              payload.Base,
			State:             payload.State,
			ChecksStatus:      payload.ChecksStatus,
			MergeStatus:       payload.MergeStatus,
			ReviewStatus:      payload.ReviewStatus,
			FeedbackSignature: payload.FeedbackSignature,
			Prompt:            payload.Prompt,
		}
		if index, ok := itemByPullRequest[item.PullRequestID]; ok {
			items[index] = item
			continue
		}
		itemByPullRequest[item.PullRequestID] = len(items)
		items = append(items, item)
	}
	return items
}

func pullRequestFeedbackAlreadyPending(snapshot core.Snapshot, taskID string, prID string) bool {
	var current core.PullRequest
	for _, pr := range snapshot.PullRequests {
		if pr.ID == prID {
			current = pr
			break
		}
	}
	currentSignature := pullRequestFeedbackSignature(current.Metadata)
	hasUntriggeredFeedback := currentSignature != "" && pullRequestHasUntriggeredFeedback(current)
	latestPlanned := int64(0)
	for _, event := range snapshot.Events {
		if event.TaskID == taskID && event.Type == core.EventTaskPlanned && event.ID > latestPlanned {
			latestPlanned = event.ID
		}
	}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventPRFollowUp || event.ID <= latestPlanned {
			continue
		}
		var payload struct {
			ID                string `json:"id"`
			FeedbackSignature string `json:"feedbackSignature"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.ID != prID {
			continue
		}
		signature := strings.TrimSpace(payload.FeedbackSignature)
		if currentSignature == "" || signature == currentSignature || (signature == "" && !hasUntriggeredFeedback) {
			return true
		}
	}
	return false
}

func (s *Service) pendingPullRequestFeedback(ctx context.Context, taskID string) []PullRequestFeedbackItem {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	return pendingPullRequestFeedback(snapshot, taskID)
}

func firstPendingPullRequestFeedback(snapshot core.Snapshot, taskID string) (core.PullRequest, bool) {
	items := pendingPullRequestFeedback(snapshot, taskID)
	if len(items) == 0 {
		return core.PullRequest{}, false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == items[0].PullRequestID {
			return pr, true
		}
	}
	return core.PullRequest{
		ID:           items[0].PullRequestID,
		TaskID:       taskID,
		Repo:         items[0].Repo,
		Number:       items[0].Number,
		URL:          items[0].URL,
		Branch:       items[0].Branch,
		Base:         items[0].Base,
		State:        items[0].State,
		ChecksStatus: items[0].ChecksStatus,
		MergeStatus:  items[0].MergeStatus,
		ReviewStatus: items[0].ReviewStatus,
	}, true
}

func (s *Service) firstPendingPullRequestFeedback(ctx context.Context, taskID string) (core.PullRequest, bool) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false
	}
	return firstPendingPullRequestFeedback(snapshot, taskID)
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func annotatePullRequestFollowUpPlan(plan Plan, pr core.PullRequest) Plan {
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Prompt = appendPullRequestFollowUpWorkerInstruction(plan.Prompt, pr)
	plan.Metadata["pullRequestID"] = pr.ID
	plan.Metadata["pullRequestRepo"] = pr.Repo
	if pr.Number > 0 {
		plan.Metadata["pullRequestNumber"] = pr.Number
	}
	if strings.TrimSpace(pr.Branch) != "" {
		plan.Metadata["pullRequestBranch"] = pr.Branch
		if stringMetadata(plan.Metadata, "workspaceBaseRef") == "" {
			plan.Metadata["workspaceBaseRef"] = pr.Branch
			plan.Metadata["workspaceBaseRefKind"] = "pull_request_head"
		}
	}
	if strings.TrimSpace(pr.Base) != "" {
		plan.Metadata["pullRequestBase"] = pr.Base
	}
	if strings.TrimSpace(pr.URL) != "" {
		plan.Metadata["pullRequestURL"] = pr.URL
	}
	return plan
}

func appendPullRequestFollowUpWorkerInstruction(prompt string, pr core.PullRequest) string {
	instruction := pullRequestFollowUpWorkerInstruction(pr)
	if strings.Contains(prompt, instruction) {
		return prompt
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return instruction
	}
	return prompt + "\n\n" + instruction
}

func pullRequestFollowUpWorkerInstruction(pr core.PullRequest) string {
	var b strings.Builder
	b.WriteString("This worker is repairing or inspecting an existing GitHub pull request")
	if strings.TrimSpace(pr.Repo) != "" && pr.Number > 0 {
		b.WriteString(fmt.Sprintf(" %s#%d", pr.Repo, pr.Number))
	}
	if strings.TrimSpace(pr.URL) != "" {
		b.WriteString(" (")
		b.WriteString(strings.TrimSpace(pr.URL))
		b.WriteString(")")
	}
	b.WriteString(". Decide whether a GitHub PR comment is warranted after inspecting the feedback and outcome. If a direct response would help reviewers or explain the result, leave a concise comment on the pull request using the available GitHub tooling. Do not post a noisy comment when the code/check results already speak for themselves. In the final report, state whether you posted a PR comment and summarize its content.")
	return b.String()
}

func normalizePullRequestFollowUpPlan(plan Plan) Plan {
	if len(plan.Spawns) == 0 {
		return ensurePullRequestFollowUpUpdateAction(plan)
	}
	if !planReturnsToPullRequestWatch(plan) {
		return plan
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["suppressedSpawns"] = plan.Spawns
	plan.Metadata["spawnsSuppressedReason"] = "pull_request_followup_returns_to_github_monitor"
	plan.Spawns = nil
	return ensurePullRequestFollowUpUpdateAction(plan)
}

func planReturnsToPullRequestWatch(plan Plan) bool {
	for _, action := range plan.Actions {
		if strings.TrimSpace(action.Kind) == "watch_pull_requests" && strings.TrimSpace(action.When) != "immediate" {
			return true
		}
	}
	return false
}

func ensurePullRequestFollowUpUpdateAction(plan Plan) Plan {
	if !planReturnsToPullRequestWatch(plan) || planHasPullRequestMutation(plan) {
		return plan
	}
	action := PlanAction{
		Kind:   "update_pull_request",
		When:   "after_success",
		Reason: "Apply successful follow-up worker changes to the existing pull request before returning it to GitHub monitoring.",
		Inputs: pullRequestUpdateInputsFromPlan(plan),
	}
	actions := make([]PlanAction, 0, len(plan.Actions)+1)
	inserted := false
	for _, existing := range plan.Actions {
		if !inserted && strings.TrimSpace(existing.Kind) == "watch_pull_requests" && strings.TrimSpace(existing.When) != "immediate" {
			actions = append(actions, action)
			inserted = true
		}
		actions = append(actions, existing)
	}
	if !inserted {
		actions = append(actions, action)
	}
	plan.Actions = actions
	return plan
}

func planHasPullRequestMutation(plan Plan) bool {
	for _, action := range plan.Actions {
		switch strings.TrimSpace(action.Kind) {
		case "publish_pull_request", "update_pull_request":
			if strings.TrimSpace(action.When) != "immediate" {
				return true
			}
		}
	}
	return false
}

func pullRequestUpdateInputsFromPlan(plan Plan) map[string]any {
	inputs := map[string]any{}
	if id := stringMetadata(plan.Metadata, "pullRequestID"); id != "" {
		inputs["id"] = id
	}
	if repo := stringMetadata(plan.Metadata, "pullRequestRepo"); repo != "" {
		inputs["repo"] = repo
	}
	if number := intMetadata(plan.Metadata, "pullRequestNumber"); number > 0 {
		inputs["number"] = number
	}
	if branch := stringMetadata(plan.Metadata, "pullRequestBranch"); branch != "" {
		inputs["branch"] = branch
	}
	if base := stringMetadata(plan.Metadata, "pullRequestBase"); base != "" {
		inputs["base"] = base
	}
	if url := stringMetadata(plan.Metadata, "pullRequestURL"); url != "" {
		inputs["url"] = url
	}
	return inputs
}

func (s *Service) openPullRequestForTask(ctx context.Context, taskID string) (core.PullRequest, bool) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID && !isTerminalPullRequestState(pr.State) {
			return pr, true
		}
	}
	return core.PullRequest{}, false
}

func (s *Service) pullRequestForUpdateAction(ctx context.Context, taskID string, action PlanAction) (core.PullRequest, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	inputs := action.Inputs
	id := stringMetadata(inputs, "id")
	repo := strings.ToLower(strings.TrimSpace(stringMetadata(inputs, "repo")))
	number := intMetadata(inputs, "number")
	url := strings.TrimSpace(stringMetadata(inputs, "url"))
	branch := strings.TrimSpace(stringMetadata(inputs, "branch"))
	hasExplicitTarget := id != "" || url != "" || (repo != "" && number > 0) || branch != ""
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID || !pullRequestMatchesUpdateTarget(pr, id, repo, number, url, branch) {
			continue
		}
		if isTerminalPullRequestState(pr.State) {
			return pr, errTerminalPullRequest
		}
		return pr, nil
	}
	if hasExplicitTarget {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	if pr, ok := latestPullRequestFollowUp(snapshot, taskID); ok {
		if isTerminalPullRequestState(pr.State) {
			return core.PullRequest{}, eventstore.ErrNotFound
		}
		return pr, nil
	}
	if pr, ok := firstOpenPullRequest(snapshot, taskID); ok {
		return pr, nil
	}
	return core.PullRequest{}, eventstore.ErrNotFound
}

func pullRequestMatchesUpdateTarget(pr core.PullRequest, id string, repo string, number int, url string, branch string) bool {
	if id != "" && pr.ID == id {
		return true
	}
	if url != "" && strings.EqualFold(pr.URL, url) {
		return true
	}
	if repo != "" && number > 0 && strings.EqualFold(pr.Repo, repo) && pr.Number == number {
		return true
	}
	return branch != "" && pr.Branch == branch && (repo == "" || strings.EqualFold(pr.Repo, repo))
}

func firstOpenPullRequest(snapshot core.Snapshot, taskID string) (core.PullRequest, bool) {
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID && !isTerminalPullRequestState(pr.State) {
			return pr, true
		}
	}
	return core.PullRequest{}, false
}

func publishPullRequestRequestFromAction(action PlanAction) core.PublishPullRequestRequest {
	inputs := action.Inputs
	return core.PublishPullRequestRequest{
		Title:                stringMetadata(inputs, "title"),
		Body:                 stringMetadata(inputs, "body"),
		Repo:                 stringMetadata(inputs, "repo"),
		Base:                 stringMetadata(inputs, "base"),
		Branch:               stringMetadata(inputs, "branch"),
		Draft:                boolMetadata(inputs, "draft"),
		ContinueAfterPublish: boolMetadata(inputs, "continueAfterPublish"),
	}
}

func updatePullRequestRequestFromAction(action PlanAction) core.PublishPullRequestRequest {
	return publishPullRequestRequestFromAction(action)
}

func watchPullRequestsRequestFromAction(action PlanAction) core.WatchPullRequestsRequest {
	inputs := action.Inputs
	return core.WatchPullRequestsRequest{
		Repo:       stringMetadata(inputs, "repo"),
		Number:     intMetadata(inputs, "number"),
		URL:        stringMetadata(inputs, "url"),
		State:      stringMetadata(inputs, "state"),
		Author:     stringMetadata(inputs, "author"),
		HeadBranch: stringMetadata(inputs, "headBranch"),
		Limit:      intMetadata(inputs, "limit"),
	}
}

func pullRequestTargetRepo(req core.PublishPullRequestRequest, project core.Project, task core.Task) string {
	var metadata map[string]any
	if len(task.Metadata) > 0 {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	return nonEmpty(req.Repo, project.UpstreamRepo, project.Repo, stringMetadataValue(metadata["repo"]))
}

func pullRequestHeadRepoOwner(project core.Project) string {
	if owner := strings.TrimSpace(project.HeadRepoOwner); owner != "" {
		return owner
	}
	if strings.TrimSpace(project.UpstreamRepo) == "" || strings.EqualFold(strings.TrimSpace(project.UpstreamRepo), strings.TrimSpace(project.Repo)) {
		return ""
	}
	return repoOwner(project.Repo)
}
