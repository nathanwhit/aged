package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"

	"aged/internal/core"
)

const (
	replanPromptTotalTokenBudget               = 60000
	replanPromptRecentResultsTokenBudget       = 16000
	replanPromptContextLedgerTokenBudget       = 16000
	replanPromptPullRequestFeedbackTokenBudget = 8000
	replanPromptArtifactsTokenBudget           = 4000
	replanPromptWorkerSummaryTokenBudget       = 2000
	replanPromptRecentResultCount              = 8
	replanPromptTinyArtifactContentBytes       = 800
)

type ReplanPromptBudgeter struct {
	TotalTokens               int
	RecentResultsTokens       int
	ContextLedgerTokens       int
	PullRequestFeedbackTokens int
	ArtifactsTokens           int
	WorkerSummaryTokens       int
	RecentResultCount         int
}

type ReplanPromptState struct {
	InitialPlan                Plan                      `json:"initialPlan"`
	WorkPlan                   *core.WorkPlan            `json:"workPlan,omitempty"`
	RecentResults              []WorkerTurnResult        `json:"recentResults"`
	ContextLedger              []ContextLedgerEntry      `json:"contextLedger,omitempty"`
	Artifacts                  []ReplanPromptArtifact    `json:"artifacts,omitempty"`
	PullRequests               []ReplanPullRequestState  `json:"pullRequests,omitempty"`
	TaskSteering               []string                  `json:"taskSteering,omitempty"`
	PendingPullRequestFeedback []PullRequestFeedbackItem `json:"pendingPullRequestFeedback,omitempty"`
	PendingWorkerSteering      []WorkerSteeringItem      `json:"pendingWorkerSteering,omitempty"`
	Turn                       int                       `json:"turn"`
	RecoveryHint               string                    `json:"recoveryHint,omitempty"`
	PromptBudget               ReplanPromptBudgetSummary `json:"promptBudget"`
}

type ReplanPromptArtifact struct {
	ID       string         `json:"id"`
	Kind     string         `json:"kind"`
	Name     string         `json:"name,omitempty"`
	URL      string         `json:"url,omitempty"`
	Ref      string         `json:"ref,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type ReplanPromptBudgetSummary struct {
	ApproxTokens               int `json:"approxTokens"`
	TotalTokenBudget           int `json:"totalTokenBudget"`
	OriginalResultCount        int `json:"originalResultCount"`
	RecentResultCount          int `json:"recentResultCount"`
	OmittedResultCount         int `json:"omittedResultCount"`
	OriginalArtifactCount      int `json:"originalArtifactCount"`
	ArtifactCount              int `json:"artifactCount"`
	OmittedArtifactCount       int `json:"omittedArtifactCount"`
	OriginalContextLedgerCount int `json:"originalContextLedgerCount"`
	ContextLedgerCount         int `json:"contextLedgerCount"`
	OmittedContextLedgerCount  int `json:"omittedContextLedgerCount"`
	OriginalPRFeedbackCount    int `json:"originalPullRequestFeedbackCount"`
	PullRequestFeedbackCount   int `json:"pullRequestFeedbackCount"`
	OmittedPullRequestFeedback int `json:"omittedPullRequestFeedbackCount"`
}

func DefaultReplanPromptBudgeter() ReplanPromptBudgeter {
	return ReplanPromptBudgeter{
		TotalTokens:               replanPromptTotalTokenBudget,
		RecentResultsTokens:       replanPromptRecentResultsTokenBudget,
		ContextLedgerTokens:       replanPromptContextLedgerTokenBudget,
		PullRequestFeedbackTokens: replanPromptPullRequestFeedbackTokenBudget,
		ArtifactsTokens:           replanPromptArtifactsTokenBudget,
		WorkerSummaryTokens:       replanPromptWorkerSummaryTokenBudget,
		RecentResultCount:         replanPromptRecentResultCount,
	}
}

func (b ReplanPromptBudgeter) PromptPayload(task core.Task, state OrchestrationState) map[string]any {
	payload := map[string]any{
		"task":  replanTaskPromptPayload(task),
		"state": b.BoundState(state),
		"availableWorkers": []map[string]string{
			{"kind": "codex", "description": "Autonomous software engineering worker using Codex CLI headless mode."},
			{"kind": "claude", "description": "Autonomous software engineering worker using Claude Code headless mode."},
			{"kind": "mock", "description": "No-op deterministic worker for smoke tests and scheduler validation."},
		},
	}
	if approxJSONTokens(payload) <= b.TotalTokens {
		return payload
	}
	bounded, _ := payload["state"].(ReplanPromptState)
	bounded = b.degradeToTotalBudget(payload, bounded)
	payload["state"] = bounded
	return payload
}

func (b ReplanPromptBudgeter) BoundState(state OrchestrationState) ReplanPromptState {
	if b.TotalTokens <= 0 {
		b = DefaultReplanPromptBudgeter()
	}
	bounded := ReplanPromptState{
		InitialPlan:                compactPlanForPrompt(state.InitialPlan),
		WorkPlan:                   compactWorkPlanForPrompt(state.WorkPlan),
		RecentResults:              b.compactRecentResults(state.Results),
		ContextLedger:              b.compactContextLedger(state.ContextLedger),
		Artifacts:                  b.compactArtifacts(state.Artifacts),
		PullRequests:               compactPullRequestsForPrompt(state.PullRequests),
		TaskSteering:               compactTaskSteeringForPrompt(state.TaskSteering),
		PendingPullRequestFeedback: b.compactPullRequestFeedback(state.PendingPullRequestFeedback),
		PendingWorkerSteering:      compactWorkerSteeringForPrompt(state.PendingWorkerSteering),
		Turn:                       state.Turn,
		RecoveryHint:               truncateStringForPrompt(state.RecoveryHint, tokensToApproxChars(1000)),
	}
	bounded.PromptBudget = ReplanPromptBudgetSummary{
		ApproxTokens:               approxJSONTokens(bounded),
		TotalTokenBudget:           b.TotalTokens,
		OriginalResultCount:        len(state.Results),
		RecentResultCount:          len(bounded.RecentResults),
		OmittedResultCount:         max(0, len(state.Results)-len(bounded.RecentResults)),
		OriginalArtifactCount:      len(state.Artifacts),
		ArtifactCount:              len(bounded.Artifacts),
		OmittedArtifactCount:       max(0, len(state.Artifacts)-len(bounded.Artifacts)),
		OriginalContextLedgerCount: len(state.ContextLedger),
		ContextLedgerCount:         len(bounded.ContextLedger),
		OmittedContextLedgerCount:  max(0, len(state.ContextLedger)-len(bounded.ContextLedger)),
		OriginalPRFeedbackCount:    len(state.PendingPullRequestFeedback),
		PullRequestFeedbackCount:   len(bounded.PendingPullRequestFeedback),
		OmittedPullRequestFeedback: max(0, len(state.PendingPullRequestFeedback)-len(bounded.PendingPullRequestFeedback)),
	}
	return bounded
}

func (b ReplanPromptBudgeter) compactRecentResults(results []WorkerTurnResult) []WorkerTurnResult {
	if len(results) == 0 {
		return nil
	}
	keep := map[int]bool{}
	start := len(results) - b.RecentResultCount
	if start < 0 {
		start = 0
	}
	for i := start; i < len(results); i++ {
		keep[i] = true
	}
	for i, result := range results {
		if result.Status == core.WorkerFailed || result.Status == core.WorkerCanceled || result.Status == core.WorkerWaiting {
			keep[i] = true
		}
	}
	recent := make([]WorkerTurnResult, 0, len(keep))
	for i, result := range results {
		if !keep[i] {
			continue
		}
		recent = append(recent, b.compactWorkerResult(result))
	}
	recentTokens := newJSONArrayTokenSizer(recent)
	for recentTokens.tokens() > b.RecentResultsTokens {
		dropped := false
		for i, result := range recent {
			if isHighPriorityPromptResult(result) {
				continue
			}
			recent = append(recent[:i], recent[i+1:]...)
			recentTokens.drop(i)
			dropped = true
			break
		}
		if dropped {
			continue
		}
		for i := range recent {
			recent[i].Summary = truncateStringForPrompt(recent[i].Summary, tokensToApproxChars(500))
			recent[i].Error = truncateStringForPrompt(recent[i].Error, tokensToApproxChars(500))
			if len(recent[i].Changes.ChangedFiles) > 8 {
				recent[i].Changes.ChangedFiles = recent[i].Changes.ChangedFiles[:8]
			}
		}
		break
	}
	return recent
}

func (b ReplanPromptBudgeter) compactWorkerResult(result WorkerTurnResult) WorkerTurnResult {
	result.Summary = truncateStringForPrompt(result.Summary, tokensToApproxChars(b.WorkerSummaryTokens))
	result.Error = truncateStringForPrompt(result.Error, tokensToApproxChars(1000))
	result.Changes.Root = ""
	result.Changes.CWD = ""
	result.Changes.WorkspaceName = ""
	result.Changes.Status = truncateStringForPrompt(result.Changes.Status, tokensToApproxChars(200))
	result.Changes.DiffStat = truncateStringForPrompt(result.Changes.DiffStat, tokensToApproxChars(300))
	result.Changes.Diff = ""
	result.Changes.PublishDiff = ""
	result.Changes.Error = truncateStringForPrompt(result.Changes.Error, tokensToApproxChars(1000))
	if len(result.Changes.ChangedFiles) > maxPromptChangedFiles {
		omitted := len(result.Changes.ChangedFiles) - maxPromptChangedFiles
		result.Changes.ChangedFiles = append(append([]WorkspaceChangedFile{}, result.Changes.ChangedFiles[:maxPromptChangedFiles]...), WorkspaceChangedFile{
			Path:   fmt.Sprintf("... %d additional changed files omitted ...", omitted),
			Status: "omitted",
		})
	}
	result.Changes.Artifacts = compactWorkspaceArtifactsForPrompt(result.Changes.Artifacts)
	return result
}

func (b ReplanPromptBudgeter) compactContextLedger(entries []ContextLedgerEntry) []ContextLedgerEntry {
	compact := compactContextLedgerForPrompt(entries)
	compactTokens := newJSONArrayTokenSizer(compact)
	for compactTokens.tokens() > b.ContextLedgerTokens && len(compact) > 0 {
		dropIndex := -1
		for i, entry := range compact {
			if entry.Error == "" && !strings.Contains(entry.Kind, "candidate") {
				dropIndex = i
				break
			}
		}
		if dropIndex < 0 {
			dropIndex = 0
		}
		compact = append(compact[:dropIndex], compact[dropIndex+1:]...)
		compactTokens.drop(dropIndex)
	}
	return compact
}

func (b ReplanPromptBudgeter) compactPullRequestFeedback(items []PullRequestFeedbackItem) []PullRequestFeedbackItem {
	compact := append([]PullRequestFeedbackItem{}, items...)
	for i := range compact {
		compact[i].Prompt = truncateStringForPrompt(compact[i].Prompt, tokensToApproxChars(1000))
	}
	compactTokens := newJSONArrayTokenSizer(compact)
	for compactTokens.tokens() > b.PullRequestFeedbackTokens && len(compact) > 0 {
		compact[0].Prompt = truncateStringForPrompt(compact[0].Prompt, tokensToApproxChars(250))
		compactTokens.update(0, compact[0])
		if compactTokens.tokens() > b.PullRequestFeedbackTokens {
			compact = compact[1:]
			compactTokens.drop(0)
		}
	}
	return compact
}

func (b ReplanPromptBudgeter) compactArtifacts(artifacts []core.TaskArtifact) []ReplanPromptArtifact {
	compact := make([]ReplanPromptArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		item := ReplanPromptArtifact{
			ID:       artifact.ID,
			Kind:     artifact.Kind,
			Name:     truncateStringForPrompt(artifact.Name, tokensToApproxChars(100)),
			URL:      artifact.URL,
			Ref:      artifact.Ref,
			Metadata: compactArtifactMetadataForPrompt(artifact.Metadata),
		}
		compact = append(compact, item)
	}
	compactTokens := newJSONArrayTokenSizer(compact)
	for compactTokens.tokens() > b.ArtifactsTokens && len(compact) > 0 {
		dropIndex := 0
		for i, artifact := range compact {
			if artifact.Kind != "worker_result_digest" {
				dropIndex = i
				break
			}
		}
		compact = append(compact[:dropIndex], compact[dropIndex+1:]...)
		compactTokens.drop(dropIndex)
	}
	return compact
}

func (b ReplanPromptBudgeter) degradeToTotalBudget(payload map[string]any, state ReplanPromptState) ReplanPromptState {
	currentTokens := approxJSONTokens(payload)
	for currentTokens > b.TotalTokens {
		switch {
		case len(state.Artifacts) > 0:
			state.Artifacts = state.Artifacts[1:]
		case len(state.ContextLedger) > 0:
			state.ContextLedger = state.ContextLedger[1:]
		case len(state.RecentResults) > 1:
			state.RecentResults = state.RecentResults[1:]
		default:
			for i := range state.RecentResults {
				state.RecentResults[i].Summary = truncateStringForPrompt(state.RecentResults[i].Summary, tokensToApproxChars(250))
				state.RecentResults[i].Error = truncateStringForPrompt(state.RecentResults[i].Error, tokensToApproxChars(250))
				state.RecentResults[i].Changes.ChangedFiles = nil
			}
			state.RecoveryHint = truncateStringForPrompt(state.RecoveryHint, tokensToApproxChars(250))
			payload["state"] = state
			state.PromptBudget.ApproxTokens = approxJSONTokens(payload)
			return state
		}
		state.PromptBudget.RecentResultCount = len(state.RecentResults)
		state.PromptBudget.ArtifactCount = len(state.Artifacts)
		state.PromptBudget.ContextLedgerCount = len(state.ContextLedger)
		payload["state"] = state
		currentTokens = approxJSONTokens(payload)
		state.PromptBudget.ApproxTokens = currentTokens
	}
	state.PromptBudget.ApproxTokens = currentTokens
	return state
}

func compactWorkerSteeringForPrompt(items []WorkerSteeringItem) []WorkerSteeringItem {
	compact := append([]WorkerSteeringItem{}, items...)
	for i := range compact {
		compact[i].Message = truncateStringForPrompt(compact[i].Message, tokensToApproxChars(1000))
	}
	return compact
}

func compactTaskSteeringForPrompt(items []string) []string {
	compact := dedupeTrimmedStrings(items)
	if len(compact) > 20 {
		compact = compact[len(compact)-20:]
	}
	for i := range compact {
		compact[i] = truncateStringForPrompt(compact[i], tokensToApproxChars(1000))
	}
	compactTokens := newJSONArrayTokenSizer(compact)
	for compactTokens.tokens() > 4000 && len(compact) > 1 {
		compact = compact[1:]
		compactTokens.drop(0)
	}
	return compact
}

func compactPullRequestsForPrompt(items []ReplanPullRequestState) []ReplanPullRequestState {
	compact := append([]ReplanPullRequestState{}, items...)
	for i := range compact {
		compact[i].Title = truncateStringForPrompt(compact[i].Title, tokensToApproxChars(200))
	}
	return compact
}

func compactWorkspaceArtifactsForPrompt(artifacts []WorkspaceArtifact) []WorkspaceArtifact {
	if len(artifacts) == 0 {
		return nil
	}
	limit := maxPromptArtifacts
	if len(artifacts) < limit {
		limit = len(artifacts)
	}
	compact := make([]WorkspaceArtifact, 0, limit)
	for _, artifact := range artifacts[:limit] {
		item := artifact
		item.Content = tinyArtifactContent(item.Content)
		item.Metadata = compactMetadataMapForPrompt(item.Metadata)
		compact = append(compact, item)
	}
	return compact
}

func compactArtifactMetadataForPrompt(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return map[string]any{"decodeError": err.Error()}
	}
	return compactMetadataMapForPrompt(metadata)
}

func compactMetadataMapForPrompt(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range metadata {
		switch typed := value.(type) {
		case string:
			if key == "content" {
				if len(typed) <= replanPromptTinyArtifactContentBytes {
					out["content"] = typed
				} else {
					out["contentOmittedBytes"] = len(typed)
					out["contentPreview"] = truncateStringForPrompt(typed, replanPromptTinyArtifactContentBytes)
				}
				continue
			}
			out[key] = truncateStringForPrompt(typed, tokensToApproxChars(300))
		default:
			out[key] = typed
		}
	}
	return out
}

func tinyArtifactContent(content string) string {
	if len(content) <= replanPromptTinyArtifactContentBytes {
		return content
	}
	return ""
}

func isHighPriorityPromptResult(result WorkerTurnResult) bool {
	return result.Status == core.WorkerFailed || result.Status == core.WorkerCanceled || result.Status == core.WorkerWaiting || resultHasCandidateChanges(result)
}

func approxJSONTokens(value any) int {
	data, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return approxTokensForString(string(data))
}

type jsonArrayTokenSizer[T any] struct {
	nilSlice  bool
	itemBytes []int
}

func newJSONArrayTokenSizer[T any](items []T) *jsonArrayTokenSizer[T] {
	sizer := &jsonArrayTokenSizer[T]{
		nilSlice:  items == nil,
		itemBytes: make([]int, 0, len(items)),
	}
	for _, item := range items {
		sizer.itemBytes = append(sizer.itemBytes, jsonEncodedBytes(item))
	}
	return sizer
}

func (s *jsonArrayTokenSizer[T]) tokens() int {
	return approxTokensForBytes(s.bytes())
}

func (s *jsonArrayTokenSizer[T]) bytes() int {
	if s == nil || s.nilSlice {
		return len("null")
	}
	if len(s.itemBytes) == 0 {
		return len("[]")
	}
	total := len("[]") + len(s.itemBytes) - 1
	for _, bytes := range s.itemBytes {
		total += bytes
	}
	return total
}

func (s *jsonArrayTokenSizer[T]) drop(index int) {
	if s == nil || index < 0 || index >= len(s.itemBytes) {
		return
	}
	s.nilSlice = false
	s.itemBytes = append(s.itemBytes[:index], s.itemBytes[index+1:]...)
}

func (s *jsonArrayTokenSizer[T]) update(index int, item T) {
	if s == nil || index < 0 || index >= len(s.itemBytes) {
		return
	}
	s.nilSlice = false
	s.itemBytes[index] = jsonEncodedBytes(item)
}

func jsonEncodedBytes(value any) int {
	data, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(data)
}

func approxTokensForBytes(bytes int) int {
	if bytes <= 0 {
		return 0
	}
	return (bytes + 3) / 4
}

func approxTokensForString(value string) int {
	if value == "" {
		return 0
	}
	return approxTokensForBytes(len(value))
}

func tokensToApproxChars(tokens int) int {
	if tokens <= 0 {
		return 0
	}
	return tokens * 4
}
