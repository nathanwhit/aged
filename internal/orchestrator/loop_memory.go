package orchestrator

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"aged/internal/core"
)

const (
	loopTaskMemoryVersion        = 1
	defaultLoopMemoryEvery       = 10
	maxLoopMemoryObjectiveBytes  = 1200
	maxLoopMemoryTextBytes       = 700
	maxLoopMemoryArtifactBytes   = 500
	maxLoopMemoryDecisions       = 8
	maxLoopMemoryBlockers        = 6
	maxLoopMemoryThemes          = 5
	maxLoopMemoryTerminalPRs     = 5
	maxLoopTaskMemoryJSONBytes   = 6 * 1024
	loopMemoryStatusIterationErr = "iteration_"
)

func (s *Service) maybeRefreshLoopTaskMemory(ctx context.Context, task core.Task, iteration int, latestStatus string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	latestTask, ok := findTask(snapshot, task.ID)
	if ok {
		task = latestTask
	}
	events, err := s.store.ListTaskLedgerEvents(ctx, task.ID)
	if err != nil {
		return err
	}
	latestEventID := latestTaskEventID(events)
	if task.Memory != nil && latestEventID <= task.Memory.Coverage.EventCutoffID {
		return nil
	}
	if !shouldRefreshLoopTaskMemory(task, events, iteration, latestStatus) {
		return nil
	}
	memory := synthesizeLoopTaskMemory(task, snapshot, events, time.Now().UTC())
	_, err = s.append(ctx, core.Event{
		Type:    core.EventTaskMemoryUpdated,
		TaskID:  task.ID,
		Payload: core.MustJSON(memory),
	})
	return err
}

func shouldRefreshLoopTaskMemory(task core.Task, events []core.Event, iteration int, latestStatus string) bool {
	latestEventID := latestTaskEventID(events)
	if task.Memory != nil && latestEventID <= task.Memory.Coverage.EventCutoffID {
		return false
	}
	if task.Memory == nil {
		return true
	}
	if loopMemoryLatestStatusIsImportant(latestStatus) {
		return true
	}
	if loopMemoryHasImportantEventSince(events, task.Memory.Coverage.EventCutoffID) {
		return true
	}
	every := loopMemoryEvery(task)
	return every > 0 && iteration > 0 && iteration%every == 0
}

func loopMemoryEvery(task core.Task) int {
	metadata := taskMetadataMap(task)
	if _, ok := metadata["loopMemoryEvery"]; ok {
		return intMetadata(metadata, "loopMemoryEvery")
	}
	return defaultLoopMemoryEvery
}

func loopMemoryLatestStatusIsImportant(status string) bool {
	status = strings.TrimSpace(status)
	if status == "" || status == "iteration_completed" {
		return false
	}
	return status == "waiting_for_input" || strings.HasPrefix(status, loopMemoryStatusIterationErr)
}

func loopMemoryHasImportantEventSince(events []core.Event, cutoff int64) bool {
	for _, event := range events {
		if event.ID <= cutoff {
			continue
		}
		switch event.Type {
		case core.EventApprovalNeeded, core.EventPRPublished, core.EventPRUpdated, core.EventPRStatusChecked, core.EventTaskArtifact:
			return true
		case core.EventTaskMilestone:
			var payload struct {
				Name string `json:"name"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil && strings.HasPrefix(payload.Name, "pr_") {
				return true
			}
		}
	}
	return false
}

func latestTaskEventID(events []core.Event) int64 {
	var latest int64
	for _, event := range events {
		if event.ID > latest {
			latest = event.ID
		}
	}
	return latest
}

func synthesizeLoopTaskMemory(task core.Task, snapshot core.Snapshot, events []core.Event, updatedAt time.Time) core.TaskMemory {
	fromIteration, throughIteration := loopMemoryIterationCoverage(events)
	decisions := map[string]core.TaskMemoryNote{}
	blockers := map[string]core.TaskMemoryNote{}
	themes := map[string]core.TaskMemoryNote{}
	artifactIterations := loopMemoryArtifactIterations(events)
	var currentIteration int
	for _, event := range events {
		if event.TaskID != task.ID {
			continue
		}
		switch event.Type {
		case core.EventTaskAction:
			var payload struct {
				Kind      string `json:"kind"`
				Status    string `json:"status"`
				Reason    string `json:"reason"`
				Summary   string `json:"summary"`
				Error     string `json:"error"`
				Iteration int    `json:"iteration"`
			}
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			if payload.Iteration > 0 {
				currentIteration = payload.Iteration
			}
			text := nonEmpty(payload.Reason, payload.Summary, payload.Error)
			switch {
			case payload.Kind == loopActionKind && payload.Status == "iteration_completed" && strings.TrimSpace(payload.Summary) != "":
				addLoopMemoryTheme(themes, payload.Summary, payload.Iteration)
				if highValueLedgerText(payload.Summary) {
					addLoopMemoryNote(decisions, payload.Summary, payload.Iteration)
				}
			case loopMemoryTaskActionIsBlocker(payload.Status):
				addLoopMemoryNote(blockers, text, payload.Iteration)
			case highValueLedgerText(text):
				addLoopMemoryNote(decisions, text, payload.Iteration)
			}
		case core.EventTaskMilestone:
			var payload struct {
				Name    string `json:"name"`
				Phase   string `json:"phase"`
				Summary string `json:"summary"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil {
				addLoopMemoryNote(decisions, nonEmpty(payload.Summary, payload.Name), currentIteration)
				if strings.HasPrefix(payload.Name, "pr_") && (payload.Phase == "pr_needs_work" || payload.Phase == "pr_closed") {
					addLoopMemoryNote(blockers, nonEmpty(payload.Summary, payload.Name), currentIteration)
				}
			}
		case core.EventApprovalNeeded:
			var payload struct {
				Reason   string `json:"reason"`
				Question string `json:"question"`
				Summary  string `json:"summary"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil {
				addLoopMemoryNote(blockers, nonEmpty(payload.Question, payload.Summary, payload.Reason), currentIteration)
			}
		case core.EventTaskReplanned:
			var payload struct {
				Error    string         `json:"error"`
				Decision ReplanDecision `json:"decision"`
			}
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			text := nonEmpty(payload.Decision.Message, payload.Decision.Rationale, payload.Error)
			if payload.Decision.Action == "wait" || payload.Decision.Action == "fail" {
				addLoopMemoryNote(blockers, text, currentIteration)
			} else if highValueLedgerText(text) {
				addLoopMemoryNote(decisions, text, currentIteration)
			}
		}
	}
	memory := core.TaskMemory{
		Version:   loopTaskMemoryVersion,
		UpdatedAt: updatedAt,
		Coverage: core.TaskMemoryCoverage{
			FromIteration:    fromIteration,
			ThroughIteration: throughIteration,
			EventCutoffID:    latestTaskEventID(events),
		},
		Objective: loopMemoryObjective(task),
		Decisions: sortedLoopMemoryNotes(decisions, maxLoopMemoryDecisions, false),
		Blockers:  sortedLoopMemoryNotes(blockers, maxLoopMemoryBlockers, false),
		Themes:    sortedLoopMemoryNotes(themes, maxLoopMemoryThemes, true),
		Artifacts: loopMemoryArtifacts(task, snapshot, artifactIterations),
	}
	return compactLoopTaskMemory(memory)
}

func loopMemoryTaskActionIsBlocker(status string) bool {
	return status == "waiting_for_input" || status == "waiting" || status == "failed" || (status != "iteration_completed" && strings.HasPrefix(status, loopMemoryStatusIterationErr))
}

func loopMemoryObjective(task core.Task) string {
	metadata := taskMetadataMap(task)
	return nonEmpty(stringMetadataValue(metadata["loopPrompt"]), task.Prompt)
}

func loopMemoryIterationCoverage(events []core.Event) (int, int) {
	from := 0
	through := 0
	for _, event := range events {
		if event.Type != core.EventTaskAction {
			continue
		}
		var payload struct {
			Kind      string `json:"kind"`
			Iteration int    `json:"iteration"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil || payload.Kind != loopActionKind || payload.Iteration <= 0 {
			continue
		}
		if from == 0 || payload.Iteration < from {
			from = payload.Iteration
		}
		if payload.Iteration > through {
			through = payload.Iteration
		}
	}
	return from, through
}

func addLoopMemoryTheme(notes map[string]core.TaskMemoryNote, text string, iteration int) {
	text = loopMemoryFirstSentence(text)
	key := loopMemoryThemeKey(text)
	if key == "" {
		return
	}
	addLoopMemoryNoteWithKey(notes, key, text, iteration)
}

func addLoopMemoryNote(notes map[string]core.TaskMemoryNote, text string, iteration int) {
	key := loopMemoryNoteKey(text)
	if key == "" {
		return
	}
	addLoopMemoryNoteWithKey(notes, key, text, iteration)
}

func addLoopMemoryNoteWithKey(notes map[string]core.TaskMemoryNote, key string, text string, iteration int) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	note := notes[key]
	if note.Count == 0 {
		note = core.TaskMemoryNote{Text: text, FirstSeenIteration: iteration, LastSeenIteration: iteration, Count: 1}
	} else {
		note.Count++
		note.Text = text
		if note.FirstSeenIteration == 0 || (iteration > 0 && iteration < note.FirstSeenIteration) {
			note.FirstSeenIteration = iteration
		}
		if iteration > note.LastSeenIteration {
			note.LastSeenIteration = iteration
		}
	}
	notes[key] = note
}

func sortedLoopMemoryNotes(notes map[string]core.TaskMemoryNote, limit int, byCount bool) []core.TaskMemoryNote {
	out := make([]core.TaskMemoryNote, 0, len(notes))
	for _, note := range notes {
		out = append(out, note)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if byCount && out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].LastSeenIteration != out[j].LastSeenIteration {
			return out[i].LastSeenIteration > out[j].LastSeenIteration
		}
		return out[i].Text < out[j].Text
	})
	if len(out) > limit {
		out = out[:limit]
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FirstSeenIteration != out[j].FirstSeenIteration {
			return out[i].FirstSeenIteration < out[j].FirstSeenIteration
		}
		return out[i].Text < out[j].Text
	})
	return out
}

func loopMemoryFirstSentence(text string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	if text == "" {
		return ""
	}
	for _, separator := range []string{". ", "\n", "; "} {
		if index := strings.Index(text, separator); index > 0 {
			text = text[:index+1]
			break
		}
	}
	return truncateStringForPrompt(text, 240)
}

func loopMemoryNoteKey(text string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(text)), " "))
}

func loopMemoryThemeKey(text string) string {
	key := loopMemoryNoteKey(text)
	if len(key) > 60 {
		key = key[:60]
	}
	return key
}

func loopMemoryArtifactIterations(events []core.Event) map[string]int {
	iterations := map[string]int{}
	var currentIteration int
	for _, event := range events {
		switch event.Type {
		case core.EventTaskAction:
			var payload struct {
				Kind      string `json:"kind"`
				Iteration int    `json:"iteration"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Kind == loopActionKind && payload.Iteration > 0 {
				currentIteration = payload.Iteration
			}
		case core.EventPRPublished, core.EventPRUpdated, core.EventPRStatusChecked:
			var payload struct {
				ID     string `json:"id"`
				Repo   string `json:"repo"`
				Number int    `json:"number"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil {
				id := nonEmpty(payload.ID, pullRequestFallbackID(payload.Repo, payload.Number))
				if id != "" && iterations[id] == 0 {
					iterations[id] = currentIteration
				}
			}
		case core.EventTaskArtifact:
			var payload struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil && payload.ID != "" && iterations[payload.ID] == 0 {
				iterations[payload.ID] = currentIteration
			}
		}
	}
	return iterations
}

func pullRequestFallbackID(repo string, number int) string {
	if strings.TrimSpace(repo) == "" || number == 0 {
		return ""
	}
	return strings.TrimSpace(repo) + "#" + strconv.Itoa(number)
}

func loopMemoryArtifacts(task core.Task, snapshot core.Snapshot, iterations map[string]int) []core.TaskMemoryArtifact {
	artifacts := map[string]core.TaskMemoryArtifact{}
	for _, artifact := range task.Artifacts {
		if artifact.ID == "" {
			continue
		}
		state, checks, merge := loopMemoryArtifactMetadata(artifact.Metadata)
		artifacts[artifact.ID] = core.TaskMemoryArtifact{
			ID:                   artifact.ID,
			Kind:                 artifact.Kind,
			URL:                  artifact.URL,
			Title:                artifact.Name,
			State:                state,
			ChecksConclusion:     checks,
			MergeStatus:          merge,
			PublishedAtIteration: iterations[artifact.ID],
		}
	}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != task.ID || pr.ID == "" {
			continue
		}
		artifacts[pr.ID] = core.TaskMemoryArtifact{
			ID:                   pr.ID,
			Kind:                 "github_pull_request",
			URL:                  pr.URL,
			Title:                pr.Title,
			State:                pr.State,
			ChecksConclusion:     nonEmpty(pr.ChecksConclusion, pr.ChecksStatus),
			MergeStatus:          nonEmpty(pr.MergeStatus, pr.Mergeable),
			PublishedAtIteration: iterations[pr.ID],
		}
	}
	out := make([]core.TaskMemoryArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		out = append(out, artifact)
	}
	sort.SliceStable(out, func(i, j int) bool {
		iOpen := loopMemoryArtifactIsOpen(out[i])
		jOpen := loopMemoryArtifactIsOpen(out[j])
		if iOpen != jOpen {
			return iOpen
		}
		if out[i].PublishedAtIteration != out[j].PublishedAtIteration {
			return out[i].PublishedAtIteration > out[j].PublishedAtIteration
		}
		return out[i].ID < out[j].ID
	})
	var terminal int
	filtered := out[:0]
	for _, artifact := range out {
		if loopMemoryArtifactIsOpen(artifact) {
			filtered = append(filtered, artifact)
			continue
		}
		if terminal < maxLoopMemoryTerminalPRs {
			filtered = append(filtered, artifact)
			terminal++
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].PublishedAtIteration != filtered[j].PublishedAtIteration {
			return filtered[i].PublishedAtIteration < filtered[j].PublishedAtIteration
		}
		return filtered[i].ID < filtered[j].ID
	})
	return filtered
}

func loopMemoryArtifactMetadata(raw json.RawMessage) (string, string, string) {
	var metadata struct {
		State            string `json:"state"`
		ChecksStatus     string `json:"checksStatus"`
		ChecksConclusion string `json:"checksConclusion"`
		MergeStatus      string `json:"mergeStatus"`
		Mergeable        string `json:"mergeable"`
	}
	_ = json.Unmarshal(raw, &metadata)
	return metadata.State, nonEmpty(metadata.ChecksConclusion, metadata.ChecksStatus), nonEmpty(metadata.MergeStatus, metadata.Mergeable)
}

func loopMemoryArtifactIsOpen(artifact core.TaskMemoryArtifact) bool {
	state := strings.ToLower(strings.TrimSpace(artifact.State))
	return state == "" || state == "open"
}

func compactLoopTaskMemory(memory core.TaskMemory) core.TaskMemory {
	memory.Objective = truncateStringForPrompt(strings.TrimSpace(memory.Objective), maxLoopMemoryObjectiveBytes)
	memory.Decisions = compactLoopMemoryNotes(memory.Decisions, maxLoopMemoryDecisions, maxLoopMemoryTextBytes)
	memory.Blockers = compactLoopMemoryNotes(memory.Blockers, maxLoopMemoryBlockers, maxLoopMemoryTextBytes)
	memory.Themes = compactLoopMemoryNotes(memory.Themes, maxLoopMemoryThemes, maxLoopMemoryTextBytes)
	memory.Artifacts = compactLoopMemoryArtifacts(memory.Artifacts)
	if loopTaskMemoryJSONSize(memory) <= maxLoopTaskMemoryJSONBytes {
		return memory
	}
	memory.Decisions = compactLoopMemoryNotes(memory.Decisions, maxLoopMemoryDecisions, maxLoopMemoryTextBytes/2)
	memory.Blockers = compactLoopMemoryNotes(memory.Blockers, maxLoopMemoryBlockers, maxLoopMemoryTextBytes/2)
	memory.Themes = compactLoopMemoryNotes(memory.Themes, maxLoopMemoryThemes, maxLoopMemoryTextBytes/2)
	for loopTaskMemoryJSONSize(memory) > maxLoopTaskMemoryJSONBytes && len(memory.Themes) > 0 {
		memory.Themes = memory.Themes[:len(memory.Themes)-1]
	}
	for loopTaskMemoryJSONSize(memory) > maxLoopTaskMemoryJSONBytes && len(memory.Decisions) > 0 {
		memory.Decisions = memory.Decisions[:len(memory.Decisions)-1]
	}
	for loopTaskMemoryJSONSize(memory) > maxLoopTaskMemoryJSONBytes && len(memory.Blockers) > 0 {
		memory.Blockers = memory.Blockers[:len(memory.Blockers)-1]
	}
	for loopTaskMemoryJSONSize(memory) > maxLoopTaskMemoryJSONBytes && len(memory.Artifacts) > 0 {
		memory.Artifacts = memory.Artifacts[:len(memory.Artifacts)-1]
	}
	return memory
}

func compactLoopMemoryNotes(notes []core.TaskMemoryNote, limit int, textBytes int) []core.TaskMemoryNote {
	if len(notes) > limit {
		notes = notes[:limit]
	}
	out := make([]core.TaskMemoryNote, 0, len(notes))
	for _, note := range notes {
		note.Text = truncateStringForPrompt(strings.TrimSpace(note.Text), textBytes)
		if note.Text != "" {
			out = append(out, note)
		}
	}
	return out
}

func compactLoopMemoryArtifacts(artifacts []core.TaskMemoryArtifact) []core.TaskMemoryArtifact {
	out := make([]core.TaskMemoryArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		artifact.ID = truncateStringForPrompt(strings.TrimSpace(artifact.ID), maxLoopMemoryArtifactBytes)
		artifact.Kind = truncateStringForPrompt(strings.TrimSpace(artifact.Kind), maxLoopMemoryArtifactBytes)
		artifact.URL = truncateStringForPrompt(strings.TrimSpace(artifact.URL), maxLoopMemoryArtifactBytes)
		artifact.Title = truncateStringForPrompt(strings.TrimSpace(artifact.Title), maxLoopMemoryArtifactBytes)
		artifact.State = truncateStringForPrompt(strings.TrimSpace(artifact.State), 80)
		artifact.ChecksConclusion = truncateStringForPrompt(strings.TrimSpace(artifact.ChecksConclusion), 80)
		artifact.MergeStatus = truncateStringForPrompt(strings.TrimSpace(artifact.MergeStatus), 80)
		if artifact.ID != "" {
			out = append(out, artifact)
		}
	}
	return out
}

func loopTaskMemoryJSONSize(memory core.TaskMemory) int {
	data, err := json.Marshal(memory)
	if err != nil {
		return maxLoopTaskMemoryJSONBytes + 1
	}
	return len(data)
}

func renderTaskMemoryForWorkerPrompt(memory *core.TaskMemory) string {
	if memory == nil {
		return ""
	}
	memoryValue := compactLoopTaskMemory(*memory)
	var builder strings.Builder
	builder.WriteString("# Task Memory\n\n")
	builder.WriteString("Long-horizon summary synthesized from earlier durable-loop events. Treat this as compact task-local memory.\n")
	if memoryValue.Objective != "" {
		builder.WriteString("\nObjective: ")
		builder.WriteString(memoryValue.Objective)
		builder.WriteString("\n")
	}
	if len(memoryValue.Decisions) > 0 {
		builder.WriteString("\nDecisions:\n")
		for _, note := range memoryValue.Decisions {
			writeLoopMemoryNote(&builder, note)
		}
	}
	if len(memoryValue.Blockers) > 0 {
		builder.WriteString("\nOpen Blockers / Risks:\n")
		for _, note := range memoryValue.Blockers {
			writeLoopMemoryNote(&builder, note)
		}
	}
	if len(memoryValue.Themes) > 0 {
		builder.WriteString("\nRecurring Themes:\n")
		for _, note := range memoryValue.Themes {
			writeLoopMemoryNote(&builder, note)
		}
	}
	if len(memoryValue.Artifacts) > 0 {
		builder.WriteString("\nArtifacts:\n")
		for _, artifact := range memoryValue.Artifacts {
			builder.WriteString("- ")
			builder.WriteString(nonEmpty(artifact.Title, artifact.ID))
			if artifact.URL != "" {
				builder.WriteString(" ")
				builder.WriteString(artifact.URL)
			}
			details := []string{}
			if artifact.State != "" {
				details = append(details, "state="+artifact.State)
			}
			if artifact.ChecksConclusion != "" {
				details = append(details, "checks="+artifact.ChecksConclusion)
			}
			if artifact.MergeStatus != "" {
				details = append(details, "merge="+artifact.MergeStatus)
			}
			if artifact.PublishedAtIteration > 0 {
				details = append(details, "iter="+strconv.Itoa(artifact.PublishedAtIteration))
			}
			if len(details) > 0 {
				builder.WriteString(" (")
				builder.WriteString(strings.Join(details, ", "))
				builder.WriteString(")")
			}
			builder.WriteString("\n")
		}
	}
	builder.WriteString("\n")
	return builder.String()
}

func writeLoopMemoryNote(builder *strings.Builder, note core.TaskMemoryNote) {
	builder.WriteString("- ")
	builder.WriteString(note.Text)
	annotations := []string{}
	if note.FirstSeenIteration > 0 && note.LastSeenIteration > 0 {
		if note.FirstSeenIteration == note.LastSeenIteration {
			annotations = append(annotations, "iter "+strconv.Itoa(note.LastSeenIteration))
		} else {
			annotations = append(annotations, "iter "+strconv.Itoa(note.FirstSeenIteration)+"-"+strconv.Itoa(note.LastSeenIteration))
		}
	} else if note.LastSeenIteration > 0 {
		annotations = append(annotations, "iter "+strconv.Itoa(note.LastSeenIteration))
	} else if note.FirstSeenIteration > 0 {
		annotations = append(annotations, "iter "+strconv.Itoa(note.FirstSeenIteration))
	}
	if note.Count > 1 {
		annotations = append(annotations, "count "+strconv.Itoa(note.Count))
	}
	if len(annotations) > 0 {
		builder.WriteString(" (")
		builder.WriteString(strings.Join(annotations, ", "))
		builder.WriteString(")")
	}
	builder.WriteString("\n")
}
