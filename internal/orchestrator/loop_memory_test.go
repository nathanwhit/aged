package orchestrator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
)

func TestSynthesizeLoopTaskMemoryRetainsLongHorizonSignals(t *testing.T) {
	task := core.Task{
		ID:     "task-loop",
		Prompt: "Keep improving the durable loop.",
		Artifacts: []core.TaskArtifact{{
			ID:   "pr-artifact",
			Kind: "github_pull_request",
			Name: "Artifact PR",
			URL:  "https://github.com/acme/repo/pull/99",
			Metadata: core.MustJSON(map[string]any{
				"state":            "open",
				"checksConclusion": "success",
				"mergeStatus":      "CLEAN",
			}),
		}},
	}
	events := []core.Event{}
	var id int64
	for iteration := 1; iteration <= 50; iteration++ {
		id++
		events = append(events, loopMemoryTestActionEvent(id, task.ID, iteration, "iteration_completed", "Refined retry handling. detail varies per turn."))
	}
	id++
	events = append(events, loopMemoryTestActionEvent(id, task.ID, 51, "iteration_completed", "decision: keep memory synthesis deterministic"))
	id++
	events = append(events, core.Event{
		ID:     id,
		Type:   core.EventApprovalNeeded,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"question": "Need user credentials before refreshing external status.",
			"reason":   "blocked on credentials",
		}),
	})
	id++
	events = append(events, core.Event{
		ID:     id,
		Type:   core.EventPRPublished,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-open",
			"repo":   "acme/repo",
			"number": 42,
			"url":    "https://github.com/acme/repo/pull/42",
			"title":  "Open durable-loop PR",
			"state":  "open",
		}),
	})
	prs := []core.PullRequest{{
		ID:               "pr-open",
		TaskID:           task.ID,
		URL:              "https://github.com/acme/repo/pull/42",
		Title:            "Open durable-loop PR",
		State:            "open",
		ChecksConclusion: "success",
		MergeStatus:      "CLEAN",
	}}
	for i := 0; i < maxLoopMemoryTerminalPRs+2; i++ {
		prs = append(prs, core.PullRequest{
			ID:     "pr-closed-" + string(rune('a'+i)),
			TaskID: task.ID,
			URL:    "https://github.com/acme/repo/pull/closed",
			Title:  "Closed PR",
			State:  "closed",
		})
	}
	memory := synthesizeLoopTaskMemory(task, core.Snapshot{PullRequests: prs}, events, time.Unix(10, 0).UTC())

	if memory.Coverage.FromIteration != 1 || memory.Coverage.ThroughIteration != 51 || memory.Coverage.EventCutoffID != id {
		t.Fatalf("coverage = %+v", memory.Coverage)
	}
	if len(memory.Themes) == 0 || memory.Themes[0].Count != 50 || !strings.Contains(memory.Themes[0].Text, "Refined retry handling") {
		t.Fatalf("themes did not retain repeated summary: %+v", memory.Themes)
	}
	if !loopMemoryTestNotesContain(memory.Decisions, "decision: keep memory synthesis deterministic") {
		t.Fatalf("decisions missing important action summary: %+v", memory.Decisions)
	}
	if !loopMemoryTestNotesContain(memory.Blockers, "Need user credentials") {
		t.Fatalf("blockers missing approval-needed reason: %+v", memory.Blockers)
	}
	if !loopMemoryTestArtifactsContain(memory.Artifacts, "pr-open") || !loopMemoryTestArtifactsContain(memory.Artifacts, "pr-artifact") {
		t.Fatalf("artifacts missing open PR/artifact state: %+v", memory.Artifacts)
	}
	terminal := 0
	for _, artifact := range memory.Artifacts {
		if strings.EqualFold(artifact.State, "closed") {
			terminal++
		}
	}
	if terminal > maxLoopMemoryTerminalPRs {
		t.Fatalf("terminal artifacts = %d, want <= %d: %+v", terminal, maxLoopMemoryTerminalPRs, memory.Artifacts)
	}
}

func TestLoopTaskMemoryRefreshTriggerSkipsUnchangedCutoff(t *testing.T) {
	task := core.Task{
		ID: "task-loop",
		Memory: &core.TaskMemory{Coverage: core.TaskMemoryCoverage{
			EventCutoffID: 2,
		}},
	}
	events := []core.Event{
		{ID: 1, Type: core.EventTaskAction, TaskID: task.ID, Payload: core.MustJSON(map[string]any{"kind": loopActionKind, "iteration": 1, "status": "iteration_completed"})},
		{ID: 2, Type: core.EventTaskAction, TaskID: task.ID, Payload: core.MustJSON(map[string]any{"kind": loopActionKind, "iteration": 2, "status": "iteration_completed"})},
	}
	if shouldRefreshLoopTaskMemory(task, events, 10, "iteration_completed") {
		t.Fatalf("refresh should skip when event cutoff is unchanged")
	}
	events = append(events, core.Event{ID: 3, Type: core.EventTaskAction, TaskID: task.ID, Payload: core.MustJSON(map[string]any{"kind": loopActionKind, "iteration": 10, "status": "iteration_completed"})})
	if !shouldRefreshLoopTaskMemory(task, events, 10, "iteration_completed") {
		t.Fatalf("refresh should run on cadence when new events exist")
	}
	if shouldRefreshLoopTaskMemory(task, events, 11, "iteration_completed") {
		t.Fatalf("refresh should skip routine non-cadence iterations")
	}
	events[2] = core.Event{ID: 3, Type: core.EventApprovalNeeded, TaskID: task.ID, Payload: core.MustJSON(map[string]any{"question": "Need input"})}
	if !shouldRefreshLoopTaskMemory(task, events, 11, "iteration_completed") {
		t.Fatalf("refresh should run promptly for approval-needed events")
	}
}

func TestSynthesizeLoopTaskMemoryBoundsPayload(t *testing.T) {
	task := core.Task{ID: "task-loop", Prompt: strings.Repeat("objective ", 500)}
	events := []core.Event{}
	for iteration := 1; iteration <= 80; iteration++ {
		events = append(events, loopMemoryTestActionEvent(int64(iteration), task.ID, iteration, "iteration_completed", "decision: "+strings.Repeat("important memory text ", 80)))
	}
	memory := synthesizeLoopTaskMemory(task, core.Snapshot{}, events, time.Unix(20, 0).UTC())
	data, err := json.Marshal(memory)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxLoopTaskMemoryJSONBytes {
		t.Fatalf("memory JSON bytes = %d, want <= %d", len(data), maxLoopTaskMemoryJSONBytes)
	}
	if len(memory.Objective) > maxLoopMemoryObjectiveBytes+len("\n... truncated for replanning prompt ...\n") {
		t.Fatalf("objective was not bounded: %d bytes", len(memory.Objective))
	}
}

func loopMemoryTestActionEvent(id int64, taskID string, iteration int, status string, summary string) core.Event {
	return core.Event{
		ID:     id,
		Type:   core.EventTaskAction,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"kind":      loopActionKind,
			"status":    status,
			"iteration": iteration,
			"summary":   summary,
		}),
	}
}

func loopMemoryTestNotesContain(notes []core.TaskMemoryNote, want string) bool {
	for _, note := range notes {
		if strings.Contains(note.Text, want) {
			return true
		}
	}
	return false
}

func loopMemoryTestArtifactsContain(artifacts []core.TaskMemoryArtifact, wantID string) bool {
	for _, artifact := range artifacts {
		if artifact.ID == wantID {
			return true
		}
	}
	return false
}
