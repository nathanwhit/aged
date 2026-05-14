package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"aged/internal/core"
)

const (
	maxContextLedgerEntries      = 24
	maxContextLedgerTextBytes    = 900
	maxContextLedgerChangedFiles = 12
)

type projectedLedgerEntry struct {
	entry ContextLedgerEntry
	score int
	order int
}

type contextLedgerWorkerInfo struct {
	kind         string
	nodeID       string
	role         string
	spawnID      string
	baseWorkerID string
}

func (s *Service) taskContextLedger(ctx context.Context, taskID string) []ContextLedgerEntry {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil
	}
	return projectTaskContextLedger(snapshot.Events, taskID)
}

func projectTaskContextLedger(events []core.Event, taskID string) []ContextLedgerEntry {
	workers := map[string]contextLedgerWorkerInfo{}
	var projected []projectedLedgerEntry
	for index, event := range events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventExecutionPlanned:
			var payload struct {
				NodeID     string         `json:"nodeId"`
				WorkerID   string         `json:"workerId"`
				WorkerKind string         `json:"workerKind"`
				Role       string         `json:"role"`
				SpawnID    string         `json:"spawnId"`
				Metadata   map[string]any `json:"metadata"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil {
				workerID := nonEmpty(payload.WorkerID, event.WorkerID)
				info := workers[workerID]
				info.nodeID = nonEmpty(payload.NodeID, info.nodeID)
				info.kind = nonEmpty(payload.WorkerKind, info.kind)
				info.role = nonEmpty(payload.Role, info.role)
				info.spawnID = nonEmpty(payload.SpawnID, info.spawnID)
				info.baseWorkerID = nonEmpty(stringMetadata(payload.Metadata, "baseWorkerID"), info.baseWorkerID)
				workers[workerID] = info
			}
		case core.EventWorkerCreated:
			var payload struct {
				Kind     string         `json:"kind"`
				Metadata map[string]any `json:"metadata"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil {
				info := workers[event.WorkerID]
				info.kind = nonEmpty(payload.Kind, info.kind)
				info.role = nonEmpty(stringMetadata(payload.Metadata, "spawnRole"), info.role)
				info.spawnID = nonEmpty(stringMetadata(payload.Metadata, "spawnID"), info.spawnID)
				info.baseWorkerID = nonEmpty(stringMetadata(payload.Metadata, "baseWorkerID"), info.baseWorkerID)
				workers[event.WorkerID] = info
			}
		case core.EventWorkerCompleted:
			if ledgerEntry, score, ok := contextLedgerWorkerCompletion(event, workers[event.WorkerID]); ok {
				projected = append(projected, projectedLedgerEntry{entry: ledgerEntry, score: score, order: index})
			}
		case core.EventApprovalNeeded:
			if ledgerEntry, ok := contextLedgerApprovalNeeded(event); ok {
				projected = append(projected, projectedLedgerEntry{entry: ledgerEntry, score: 95, order: index})
			}
		case core.EventApprovalDecided:
			if ledgerEntry, ok := contextLedgerApprovalDecided(event); ok {
				projected = append(projected, projectedLedgerEntry{entry: ledgerEntry, score: 90, order: index})
			}
		case core.EventTaskAction:
			if ledgerEntry, score, ok := contextLedgerTaskAction(event); ok {
				projected = append(projected, projectedLedgerEntry{entry: ledgerEntry, score: score, order: index})
			}
		case core.EventTaskMilestone:
			if ledgerEntry, ok := contextLedgerTaskMilestone(event); ok {
				projected = append(projected, projectedLedgerEntry{entry: ledgerEntry, score: 85, order: index})
			}
		case core.EventTaskReplanned:
			if ledgerEntry, score, ok := contextLedgerTaskReplanned(event); ok {
				projected = append(projected, projectedLedgerEntry{entry: ledgerEntry, score: score, order: index})
			}
		}
	}
	if len(projected) == 0 {
		return nil
	}
	if len(projected) > maxContextLedgerEntries {
		sort.SliceStable(projected, func(i, j int) bool {
			if projected[i].score != projected[j].score {
				return projected[i].score > projected[j].score
			}
			return projected[i].order > projected[j].order
		})
		projected = projected[:maxContextLedgerEntries]
	}
	sort.SliceStable(projected, func(i, j int) bool {
		return projected[i].order < projected[j].order
	})
	entries := make([]ContextLedgerEntry, 0, len(projected))
	for _, item := range projected {
		entries = append(entries, compactContextLedgerEntry(item.entry))
	}
	return entries
}

func contextLedgerWorkerCompletion(event core.Event, info contextLedgerWorkerInfo) (ContextLedgerEntry, int, bool) {
	var payload struct {
		Status           core.WorkerStatus      `json:"status"`
		Summary          string                 `json:"summary,omitempty"`
		Error            string                 `json:"error,omitempty"`
		ChangedFiles     []WorkspaceChangedFile `json:"changedFiles,omitempty"`
		WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ContextLedgerEntry{}, 0, false
	}
	changedFiles := payload.ChangedFiles
	if len(changedFiles) == 0 {
		changedFiles = payload.WorkspaceChanges.ChangedFiles
	}
	changes := payload.WorkspaceChanges
	if len(changes.ChangedFiles) == 0 {
		changes.ChangedFiles = changedFiles
	}
	result := WorkerTurnResult{Status: payload.Status, Summary: payload.Summary, Error: payload.Error, Changes: changes}
	score := 0
	kind := "worker_result"
	switch {
	case resultHasCandidateChanges(result):
		score = 100
		kind = "candidate_result"
	case payload.Status == core.WorkerFailed || payload.Status == core.WorkerCanceled || payload.Status == core.WorkerWaiting:
		score = 92
		kind = "worker_terminal_state"
	case highValueLedgerText(payload.Summary) || highValueLedgerText(payload.Error):
		score = 82
	default:
		return ContextLedgerEntry{}, 0, false
	}
	return ContextLedgerEntry{
		Kind:         kind,
		SourceEvent:  string(event.Type),
		WorkerID:     event.WorkerID,
		NodeID:       info.nodeID,
		WorkerKind:   info.kind,
		Role:         info.role,
		SpawnID:      info.spawnID,
		BaseWorkerID: info.baseWorkerID,
		Status:       string(payload.Status),
		Summary:      payload.Summary,
		Error:        payload.Error,
		ChangedFiles: changedFiles,
	}, score, true
}

func contextLedgerApprovalNeeded(event core.Event) (ContextLedgerEntry, bool) {
	var payload struct {
		Reason   string         `json:"reason"`
		Question string         `json:"question"`
		Summary  string         `json:"summary"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ContextLedgerEntry{}, false
	}
	return ContextLedgerEntry{
		Kind:        "user_input_needed",
		SourceEvent: string(event.Type),
		WorkerID:    event.WorkerID,
		Summary:     nonEmpty(payload.Question, payload.Summary, payload.Reason),
		Metadata: compactLedgerMetadata(map[string]any{
			"reason": payload.Reason,
		}),
	}, true
}

func contextLedgerApprovalDecided(event core.Event) (ContextLedgerEntry, bool) {
	var payload struct {
		Reason   string `json:"reason"`
		Answer   string `json:"answer"`
		Question string `json:"question"`
		Approved *bool  `json:"approved"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ContextLedgerEntry{}, false
	}
	metadata := map[string]any{"reason": payload.Reason}
	if payload.Approved != nil {
		metadata["approved"] = *payload.Approved
	}
	return ContextLedgerEntry{
		Kind:        "user_input_answered",
		SourceEvent: string(event.Type),
		WorkerID:    event.WorkerID,
		Summary:     nonEmpty(payload.Answer, payload.Question, payload.Reason),
		Metadata:    compactLedgerMetadata(metadata),
	}, true
}

func contextLedgerTaskAction(event core.Event) (ContextLedgerEntry, int, bool) {
	var payload struct {
		Kind     string `json:"kind"`
		Status   string `json:"status"`
		Reason   string `json:"reason"`
		WorkerID string `json:"workerId"`
		Error    string `json:"error"`
		Summary  string `json:"summary"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ContextLedgerEntry{}, 0, false
	}
	text := nonEmpty(payload.Reason, payload.Summary, payload.Error)
	important := payload.Status == "rejected" || payload.Status == "failed" || payload.Status == "waiting" || highValueLedgerText(text)
	if !important {
		return ContextLedgerEntry{}, 0, false
	}
	score := 84
	if payload.Status == "rejected" || payload.Status == "failed" {
		score = 90
	}
	return ContextLedgerEntry{
		Kind:        nonEmpty(payload.Kind, "task_action"),
		SourceEvent: string(event.Type),
		WorkerID:    nonEmpty(payload.WorkerID, event.WorkerID),
		Status:      payload.Status,
		Summary:     text,
		Error:       payload.Error,
	}, score, true
}

func contextLedgerTaskMilestone(event core.Event) (ContextLedgerEntry, bool) {
	var payload struct {
		Name     string         `json:"name"`
		Phase    string         `json:"phase"`
		Summary  string         `json:"summary"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ContextLedgerEntry{}, false
	}
	return ContextLedgerEntry{
		Kind:        "task_milestone",
		SourceEvent: string(event.Type),
		Status:      payload.Phase,
		Summary:     nonEmpty(payload.Summary, payload.Name),
	}, true
}

func contextLedgerTaskReplanned(event core.Event) (ContextLedgerEntry, int, bool) {
	var payload struct {
		Turn     int            `json:"turn"`
		Fallback bool           `json:"fallback"`
		Error    string         `json:"error"`
		Decision ReplanDecision `json:"decision"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ContextLedgerEntry{}, 0, false
	}
	action := strings.TrimSpace(payload.Decision.Action)
	text := nonEmpty(payload.Decision.Message, payload.Decision.Rationale, payload.Error)
	if !payload.Fallback && action != "wait" && action != "fail" && !highValueLedgerText(text) {
		return ContextLedgerEntry{}, 0, false
	}
	score := 80
	if payload.Fallback || action == "wait" || action == "fail" {
		score = 88
	}
	return ContextLedgerEntry{
		Kind:        "replan_decision",
		SourceEvent: string(event.Type),
		Status:      action,
		Summary:     text,
		Metadata: compactLedgerMetadata(map[string]any{
			"turn":     payload.Turn,
			"fallback": payload.Fallback,
		}),
	}, score, true
}

func highValueLedgerText(value string) bool {
	text := strings.ToLower(value)
	if strings.TrimSpace(text) == "" {
		return false
	}
	keywords := []string{
		"ledger_fact",
		"context fact",
		"important:",
		"decision:",
		"root cause",
		"constraint",
		"blocked",
		"discovered",
		"found",
		"validated",
		"benchmark",
		"regression",
	}
	for _, keyword := range keywords {
		if strings.Contains(text, keyword) {
			return true
		}
	}
	return false
}

func compactContextLedgerEntry(entry ContextLedgerEntry) ContextLedgerEntry {
	entry.Summary = truncateStringForPrompt(strings.TrimSpace(entry.Summary), maxContextLedgerTextBytes)
	entry.Error = truncateStringForPrompt(strings.TrimSpace(entry.Error), maxContextLedgerTextBytes)
	if len(entry.ChangedFiles) > maxContextLedgerChangedFiles {
		omitted := len(entry.ChangedFiles) - maxContextLedgerChangedFiles
		entry.ChangedFiles = append(entry.ChangedFiles[:maxContextLedgerChangedFiles], WorkspaceChangedFile{
			Path:   fmt.Sprintf("... %d additional changed files omitted ...", omitted),
			Status: "omitted",
		})
	}
	entry.Metadata = compactLedgerMetadata(entry.Metadata)
	return entry
}

func compactLedgerMetadata(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range metadata {
		switch v := value.(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				out[key] = truncateStringForPrompt(v, maxContextLedgerTextBytes)
			}
		case bool:
			out[key] = v
		case int:
			if v != 0 {
				out[key] = v
			}
		case float64:
			if v != 0 {
				out[key] = v
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func compactContextLedgerForPrompt(entries []ContextLedgerEntry) []ContextLedgerEntry {
	if len(entries) == 0 {
		return nil
	}
	projected := make([]ContextLedgerEntry, 0, len(entries))
	keepFrom := 0
	if len(entries) > maxContextLedgerEntries {
		keepFrom = len(entries) - maxContextLedgerEntries
	}
	for _, entry := range entries[keepFrom:] {
		projected = append(projected, compactContextLedgerEntry(entry))
	}
	return projected
}

func renderContextLedgerForWorkerPrompt(entries []ContextLedgerEntry) string {
	if len(entries) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("# Context Ledger\n\n")
	builder.WriteString("Persisted high-signal context from earlier task events. Treat this as compact memory, not as a full transcript.\n")
	for _, entry := range compactContextLedgerForPrompt(entries) {
		builder.WriteString("- ")
		builder.WriteString(entry.Kind)
		if entry.WorkerID != "" {
			builder.WriteString(" worker=")
			builder.WriteString(entry.WorkerID)
		}
		if entry.Status != "" {
			builder.WriteString(" status=")
			builder.WriteString(entry.Status)
		}
		if entry.WorkerKind != "" {
			builder.WriteString(" kind=")
			builder.WriteString(entry.WorkerKind)
		}
		text := nonEmpty(entry.Summary, entry.Error)
		if text != "" {
			builder.WriteString(": ")
			builder.WriteString(text)
		}
		if len(entry.ChangedFiles) > 0 {
			builder.WriteString(" [files: ")
			for index, file := range entry.ChangedFiles {
				if index > 0 {
					builder.WriteString(", ")
				}
				builder.WriteString(file.Path)
				if file.Status != "" {
					builder.WriteString(" ")
					builder.WriteString(file.Status)
				}
			}
			builder.WriteString("]")
		}
		builder.WriteString("\n")
	}
	builder.WriteString("\n")
	return builder.String()
}
