package orchestrator

import (
	"encoding/json"
	"strings"
)

const (
	maxWorkerCheckpointTextBytes = 700
	maxWorkerCheckpointItems     = 8
	maxPromptCheckpointTextBytes = 300
	maxPromptCheckpointItems     = 4
)

type WorkerCheckpoint struct {
	CurrentHypothesis            string   `json:"currentHypothesis,omitempty"`
	TouchedSubsystems            []string `json:"touchedSubsystems,omitempty"`
	CommandsRun                  []string `json:"commandsRun,omitempty"`
	PendingChecks                []string `json:"pendingChecks,omitempty"`
	Risks                        []string `json:"risks,omitempty"`
	RecommendedNextWorkerPrompts []string `json:"recommendedNextWorkerPrompts,omitempty"`
}

func (c WorkerCheckpoint) empty() bool {
	return strings.TrimSpace(c.CurrentHypothesis) == "" &&
		len(c.TouchedSubsystems) == 0 &&
		len(c.CommandsRun) == 0 &&
		len(c.PendingChecks) == 0 &&
		len(c.Risks) == 0 &&
		len(c.RecommendedNextWorkerPrompts) == 0
}

func parseWorkerCheckpointText(text string) (WorkerCheckpoint, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return WorkerCheckpoint{}, false
	}
	if checkpoint, ok := parseWorkerCheckpointJSON([]byte(text)); ok {
		return checkpoint, true
	}
	for _, block := range fencedJSONBlocks(text) {
		if checkpoint, ok := parseWorkerCheckpointJSON([]byte(block)); ok {
			return checkpoint, true
		}
	}
	for _, marker := range []string{"worker checkpoint:", "checkpoint:"} {
		if checkpoint, ok := parseMarkedWorkerCheckpoint(text, marker); ok {
			return checkpoint, true
		}
	}
	return WorkerCheckpoint{}, false
}

func parseMarkedWorkerCheckpoint(text string, marker string) (WorkerCheckpoint, bool) {
	lower := strings.ToLower(text)
	index := strings.Index(lower, marker)
	if index < 0 {
		return WorkerCheckpoint{}, false
	}
	candidate := strings.TrimSpace(text[index+len(marker):])
	if start := strings.Index(candidate, "{"); start >= 0 {
		candidate = candidate[start:]
	}
	return parseWorkerCheckpointJSON([]byte(candidate))
}

func parseWorkerCheckpointJSON(data []byte) (WorkerCheckpoint, bool) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return WorkerCheckpoint{}, false
	}
	for _, key := range []string{"checkpoint", "workerCheckpoint", "worker_checkpoint"} {
		if raw := envelope[key]; len(raw) > 0 {
			return decodeWorkerCheckpoint(raw)
		}
	}
	return decodeWorkerCheckpoint(data)
}

func decodeWorkerCheckpoint(data []byte) (WorkerCheckpoint, bool) {
	var checkpoint WorkerCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return WorkerCheckpoint{}, false
	}
	var aliases map[string]json.RawMessage
	if err := json.Unmarshal(data, &aliases); err == nil {
		checkpoint.CurrentHypothesis = firstNonEmpty(checkpoint.CurrentHypothesis, rawStringAlias(aliases, "current_hypothesis"))
		checkpoint.TouchedSubsystems = firstNonEmptyStrings(checkpoint.TouchedSubsystems, rawStringSliceAlias(aliases, "touched_subsystems"))
		checkpoint.CommandsRun = firstNonEmptyStrings(checkpoint.CommandsRun, rawStringSliceAlias(aliases, "commands_run"))
		checkpoint.PendingChecks = firstNonEmptyStrings(checkpoint.PendingChecks, rawStringSliceAlias(aliases, "pending_checks"))
		checkpoint.Risks = firstNonEmptyStrings(checkpoint.Risks, rawStringSliceAlias(aliases, "risk"))
		checkpoint.RecommendedNextWorkerPrompts = firstNonEmptyStrings(checkpoint.RecommendedNextWorkerPrompts, rawStringSliceAlias(aliases, "recommended_next_worker_prompts"))
	}
	checkpoint = compactWorkerCheckpoint(checkpoint)
	if checkpoint.empty() {
		return WorkerCheckpoint{}, false
	}
	return checkpoint, true
}

func rawStringAlias(values map[string]json.RawMessage, key string) string {
	raw := values[key]
	if len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return value
}

func rawStringSliceAlias(values map[string]json.RawMessage, key string) []string {
	raw := values[key]
	if len(raw) == 0 {
		return nil
	}
	var items []string
	if err := json.Unmarshal(raw, &items); err == nil {
		return items
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
		return []string{value}
	}
	return nil
}

func firstNonEmptyStrings(values ...[]string) []string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func fencedJSONBlocks(text string) []string {
	lines := strings.Split(text, "\n")
	blocks := []string{}
	inBlock := false
	var builder strings.Builder
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			label := strings.TrimSpace(strings.TrimPrefix(trimmed, "```"))
			if !inBlock {
				if label == "" || strings.EqualFold(label, "json") {
					inBlock = true
					builder.Reset()
				}
				continue
			}
			blocks = append(blocks, builder.String())
			inBlock = false
			continue
		}
		if inBlock {
			builder.WriteString(line)
			builder.WriteString("\n")
		}
	}
	return blocks
}

func compactWorkerCheckpoint(checkpoint WorkerCheckpoint) WorkerCheckpoint {
	checkpoint.CurrentHypothesis = truncateStringForPrompt(strings.TrimSpace(checkpoint.CurrentHypothesis), maxWorkerCheckpointTextBytes)
	checkpoint.TouchedSubsystems = compactCheckpointStrings(checkpoint.TouchedSubsystems)
	checkpoint.CommandsRun = compactCheckpointStrings(checkpoint.CommandsRun)
	checkpoint.PendingChecks = compactCheckpointStrings(checkpoint.PendingChecks)
	checkpoint.Risks = compactCheckpointStrings(checkpoint.Risks)
	checkpoint.RecommendedNextWorkerPrompts = compactCheckpointStrings(checkpoint.RecommendedNextWorkerPrompts)
	return checkpoint
}

func compactOptionalWorkerCheckpoint(checkpoint *WorkerCheckpoint) *WorkerCheckpoint {
	if checkpoint == nil || checkpoint.empty() {
		return nil
	}
	compact := compactWorkerCheckpoint(*checkpoint)
	if compact.empty() {
		return nil
	}
	return &compact
}

func compactWorkerCheckpointForPrompt(checkpoint WorkerCheckpoint) WorkerCheckpoint {
	checkpoint.CurrentHypothesis = truncateStringForPrompt(strings.TrimSpace(checkpoint.CurrentHypothesis), maxPromptCheckpointTextBytes)
	checkpoint.TouchedSubsystems = compactCheckpointStringsWithLimit(checkpoint.TouchedSubsystems, maxPromptCheckpointItems, maxPromptCheckpointTextBytes)
	checkpoint.CommandsRun = compactCheckpointStringsWithLimit(checkpoint.CommandsRun, maxPromptCheckpointItems, maxPromptCheckpointTextBytes)
	checkpoint.PendingChecks = compactCheckpointStringsWithLimit(checkpoint.PendingChecks, maxPromptCheckpointItems, maxPromptCheckpointTextBytes)
	checkpoint.Risks = compactCheckpointStringsWithLimit(checkpoint.Risks, maxPromptCheckpointItems, maxPromptCheckpointTextBytes)
	checkpoint.RecommendedNextWorkerPrompts = compactCheckpointStringsWithLimit(checkpoint.RecommendedNextWorkerPrompts, maxPromptCheckpointItems, maxPromptCheckpointTextBytes)
	return checkpoint
}

func compactOptionalWorkerCheckpointForPrompt(checkpoint *WorkerCheckpoint) *WorkerCheckpoint {
	if checkpoint == nil || checkpoint.empty() {
		return nil
	}
	compact := compactWorkerCheckpointForPrompt(*checkpoint)
	if compact.empty() {
		return nil
	}
	return &compact
}

func compactCheckpointStrings(values []string) []string {
	return compactCheckpointStringsWithLimit(values, maxWorkerCheckpointItems, maxWorkerCheckpointTextBytes)
}

func compactCheckpointStringsWithLimit(values []string, maxItems int, maxBytes int) []string {
	out := make([]string, 0, min(len(values), maxItems))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out = append(out, truncateStringForPrompt(value, maxBytes))
		if len(out) == maxItems {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func renderWorkerCheckpointForPrompt(checkpoint *WorkerCheckpoint) string {
	if checkpoint == nil || checkpoint.empty() {
		return ""
	}
	compact := compactWorkerCheckpointForPrompt(*checkpoint)
	var builder strings.Builder
	if compact.CurrentHypothesis != "" {
		builder.WriteString("  Current hypothesis: ")
		builder.WriteString(compact.CurrentHypothesis)
		builder.WriteString("\n")
	}
	writeCheckpointList(&builder, "  Touched subsystems", compact.TouchedSubsystems)
	writeCheckpointList(&builder, "  Commands run", compact.CommandsRun)
	writeCheckpointList(&builder, "  Pending checks", compact.PendingChecks)
	writeCheckpointList(&builder, "  Risks", compact.Risks)
	writeCheckpointList(&builder, "  Recommended next worker prompts", compact.RecommendedNextWorkerPrompts)
	return builder.String()
}

func writeCheckpointList(builder *strings.Builder, label string, values []string) {
	if len(values) == 0 {
		return
	}
	builder.WriteString(label)
	builder.WriteString(":\n")
	for _, value := range values {
		builder.WriteString("  - ")
		builder.WriteString(value)
		builder.WriteString("\n")
	}
}
