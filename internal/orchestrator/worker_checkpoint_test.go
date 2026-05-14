package orchestrator

import (
	"strings"
	"testing"
)

func TestParseWorkerCheckpointFromJSONEnvelope(t *testing.T) {
	checkpoint, ok := parseWorkerCheckpointText(`{
		"summary": "human summary remains separate",
		"checkpoint": {
			"currentHypothesis": "worker streams become completion payloads in service.go",
			"touchedSubsystems": ["service", "eventstore"],
			"commandsRun": ["go test ./internal/orchestrator"],
			"pendingChecks": ["run full package tests"],
			"risks": ["prompt growth"],
			"recommendedNextWorkerPrompts": ["Add projection tests"]
		}
	}`)
	if !ok {
		t.Fatal("checkpoint was not parsed")
	}
	if checkpoint.CurrentHypothesis != "worker streams become completion payloads in service.go" {
		t.Fatalf("current hypothesis = %q", checkpoint.CurrentHypothesis)
	}
	if len(checkpoint.CommandsRun) != 1 || checkpoint.CommandsRun[0] != "go test ./internal/orchestrator" {
		t.Fatalf("commands run = %+v", checkpoint.CommandsRun)
	}
	if len(checkpoint.RecommendedNextWorkerPrompts) != 1 {
		t.Fatalf("recommended prompts = %+v", checkpoint.RecommendedNextWorkerPrompts)
	}
}

func TestParseWorkerCheckpointFromFencedSnakeCaseJSON(t *testing.T) {
	text := `Done.

` + "```json" + `
{
  "worker_checkpoint": {
    "current_hypothesis": "state projection can reuse task artifacts",
    "touched_subsystems": ["projection"],
    "commands_run": "go test ./internal/eventstore",
    "pending_checks": ["prompt bounding"]
  }
}
` + "```"
	checkpoint, ok := parseWorkerCheckpointText(text)
	if !ok {
		t.Fatal("checkpoint was not parsed")
	}
	if checkpoint.CurrentHypothesis != "state projection can reuse task artifacts" {
		t.Fatalf("current hypothesis = %q", checkpoint.CurrentHypothesis)
	}
	if len(checkpoint.CommandsRun) != 1 || checkpoint.CommandsRun[0] != "go test ./internal/eventstore" {
		t.Fatalf("commands run = %+v", checkpoint.CommandsRun)
	}
}

func TestParseWorkerCheckpointMalformedOrEmptyIsIgnored(t *testing.T) {
	for _, text := range []string{
		"plain worker summary",
		`{"checkpoint": {"currentHypothesis": ""}}`,
		"checkpoint: {not json",
	} {
		if checkpoint, ok := parseWorkerCheckpointText(text); ok {
			t.Fatalf("parsed malformed/empty checkpoint from %q: %+v", text, checkpoint)
		}
	}
}

func TestCompactWorkerCheckpointBoundsFields(t *testing.T) {
	items := make([]string, maxWorkerCheckpointItems+5)
	for index := range items {
		items[index] = strings.Repeat("x", maxWorkerCheckpointTextBytes+200)
	}
	checkpoint := compactWorkerCheckpoint(WorkerCheckpoint{
		CurrentHypothesis: strings.Repeat("h", maxWorkerCheckpointTextBytes+200),
		CommandsRun:       items,
	})
	if len(checkpoint.CurrentHypothesis) > maxWorkerCheckpointTextBytes+len("\n... truncated for replanning prompt ...\n") {
		t.Fatalf("current hypothesis was not bounded: %d", len(checkpoint.CurrentHypothesis))
	}
	if len(checkpoint.CommandsRun) != maxWorkerCheckpointItems {
		t.Fatalf("commands = %d, want %d", len(checkpoint.CommandsRun), maxWorkerCheckpointItems)
	}
	for _, command := range checkpoint.CommandsRun {
		if len(command) > maxWorkerCheckpointTextBytes+len("\n... truncated for replanning prompt ...\n") {
			t.Fatalf("command was not bounded: %d", len(command))
		}
	}
}
