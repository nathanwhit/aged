package orchestrator

import (
	"strings"
	"testing"

	"aged/internal/core"
)

func TestDurableLoopPromptIncludesGenericLoopPlaybook(t *testing.T) {
	prompt := durableLoopPrompt(core.Task{Prompt: "Keep improving the product."}, durableLoopConfig{
		Role:   "generalist",
		Prompt: "Keep improving the product.",
	}, 3, nil)

	if !strings.Contains(prompt, "iteration 3") {
		t.Fatalf("prompt missing iteration:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Role: generalist") {
		t.Fatalf("prompt missing role:\n%s", prompt)
	}
	assertDurableLoopPlaybookGuidance(t, prompt)
	if strings.Contains(strings.ToLower(prompt), "maintenance") {
		t.Fatalf("prompt should stay generic, got maintenance-specific wording:\n%s", prompt)
	}
}

func TestDurableLoopPromptIncludesTaskMemoryWhenPresent(t *testing.T) {
	task := core.Task{
		Prompt: "Keep improving the product.",
		Memory: &core.TaskMemory{
			Objective: "Long-horizon objective",
			Decisions: []core.TaskMemoryNote{{
				Text:               "decision: keep deterministic loop memory",
				FirstSeenIteration: 2,
				LastSeenIteration:  4,
				Count:              2,
			}},
			Blockers: []core.TaskMemoryNote{{
				Text:              "Need credentials before checking external status.",
				LastSeenIteration: 5,
			}},
			Artifacts: []core.TaskMemoryArtifact{{
				ID:                   "pr-1",
				Title:                "Loop memory PR",
				URL:                  "https://github.com/acme/repo/pull/1",
				State:                "open",
				ChecksConclusion:     "success",
				MergeStatus:          "CLEAN",
				PublishedAtIteration: 3,
			}},
		},
	}
	prompt := durableLoopPrompt(task, durableLoopConfig{
		Role:   "generalist",
		Prompt: "Keep improving the product.",
	}, 6, nil)

	for _, want := range []string{
		"# Task Memory",
		"Long-horizon objective",
		"decision: keep deterministic loop memory (iter 2-4, count 2)",
		"Need credentials before checking external status. (iter 5)",
		"Loop memory PR https://github.com/acme/repo/pull/1 (state=open, checks=success, merge=CLEAN, iter=3)",
		"# Task Objective",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing memory content %q:\n%s", want, prompt)
		}
	}
	if strings.Index(prompt, "# Task Memory") > strings.Index(prompt, "# Task Objective") {
		t.Fatalf("task memory should render before task objective:\n%s", prompt)
	}
}

func assertDurableLoopPlaybookGuidance(t *testing.T, prompt string) {
	t.Helper()
	for _, want := range []string{
		"# Loop Playbook",
		"Inspect the current repository and workspace state before choosing the next work item.",
		"Check existing task artifacts and any open pull request context when available or obvious",
		"Prefer one bounded, coherent unit of progress for this iteration",
		"provided `aged-publish-pr` helper only when this iteration produced a real material change",
		"After publishing, continue the durable objective in later iterations unless the loop is canceled or you are blocked.",
		"Ask for user input only for user-owned blockers such as missing credentials, permissions, ambiguous scope, or risky setup choices.",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing durable loop guidance %q:\n%s", want, prompt)
		}
	}
}
