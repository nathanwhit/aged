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
