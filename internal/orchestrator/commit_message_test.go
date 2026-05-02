package orchestrator

import "testing"

func TestChangeCommitMessageUsesMeaningfulContext(t *testing.T) {
	got := changeCommitMessage(changeCommitMessageContext{
		Fallback:      "Base worker candidate",
		WorkerSummary: "Fix generated commit messages for publish paths.\n\nTests pass.",
		TaskTitle:     "Generic task title",
		ChangedFiles:  []string{"internal/orchestrator/pull_request.go"},
	})
	if got != "Fix generated commit messages for publish paths" {
		t.Fatalf("message = %q", got)
	}
}

func TestChangeCommitMessageSkipsGenericContextForChangedFiles(t *testing.T) {
	got := changeCommitMessage(changeCommitMessageContext{
		Fallback:     "Publish feature",
		PullTitle:    "CI",
		ChangedFiles: []string{".github/workflows/ci.yml"},
	})
	if got != "Update GitHub workflows" {
		t.Fatalf("message = %q", got)
	}
}

func TestChangeCommitMessagePreservesFallbackWithoutContext(t *testing.T) {
	got := changeCommitMessage(changeCommitMessageContext{
		Fallback: "Base worker candidate",
	})
	if got != "Base worker candidate" {
		t.Fatalf("message = %q", got)
	}
}
