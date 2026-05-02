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

func TestChangeCommitMessageSkipsWorkerReportHeadings(t *testing.T) {
	tests := []struct {
		name          string
		workerSummary string
		want          string
	}{
		{
			name:          "markdown findings heading",
			workerSummary: "**Findings**\nFixed generated commit titles for worker report summaries.\n\n**Commands Run**\ngo test ./internal/orchestrator",
			want:          "Fixed generated commit titles for worker report summaries",
		},
		{
			name:          "summary label",
			workerSummary: "Summary:\nUse substantive worker summary lines for commit subjects.\n\nTests pass.",
			want:          "Use substantive worker summary lines for commit subjects",
		},
		{
			name:          "hash findings heading",
			workerSummary: "# Findings\nPreserve changed-file commit fallback for generic worker reports.",
			want:          "Preserve changed-file commit fallback for generic worker reports",
		},
		{
			name:          "inline summary label",
			workerSummary: "Summary: Avoid report labels in generated commit subjects.",
			want:          "Avoid report labels in generated commit subjects",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := changeCommitMessage(changeCommitMessageContext{
				Fallback:      "Base worker candidate",
				WorkerSummary: tt.workerSummary,
				ChangedFiles:  []string{"internal/orchestrator/commit_message.go"},
			})
			if got != tt.want {
				t.Fatalf("message = %q, want %q", got, tt.want)
			}
		})
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
