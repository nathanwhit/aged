package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aged/internal/core"
)

func TestPromptSetRegistryRendersFileBackedDefault(t *testing.T) {
	registry := NewPromptSetRegistry([]core.PromptSet{{
		ID:      defaultPromptSetID,
		Name:    "Default",
		BuiltIn: true,
		Templates: map[string]string{
			PromptTemplateSystem: "SYSTEM",
			PromptTemplatePlan:   "{{system}}\n\n{{input_json}}",
		},
	}}, "")

	rendered, ok := registry.Render(core.Task{ID: "task-1", Title: "Title", Prompt: "Prompt"}, PromptTemplatePlan, map[string]any{
		PromptTemplateSystem: "fallback system",
		"input_json":         map[string]string{"task": "value"},
	})
	if !ok {
		t.Fatal("expected default prompt to render")
	}
	if rendered.PromptSetID != defaultPromptSetID {
		t.Fatalf("PromptSetID = %q, want %q", rendered.PromptSetID, defaultPromptSetID)
	}
	if !strings.Contains(rendered.Prompt, "SYSTEM") || strings.Contains(rendered.Prompt, "fallback system") {
		t.Fatalf("rendered prompt did not use prompt-set system template: %q", rendered.Prompt)
	}
}

func TestLoadDefaultPromptSet(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"system.md":                "SYSTEM",
		"plan.md":                  "{{system}}\n{{input_json}}",
		"github_review_request.md": "review {{input_json}}",
		"code_review.md":           "code review {{input_json}}",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	promptSet, err := LoadDefaultPromptSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	if promptSet.ID != defaultPromptSetID || !promptSet.BuiltIn {
		t.Fatalf("loaded prompt set = %+v", promptSet)
	}
	if promptSet.Templates[PromptTemplateSystem] != "SYSTEM" || promptSet.Templates[PromptTemplatePlan] == "" || promptSet.Templates[PromptTemplateGitHubReview] == "" || promptSet.Templates[PromptTemplateCodeReview] == "" {
		t.Fatalf("templates = %+v", promptSet.Templates)
	}
}

func TestPlanTemplateNamesSpecializesGitHubReviewRequests(t *testing.T) {
	task := core.Task{Metadata: core.MustJSON(map[string]any{
		"source":      "github-mention",
		"reason":      "review_requested",
		"subjectType": "PullRequest",
	})}

	got := planTemplateNames(task)
	want := []string{PromptTemplateGitHubReview, PromptTemplatePlan}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("planTemplateNames = %+v, want %+v", got, want)
	}
}
