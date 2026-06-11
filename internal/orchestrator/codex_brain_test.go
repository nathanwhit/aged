package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"aged/internal/core"
)

func TestCodexBrainPlansFromAgentMessage(t *testing.T) {
	brain := newTestCodexBrain(t, "valid", nil)
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Implement task",
		Prompt: "Make the scheduler use Codex.",
	}, []string{"keep it small"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.WorkItems) != 1 || plan.WorkItems[0].WorkerKind != "codex" {
		t.Fatalf("workItems = %+v", plan.WorkItems)
	}
	if plan.WorkItems[0].Prompt != "Implement the requested scheduler change." {
		t.Fatalf("Prompt = %q", plan.WorkItems[0].Prompt)
	}
	if plan.Metadata["brain"] != "codex" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
}

func TestCodexBrainExecArgsUseYoloPermissions(t *testing.T) {
	brain := &CodexBrain{workDir: "/tmp/aged-work"}
	got := brain.execArgs()
	want := []string{
		"exec",
		"--dangerously-bypass-approvals-and-sandbox",
		"--json",
		"--cd",
		"/tmp/aged-work",
		"-",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestClaudeBrainPlansFromStreamResult(t *testing.T) {
	brain := newTestClaudeBrain(t, "valid", nil)
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Implement task",
		Prompt: "Make the scheduler use Claude.",
	}, []string{"keep it small"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.WorkItems) != 1 || plan.WorkItems[0].WorkerKind != "claude" {
		t.Fatalf("workItems = %+v", plan.WorkItems)
	}
	if plan.WorkItems[0].Prompt != "Investigate and implement the requested scheduler change." {
		t.Fatalf("Prompt = %q", plan.WorkItems[0].Prompt)
	}
	if plan.Metadata["brain"] != "claude" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
}

func TestClaudeBrainArgsUseStreamJSONAndSkipPermissions(t *testing.T) {
	brain := &CodexBrain{provider: "claude"}
	got := brain.claudeArgs()
	want := []string{
		"--print",
		"--output-format",
		"stream-json",
		"--verbose",
		"--dangerously-skip-permissions",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestCodexBrainSendsSchedulerPromptOnStdin(t *testing.T) {
	dir := t.TempDir()
	templatePath := filepath.Join(dir, "scheduler.md")
	if err := os.WriteFile(templatePath, []byte("schedule the work"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdinPath := filepath.Join(dir, "stdin.txt")
	codexPath := filepath.Join(dir, "codex")
	script := "#!/bin/sh\n" +
		"cat > " + shellQuoteTest(stdinPath) + "\n" +
		"printf '%s\\n' " + strconv.Quote(testCodexBrainOutput(t, "valid")) + "\n"
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	brain, err := NewCodexBrain(CodexBrainConfig{
		CodexPath:    codexPath,
		TemplatePath: templatePath,
		WorkDir:      dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = brain.Plan(context.Background(), core.Task{
		ID:     "task-stdin",
		Title:  "Stdin",
		Prompt: strings.Repeat("large prompt ", 1000),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stdin), "large prompt large prompt") {
		t.Fatalf("scheduler prompt was not sent on stdin: %.200q", stdin)
	}
}

func TestCodexBrainIncludesTaskMetadataInPromptPayload(t *testing.T) {
	brain := &CodexBrain{template: "schedule the work"}
	prompt := brain.taskMessage(core.Task{
		ID:     "task-broad",
		Title:  "Large migration",
		Prompt: "Split the migration into reviewable PRs.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	}, nil)

	if !strings.Contains(prompt, `"objectiveMode": "broad"`) {
		t.Fatalf("scheduler prompt missing objectiveMode metadata:\n%s", prompt)
	}
	if strings.Contains(prompt, `"completionMode"`) {
		t.Fatalf("scheduler prompt should not promote completionMode:\n%s", prompt)
	}

	replanPayload := replanPromptPayload(core.Task{
		ID:     "task-broad",
		Title:  "Large migration",
		Prompt: "Split the migration into reviewable PRs.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	}, OrchestrationState{})
	data, err := json.Marshal(replanPayload)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"objectiveMode":"broad"`) {
		t.Fatalf("replan prompt payload missing objectiveMode metadata: %s", data)
	}

	payload := taskPromptPayload(core.Task{
		ID:     "task-broad-no-completion",
		Title:  "Large migration",
		Prompt: "Split the migration into reviewable PRs.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	})
	if _, ok := payload["completionMode"]; ok {
		t.Fatalf("broad objective without explicit completion mode should not invent one: %+v", payload)
	}
}

func TestReplanDecisionAllowsFinishObjective(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
			"action": "finish_objective",
			"pullRequestBody": "",
			"rationale": "all reviewable slices landed",
		"message": "Objective finished.",
		"plan": null
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err != nil {
		t.Fatalf("finish_objective replan decision rejected: %v", err)
	}
}

func TestReplanDecisionAllowsWorkerBoundActionOnlyPublish(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "continue",
		"rationale": "validated candidate should be published",
		"plan": {
			"rationale": "publish the already-completed validation worker result",
			"steps": [],
			"requiredApprovals": [],
			"actions": [{
				"kind": "publish_pull_request",
				"when": "after_success",
				"reason": "Publish the validated manager list slice.",
				"workerId": "worker-1",
				"inputs": {
					"title": "Add compact manager objective rows",
					"body": "## Summary\n- add compact manager objective rows\n\n## Validation\n- npm run build"
				}
			}],
			"workItems": []
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err != nil {
		t.Fatalf("worker-bound publish-only replan decision rejected: %v", err)
	}
}

func TestReplanDecisionRejectsUnboundActionOnlyDeferredPublish(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "continue",
		"rationale": "invalid deferred publish",
		"plan": {
			"actions": [{
				"kind": "publish_pull_request",
				"when": "after_success",
				"reason": "Publish something later.",
				"inputs": {
					"title": "Update manager console",
					"body": "## Summary\n- update manager console"
				}
			}],
			"workItems": []
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err == nil {
		t.Fatal("expected unbound deferred publish-only decision to be rejected")
	}
}

func TestCodexBrainReplanPromptCompactsLargeState(t *testing.T) {
	brain := &CodexBrain{template: "schedule the work"}
	results := make([]WorkerTurnResult, 120)
	for index := range results {
		changedFiles := make([]WorkspaceChangedFile, 120)
		for fileIndex := range changedFiles {
			changedFiles[fileIndex] = WorkspaceChangedFile{
				Path:   "file-" + strconv.Itoa(index) + "-" + strconv.Itoa(fileIndex) + ".go",
				Status: "modified",
			}
		}
		results[index] = WorkerTurnResult{
			WorkerID: "worker-" + strconv.Itoa(index),
			Status:   core.WorkerSucceeded,
			Kind:     "codex",
			Summary:  "summary-marker-" + strconv.Itoa(index) + strings.Repeat("s", 50000),
			Error:    "error-marker-" + strconv.Itoa(index) + strings.Repeat("e", 50000),
			Changes: WorkspaceChanges{
				Dirty:        true,
				Status:       strings.Repeat("status", 2000),
				DiffStat:     strings.Repeat("diffstat", 2000),
				Diff:         "DIFF_SHOULD_NOT_BE_INCLUDED" + strings.Repeat("d", 50000),
				ChangedFiles: changedFiles,
				Artifacts: []WorkspaceArtifact{{
					ID:      "artifact-" + strconv.Itoa(index),
					Kind:    "log",
					Content: "artifact-marker-" + strconv.Itoa(index) + strings.Repeat("a", 50000),
				}},
			},
		}
	}

	prompt := brain.replanPrompt(core.Task{
		ID:     "task-1",
		Title:  "Improve Long-Term Planning Intelligence",
		Prompt: strings.Repeat("plan better ", 1000),
	}, OrchestrationState{
		InitialPlan: Plan{
			WorkerKind: "codex",
			Prompt:     strings.Repeat("initial plan ", 5000),
			WorkPlan: &core.WorkPlan{
				Summary: strings.Repeat("initial work plan ", 5000),
			},
		},
		WorkPlan: &core.WorkPlan{
			Summary: strings.Repeat("current work plan ", 5000),
			Workstreams: []core.WorkPlanItem{{
				ID:       "slice-1",
				Goal:     strings.Repeat("large workstream ", 5000),
				Status:   "running",
				DoneWhen: strings.Repeat("done when ", 5000),
			}},
			Risks: []string{strings.Repeat("risk ", 5000)},
		},
		ContextLedger: []ContextLedgerEntry{{
			Kind:     "worker_result",
			WorkerID: "ancient-worker",
			Summary:  "LEDGER_FACT: preserve the old architecture decision while routine old worker results are trimmed" + strings.Repeat("l", 50000),
		}},
		Results:      results,
		Turn:         2,
		RecoveryHint: "repair the blocked candidate" + strings.Repeat("h", 50000),
	})

	if len(prompt) >= 1_048_576 {
		t.Fatalf("replan prompt length = %d, want below Codex input limit", len(prompt))
	}
	if len(prompt) >= 300_000 {
		t.Fatalf("replan prompt length = %d, want tightly bounded prompt", len(prompt))
	}
	if !strings.Contains(prompt, "worker-119") {
		t.Fatalf("prompt dropped latest result")
	}
	if !strings.Contains(prompt, "worker-1") {
		t.Fatalf("prompt dropped blocked candidate result")
	}
	if strings.Contains(prompt, `"workerId": "worker-0"`) {
		t.Fatalf("prompt kept older non-blocked result")
	}
	if !strings.Contains(prompt, "ancient-worker") || !strings.Contains(prompt, "LEDGER_FACT") {
		t.Fatalf("prompt dropped older high-value ledger fact")
	}
	if strings.Contains(prompt, "DIFF_SHOULD_NOT_BE_INCLUDED") {
		t.Fatalf("prompt included raw diff")
	}
	if !strings.Contains(prompt, "truncated for replanning prompt") {
		t.Fatalf("prompt did not mark truncated context")
	}
}

func TestCodexBrainReplanPayloadTruncatesHugeTaskPrompt(t *testing.T) {
	payload := replanPromptPayload(core.Task{
		ID:     "task-1",
		Title:  "Huge task",
		Prompt: "important beginning " + strings.Repeat("large task prompt ", 10000) + " important ending",
	}, OrchestrationState{})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) >= 40_000 {
		t.Fatalf("payload length = %d, want bounded task prompt", len(data))
	}
	text := string(data)
	if !strings.Contains(text, "important beginning") || !strings.Contains(text, "important ending") {
		t.Fatalf("truncated task prompt did not keep head and tail: %s", text)
	}
	if !strings.Contains(text, "truncated for replanning prompt") {
		t.Fatalf("missing task prompt truncation marker")
	}
}

func TestReplanPromptBudgeterSplitsBoundedStateAndOmitsLargeArtifactContents(t *testing.T) {
	largeArtifact := strings.Repeat("artifact-content-", 1000)
	payload := replanPromptPayload(core.Task{
		ID:     "task-1",
		Title:  "Budget",
		Prompt: "keep it bounded",
	}, OrchestrationState{
		Results: []WorkerTurnResult{{
			WorkerID: "worker-1",
			Status:   core.WorkerSucceeded,
			Kind:     "codex",
			Summary:  "implemented useful change",
			Changes: WorkspaceChanges{
				Dirty:        true,
				Diff:         strings.Repeat("raw diff", 1000),
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
				Artifacts: []WorkspaceArtifact{{
					ID:      "workspace-artifact",
					Kind:    "log",
					Content: largeArtifact,
				}},
			},
		}},
		ContextLedger: []ContextLedgerEntry{{
			Kind:     "worker_result_digest",
			WorkerID: "old-worker",
			Summary:  "important compact fact",
		}},
		Artifacts: []core.TaskArtifact{{
			ID:   "artifact-1",
			Kind: "worker_result_digest",
			Metadata: core.MustJSON(map[string]any{
				"workerId": "worker-1",
				"content":  largeArtifact,
				"summary":  "compact summary",
			}),
		}},
	})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"recentResults"`) || strings.Contains(text, `"results"`) {
		t.Fatalf("payload should expose recentResults instead of raw results: %s", text)
	}
	if !strings.Contains(text, "important compact fact") {
		t.Fatalf("payload missing context ledger fact: %s", text)
	}
	if strings.Contains(text, largeArtifact) || strings.Contains(text, "raw diffraw diff") {
		t.Fatalf("payload retained large raw artifact content or diff")
	}
	if !strings.Contains(text, "contentOmittedBytes") {
		t.Fatalf("payload did not record omitted artifact content metadata: %s", text)
	}
}

func TestReviewPromptPayloadsCompactLargeCandidates(t *testing.T) {
	task := core.Task{ID: "task-1", Title: "Large candidate", Prompt: "Review a large candidate."}
	candidate := largePromptCandidate()

	completionData, err := json.Marshal(completionReviewPayload(task, candidate, "done"))
	if err != nil {
		t.Fatal(err)
	}
	publicationData, err := json.Marshal(publicationReviewPayload(task, candidate, PlanAction{Kind: "publish_pull_request"}))
	if err != nil {
		t.Fatal(err)
	}
	brain := &CodexBrain{template: "schedule the work"}
	completionPrompt := brain.completionReviewPrompt(task, candidate, "done")
	publicationPrompt := brain.publicationReviewPrompt(task, candidate, PlanAction{Kind: "publish_pull_request"})

	for name, data := range map[string][]byte{
		"completion":  completionData,
		"publication": publicationData,
	} {
		text := string(data)
		if strings.Contains(text, "RAW_DIFF_MARKER") || strings.Contains(text, "RAW_PUBLISH_DIFF_MARKER") {
			t.Fatalf("%s review payload retained raw diff content", name)
		}
		if strings.Contains(text, "artifact-content-artifact-content") {
			t.Fatalf("%s review payload retained large artifact content", name)
		}
		if !strings.Contains(text, "additional changed files omitted") {
			t.Fatalf("%s review payload did not cap changed files: %s", name, text)
		}
		if len(data) >= 40_000 {
			t.Fatalf("%s review payload length = %d, want compact candidate payload", name, len(data))
		}
	}
	for name, prompt := range map[string]string{
		"completion":  completionPrompt,
		"publication": publicationPrompt,
	} {
		if strings.Contains(prompt, "RAW_DIFF_MARKER") || strings.Contains(prompt, "RAW_PUBLISH_DIFF_MARKER") {
			t.Fatalf("%s review prompt retained raw diff content", name)
		}
	}

	rawPayload := map[string]any{
		"task":              taskPromptPayload(task),
		"selectedCandidate": candidate,
		"completionReason":  "done",
	}
	rawData, err := json.Marshal(rawPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(completionData)*10 >= len(rawData) {
		t.Fatalf("completion review payload length = %d, raw length = %d, want at least 10x smaller", len(completionData), len(rawData))
	}
	t.Logf("completion review payload bytes: compact=%d raw=%d reduction=%.1fx", len(completionData), len(rawData), float64(len(rawData))/float64(len(completionData)))
}

func TestPublicationReviewPayloadSummarizesPublishDiffOnlyCandidate(t *testing.T) {
	task := core.Task{ID: "task-1", Title: "Publish UI slice", Prompt: "Open a reviewable UI PR."}
	candidate := WorkerTurnResult{
		WorkerID: "worker-1",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			PublishDiff: strings.Join([]string{
				"diff --git a/web/src/main.tsx b/web/src/main.tsx",
				"index 1111111..2222222 100644",
				"--- a/web/src/main.tsx",
				"+++ b/web/src/main.tsx",
				"@@ -1 +1 @@",
				"-old",
				"+new",
				"diff --git a/web/src/styles.css b/web/src/styles.css",
				"index 3333333..4444444 100644",
				"--- a/web/src/styles.css",
				"+++ b/web/src/styles.css",
				"@@ -1 +1 @@",
				"-old",
				"+new",
			}, "\n"),
		},
	}

	payload := publicationReviewPayload(task, candidate, PlanAction{Kind: "publish_pull_request"})
	bounded, ok := payload["candidate"].(WorkerTurnResult)
	if !ok {
		t.Fatalf("candidate payload type = %T", payload["candidate"])
	}
	if !bounded.Changes.Dirty {
		t.Fatalf("publish-diff-only candidate should be marked dirty for publication review")
	}
	if bounded.Changes.PublishDiff != "" {
		t.Fatalf("publication review payload should not include raw publish diff")
	}
	if len(bounded.Changes.ChangedFiles) != 2 {
		t.Fatalf("changed files = %+v, want two files from publish diff", bounded.Changes.ChangedFiles)
	}
	if bounded.Changes.ChangedFiles[0].Path != "web/src/main.tsx" || bounded.Changes.ChangedFiles[1].Path != "web/src/styles.css" {
		t.Fatalf("unexpected changed files from publish diff: %+v", bounded.Changes.ChangedFiles)
	}
	if !strings.Contains(bounded.Changes.DiffStat, "Cumulative publish patch touches 2 file(s)") {
		t.Fatalf("missing publish diff summary: %q", bounded.Changes.DiffStat)
	}
}

func TestCodeReviewPromptPayloadBoundsDiff(t *testing.T) {
	task := core.Task{ID: "task-1", Title: "Large candidate", Prompt: "Review a large candidate."}
	candidate := largePromptCandidate()

	payload := codeReviewPromptPayload(task, candidate, core.ReviewPolicy{Enabled: true}, "completion")
	bounded, ok := payload["candidate"].(WorkerTurnResult)
	if !ok {
		t.Fatalf("candidate payload type = %T", payload["candidate"])
	}
	if len(bounded.Changes.Diff) > maxPromptCandidateDiffBytes {
		t.Fatalf("bounded diff length = %d, want <= %d", len(bounded.Changes.Diff), maxPromptCandidateDiffBytes)
	}
	if !strings.Contains(bounded.Changes.Diff, codeReviewDiffTruncateMarker) {
		t.Fatalf("bounded diff missing truncation marker")
	}
	if !strings.Contains(bounded.Changes.Diff, "RAW_DIFF_MARKER") {
		t.Fatalf("bounded diff dropped useful diff header")
	}
	if strings.Contains(bounded.Changes.Diff, "DIFF_MIDDLE_SHOULD_BE_OMITTED") {
		t.Fatalf("bounded diff retained omitted middle content")
	}
	if bounded.Changes.PublishDiff != "" {
		t.Fatalf("publish diff was not stripped")
	}

	prompt := (&CodexBrain{template: "schedule the work"}).codeReviewPrompt(task, candidate, core.ReviewPolicy{Enabled: true}, "completion")
	if !strings.Contains(prompt, "truncated for code review prompt") {
		t.Fatalf("code review prompt missing diff truncation marker")
	}
	if strings.Contains(prompt, "RAW_PUBLISH_DIFF_MARKER") {
		t.Fatalf("code review prompt retained publish diff")
	}
	if len(prompt) >= 120_000 {
		t.Fatalf("code review prompt length = %d, want bounded prompt", len(prompt))
	}
	t.Logf("code review prompt bytes: prompt=%d rawDiff=%d boundedDiff=%d", len(prompt), len(candidate.Changes.Diff), len(bounded.Changes.Diff))
}

func TestReplanPromptPayloadIncludesTaskSteering(t *testing.T) {
	payload := replanPromptPayload(core.Task{
		ID:     "task-1",
		Title:  "Steered",
		Prompt: "continue the task",
	}, OrchestrationState{
		TaskSteering: []string{
			"Go for bolder changes.",
			" Go for bolder changes. ",
			"Use release-lite builds.",
		},
	})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"taskSteering":["Go for bolder changes.","Use release-lite builds."]`) {
		t.Fatalf("payload missing deduplicated task steering: %s", text)
	}

	prompt := (&CodexBrain{template: "schedule the work"}).replanPrompt(core.Task{
		ID:     "task-1",
		Title:  "Steered",
		Prompt: "continue the task",
	}, OrchestrationState{TaskSteering: []string{"Go for bolder changes."}})
	if !strings.Contains(prompt, "Treat state.taskSteering as current user steering for this whole task") {
		t.Fatalf("replan prompt missing task steering instruction")
	}
}

func TestCodexBrainFallsBackOnInvalidPlan(t *testing.T) {
	brain := newTestCodexBrain(t, "invalid", StaticBrain{WorkerKind: "mock"})
	plan, err := brain.Plan(context.Background(), core.Task{
		ID:     "task-1",
		Title:  "Implement task",
		Prompt: "Make the scheduler use Codex.",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.WorkItems) != 1 || plan.WorkItems[0].WorkerKind != "mock" {
		t.Fatalf("workItems = %+v", plan.WorkItems)
	}
	if plan.Metadata["brain"] != "codex-fallback" {
		t.Fatalf("metadata brain = %v", plan.Metadata["brain"])
	}
	if plan.Metadata["fallbackReason"] == "" {
		t.Fatalf("missing fallback reason")
	}
}

func TestDecodeCodexPlanRejectsOldWorkerShape(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`{
		"workerKind": "mock",
		"workerPrompt": "Run a smoke test.",
		"rationale": "The request asks for scheduler validation.",
		"steps": ["Run mock worker"],
		"requiredApprovals": ["Confirm external upload"],
		"spawns": ["reviewer"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err == nil {
		t.Fatalf("old worker/spawn shape unexpectedly validated: %+v", plan)
	}
}

func TestDecodeCodexPlanAcceptsObjectLists(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`{
		"workerKind": "mock",
		"workerPrompt": "Run a smoke test.",
		"rationale": "The request asks for scheduler validation.",
		"steps": [{"title": "Run", "description": "Run mock worker"}],
		"requiredApprovals": [{"title": "Approval", "reason": "Needed"}],
		"workItems": [{"id": "review", "kind": "objective.validate", "reason": "Check output", "prompt": "Check output", "targetKind": "objective", "targetId": "task-1", "workerKind": "claude", "reasoningEffort": "low", "dependsOn": ["test"], "metadata": {}}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Steps[0] != (PlanStep{Title: "Run", Description: "Run mock worker"}) {
		t.Fatalf("steps = %+v", plan.Steps)
	}
	if plan.RequiredApprovals[0] != (ApprovalRequest{Title: "Approval", Reason: "Needed"}) {
		t.Fatalf("approvals = %+v", plan.RequiredApprovals)
	}
	if !reflect.DeepEqual(plan.WorkItems[0], WorkItemRequest{ID: "review", Kind: "objective.validate", Reason: "Check output", Prompt: "Check output", TargetKind: "objective", TargetID: "task-1", WorkerKind: "claude", ReasoningEffort: "low", DependsOn: []string{"test"}, Metadata: map[string]any{}}) {
		t.Fatalf("workItems = %+v", plan.WorkItems)
	}
}

func TestDecodeCodexPlanAcceptsInitialWorkItems(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`{
		"rationale": "Split independent work up front.",
		"reasoningEffort": "medium",
		"workPlan": {
			"summary": "Audit the API and UI independently, then consolidate.",
			"workstreams": [{"id": "api", "goal": "Inspect API paths.", "status": "pending", "doneWhen": "API findings are reported.", "dependsOn": []}],
			"validation": [{"id": "validate", "goal": "Check proposed fixes.", "status": "pending", "doneWhen": "Validation command is reported.", "dependsOn": ["api"]}],
			"risks": ["The two audits may find overlapping issues."]
		},
		"steps": [{"title": "Audit", "description": "Run parallel audits."}],
		"requiredApprovals": [],
		"actions": [],
		"workItems": [
			{"id": "api", "kind": "objective.slice", "reason": "Inspect API paths.", "prompt": "Inspect the API paths.", "targetKind": "objective", "targetId": "task-1", "workerKind": "claude", "reasoningEffort": "low", "dependsOn": [], "metadata": {}},
			{"id": "ui", "kind": "objective.slice", "reason": "Inspect UI paths.", "prompt": "Inspect the UI paths.", "targetKind": "objective", "targetId": "task-1", "workerKind": "codex", "reasoningEffort": "low", "dependsOn": [], "metadata": {}}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.WorkItems) != 2 {
		t.Fatalf("workItems = %+v", plan.WorkItems)
	}
	if plan.WorkPlan == nil || plan.WorkPlan.Workstreams[0].ID != "api" {
		t.Fatalf("workPlan = %+v", plan.WorkPlan)
	}
	if plan.WorkItems[0].ID != "api" || plan.WorkItems[0].Prompt != "Inspect the API paths." {
		t.Fatalf("first work item = %+v", plan.WorkItems[0])
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeCodexPlanAcceptsLooseWorkPlanShapes(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`{
		"rationale": "Recover after worker failure.",
		"workPlan": {
			"summary": "Retry the implementation with a fixed environment.",
			"workstreams": "Retry the UI modernization worker.",
			"validation": "Run the frontend checks after the retry.",
			"risks": "The remote target may still need environment setup."
		},
		"steps": ["Retry worker"],
		"requiredApprovals": [],
		"actions": [],
		"workItems": [
			{"id": "retry", "kind": "objective.implement", "reason": "Retry after fixing target setup", "prompt": "retry the UI modernization", "targetKind": "objective", "targetId": "task-1", "workerKind": "codex", "reasoningEffort": "medium", "dependsOn": [], "metadata": {}}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkPlan == nil {
		t.Fatal("workPlan is nil")
	}
	if len(plan.WorkPlan.Workstreams) != 1 || plan.WorkPlan.Workstreams[0].Goal != "Retry the UI modernization worker." {
		t.Fatalf("workstreams = %+v", plan.WorkPlan.Workstreams)
	}
	if len(plan.WorkPlan.Validation) != 1 || plan.WorkPlan.Validation[0].Goal != "Run the frontend checks after the retry." {
		t.Fatalf("validation = %+v", plan.WorkPlan.Validation)
	}
	if len(plan.WorkPlan.Risks) != 1 || plan.WorkPlan.Risks[0] != "The remote target may still need environment setup." {
		t.Fatalf("risks = %+v", plan.WorkPlan.Risks)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeReplanDecisionContinue(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "continue",
		"rationale": "review found a missing case",
		"message": "run an incorporation worker",
		"workPlan": {
			"summary": "Initial implementation needs a feedback incorporation turn.",
			"workstreams": [{"id": "implement", "goal": "Incorporate reviewer feedback.", "status": "running", "doneWhen": "The missing case is fixed.", "dependsOn": []}],
			"validation": [],
			"risks": []
		},
		"plan": {
			"workPlan": {
				"summary": "Initial implementation needs a feedback incorporation turn.",
				"workstreams": [{"id": "implement", "goal": "Incorporate reviewer feedback.", "status": "running", "doneWhen": "The missing case is fixed.", "dependsOn": []}],
				"validation": [],
				"risks": []
			},
			"rationale": "review found a missing case",
			"steps": [{"title": "Fix", "description": "Patch the missing case"}],
			"requiredApprovals": [],
			"actions": [],
			"workItems": [{"id": "implement", "kind": "objective.implement", "reason": "Patch the missing case", "prompt": "incorporate review feedback", "targetKind": "objective", "targetId": "task-1", "workerKind": "codex", "reasoningEffort": "medium", "dependsOn": [], "metadata": {}}]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err != nil {
		t.Fatal(err)
	}
	if decision.Action != "continue" {
		t.Fatalf("action = %q", decision.Action)
	}
	if decision.Plan == nil || len(decision.Plan.WorkItems) != 1 || decision.Plan.WorkItems[0].Prompt != "incorporate review feedback" {
		t.Fatalf("plan = %+v", decision.Plan)
	}
	if decision.WorkPlan == nil || decision.WorkPlan.Workstreams[0].Status != "running" {
		t.Fatalf("workPlan = %+v", decision.WorkPlan)
	}
}

func TestDecodeReplanDecisionComplete(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "complete",
		"pullRequestBody": "## Summary\n- Ready to publish.",
		"rationale": "all follow-up work is done",
		"message": "ready for user review",
		"plan": null
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Validate(); err != nil {
		t.Fatal(err)
	}
	if decision.Action != "complete" {
		t.Fatalf("action = %q", decision.Action)
	}
	if decision.Plan != nil {
		t.Fatalf("plan = %+v", decision.Plan)
	}
	if !strings.Contains(decision.PullRequestBody, "Ready to publish") {
		t.Fatalf("pull request body = %q", decision.PullRequestBody)
	}
}

func TestDecodeReplanDecisionIgnoresTrailingJunk(t *testing.T) {
	decision, err := decodeReplanDecision([]byte(`{
		"action": "complete",
		"rationale": "done",
		"plan": null
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != "complete" {
		t.Fatalf("action = %q", decision.Action)
	}
}

func largePromptCandidate() WorkerTurnResult {
	changedFiles := make([]WorkspaceChangedFile, maxPromptChangedFiles+12)
	for index := range changedFiles {
		changedFiles[index] = WorkspaceChangedFile{
			Path:   "internal/orchestrator/file-" + strconv.Itoa(index) + ".go",
			Status: "modified",
		}
	}
	return WorkerTurnResult{
		WorkerID: "worker-large",
		Status:   core.WorkerSucceeded,
		Kind:     "codex",
		Summary:  "summary-head " + strings.Repeat("summary ", 3000) + " summary-tail",
		Error:    "error-head " + strings.Repeat("error ", 1000) + " error-tail",
		Changes: WorkspaceChanges{
			Root:         "/tmp/workspace",
			CWD:          "/tmp/workspace",
			Status:       "status-head " + strings.Repeat("status ", 600) + " status-tail",
			DiffStat:     "diffstat-head " + strings.Repeat("diffstat ", 800) + " diffstat-tail",
			Diff:         "diff --git a/main.go b/main.go\nRAW_DIFF_MARKER\n" + strings.Repeat("a", 90_000) + "DIFF_MIDDLE_SHOULD_BE_OMITTED" + strings.Repeat("b", 90_000) + "diff-tail",
			PublishDiff:  "diff --git a/publish.go b/publish.go\nRAW_PUBLISH_DIFF_MARKER\n" + strings.Repeat("p", 90_000),
			ChangedFiles: changedFiles,
			Dirty:        true,
			Error:        "change-error-head " + strings.Repeat("change-error ", 1000) + " change-error-tail",
			Artifacts: []WorkspaceArtifact{{
				ID:      "artifact-1",
				Kind:    "log",
				Content: strings.Repeat("artifact-content-", 2000),
				Metadata: map[string]any{
					"content": strings.Repeat("metadata-content-", 2000),
				},
			}},
		},
	}
}

func TestCodexBrainReviewPromptsRejectTestsOnlyFixCandidates(t *testing.T) {
	brain := &CodexBrain{template: "schedule the work"}
	task := core.Task{
		ID:     "task-1",
		Title:  "Fix stale PR publication",
		Prompt: "Fix the issue where worker PRs include unrelated dirty checkout changes.",
	}
	candidate := WorkerTurnResult{
		WorkerID: "worker-1",
		Status:   core.WorkerSucceeded,
		Changes: WorkspaceChanges{
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/pull_request_test.go", Status: "modified"}},
		},
	}

	completionPrompt := brain.completionReviewPrompt(task, candidate, "done")
	if !strings.Contains(completionPrompt, "only adds or changes tests") {
		t.Fatalf("completion prompt missing tests-only rejection:\n%s", completionPrompt)
	}
	publicationPrompt := brain.publicationReviewPrompt(task, candidate, PlanAction{Kind: "publish_pull_request"})
	if !strings.Contains(publicationPrompt, "pull request would only add or change tests") {
		t.Fatalf("publication prompt missing tests-only rejection:\n%s", publicationPrompt)
	}
}

func TestCodexBrainCodeReviewPromptUsesProjectPromptSet(t *testing.T) {
	brain := &CodexBrain{
		template: "schedule the work",
		promptSets: NewPromptSetRegistry([]core.PromptSet{
			{
				ID: "default",
				Templates: map[string]string{
					PromptTemplateCodeReview: "default {{task_id}}",
				},
				BuiltIn: true,
			},
			{
				ID: "project-review",
				Templates: map[string]string{
					PromptTemplateCodeReview: "custom {{input_json}}",
				},
			},
		}, "default"),
	}
	prompt := brain.CodeReviewPrompt(
		core.Task{ID: "task-1", Title: "Task", Prompt: "Ship it"},
		WorkerTurnResult{WorkerID: "worker-1", Status: core.WorkerSucceeded},
		core.ReviewPolicy{
			Enabled:      true,
			PromptSetID:  "project-review",
			Instructions: "Check event replay.",
		},
		"completion",
	)
	if !strings.Contains(prompt, "custom {") || !strings.Contains(prompt, `"phase": "completion"`) || !strings.Contains(prompt, `"instructions": "Check event replay."`) {
		t.Fatalf("prompt = %q", prompt)
	}
}

func TestCodexBrainReplanPromptInstructsHumanStylePRBody(t *testing.T) {
	brain := &CodexBrain{template: "schedule the work"}
	prompt := brain.replanPrompt(core.Task{
		ID:     "task-1",
		Title:  "Implement feature",
		Prompt: "Implement the feature.",
	}, OrchestrationState{})

	humanContributorHint := "human contributor"
	if !strings.Contains(prompt, humanContributorHint) {
		t.Fatalf("replan prompt missing human-contributor framing for PR body:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Do not restate") {
		t.Fatalf("replan prompt does not forbid restating the task prompt:\n%s", prompt)
	}
	for _, forbidden := range []string{"worker ids", "task ids", "candidate", "aged"} {
		if !strings.Contains(prompt, forbidden) {
			t.Fatalf("replan prompt does not forbid orchestration internal %q:\n%s", forbidden, prompt)
		}
	}
	if !strings.Contains(prompt, "## Test plan") && !strings.Contains(prompt, "## Validation") {
		t.Fatalf("replan prompt does not require a Test plan / Validation section:\n%s", prompt)
	}
	for _, required := range []string{
		"provide inputs.commitMessage",
		"short imperative or conventional-commit subject",
		"never use worker status narration",
	} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("replan prompt missing commit-message instruction %q:\n%s", required, prompt)
		}
	}
}

func TestCodexBrainReplanPromptDoesNotBlockBroadObjectivesOnIntermediatePRs(t *testing.T) {
	brain := &CodexBrain{template: "schedule the work"}
	prompt := brain.replanPrompt(core.Task{
		ID:     "task-1",
		Title:  "Port a project",
		Prompt: "Port this project in multiple reviewable slices.",
		Metadata: core.MustJSON(map[string]any{
			"objectiveMode": "broad",
		}),
	}, OrchestrationState{})

	if strings.Contains(prompt, "then wait for GitHub state") {
		t.Fatalf("replan prompt still tells broad intermediate PRs to wait:\n%s", prompt)
	}
	for _, required := range []string{
		"continueAfterPublish",
		"keep replanning the objective immediately",
		"PR babysitting happens in parallel",
		"Do not use wait_external or a standalone watch_pull_requests action merely because an intermediate PR was opened",
	} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("replan prompt missing %q:\n%s", required, prompt)
		}
	}
}

func TestCodexBrainReplanPromptDoesNotInlineSchedulerTemplate(t *testing.T) {
	schedulerMarker := "FULL_SCHEDULER_PROMPT_MARKER"
	brain := &CodexBrain{template: schedulerMarker + strings.Repeat(" static scheduler text", 1000)}
	prompt := brain.replanPrompt(core.Task{
		ID:     "task-1",
		Title:  "Continue task",
		Prompt: "Continue after worker results.",
	}, OrchestrationState{})

	if strings.Contains(prompt, schedulerMarker) {
		t.Fatalf("replan prompt inlined the full scheduler template")
	}
	if !strings.Contains(prompt, builtinReplanHeader) || !strings.Contains(prompt, "Dynamic replanning input") {
		t.Fatalf("replan prompt missing compact replanning instructions:\n%s", prompt)
	}
}

func TestDefaultReplanPromptDoesNotInlineDefaultSystemPrompt(t *testing.T) {
	data, err := os.ReadFile("../../prompts/default/replan.md")
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	if strings.Contains(body, "{{system}}") {
		t.Fatalf("default replan prompt should be self-contained instead of expanding the full scheduler system prompt")
	}
	if !strings.Contains(body, builtinReplanHeader) || !strings.Contains(body, "Dynamic replanning input") {
		t.Fatalf("default replan prompt missing compact replanning instructions:\n%s", body)
	}
}

func TestCodexBrainReviewPromptsDoNotInlineSchedulerTemplate(t *testing.T) {
	schedulerMarker := "FULL_SCHEDULER_PROMPT_MARKER"
	brain := &CodexBrain{template: schedulerMarker + strings.Repeat(" static scheduler text", 1000)}
	task := core.Task{
		ID:     "task-1",
		Title:  "Review candidate",
		Prompt: "Review whether this candidate is ready.",
	}
	candidate := WorkerTurnResult{
		WorkerID: "worker-1",
		Status:   core.WorkerSucceeded,
		Summary:  "implemented the requested change",
		Changes: WorkspaceChanges{
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/codex_brain.go", Status: "modified"}},
		},
	}
	prompts := map[string]string{
		"completion":  brain.completionReviewPrompt(task, candidate, "completion candidate ready"),
		"publication": brain.publicationReviewPrompt(task, candidate, PlanAction{Kind: "publish_pull_request"}),
		"pr_update": brain.publicationReviewPrompt(task, candidate, PlanAction{
			Kind: "update_pull_request",
			Inputs: map[string]any{
				"existingPullRequest": map[string]any{"repo": "owner/repo", "number": 7},
			},
		}),
		"code_review": brain.codeReviewPrompt(task, candidate, core.ReviewPolicy{BlockingSeverities: []string{"P0", "P1"}}, "completion"),
	}
	for name, prompt := range prompts {
		t.Run(name, func(t *testing.T) {
			if strings.Contains(prompt, schedulerMarker) {
				t.Fatalf("%s prompt inlined the full scheduler template", name)
			}
			if !strings.Contains(prompt, builtinReviewHeader) {
				t.Fatalf("%s prompt missing compact review header:\n%s", name, prompt)
			}
		})
	}
}

func TestCodexBrainPublicationReviewPromptCoversSemanticPRUpdates(t *testing.T) {
	brain := &CodexBrain{}
	prompt := brain.publicationReviewPrompt(core.Task{
		ID:     "task-1",
		Title:  "Broad objective",
		Prompt: "Split broad work into focused PRs and babysit each PR.",
	}, WorkerTurnResult{
		WorkerID: "worker-1",
		Status:   core.WorkerSucceeded,
		Summary:  "Addressed PR feedback with a focused helper.",
		Changes: WorkspaceChanges{
			ChangedFiles: []WorkspaceChangedFile{{Path: "tests/helper_test.go", Status: "added"}},
		},
	}, PlanAction{
		Kind: "update_pull_request",
		Inputs: map[string]any{
			"existingPullRequest": map[string]any{"repo": "owner/repo", "number": 7},
		},
	})
	for _, want := range []string{
		`For "update_pull_request" actions`,
		"even if that requires adding a new test or helper file",
		"Do not reject only because the patch touches a new path",
		"semantically belongs in that existing PR",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("publication review prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestDefaultReviewPromptsDoNotInlineDefaultSystemPrompt(t *testing.T) {
	for _, path := range []string{
		"../../prompts/default/github_review_request.md",
		"../../prompts/default/completion_review.md",
		"../../prompts/default/publication_review.md",
		"../../prompts/default/code_review.md",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			body := string(data)
			if strings.Contains(body, "{{system}}") {
				t.Fatalf("%s should be self-contained instead of expanding the full scheduler system prompt", path)
			}
			if !strings.Contains(body, builtinReviewHeader) {
				t.Fatalf("%s missing compact review header:\n%s", path, body)
			}
		})
	}
}

func TestSchedulerPromptInstructsHumanStylePRBody(t *testing.T) {
	for _, path := range []string{"../../prompts/scheduler.md", "../../prompts/default/system.md", "../../prompts/default/replan.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		body := string(data)
		if !strings.Contains(body, "human contributor") {
			t.Fatalf("%s missing human-contributor framing for PR body:\n%s", path, body)
		}
		if !strings.Contains(body, "Do not restate") {
			t.Fatalf("%s does not forbid restating the task prompt:\n%s", path, body)
		}
		for _, forbidden := range []string{"worker ids", "task ids", "candidate", "aged"} {
			if !strings.Contains(body, forbidden) {
				t.Fatalf("%s does not forbid orchestration internal %q", path, forbidden)
			}
		}
		if !strings.Contains(body, "## Test plan") && !strings.Contains(body, "## Validation") {
			t.Fatalf("%s does not require a Test plan / Validation section", path)
		}
	}
}

func TestDefaultPromptsKeepBroadObjectivesInTaskGraph(t *testing.T) {
	for _, path := range []string{"../../prompts/scheduler.md", "../../prompts/default/system.md", "../../prompts/default/replan.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		body := string(data)
		if !strings.Contains(body, "Task completion never publishes a pull request implicitly") &&
			!strings.Contains(body, "Completing a task never publishes a pull request") {
			t.Fatalf("%s does not require explicit pull request publication:\n%s", path, body)
		}
		if !strings.Contains(body, "continueAfterPublish") || !strings.Contains(body, "large") {
			t.Fatalf("%s does not reserve continueAfterPublish for broad large objectives:\n%s", path, body)
		}
		if strings.Contains(body, "then wait for GitHub state") {
			t.Fatalf("%s still tells intermediate PRs to wait for GitHub state:\n%s", path, body)
		}
		if !strings.Contains(body, "Do not wait on GitHub state merely because an intermediate PR was opened") &&
			!strings.Contains(body, "Do not use wait_external or a standalone watch_pull_requests action merely because an intermediate PR was opened") {
			t.Fatalf("%s does not keep intermediate PRs from blocking broad objectives:\n%s", path, body)
		}
		if !strings.Contains(body, "task's orchestration graph") && !strings.Contains(body, "this task's graph") {
			t.Fatalf("%s does not keep broad objective setup inside the task graph:\n%s", path, body)
		}
		if !strings.Contains(body, "one massive PR") {
			t.Fatalf("%s does not warn against collapsing broad objectives into one massive PR:\n%s", path, body)
		}
		if !strings.Contains(body, "multiple independent") && !strings.Contains(body, "multiple reviewable") {
			t.Fatalf("%s does not allow broad objectives to produce multiple PR outputs:\n%s", path, body)
		}
	}
}

func TestDefaultPromptsUseDurableWorkItemsSchema(t *testing.T) {
	tests := []struct {
		path      string
		required  []string
		forbidden []string
	}{
		{
			path: "../../prompts/default/system.md",
			required: []string{
				`"workItems": [`,
				"Use `workItems` for executable work",
				"Root work items with empty `dependsOn` can run in parallel immediately",
				"Work items with dependencies wait until all dependency work item ids finish",
				"Never return arrays of strings for `steps`, `requiredApprovals`, or `workItems`",
				"Never emit `workerKind`/`workerPrompt` as top-level fields. Never emit `workers` or `spawns`",
			},
			forbidden: []string{
				"Choose the worker and shape the initial execution plan",
				"one primary worker establish",
				"legacy compatibility fallback fields",
				"Spawns with no `dependsOn` can run in parallel after the initial worker succeeds",
				"Never return arrays of strings for `steps`, `requiredApprovals`, or `spawns`",
				"Use `workers` for initial execution",
			},
		},
		{
			path: "../../prompts/default/replan.md",
			required: []string{
				`"workItems": [`,
				`"dependsOn": []`,
				`"dependsOn": ["inspect"]`,
				`same exact schema as the scheduler plan: reasoningEffort, rationale, workPlan, steps, requiredApprovals, actions, workItems`,
				`Root work items with empty dependsOn can run in parallel immediately`,
				`"steps", "requiredApprovals", and "workItems" inside plan must be arrays of objects`,
			},
			forbidden: []string{
				`same exact schema as the scheduler plan: workerKind, workerPrompt`,
				`legacy compatibility fallback fields`,
				`Use workerId "" to mean the latest successful candidate worker`,
				`"steps", "requiredApprovals", and "spawns" inside plan must be arrays of objects`,
				`"workers": [`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(filepath.Base(tt.path), func(t *testing.T) {
			data, err := os.ReadFile(tt.path)
			if err != nil {
				t.Fatalf("read %s: %v", tt.path, err)
			}
			body := string(data)
			for _, want := range tt.required {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing required text %q", tt.path, want)
				}
			}
			for _, stale := range tt.forbidden {
				if strings.Contains(body, stale) {
					t.Fatalf("%s still contains stale text %q", tt.path, stale)
				}
			}
		})
	}
}

func TestDecodeCodexPlanExtractsObjectFromProse(t *testing.T) {
	plan, err := decodeCodexPlan([]byte(`Here is the plan:
	{
		"rationale": "test",
		"steps": [],
		"requiredApprovals": [],
		"actions": [],
		"workItems": [{
			"id": "smoke",
			"kind": "objective.validate",
			"reason": "Run a smoke test.",
			"prompt": "Run a smoke test.",
			"targetKind": "objective",
			"targetId": "task-1",
			"workerKind": "mock",
			"reasoningEffort": "low",
			"dependsOn": [],
			"metadata": {}
		}]
	}
	Thanks.`))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.WorkItems) != 1 || plan.WorkItems[0].WorkerKind != "mock" || plan.WorkItems[0].Prompt != "Run a smoke test." {
		t.Fatalf("plan = %+v", plan)
	}
}

func newTestCodexBrain(t *testing.T, mode string, fallback BrainProvider) *CodexBrain {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "scheduler.md")
	if err := os.WriteFile(path, []byte("schedule the work"), 0o644); err != nil {
		t.Fatal(err)
	}
	codexPath := filepath.Join(dir, "codex")
	script := "#!/bin/sh\n" +
		"case \" $* \" in *\" --dangerously-bypass-approvals-and-sandbox \"*) ;; *) echo missing yolo permissions >&2; exit 42;; esac\n" +
		"printf '%s\\n' " + strconv.Quote(testCodexBrainOutput(t, mode)) + "\n"
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	brain, err := NewCodexBrain(CodexBrainConfig{
		CodexPath:    codexPath,
		TemplatePath: path,
		WorkDir:      dir,
		Fallback:     fallback,
	})
	if err != nil {
		t.Fatal(err)
	}
	return brain
}

func newTestClaudeBrain(t *testing.T, mode string, fallback BrainProvider) *CodexBrain {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "scheduler.md")
	if err := os.WriteFile(path, []byte("schedule the work"), 0o644); err != nil {
		t.Fatal(err)
	}
	claudePath := filepath.Join(dir, "claude")
	script := "#!/bin/sh\n" +
		"case \" $* \" in *\" --dangerously-skip-permissions \"*) ;; *) echo missing skip permissions >&2; exit 42;; esac\n" +
		"printf '%s\\n' " + strconv.Quote(testClaudeBrainOutput(t, mode)) + "\n"
	if err := os.WriteFile(claudePath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	brain, err := NewClaudeBrain(ClaudeBrainConfig{
		ClaudePath:   claudePath,
		TemplatePath: path,
		WorkDir:      dir,
		Fallback:     fallback,
	})
	if err != nil {
		t.Fatal(err)
	}
	return brain
}

func testCodexBrainOutput(t *testing.T, mode string) string {
	t.Helper()
	switch mode {
	case "valid":
		plan := Plan{
			Rationale: "The task edits this Go codebase.",
			Steps: []PlanStep{{
				Title:       "Implement",
				Description: "Make the scheduler run through Codex.",
			}},
			RequiredApprovals: []ApprovalRequest{},
			WorkItems: []WorkItemRequest{{
				ID:              "implement",
				Kind:            "objective.implement",
				Reason:          "Implement the requested scheduler change.",
				Prompt:          "Implement the requested scheduler change.",
				TargetKind:      "objective",
				TargetID:        "task-1",
				WorkerKind:      "codex",
				ReasoningEffort: "medium",
				DependsOn:       []string{},
				Metadata:        map[string]any{},
			}},
		}
		return codexAgentMessageLine(t, plan)
	case "invalid":
		return `{"type":"item.completed","item":{"type":"agent_message","text":"{\"workerKind\":\"codex\"}"}}`
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	return ""
}

func testClaudeBrainOutput(t *testing.T, mode string) string {
	t.Helper()
	switch mode {
	case "valid":
		plan := Plan{
			Rationale: "The task benefits from a Claude-backed scheduler.",
			Steps: []PlanStep{{
				Title:       "Implement",
				Description: "Make the scheduler run through Claude.",
			}},
			RequiredApprovals: []ApprovalRequest{},
			WorkItems: []WorkItemRequest{{
				ID:              "implement",
				Kind:            "objective.implement",
				Reason:          "Investigate and implement the requested scheduler change.",
				Prompt:          "Investigate and implement the requested scheduler change.",
				TargetKind:      "objective",
				TargetID:        "task-1",
				WorkerKind:      "claude",
				ReasoningEffort: "medium",
				DependsOn:       []string{},
				Metadata:        map[string]any{},
			}},
		}
		content, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		line, err := json.Marshal(map[string]any{
			"type":    "result",
			"subtype": "success",
			"result":  string(content),
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(line)
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	return ""
}

func codexAgentMessageLine(t *testing.T, plan Plan) string {
	t.Helper()
	content, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal(map[string]any{
		"type": "item.completed",
		"item": map[string]any{
			"type": "agent_message",
			"text": string(content),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(line)
}
