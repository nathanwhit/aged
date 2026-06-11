package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"aged/internal/core"
)

func TestJSONArrayTokenSizerMatchesMarshalEstimate(t *testing.T) {
	var nilItems []string
	if got, want := newJSONArrayTokenSizer(nilItems).tokens(), approxJSONTokens(nilItems); got != want {
		t.Fatalf("nil slice tokens = %d, want %d", got, want)
	}

	items := []string{"alpha", strings.Repeat("beta", 20), "gamma"}
	sizer := newJSONArrayTokenSizer(items)
	if got, want := sizer.tokens(), approxJSONTokens(items); got != want {
		t.Fatalf("initial tokens = %d, want %d", got, want)
	}

	items = append(items[:1], items[2:]...)
	sizer.drop(1)
	if got, want := sizer.tokens(), approxJSONTokens(items); got != want {
		t.Fatalf("tokens after drop = %d, want %d", got, want)
	}

	items[0] = strings.Repeat("delta", 30)
	sizer.update(0, items[0])
	if got, want := sizer.tokens(), approxJSONTokens(items); got != want {
		t.Fatalf("tokens after update = %d, want %d", got, want)
	}

	items = items[:0]
	sizer.drop(0)
	sizer.drop(0)
	if got, want := sizer.tokens(), approxJSONTokens(items); got != want {
		t.Fatalf("empty slice tokens = %d, want %d", got, want)
	}
}

func TestCompactContextLedgerUsesBudget(t *testing.T) {
	budgeter := DefaultReplanPromptBudgeter()
	budgeter.ContextLedgerTokens = 600
	entries := largeContextLedgerEntries(24)

	compact := budgeter.compactContextLedger(entries)
	if got := approxJSONTokens(compact); got > budgeter.ContextLedgerTokens {
		t.Fatalf("compact context ledger tokens = %d, want <= %d", got, budgeter.ContextLedgerTokens)
	}
	if len(compact) >= len(entries) {
		t.Fatalf("compact context ledger count = %d, want fewer than %d", len(compact), len(entries))
	}
}

func TestCompactArtifactsOmitsWorkerLogs(t *testing.T) {
	budgeter := DefaultReplanPromptBudgeter()
	stdoutMetadata := core.MustJSON(map[string]any{
		"bytes":   2_800_000,
		"content": strings.Repeat("remote stdout line\n", 10_000),
	})
	artifacts := []core.TaskArtifact{
		{
			ID:       "stdout-1",
			Kind:     "worker_log",
			Name:     "Remote stdout",
			Ref:      "/home/bot/work/worker/stdout.log",
			Metadata: stdoutMetadata,
		},
		{
			ID:   "pr-1",
			Kind: "github_pull_request",
			Name: "Add compact manager objective rows",
			URL:  "https://github.com/nathanwhit/aged/pull/123",
			Metadata: core.MustJSON(map[string]any{
				"number": 123,
			}),
		},
	}

	compact := budgeter.compactArtifacts(artifacts)
	if len(compact) != 1 {
		t.Fatalf("compact artifacts count = %d, want 1: %+v", len(compact), compact)
	}
	if compact[0].Kind != "github_pull_request" || compact[0].ID != "pr-1" {
		t.Fatalf("unexpected compact artifact: %+v", compact[0])
	}
	data, err := json.Marshal(compact)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "remote stdout") || strings.Contains(string(data), "contentPreview") || strings.Contains(string(data), "contentOmittedBytes") {
		t.Fatalf("worker log leaked into prompt artifacts: %s", data)
	}
}

func BenchmarkCompactContextLedgerLarge(b *testing.B) {
	budgeter := DefaultReplanPromptBudgeter()
	budgeter.ContextLedgerTokens = 6000
	entries := largeContextLedgerEntries(240)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compact := budgeter.compactContextLedger(entries)
		if len(compact) == 0 {
			b.Fatal("empty compact ledger")
		}
	}
}

func largeContextLedgerEntries(count int) []ContextLedgerEntry {
	entries := make([]ContextLedgerEntry, 0, count)
	for i := 0; i < count; i++ {
		entries = append(entries, ContextLedgerEntry{
			Kind:    "task_action",
			Status:  "rejected",
			Summary: fmt.Sprintf("context fact %03d: %s", i, strings.Repeat("large prompt detail ", 80)),
			Metadata: map[string]any{
				"turn": i,
			},
		})
	}
	return entries
}
