package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"aged/internal/worker"
)

func TestParseClaudeUsageCaptureFixture(t *testing.T) {
	usage := parseClaudeUsage(readUsageFixture(t, "claude_usage_capture.txt"), time.Unix(100, 0))
	if !usage.Available {
		t.Fatalf("usage unavailable: %+v", usage)
	}
	assertUsageWindow(t, usage, "Current session", 7, "3pm (America/Los_Angeles)")
	assertUsageWindow(t, usage, "Current week (all models)", 12, "May 24 at 7pm (America/Los_Angeles)")
	pressure, ok := providerUsagePressure(usage)
	if !ok || pressure != 12 {
		t.Fatalf("pressure = %d, %t; want 12 true", pressure, ok)
	}
}

func TestParseCodexStatusCaptureFixtureIgnoresConfiguredStatusLine(t *testing.T) {
	usage := parseCodexStatusUsage(readUsageFixture(t, "codex_status_capture.txt"), time.Unix(100, 0))
	if !usage.Available {
		t.Fatalf("usage unavailable: %+v", usage)
	}
	assertUsageWindow(t, usage, "5h limit", 1, "16:33")
	assertUsageWindow(t, usage, "Weekly limit", 0, "11:33 on 26 May")
	pressure, ok := providerUsagePressure(usage)
	if !ok || pressure != 1 {
		t.Fatalf("pressure = %d, %t; want 1 true", pressure, ok)
	}
}

func TestRebalancePlanWorkerKindSwitchesToLessUsedProvider(t *testing.T) {
	service := &Service{
		runners: map[string]worker.Runner{
			"codex":  eventRunner{kind: "codex"},
			"claude": eventRunner{kind: "claude"},
		},
		usageSource: staticProviderUsageSource{snapshot: ProviderUsageSnapshot{Providers: map[string]ProviderUsage{
			"codex":  usageWithPressure("codex", 98),
			"claude": usageWithPressure("claude", 25),
		}}},
	}
	plan := service.rebalancePlanWorkerKind(context.Background(), Plan{WorkerKind: "codex"})
	if plan.WorkerKind != "claude" {
		t.Fatalf("worker kind = %q, want claude", plan.WorkerKind)
	}
	if plan.Metadata["usageOriginalWorkerKind"] != "codex" || plan.Metadata["usageSelectedWorkerKind"] != "claude" {
		t.Fatalf("metadata = %+v", plan.Metadata)
	}
}

func TestRebalancePlanWorkerKindKeepsCloseProviderPressure(t *testing.T) {
	service := &Service{
		runners: map[string]worker.Runner{
			"codex":  eventRunner{kind: "codex"},
			"claude": eventRunner{kind: "claude"},
		},
		usageSource: staticProviderUsageSource{snapshot: ProviderUsageSnapshot{Providers: map[string]ProviderUsage{
			"codex":  usageWithPressure("codex", 55),
			"claude": usageWithPressure("claude", 48),
		}}},
	}
	plan := service.rebalancePlanWorkerKind(context.Background(), Plan{WorkerKind: "codex"})
	if plan.WorkerKind != "codex" {
		t.Fatalf("worker kind = %q, want codex", plan.WorkerKind)
	}
}

func TestRebalancePlanWorkerKindSwitchesWhenCurrentProviderUnavailable(t *testing.T) {
	service := &Service{
		runners: map[string]worker.Runner{
			"codex":  eventRunner{kind: "codex"},
			"claude": eventRunner{kind: "claude"},
		},
		usageSource: staticProviderUsageSource{snapshot: ProviderUsageSnapshot{Providers: map[string]ProviderUsage{
			"claude": {Kind: "claude", Available: false, Error: "usage limit reached"},
		}}},
	}
	plan := service.rebalancePlanWorkerKind(context.Background(), Plan{WorkerKind: "claude"})
	if plan.WorkerKind != "codex" {
		t.Fatalf("worker kind = %q, want codex", plan.WorkerKind)
	}
	if got := plan.Metadata["usageSelectionReason"]; got == "" {
		t.Fatalf("missing usage selection reason: %+v", plan.Metadata)
	}
}

func TestRebalancePlanWorkerKindKeepsPinnedWorkerKind(t *testing.T) {
	service := &Service{
		runners: map[string]worker.Runner{
			"codex":  eventRunner{kind: "codex"},
			"claude": eventRunner{kind: "claude"},
		},
		usageSource: staticProviderUsageSource{snapshot: ProviderUsageSnapshot{Providers: map[string]ProviderUsage{
			"codex":  usageWithPressure("codex", 100),
			"claude": usageWithPressure("claude", 0),
		}}},
	}
	plan := service.rebalancePlanWorkerKind(context.Background(), Plan{
		WorkerKind: "codex",
		Metadata:   map[string]any{"workerKindPinned": true},
	})
	if plan.WorkerKind != "codex" {
		t.Fatalf("worker kind = %q, want pinned codex", plan.WorkerKind)
	}
	if _, ok := plan.Metadata["usageSelectedWorkerKind"]; ok {
		t.Fatalf("pinned plan was usage-rebalanced: %+v", plan.Metadata)
	}
}

func TestClassifyProviderUsageExhaustion(t *testing.T) {
	exhaustion, ok := classifyProviderUsageExhaustion("claude", "Claude usage limit reached. Your limit resets at 3pm.")
	if !ok {
		t.Fatal("expected usage exhaustion")
	}
	if exhaustion.Provider != "claude" {
		t.Fatalf("provider = %q, want claude", exhaustion.Provider)
	}
}

func TestClassifyProviderUsageExhaustionIgnoresContextWindow(t *testing.T) {
	if exhaustion, ok := classifyProviderUsageExhaustion("codex", "model_context_window_exceeded: too many tokens"); ok {
		t.Fatalf("unexpected usage exhaustion: %+v", exhaustion)
	}
}
func readUsageFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertUsageWindow(t *testing.T, usage ProviderUsage, name string, used int, reset string) {
	t.Helper()
	for _, window := range usage.Windows {
		if window.Name == name {
			if window.UsedPercent != used || window.Reset != reset {
				t.Fatalf("window %q = %+v, want used=%d reset=%q", name, window, used, reset)
			}
			return
		}
	}
	t.Fatalf("missing usage window %q in %+v", name, usage.Windows)
}

func usageWithPressure(kind string, used int) ProviderUsage {
	return ProviderUsage{
		Kind:      kind,
		Available: true,
		Windows:   []ProviderUsageWindow{{Name: "test", UsedPercent: used}},
	}
}

type staticProviderUsageSource struct {
	snapshot ProviderUsageSnapshot
}

func (s staticProviderUsageSource) Snapshot(context.Context) ProviderUsageSnapshot {
	return s.snapshot
}
