package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestLoadPluginRegistryIncludesBuiltinsAndConfiguredPlugins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plugins.json")
	if err := os.WriteFile(path, []byte(`{
		"plugins": [
			{
				"id": "driver:linear",
				"name": "Linear Driver",
				"kind": "driver",
				"enabled": true,
				"command": ["aged-linear"],
				"capabilities": ["issues"]
			}
		]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	registry, err := LoadPluginRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	plugins := registry.Snapshot()
	if len(plugins) < 2 {
		t.Fatalf("plugins = %+v, want builtins plus configured plugin", plugins)
	}
	var foundBuiltin, foundConfigured bool
	for _, plugin := range plugins {
		if plugin.ID == "runner:codex" && plugin.Enabled {
			foundBuiltin = true
		}
		if plugin.ID == "driver:linear" && plugin.Enabled && len(plugin.Command) == 1 {
			foundConfigured = true
		}
	}
	if !foundBuiltin || !foundConfigured {
		t.Fatalf("found builtin=%v configured=%v in %+v", foundBuiltin, foundConfigured, plugins)
	}
}

func TestPluginRegistryRejectsBuiltinMutation(t *testing.T) {
	registry := NewPluginRegistry(builtinPlugins())

	if err := registry.Delete("runner:codex"); err == nil || !strings.Contains(err.Error(), "built-in") {
		t.Fatalf("delete built-in err = %v", err)
	}
	if _, err := registry.Register(core.Plugin{ID: "runner:codex", Name: "Custom Codex", Kind: "runner", Enabled: false}); err == nil || !strings.Contains(err.Error(), "built-in") {
		t.Fatalf("replace built-in err = %v", err)
	}
	if _, err := registry.Register(core.Plugin{ID: "runner:codex", Name: "Custom Codex", Kind: "runner", Enabled: false, BuiltIn: true}); err == nil || !strings.Contains(err.Error(), "built-in") {
		t.Fatalf("replace built-in with builtIn=true err = %v", err)
	}

	plugins := registry.Snapshot()
	for _, plugin := range plugins {
		if plugin.ID == "runner:codex" && (!plugin.BuiltIn || !plugin.Enabled || plugin.Name != "Codex CLI Worker") {
			t.Fatalf("built-in plugin mutated: %+v", plugin)
		}
	}
}

func TestServiceRejectsBuiltinPluginMutation(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	service := NewService(store, StaticBrain{}, nil, t.TempDir())

	if _, err := service.RegisterPlugin(ctx, core.Plugin{
		ID:      "driver:discord",
		Name:    "Custom Discord Driver",
		Kind:    "driver",
		Enabled: true,
		BuiltIn: true,
	}); err == nil || !strings.Contains(err.Error(), "built-in") {
		t.Fatalf("service replace built-in err = %v", err)
	}

	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plugin, ok := pluginByID(snapshot.Plugins, "driver:discord")
	if !ok {
		t.Fatalf("missing built-in discord plugin: %+v", snapshot.Plugins)
	}
	if !plugin.BuiltIn || plugin.Name != "Discord Chat Driver" || plugin.Enabled {
		t.Fatalf("built-in plugin mutated: %+v", plugin)
	}

	persisted, err := store.ListPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 0 {
		t.Fatalf("persisted plugins = %+v, want none", persisted)
	}
}

func TestPluginRegistryDeleteMissingWrapsNotFound(t *testing.T) {
	registry := NewPluginRegistry(builtinPlugins())

	err := registry.Delete("integration:missing")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("delete missing err = %v, want ErrNotFound", err)
	}
	if err.Error() != "plugin not found" {
		t.Fatalf("delete missing message = %q", err.Error())
	}
}

func TestPluginRegistryProbesExecutablePluginDescribe(t *testing.T) {
	registry := NewPluginRegistry(corePluginFixture("driver:linear"))
	registry.probeCommand = func(_ context.Context, argv []string) ([]byte, error) {
		if got := strings.Join(argv, " "); got != "aged-linear describe" {
			t.Fatalf("argv = %q", got)
		}
		return []byte(`{"id":"driver:linear","name":"Linear Driver","kind":"driver","protocol":"aged-plugin-v1","capabilities":["issues","comments"]}`), nil
	}

	registry.Probe(context.Background())
	plugins := registry.Snapshot()
	if len(plugins) != 1 {
		t.Fatalf("plugins = %+v", plugins)
	}
	plugin := plugins[0]
	if plugin.Status != "ready" || plugin.Name != "Linear Driver" || len(plugin.Capabilities) != 2 {
		t.Fatalf("plugin = %+v", plugin)
	}
}

func TestPluginRegistrySupervisesDriverLifecycle(t *testing.T) {
	registry := NewPluginRegistry([]core.Plugin{managedDriverPlugin(t, "driver:test", "Test Driver", "echo driver-ready\nsleep 0.05\nexit 0")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.StartDrivers(ctx)
	plugin := waitForPluginLifecycle(t, registry.Snapshot, "driver:test", "driver did not report lifecycle state", func(plugin core.Plugin) bool {
		return plugin.Driver.Managed && (plugin.Status == "running" || plugin.Status == "stopped") && len(plugin.Driver.LogTail) > 0
	})
	if !strings.Contains(strings.Join(plugin.Driver.LogTail, "\n"), "driver-ready") {
		t.Fatalf("log tail = %+v", plugin.Driver.LogTail)
	}
}

func TestServiceDeletePluginRemovesRunningManagedDriverFromSnapshots(t *testing.T) {
	ctx := context.Background()
	plugin := managedDriverPlugin(t, "driver:delete-test", "Delete Test Driver", "echo driver-ready\nsleep 30")
	store := openTestStore(t)
	defer store.Close()
	service := NewService(store, StaticBrain{}, map[string]worker.Runner{}, t.TempDir())
	t.Cleanup(func() {
		_ = service.DeletePlugin(context.Background(), "driver:delete-test")
	})

	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}

	snapshotPlugins := func() []core.Plugin {
		snapshot, err := service.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot.Plugins
	}
	waitForPluginLifecycle(t, snapshotPlugins, "driver:delete-test", "driver did not start", runningManagedDriver)

	if err := service.DeletePlugin(ctx, "driver:delete-test"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pluginByID(snapshot.Plugins, "driver:delete-test"); ok {
		t.Fatalf("deleted plugin still present immediately after delete: %+v", snapshot.Plugins)
	}
	if _, ok := pluginByID(service.plugins.Snapshot(), "driver:delete-test"); ok {
		t.Fatalf("deleted plugin still present in runtime registry immediately after delete")
	}
	storedPlugins, err := store.ListPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pluginByID(storedPlugins, "driver:delete-test"); ok {
		t.Fatalf("deleted plugin still present in persistent plugin list: %+v", storedPlugins)
	}

	time.Sleep(150 * time.Millisecond)
	snapshot, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pluginByID(snapshot.Plugins, "driver:delete-test"); ok {
		t.Fatalf("deleted plugin was restored after supervisor exit: %+v", snapshot.Plugins)
	}
	if _, ok := pluginByID(service.plugins.Snapshot(), "driver:delete-test"); ok {
		t.Fatalf("deleted plugin was restored in runtime registry after supervisor exit")
	}
	if runner, ok := pluginByID(snapshot.Plugins, "runner:benchmark_compare"); ok && runner.Status != "ready" {
		t.Fatalf("unrelated plugin was mutated after delete: %+v", runner)
	}
}

func TestServiceDisablePluginCancelsRunningManagedDriver(t *testing.T) {
	ctx := context.Background()
	plugin := managedDriverPlugin(t, "driver:disable-test", "Disable Test Driver", "echo driver-ready\nsleep 30")
	store := openTestStore(t)
	defer store.Close()
	service := NewService(store, StaticBrain{}, map[string]worker.Runner{}, t.TempDir())
	t.Cleanup(func() {
		_ = service.DeletePlugin(context.Background(), "driver:disable-test")
	})

	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}

	snapshotPlugins := func() []core.Plugin {
		snapshot, err := service.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot.Plugins
	}
	waitForPluginLifecycle(t, snapshotPlugins, "driver:disable-test", "driver did not start", runningManagedDriver)

	plugin.Enabled = false
	disabled, err := service.RegisterPlugin(ctx, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.Status != "disabled" || disabled.Driver.Managed || disabled.Driver.PID != 0 || !disabled.Driver.StartedAt.IsZero() || disabled.Driver.RestartPolicy != "" {
		t.Fatalf("disabled plugin kept running lifecycle state: %+v", disabled)
	}
	service.plugins.mu.Lock()
	cancel := service.plugins.driverCancel["driver:disable-test"]
	service.plugins.mu.Unlock()
	if cancel != nil {
		t.Fatalf("disabled plugin kept driver cancel")
	}

	waitForPluginLifecycle(t, snapshotPlugins, "driver:disable-test", "disabled plugin lifecycle was not cleared after supervisor exit", disabledDriverLifecycleCleared)
}

func TestPluginRegistryCapturesLargeDriverLogLine(t *testing.T) {
	registry := NewPluginRegistry([]core.Plugin{managedDriverPlugin(t, "driver:test", "Test Driver", "printf '%02000000d\\n' 0\nexit 0")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.StartDrivers(ctx)
	plugin := waitForPluginLifecycle(t, registry.Snapshot, "driver:test", "driver did not capture large log line", func(plugin core.Plugin) bool {
		return len(plugin.Driver.LogTail) > 0
	})
	line := plugin.Driver.LogTail[0]
	if !strings.HasPrefix(line, "stdout: ") || len(strings.TrimPrefix(line, "stdout: ")) != 2000000 {
		t.Fatalf("log line length = %d, prefix ok = %v", len(strings.TrimPrefix(line, "stdout: ")), strings.HasPrefix(line, "stdout: "))
	}
}

func TestPluginRegistryLifecycleUpdatePreservesLargeDriverLogTail(t *testing.T) {
	registry := NewPluginRegistry([]core.Plugin{{
		ID:       "driver:test",
		Name:     "Test Driver",
		Kind:     "driver",
		Enabled:  true,
		Protocol: "aged-plugin-v1",
	}})
	stale := registry.Snapshot()[0]
	largeLine := "stdout: " + strings.Repeat("l", 2*1024*1024)
	registry.appendDriverLog(0, largeLine)

	stale.Status = "running"
	stale.Error = ""
	stale.Driver.Managed = true
	stale.Driver.PID = 1234
	registry.updatePlugin(0, stale)

	plugin := registry.Snapshot()[0]
	if plugin.Status != "running" || !plugin.Driver.Managed || plugin.Driver.PID != 1234 {
		t.Fatalf("lifecycle fields were not updated: %+v", plugin)
	}
	if len(plugin.Driver.LogTail) != 1 || plugin.Driver.LogTail[0] != largeLine {
		t.Fatalf("log tail was not preserved: len=%d", len(plugin.Driver.LogTail))
	}
}

func TestPluginRegistryExposesRunnerPlugins(t *testing.T) {
	registry := NewPluginRegistry([]core.Plugin{{
		ID:       "runner:lint",
		Name:     "Lint Runner",
		Kind:     "runner",
		Enabled:  true,
		Command:  []string{"aged-lint"},
		Protocol: "aged-runner-v1",
	}})
	runners := registry.RunnerPlugins()
	runner, ok := runners["lint"]
	if !ok {
		t.Fatalf("runners = %+v", runners)
	}
	if got := strings.Join(runner.BuildCommand(workerSpec("w1")), " "); got != "aged-lint run" {
		t.Fatalf("command = %q", got)
	}
}

func workerSpec(id string) worker.Spec {
	return worker.Spec{ID: id, Prompt: "run"}
}

func corePluginFixture(id string) []core.Plugin {
	return []core.Plugin{{
		ID:       id,
		Name:     id,
		Kind:     "driver",
		Enabled:  true,
		Command:  []string{"aged-linear"},
		Protocol: "aged-plugin-v1",
	}}
}

func managedDriverPlugin(t *testing.T, id, name, serveScript string) core.Plugin {
	t.Helper()
	path := filepath.Join(t.TempDir(), "driver.sh")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = describe ]; then
  printf '{"id":"%s","name":"%s","kind":"driver","protocol":"aged-plugin-v1"}\n'
  exit 0
fi
if [ "$1" = serve ]; then
%s
fi
`, id, name, serveScript)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return core.Plugin{
		ID:       id,
		Name:     name,
		Kind:     "driver",
		Enabled:  true,
		Command:  []string{path},
		Protocol: "aged-plugin-v1",
		Config:   map[string]string{"restart": "never"},
	}
}

func waitForPluginLifecycle(t *testing.T, snapshot func() []core.Plugin, id, failure string, predicate func(core.Plugin) bool) core.Plugin {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		plugins := snapshot()
		plugin, ok := pluginByID(plugins, id)
		if ok && predicate(plugin) {
			return plugin
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %+v", failure, plugins)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runningManagedDriver(plugin core.Plugin) bool {
	return plugin.Driver.Managed && plugin.Driver.PID != 0 && plugin.Status == "running"
}

func disabledDriverLifecycleCleared(plugin core.Plugin) bool {
	return plugin.Status == "disabled" && !plugin.Driver.Managed && plugin.Driver.PID == 0 && plugin.Driver.RestartPolicy == ""
}
