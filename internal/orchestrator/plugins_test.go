package orchestrator

import (
	"context"
	"errors"
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
	path := filepath.Join(t.TempDir(), "driver.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nif [ \"$1\" = serve ]; then echo driver-ready; sleep 0.05; exit 0; fi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	registry := NewPluginRegistry([]core.Plugin{{
		ID:       "driver:test",
		Name:     "Test Driver",
		Kind:     "driver",
		Enabled:  true,
		Command:  []string{path},
		Protocol: "aged-plugin-v1",
		Config:   map[string]string{"restart": "never"},
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.StartDrivers(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		plugin := registry.Snapshot()[0]
		if plugin.Driver.Managed && (plugin.Status == "running" || plugin.Status == "stopped") && len(plugin.Driver.LogTail) > 0 {
			if !strings.Contains(strings.Join(plugin.Driver.LogTail, "\n"), "driver-ready") {
				t.Fatalf("log tail = %+v", plugin.Driver.LogTail)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("driver did not report lifecycle state: %+v", registry.Snapshot())
}

func TestServiceDeletePluginRemovesRunningManagedDriverFromSnapshots(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "driver.sh")
	if err := os.WriteFile(path, []byte(`#!/bin/sh
if [ "$1" = describe ]; then
  printf '{"id":"driver:delete-test","name":"Delete Test Driver","kind":"driver","protocol":"aged-plugin-v1"}\n'
  exit 0
fi
if [ "$1" = serve ]; then
  echo driver-ready
  sleep 30
fi
`), 0o755); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t)
	defer store.Close()
	service := NewService(store, StaticBrain{}, map[string]worker.Runner{}, t.TempDir())
	t.Cleanup(func() {
		_ = service.DeletePlugin(context.Background(), "driver:delete-test")
	})

	if _, err := service.RegisterPlugin(ctx, core.Plugin{
		ID:       "driver:delete-test",
		Name:     "Delete Test Driver",
		Kind:     "driver",
		Enabled:  true,
		Command:  []string{path},
		Protocol: "aged-plugin-v1",
		Config:   map[string]string{"restart": "never"},
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, err := service.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plugin, ok := pluginByID(snapshot.Plugins, "driver:delete-test")
		if ok && plugin.Driver.Managed && plugin.Driver.PID != 0 && plugin.Status == "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver did not start: %+v", snapshot.Plugins)
		}
		time.Sleep(10 * time.Millisecond)
	}

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
	path := filepath.Join(t.TempDir(), "driver.sh")
	if err := os.WriteFile(path, []byte(`#!/bin/sh
if [ "$1" = describe ]; then
  printf '{"id":"driver:disable-test","name":"Disable Test Driver","kind":"driver","protocol":"aged-plugin-v1"}\n'
  exit 0
fi
if [ "$1" = serve ]; then
  echo driver-ready
  sleep 30
fi
`), 0o755); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t)
	defer store.Close()
	service := NewService(store, StaticBrain{}, map[string]worker.Runner{}, t.TempDir())
	t.Cleanup(func() {
		_ = service.DeletePlugin(context.Background(), "driver:disable-test")
	})

	if _, err := service.RegisterPlugin(ctx, core.Plugin{
		ID:       "driver:disable-test",
		Name:     "Disable Test Driver",
		Kind:     "driver",
		Enabled:  true,
		Command:  []string{path},
		Protocol: "aged-plugin-v1",
		Config:   map[string]string{"restart": "never"},
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, err := service.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plugin, ok := pluginByID(snapshot.Plugins, "driver:disable-test")
		if ok && plugin.Driver.Managed && plugin.Driver.PID != 0 && plugin.Status == "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver did not start: %+v", snapshot.Plugins)
		}
		time.Sleep(10 * time.Millisecond)
	}

	disabled, err := service.RegisterPlugin(ctx, core.Plugin{
		ID:       "driver:disable-test",
		Name:     "Disable Test Driver",
		Kind:     "driver",
		Enabled:  false,
		Command:  []string{path},
		Protocol: "aged-plugin-v1",
		Config:   map[string]string{"restart": "never"},
	})
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

	deadline = time.Now().Add(2 * time.Second)
	for {
		snapshot, err := service.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plugin, ok := pluginByID(snapshot.Plugins, "driver:disable-test")
		if ok && plugin.Status == "disabled" && !plugin.Driver.Managed && plugin.Driver.PID == 0 && plugin.Driver.RestartPolicy == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("disabled plugin lifecycle was not cleared after supervisor exit: %+v", snapshot.Plugins)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPluginRegistryCapturesLargeDriverLogLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "driver.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nif [ \"$1\" = serve ]; then printf '%02000000d\\n' 0; exit 0; fi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	registry := NewPluginRegistry([]core.Plugin{{
		ID:       "driver:test",
		Name:     "Test Driver",
		Kind:     "driver",
		Enabled:  true,
		Command:  []string{path},
		Protocol: "aged-plugin-v1",
		Config:   map[string]string{"restart": "never"},
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.StartDrivers(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		plugin := registry.Snapshot()[0]
		if len(plugin.Driver.LogTail) > 0 {
			line := plugin.Driver.LogTail[0]
			if !strings.HasPrefix(line, "stdout: ") || len(strings.TrimPrefix(line, "stdout: ")) != 2000000 {
				t.Fatalf("log line length = %d, prefix ok = %v", len(strings.TrimPrefix(line, "stdout: ")), strings.HasPrefix(line, "stdout: "))
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("driver did not capture large log line: %+v", registry.Snapshot())
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
	registry.appendDriverLog(0, "", 0, largeLine)

	stale.Status = "running"
	stale.Error = ""
	stale.Driver.Managed = true
	stale.Driver.PID = 1234
	registry.updatePlugin(0, stale, 0)

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
