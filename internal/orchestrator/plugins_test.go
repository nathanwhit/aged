package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
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
