package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aged/internal/core"
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
