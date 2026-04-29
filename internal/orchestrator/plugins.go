package orchestrator

import (
	"encoding/json"
	"os"
	"sort"
	"strings"

	"aged/internal/core"
)

type PluginRegistry struct {
	plugins []core.Plugin
}

type PluginsConfig struct {
	Plugins []core.Plugin `json:"plugins"`
}

func LoadPluginRegistry(path string) (*PluginRegistry, error) {
	plugins := builtinPlugins()
	if strings.TrimSpace(path) != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var config PluginsConfig
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
		plugins = append(plugins, config.Plugins...)
	}
	return NewPluginRegistry(plugins), nil
}

func NewPluginRegistry(plugins []core.Plugin) *PluginRegistry {
	byID := map[string]core.Plugin{}
	for _, plugin := range plugins {
		plugin.ID = strings.TrimSpace(plugin.ID)
		if plugin.ID == "" {
			continue
		}
		if plugin.Name == "" {
			plugin.Name = plugin.ID
		}
		if plugin.Kind == "" {
			plugin.Kind = "external"
		}
		byID[plugin.ID] = plugin
	}
	out := make([]core.Plugin, 0, len(byID))
	for _, plugin := range byID {
		out = append(out, plugin)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].ID < out[j].ID
		}
		return out[i].Kind < out[j].Kind
	})
	return &PluginRegistry{plugins: out}
}

func (r *PluginRegistry) Snapshot() []core.Plugin {
	if r == nil {
		return nil
	}
	out := make([]core.Plugin, len(r.plugins))
	copy(out, r.plugins)
	return out
}

func builtinPlugins() []core.Plugin {
	return []core.Plugin{
		{ID: "brain:prompt", Name: "Prompt Scheduler", Kind: "brain", Enabled: true, Capabilities: []string{"plan"}},
		{ID: "brain:codex", Name: "Codex Scheduler", Kind: "brain", Enabled: true, Capabilities: []string{"plan", "replan"}},
		{ID: "brain:api", Name: "OpenAI-Compatible Scheduler", Kind: "brain", Enabled: true, Capabilities: []string{"plan", "replan"}},
		{ID: "runner:codex", Name: "Codex CLI Worker", Kind: "runner", Enabled: true, Capabilities: []string{"code", "shell", "json-events"}},
		{ID: "runner:claude", Name: "Claude CLI Worker", Kind: "runner", Enabled: true, Capabilities: []string{"code", "review", "stream-events"}},
		{ID: "runner:shell", Name: "Shell Worker", Kind: "runner", Enabled: true, Capabilities: []string{"shell", "steering"}},
		{ID: "runner:benchmark_compare", Name: "Benchmark Comparator", Kind: "runner", Enabled: true, Capabilities: []string{"benchmark", "compare"}},
		{ID: "driver:http", Name: "HTTP Task Driver", Kind: "driver", Enabled: true, Capabilities: []string{"create-task", "dedupe-external-id"}},
		{ID: "driver:github", Name: "GitHub via HTTP Driver", Kind: "driver", Enabled: false, Capabilities: []string{"issues", "pull-requests", "status-refresh"}},
	}
}
