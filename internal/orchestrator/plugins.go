package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

type PluginRegistry struct {
	mu           sync.Mutex
	plugins      []core.Plugin
	probeCommand func(context.Context, []string) ([]byte, error)
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
		if plugin.Protocol == "" && len(plugin.Command) > 0 {
			plugin.Protocol = "aged-plugin-v1"
		}
		if plugin.Status == "" {
			if plugin.Enabled {
				plugin.Status = "ready"
			} else {
				plugin.Status = "disabled"
			}
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
	return &PluginRegistry{plugins: out, probeCommand: runPluginCommand}
}

func (r *PluginRegistry) Snapshot() []core.Plugin {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]core.Plugin, len(r.plugins))
	copy(out, r.plugins)
	return out
}

func (r *PluginRegistry) RunnerPlugins() map[string]worker.Runner {
	out := map[string]worker.Runner{}
	if r == nil {
		return out
	}
	for _, plugin := range r.Snapshot() {
		if !plugin.Enabled || plugin.Kind != "runner" || plugin.Protocol != "aged-runner-v1" || len(plugin.Command) == 0 {
			continue
		}
		kind := strings.TrimPrefix(plugin.ID, "runner:")
		if strings.TrimSpace(kind) == "" {
			continue
		}
		out[kind] = worker.NewPluginRunner(kind, plugin.Command)
	}
	return out
}

func (r *PluginRegistry) Probe(ctx context.Context) {
	if r == nil {
		return
	}
	for index, plugin := range r.Snapshot() {
		if !plugin.Enabled || len(plugin.Command) == 0 || plugin.Protocol != "aged-plugin-v1" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		out, err := r.probeCommand(probeCtx, append(append([]string{}, plugin.Command...), "describe"))
		cancel()
		if err != nil {
			plugin.Status = "error"
			plugin.Error = strings.TrimSpace(err.Error())
			r.updatePlugin(index, plugin)
			continue
		}
		var described core.Plugin
		if err := json.Unmarshal(bytes.TrimSpace(out), &described); err != nil {
			plugin.Status = "error"
			plugin.Error = "decode plugin describe: " + err.Error()
			r.updatePlugin(index, plugin)
			continue
		}
		if described.ID != "" && described.ID != plugin.ID {
			plugin.Status = "error"
			plugin.Error = "plugin described mismatched id " + described.ID
			r.updatePlugin(index, plugin)
			continue
		}
		plugin.Status = "ready"
		plugin.Error = ""
		if described.Name != "" {
			plugin.Name = described.Name
		}
		if described.Kind != "" {
			plugin.Kind = described.Kind
		}
		if described.Protocol != "" {
			plugin.Protocol = described.Protocol
		}
		if len(described.Capabilities) > 0 {
			plugin.Capabilities = described.Capabilities
		}
		if described.Endpoint != "" {
			plugin.Endpoint = described.Endpoint
		}
		if len(described.Config) > 0 {
			plugin.Config = described.Config
		}
		r.updatePlugin(index, plugin)
	}
}

func (r *PluginRegistry) StartDrivers(ctx context.Context) {
	if r == nil {
		return
	}
	for index, plugin := range r.Snapshot() {
		if !plugin.Enabled || plugin.Kind != "driver" || plugin.Protocol != "aged-plugin-v1" || len(plugin.Command) == 0 {
			continue
		}
		if plugin.Driver.Managed {
			continue
		}
		plugin.Driver.Managed = true
		plugin.Driver.RestartPolicy = nonEmpty(plugin.Config["restart"], "on_failure")
		plugin.Status = "starting"
		r.updatePlugin(index, plugin)
		go r.superviseDriver(ctx, index)
	}
}

func (r *PluginRegistry) superviseDriver(ctx context.Context, index int) {
	for {
		plugin, ok := r.pluginAt(index)
		if !ok || !plugin.Enabled {
			return
		}
		argv := append(append([]string{}, plugin.Command...), "serve")
		cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
		stdout, outErr := cmd.StdoutPipe()
		stderr, errErr := cmd.StderrPipe()
		if outErr != nil || errErr != nil {
			plugin.Status = "error"
			plugin.Error = strings.TrimSpace(nonEmpty(errorString(outErr), errorString(errErr)))
			r.updatePlugin(index, plugin)
			return
		}
		if err := cmd.Start(); err != nil {
			plugin.Status = "error"
			plugin.Error = err.Error()
			r.updatePlugin(index, plugin)
			return
		}
		plugin.Status = "running"
		plugin.Error = ""
		plugin.Driver.PID = cmd.Process.Pid
		plugin.Driver.StartedAt = time.Now().UTC()
		r.updatePlugin(index, plugin)
		go r.captureDriverLogs(index, "stdout", stdout)
		go r.captureDriverLogs(index, "stderr", stderr)
		err := cmd.Wait()
		plugin, ok = r.pluginAt(index)
		if !ok {
			return
		}
		plugin.Driver.PID = 0
		plugin.Driver.LastExitAt = time.Now().UTC()
		if ctx.Err() != nil {
			plugin.Status = "stopped"
			plugin.Error = ""
			r.updatePlugin(index, plugin)
			return
		}
		if err != nil {
			plugin.Status = "error"
			plugin.Error = err.Error()
		} else {
			plugin.Status = "stopped"
			plugin.Error = ""
		}
		r.updatePlugin(index, plugin)
		if !shouldRestartPlugin(plugin, err) {
			return
		}
		plugin.Driver.RestartCount++
		plugin.Status = "restarting"
		r.updatePlugin(index, plugin)
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartBackoff(plugin.Driver.RestartCount)):
		}
	}
}

func (r *PluginRegistry) captureDriverLogs(index int, stream string, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		r.appendDriverLog(index, stream+": "+scanner.Text())
	}
}

func (r *PluginRegistry) appendDriverLog(index int, line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.plugins) {
		return
	}
	tail := append(r.plugins[index].Driver.LogTail, line)
	if len(tail) > 50 {
		tail = tail[len(tail)-50:]
	}
	r.plugins[index].Driver.LogTail = tail
}

func (r *PluginRegistry) pluginAt(index int) (core.Plugin, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.plugins) {
		return core.Plugin{}, false
	}
	return r.plugins[index], true
}

func (r *PluginRegistry) updatePlugin(index int, plugin core.Plugin) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.plugins) {
		return
	}
	r.plugins[index] = plugin
}

func shouldRestartPlugin(plugin core.Plugin, err error) bool {
	switch strings.ToLower(nonEmpty(plugin.Driver.RestartPolicy, plugin.Config["restart"], "on_failure")) {
	case "always":
		return true
	case "never", "none":
		return false
	default:
		return err != nil
	}
}

func restartBackoff(count int) time.Duration {
	if count < 1 {
		count = 1
	}
	if count > 5 {
		count = 5
	}
	return time.Duration(count) * time.Second
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func runPluginCommand(ctx context.Context, argv []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	return cmd.Output()
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
