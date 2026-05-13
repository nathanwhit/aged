package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

type PluginRegistry struct {
	mu               sync.Mutex
	plugins          []core.Plugin
	driverCancel     map[string]context.CancelFunc
	driverGeneration map[string]uint64
	probeCommand     func(context.Context, []string) ([]byte, error)
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
		normalized, err := normalizePlugin(plugin)
		if err != nil {
			continue
		}
		if existing, ok := byID[normalized.ID]; ok && existing.BuiltIn {
			normalized.BuiltIn = true
		}
		byID[normalized.ID] = normalized
	}
	out := make([]core.Plugin, 0, len(byID))
	for _, plugin := range byID {
		out = append(out, plugin)
	}
	sortPlugins(out)
	return &PluginRegistry{plugins: out, driverCancel: map[string]context.CancelFunc{}, driverGeneration: map[string]uint64{}, probeCommand: runPluginCommand}
}

func sortPlugins(plugins []core.Plugin) {
	slices.SortFunc(plugins, func(a, b core.Plugin) int {
		if a.Kind == b.Kind {
			return strings.Compare(a.ID, b.ID)
		}
		return strings.Compare(a.Kind, b.Kind)
	})
}

func normalizePlugin(plugin core.Plugin) (core.Plugin, error) {
	plugin.ID = strings.TrimSpace(plugin.ID)
	if plugin.ID == "" {
		return core.Plugin{}, errors.New("plugin id is required")
	}
	plugin.Name = strings.TrimSpace(plugin.Name)
	if plugin.Name == "" {
		plugin.Name = plugin.ID
	}
	plugin.Kind = strings.TrimSpace(plugin.Kind)
	if plugin.Kind == "" {
		plugin.Kind = "external"
	}
	plugin.Protocol = strings.TrimSpace(plugin.Protocol)
	if plugin.Protocol == "" && len(plugin.Command) > 0 {
		plugin.Protocol = "aged-plugin-v1"
	}
	if plugin.Config == nil {
		plugin.Config = map[string]string{}
	}
	if plugin.Status == "" {
		if plugin.Enabled {
			plugin.Status = "ready"
		} else {
			plugin.Status = "disabled"
		}
	}
	return plugin, nil
}

func (r *PluginRegistry) Register(plugin core.Plugin) (core.Plugin, error) {
	if r == nil {
		return core.Plugin{}, errors.New("plugin registry is not configured")
	}
	normalized, err := normalizePlugin(plugin)
	if err != nil {
		return core.Plugin{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	replaced := false
	for index, existing := range r.plugins {
		if existing.ID == normalized.ID {
			if existing.BuiltIn && !normalized.BuiltIn {
				return core.Plugin{}, errors.New("built-in plugin cannot be replaced")
			}
			normalized.Driver = existing.Driver
			if existing.Enabled && !normalized.Enabled && existing.Kind == "driver" && existing.Driver.Managed {
				if cancel := r.driverCancel[existing.ID]; cancel != nil {
					cancel()
					delete(r.driverCancel, existing.ID)
					if r.driverGeneration == nil {
						r.driverGeneration = map[string]uint64{}
					}
					r.driverGeneration[existing.ID]++
				}
				normalized.Driver.Managed = false
				normalized.Driver.PID = 0
				normalized.Driver.StartedAt = time.Time{}
				normalized.Driver.RestartCount = 0
				normalized.Driver.RestartPolicy = ""
				normalized.Status = "disabled"
				normalized.Error = ""
			}
			r.plugins[index] = normalized
			replaced = true
			break
		}
	}
	if !replaced {
		r.plugins = append(r.plugins, normalized)
	}
	sortPlugins(r.plugins)
	return normalized, nil
}

func (r *PluginRegistry) Delete(id string) error {
	if r == nil {
		return errors.New("plugin registry is not configured")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("plugin id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for index, plugin := range r.plugins {
		if plugin.ID != id {
			continue
		}
		if plugin.BuiltIn {
			return errors.New("built-in plugin cannot be deleted")
		}
		if cancel := r.driverCancel[id]; cancel != nil {
			cancel()
			delete(r.driverCancel, id)
		}
		if r.driverGeneration == nil {
			r.driverGeneration = map[string]uint64{}
		}
		r.driverGeneration[id]++
		r.plugins = append(r.plugins[:index], r.plugins[index+1:]...)
		return nil
	}
	return notFoundError("plugin not found")
}

func (r *PluginRegistry) IsBuiltIn(id string) bool {
	if r == nil {
		return false
	}
	id = strings.TrimSpace(id)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, plugin := range r.plugins {
		if plugin.ID == id {
			return plugin.BuiltIn
		}
	}
	return false
}

func (r *PluginRegistry) Snapshot() []core.Plugin {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]core.Plugin, len(r.plugins))
	for i, plugin := range r.plugins {
		out[i] = clonePlugin(plugin)
	}
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
		out, err := r.probeCommand(probeCtx, pluginCommand(plugin, "describe"))
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
		driverCtx, cancel := context.WithCancel(ctx)
		generation := r.setDriverCancel(plugin.ID, cancel)
		go r.superviseDriver(driverCtx, index, plugin.ID, generation)
	}
}

func (r *PluginRegistry) superviseDriver(ctx context.Context, index int, id string, generation uint64) {
	for {
		plugin, ok := r.driverPluginAt(index, id, generation)
		if !ok || !plugin.Enabled {
			return
		}
		argv := pluginCommand(plugin, "serve")
		cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
		stdout, outErr := cmd.StdoutPipe()
		stderr, errErr := cmd.StderrPipe()
		if outErr != nil || errErr != nil {
			plugin.Status = "error"
			plugin.Error = strings.TrimSpace(nonEmpty(errorString(outErr), errorString(errErr)))
			r.updateDriverPlugin(index, plugin, generation)
			return
		}
		if err := cmd.Start(); err != nil {
			plugin.Status = "error"
			plugin.Error = err.Error()
			r.updateDriverPlugin(index, plugin, generation)
			return
		}
		plugin.Status = "running"
		plugin.Error = ""
		plugin.Driver.PID = cmd.Process.Pid
		plugin.Driver.StartedAt = time.Now().UTC()
		r.updateDriverPlugin(index, plugin, generation)
		go r.captureDriverLogs(ctx, index, plugin.ID, generation, "stdout", stdout)
		go r.captureDriverLogs(ctx, index, plugin.ID, generation, "stderr", stderr)
		err := cmd.Wait()
		plugin, ok = r.driverPluginAt(index, id, generation)
		if !ok {
			return
		}
		plugin.Driver.PID = 0
		plugin.Driver.LastExitAt = time.Now().UTC()
		if ctx.Err() != nil {
			if plugin.Enabled {
				plugin.Status = "stopped"
			} else {
				plugin.Status = "disabled"
				plugin.Driver.Managed = false
				plugin.Driver.StartedAt = time.Time{}
				plugin.Driver.RestartCount = 0
				plugin.Driver.RestartPolicy = ""
			}
			plugin.Error = ""
			r.updateDriverPlugin(index, plugin, generation)
			r.clearDriverCancel(plugin.ID, generation)
			return
		}
		if err != nil {
			plugin.Status = "error"
			plugin.Error = err.Error()
		} else {
			plugin.Status = "stopped"
			plugin.Error = ""
		}
		r.updateDriverPlugin(index, plugin, generation)
		if !shouldRestartPlugin(plugin, err) {
			r.clearDriverCancel(plugin.ID, generation)
			return
		}
		plugin.Driver.RestartCount++
		plugin.Status = "restarting"
		r.updateDriverPlugin(index, plugin, generation)
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartBackoff(plugin.Driver.RestartCount)):
		}
	}
}

func (r *PluginRegistry) setDriverCancel(id string, cancel context.CancelFunc) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.driverCancel == nil {
		r.driverCancel = map[string]context.CancelFunc{}
	}
	if r.driverGeneration == nil {
		r.driverGeneration = map[string]uint64{}
	}
	r.driverGeneration[id]++
	r.driverCancel[id] = cancel
	return r.driverGeneration[id]
}

func (r *PluginRegistry) clearDriverCancel(id string, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.driverGeneration[id] != generation {
		return
	}
	delete(r.driverCancel, id)
}

func (r *PluginRegistry) captureDriverLogs(ctx context.Context, index int, id string, generation uint64, stream string, reader io.Reader) {
	_ = worker.StreamReaderLines(ctx, stream, reader, func(line string) error {
		r.appendDriverLogForRun(index, id, generation, stream+": "+line)
		return nil
	}, nil)
}

func (r *PluginRegistry) appendDriverLog(index int, line string) {
	r.appendDriverLogFor(index, "", line)
}

func (r *PluginRegistry) appendDriverLogFor(index int, id string, line string) {
	r.appendDriverLogWithGeneration(index, id, 0, line)
}

func (r *PluginRegistry) appendDriverLogForRun(index int, id string, generation uint64, line string) {
	r.appendDriverLogWithGeneration(index, id, generation, line)
}

func (r *PluginRegistry) appendDriverLogWithGeneration(index int, id string, generation uint64, line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index, ok := r.indexForLocked(index, id)
	if !ok {
		return
	}
	if generation != 0 && r.driverGeneration[id] != generation {
		return
	}
	tail := append(slices.Clone(r.plugins[index].Driver.LogTail), line)
	if len(tail) > 50 {
		tail = tail[len(tail)-50:]
	}
	r.plugins[index].Driver.LogTail = tail
}

func (r *PluginRegistry) pluginAt(index int, id string) (core.Plugin, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index, ok := r.indexForLocked(index, id)
	if !ok {
		return core.Plugin{}, false
	}
	return clonePlugin(r.plugins[index]), true
}

func (r *PluginRegistry) driverPluginAt(index int, id string, generation uint64) (core.Plugin, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index, ok := r.indexForLocked(index, id)
	if !ok || r.driverGeneration[id] != generation {
		return core.Plugin{}, false
	}
	return clonePlugin(r.plugins[index]), true
}

func (r *PluginRegistry) updatePlugin(index int, plugin core.Plugin) {
	r.updatePluginForGeneration(index, plugin, 0)
}

func (r *PluginRegistry) updateDriverPlugin(index int, plugin core.Plugin, generation uint64) {
	r.updatePluginForGeneration(index, plugin, generation)
}

func (r *PluginRegistry) updatePluginForGeneration(index int, plugin core.Plugin, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index, ok := r.indexForLocked(index, plugin.ID)
	if !ok {
		return
	}
	if generation != 0 && r.driverGeneration[plugin.ID] != generation {
		return
	}
	plugin.Driver.LogTail = slices.Clone(r.plugins[index].Driver.LogTail)
	r.plugins[index] = clonePlugin(plugin)
}

func (r *PluginRegistry) indexForLocked(index int, id string) (int, bool) {
	if id == "" {
		return index, index >= 0 && index < len(r.plugins)
	}
	if index >= 0 && index < len(r.plugins) && r.plugins[index].ID == id {
		return index, true
	}
	for currentIndex, plugin := range r.plugins {
		if plugin.ID == id {
			return currentIndex, true
		}
	}
	return 0, false
}

func clonePlugin(plugin core.Plugin) core.Plugin {
	plugin.Command = slices.Clone(plugin.Command)
	plugin.Capabilities = slices.Clone(plugin.Capabilities)
	plugin.Driver.LogTail = slices.Clone(plugin.Driver.LogTail)
	plugin.Config = maps.Clone(plugin.Config)
	return plugin
}

func pluginCommand(plugin core.Plugin, subcommand string) []string {
	return append(slices.Clone(plugin.Command), subcommand)
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
		{ID: "brain:prompt", Name: "Prompt Scheduler", Kind: "brain", Enabled: true, BuiltIn: true, Capabilities: []string{"plan"}},
		{ID: "brain:codex", Name: "Codex Scheduler", Kind: "brain", Enabled: true, BuiltIn: true, Capabilities: []string{"plan", "replan"}},
		{ID: "brain:api", Name: "OpenAI-Compatible Scheduler", Kind: "brain", Enabled: true, BuiltIn: true, Capabilities: []string{"plan", "replan"}},
		{ID: "runner:codex", Name: "Codex CLI Worker", Kind: "runner", Enabled: true, BuiltIn: true, Capabilities: []string{"code", "shell", "json-events"}},
		{ID: "runner:claude", Name: "Claude CLI Worker", Kind: "runner", Enabled: true, BuiltIn: true, Capabilities: []string{"code", "review", "stream-events"}},
		{ID: "runner:shell", Name: "Shell Worker", Kind: "runner", Enabled: true, BuiltIn: true, Capabilities: []string{"shell", "steering"}},
		{ID: "runner:benchmark_compare", Name: "Benchmark Comparator", Kind: "runner", Enabled: true, BuiltIn: true, Capabilities: []string{"benchmark", "compare"}},
		{ID: "driver:http", Name: "HTTP Task Driver", Kind: "driver", Enabled: true, BuiltIn: true, Capabilities: []string{"create-task", "dedupe-external-id"}},
		{ID: "driver:github", Name: "GitHub Issue Polling Driver", Kind: "driver", Enabled: false, BuiltIn: true, Capabilities: []string{"issues", "auto-publish"}},
		{ID: "driver:discord", Name: "Discord Chat Driver", Kind: "driver", Enabled: false, BuiltIn: true, Capabilities: []string{"chat", "create-task", "manage-aged"}},
	}
}
