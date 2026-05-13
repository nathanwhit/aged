package orchestrator

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"aged/internal/core"
)

type DriverRegistry struct {
	service *Service
	github  *GitHubDriverRuntime
	discord *DiscordDriverRuntime
}

func NewDriverRegistry(service *Service) *DriverRegistry {
	registry := &DriverRegistry{service: service}
	registry.github = &GitHubDriverRuntime{service: service}
	registry.discord = &DiscordDriverRuntime{service: service}
	return registry
}

func (r *DriverRegistry) SetRuntimeContext(ctx context.Context) {
	if r == nil {
		return
	}
	r.github.SetRuntimeContext(ctx)
	r.discord.SetRuntimeContext(ctx)
}

func (r *DriverRegistry) SetGitHubClient(client GitHubClient) {
	if r == nil {
		return
	}
	r.github.SetClient(client)
}

func (r *DriverRegistry) SetDiscordClient(client DiscordClient) {
	if r == nil {
		return
	}
	r.discord.SetClient(client)
}

func (r *DriverRegistry) GitHubState() GitHubDriverRuntimeState {
	if r == nil {
		return GitHubDriverRuntimeState{}
	}
	return r.github.State()
}

func (r *DriverRegistry) DiscordState() DiscordDriverRuntimeState {
	if r == nil {
		return DiscordDriverRuntimeState{}
	}
	return r.discord.State()
}

func (r *DriverRegistry) StartGitHubDriver(ctx context.Context, config GitHubDriverConfig) (GitHubDriverRuntimeState, error) {
	if r == nil {
		return GitHubDriverRuntimeState{}, nil
	}
	return r.github.Start(ctx, config)
}

func (r *DriverRegistry) ConfigureGitHubDriver(config GitHubDriverConfig) (GitHubDriverRuntimeState, error) {
	if r == nil {
		return GitHubDriverRuntimeState{}, nil
	}
	return r.github.Configure(config)
}

func (r *DriverRegistry) StartDiscordDriver(ctx context.Context, config DiscordDriverConfig) (DiscordDriverRuntimeState, error) {
	if r == nil {
		return DiscordDriverRuntimeState{}, nil
	}
	return r.discord.Start(ctx, config)
}

func (r *DriverRegistry) ConfigureDiscordDriver(config DiscordDriverConfig) (DiscordDriverRuntimeState, error) {
	if r == nil {
		return DiscordDriverRuntimeState{}, nil
	}
	return r.discord.Configure(config)
}

func (r *DriverRegistry) Refresh() (GitHubDriverRuntimeState, error) {
	if r == nil {
		return GitHubDriverRuntimeState{}, nil
	}
	return r.github.Refresh()
}

func (r *DriverRegistry) DecoratePlugins(plugins []core.Plugin) []core.Plugin {
	if r == nil {
		return plugins
	}
	plugins = r.github.DecoratePlugin(plugins)
	return r.discord.DecoratePlugin(plugins)
}

type GitHubDriverRuntimeState struct {
	Config    GitHubDriverConfig `json:"config"`
	Running   bool               `json:"running"`
	StartedAt string             `json:"startedAt,omitempty"`
	UpdatedAt string             `json:"updatedAt,omitempty"`
	LastRunAt string             `json:"lastRunAt,omitempty"`
	LastError string             `json:"lastError,omitempty"`
}

type DiscordDriverRuntimeState struct {
	Config    DiscordDriverConfig `json:"config"`
	Running   bool                `json:"running"`
	StartedAt string              `json:"startedAt,omitempty"`
	UpdatedAt string              `json:"updatedAt,omitempty"`
	LastRunAt string              `json:"lastRunAt,omitempty"`
	LastError string              `json:"lastError,omitempty"`
}

type GitHubDriverRuntime struct {
	mu         sync.Mutex
	service    *Service
	ctx        context.Context
	cancel     context.CancelFunc
	baseConfig GitHubDriverConfig
	config     GitHubDriverConfig
	client     GitHubClient
	generation int
	running    bool
	startedAt  time.Time
	updatedAt  time.Time
	lastRunAt  time.Time
	lastError  string
}

type DiscordDriverRuntime struct {
	mu         sync.Mutex
	service    *Service
	ctx        context.Context
	cancel     context.CancelFunc
	config     DiscordDriverConfig
	client     DiscordClient
	generation int
	running    bool
	startedAt  time.Time
	updatedAt  time.Time
	lastRunAt  time.Time
	lastError  string
}

func (d *GitHubDriverRuntime) SetRuntimeContext(ctx context.Context) {
	if d == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()
}

func (d *GitHubDriverRuntime) SetClient(client GitHubClient) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.client = client
}

func (d *GitHubDriverRuntime) State() GitHubDriverRuntimeState {
	if d == nil {
		return GitHubDriverRuntimeState{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stateLocked()
}

func (d *GitHubDriverRuntime) Start(ctx context.Context, config GitHubDriverConfig) (GitHubDriverRuntimeState, error) {
	d.SetRuntimeContext(ctx)
	return d.Configure(config)
}

func (d *GitHubDriverRuntime) Configure(config GitHubDriverConfig) (GitHubDriverRuntimeState, error) {
	if d == nil {
		return GitHubDriverRuntimeState{}, nil
	}
	config = normalizeGitHubDriverConfig(config)
	effectiveConfig := d.effectiveConfig(config)
	now := time.Now().UTC()

	d.mu.Lock()
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	d.generation++
	generation := d.generation
	d.baseConfig = config
	d.config = effectiveConfig
	d.updatedAt = now
	d.lastError = ""

	if !effectiveConfig.Enabled {
		d.running = false
		d.startedAt = time.Time{}
		state := d.stateLocked()
		d.mu.Unlock()
		return state, nil
	}

	rootCtx := d.ctx
	if rootCtx == nil {
		rootCtx = context.Background()
		d.ctx = rootCtx
	}
	driverCtx, cancel := context.WithCancel(rootCtx)
	d.cancel = cancel
	d.running = true
	d.startedAt = now
	client := d.client
	service := d.service
	driver := NewGitHubDriver(service, effectiveConfig, client)
	state := d.stateLocked()
	d.mu.Unlock()

	go d.run(driverCtx, generation, driver, time.Duration(effectiveConfig.IntervalSeconds)*time.Second)
	return state, nil
}

func (d *GitHubDriverRuntime) Refresh() (GitHubDriverRuntimeState, error) {
	if d == nil {
		return GitHubDriverRuntimeState{}, nil
	}
	d.mu.Lock()
	config := d.baseConfig
	d.mu.Unlock()
	return d.Configure(config)
}

func (d *GitHubDriverRuntime) effectiveConfig(config GitHubDriverConfig) GitHubDriverConfig {
	config = normalizeGitHubDriverConfig(config)
	if d == nil || d.service == nil || d.service.projects == nil {
		return config
	}
	for _, project := range d.service.projects.Snapshot() {
		repo := strings.TrimSpace(project.UpstreamRepo)
		if repo == "" {
			repo = strings.TrimSpace(project.Repo)
		}
		if repo == "" {
			continue
		}
		if project.GitHubIssues.Enabled {
			config.Issues = append(config.Issues, GitHubIssueSourceConfig{
				Repo:        repo,
				Labels:      append([]string(nil), project.GitHubIssues.Labels...),
				ProjectID:   project.ID,
				IssueLimit:  project.GitHubIssues.IssueLimit,
				AutoPublish: project.GitHubIssues.AutoPublish,
			})
		}
		if project.GitHubMentions.Enabled {
			enabled := true
			config.Mentions.Enabled = &enabled
			if !containsFold(config.Mentions.Repos, repo) {
				config.Mentions.Repos = append(config.Mentions.Repos, repo)
			}
			for _, reason := range project.GitHubMentions.Reasons {
				if !containsFold(config.Mentions.Reasons, reason) {
					config.Mentions.Reasons = append(config.Mentions.Reasons, reason)
				}
			}
			if project.GitHubMentions.Limit > config.Mentions.Limit {
				config.Mentions.Limit = project.GitHubMentions.Limit
			}
		}
	}
	return config
}

func containsFold(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return true
	}
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), needle) {
			return true
		}
	}
	return false
}

func (d *GitHubDriverRuntime) run(ctx context.Context, generation int, driver *GitHubDriver, interval time.Duration) {
	defer d.finish(generation)
	d.runOnce(ctx, generation, driver)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runOnce(ctx, generation, driver)
		}
	}
}

func (d *GitHubDriverRuntime) runOnce(ctx context.Context, generation int, driver *GitHubDriver) {
	if err := ctx.Err(); err != nil {
		return
	}
	err := driver.RunOnce(ctx)
	d.mu.Lock()
	if d.generation == generation {
		d.lastRunAt = time.Now().UTC()
		if err != nil {
			d.lastError = err.Error()
		} else {
			d.lastError = ""
		}
	}
	d.mu.Unlock()
	if err != nil {
		slog.Warn("github driver poll failed", "error", err)
	}
}

func (d *GitHubDriverRuntime) finish(generation int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.generation != generation {
		return
	}
	d.running = false
	d.cancel = nil
}

func (d *GitHubDriverRuntime) stateLocked() GitHubDriverRuntimeState {
	return GitHubDriverRuntimeState{
		Config:    d.config,
		Running:   d.running,
		StartedAt: formatRuntimeTime(d.startedAt),
		UpdatedAt: formatRuntimeTime(d.updatedAt),
		LastRunAt: formatRuntimeTime(d.lastRunAt),
		LastError: d.lastError,
	}
}

func (d *GitHubDriverRuntime) DecoratePlugin(plugins []core.Plugin) []core.Plugin {
	d.mu.Lock()
	defer d.mu.Unlock()
	for index := range plugins {
		if plugins[index].ID != "driver:github" {
			continue
		}
		plugins[index].Enabled = d.config.Enabled
		plugins[index].Error = d.lastError
		plugins[index].Driver.Managed = true
		plugins[index].Driver.StartedAt = d.startedAt
		plugins[index].Driver.RestartPolicy = "runtime-config"
		switch {
		case d.running:
			plugins[index].Status = "running"
		case d.config.Enabled:
			plugins[index].Status = "stopped"
		default:
			plugins[index].Status = "disabled"
		}
		return plugins
	}
	return plugins
}

func (d *DiscordDriverRuntime) SetRuntimeContext(ctx context.Context) {
	if d == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()
}

func (d *DiscordDriverRuntime) SetClient(client DiscordClient) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.client = client
}

func (d *DiscordDriverRuntime) State() DiscordDriverRuntimeState {
	if d == nil {
		return DiscordDriverRuntimeState{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stateLocked()
}

func (d *DiscordDriverRuntime) Start(ctx context.Context, config DiscordDriverConfig) (DiscordDriverRuntimeState, error) {
	d.SetRuntimeContext(ctx)
	return d.Configure(config)
}

func (d *DiscordDriverRuntime) Configure(config DiscordDriverConfig) (DiscordDriverRuntimeState, error) {
	if d == nil {
		return DiscordDriverRuntimeState{}, nil
	}
	config = normalizeDiscordDriverConfig(config)
	now := time.Now().UTC()

	d.mu.Lock()
	if strings.TrimSpace(config.Token) == "" && strings.TrimSpace(d.config.Token) != "" {
		config.Token = d.config.Token
	}
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	d.generation++
	generation := d.generation
	d.config = config
	d.updatedAt = now
	d.lastError = ""

	if !config.Enabled {
		d.running = false
		d.startedAt = time.Time{}
		state := d.stateLocked()
		d.mu.Unlock()
		return state, nil
	}

	rootCtx := d.ctx
	if rootCtx == nil {
		rootCtx = context.Background()
		d.ctx = rootCtx
	}
	driverCtx, cancel := context.WithCancel(rootCtx)
	d.cancel = cancel
	d.running = true
	d.startedAt = now
	client := d.client
	service := d.service
	driver := NewDiscordDriver(service, config, client)
	state := d.stateLocked()
	d.mu.Unlock()

	go d.run(driverCtx, generation, driver, time.Duration(config.IntervalSeconds)*time.Second)
	return state, nil
}

func (d *DiscordDriverRuntime) run(ctx context.Context, generation int, driver *DiscordDriver, interval time.Duration) {
	defer d.finish(generation)
	d.runOnce(ctx, generation, driver)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runOnce(ctx, generation, driver)
		}
	}
}

func (d *DiscordDriverRuntime) runOnce(ctx context.Context, generation int, driver *DiscordDriver) {
	if err := ctx.Err(); err != nil {
		return
	}
	err := driver.RunOnce(ctx)
	d.mu.Lock()
	if d.generation == generation {
		d.lastRunAt = time.Now().UTC()
		if err != nil {
			d.lastError = err.Error()
		} else {
			d.lastError = ""
		}
	}
	d.mu.Unlock()
	if err != nil {
		slog.Warn("discord driver poll failed", "error", err)
	}
}

func (d *DiscordDriverRuntime) finish(generation int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.generation != generation {
		return
	}
	d.running = false
	d.cancel = nil
}

func (d *DiscordDriverRuntime) stateLocked() DiscordDriverRuntimeState {
	config := d.config
	config.Token = ""
	return DiscordDriverRuntimeState{
		Config:    config,
		Running:   d.running,
		StartedAt: formatRuntimeTime(d.startedAt),
		UpdatedAt: formatRuntimeTime(d.updatedAt),
		LastRunAt: formatRuntimeTime(d.lastRunAt),
		LastError: d.lastError,
	}
}

func (d *DiscordDriverRuntime) DecoratePlugin(plugins []core.Plugin) []core.Plugin {
	d.mu.Lock()
	defer d.mu.Unlock()
	for index := range plugins {
		if plugins[index].ID != "driver:discord" {
			continue
		}
		plugins[index].Enabled = d.config.Enabled
		plugins[index].Error = d.lastError
		plugins[index].Driver.Managed = true
		plugins[index].Driver.StartedAt = d.startedAt
		plugins[index].Driver.RestartPolicy = "runtime-config"
		switch {
		case d.running:
			plugins[index].Status = "running"
		case d.config.Enabled:
			plugins[index].Status = "stopped"
		default:
			plugins[index].Status = "disabled"
		}
		return plugins
	}
	return plugins
}

func formatRuntimeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
