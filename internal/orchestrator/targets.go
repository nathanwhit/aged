package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"aged/internal/core"
)

type TargetKind string

const (
	TargetKindLocal TargetKind = "local"
	TargetKindSSH   TargetKind = "ssh"
)

type TargetCapacity struct {
	MaxWorkers int     `json:"maxWorkers"`
	CPUWeight  float64 `json:"cpuWeight,omitempty"`
	MemoryGB   float64 `json:"memoryGB,omitempty"`
}

type TargetConfig struct {
	ID                    string            `json:"id"`
	Kind                  TargetKind        `json:"kind"`
	Host                  string            `json:"host,omitempty"`
	User                  string            `json:"user,omitempty"`
	Port                  int               `json:"port,omitempty"`
	IdentityFile          string            `json:"identityFile,omitempty"`
	InsecureIgnoreHostKey bool              `json:"insecureIgnoreHostKey,omitempty"`
	WorkDir               string            `json:"workDir,omitempty"`
	WorkRoot              string            `json:"workRoot,omitempty"`
	Labels                map[string]string `json:"labels,omitempty"`
	Capacity              TargetCapacity    `json:"capacity,omitempty"`
}

type TargetRegistry struct {
	mu      sync.Mutex
	targets map[string]TargetConfig
	running map[string]int
}

func NewLocalTargetRegistry() *TargetRegistry {
	return NewTargetRegistry([]TargetConfig{{
		ID:   "local",
		Kind: TargetKindLocal,
		Labels: map[string]string{
			"location": "local",
		},
		Capacity: TargetCapacity{MaxWorkers: 16, CPUWeight: 1},
	}})
}

func LoadTargetRegistry(path string) (*TargetRegistry, error) {
	if strings.TrimSpace(path) == "" {
		return NewLocalTargetRegistry(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Targets []TargetConfig `json:"targets"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	if len(payload.Targets) == 0 {
		return nil, errors.New("target config must include at least one target")
	}
	return NewTargetRegistry(payload.Targets), nil
}

func NewTargetRegistry(configs []TargetConfig) *TargetRegistry {
	registry := &TargetRegistry{
		targets: map[string]TargetConfig{},
		running: map[string]int{},
	}
	for _, config := range configs {
		if strings.TrimSpace(config.ID) == "" {
			continue
		}
		if config.Kind == "" {
			config.Kind = TargetKindLocal
		}
		if config.Capacity.MaxWorkers <= 0 {
			config.Capacity.MaxWorkers = 1
		}
		if config.Capacity.CPUWeight <= 0 {
			config.Capacity.CPUWeight = 1
		}
		if config.Labels == nil {
			config.Labels = map[string]string{}
		}
		registry.targets[config.ID] = config
	}
	if len(registry.targets) == 0 {
		registry.targets["local"] = NewLocalTargetRegistry().targets["local"]
	}
	return registry
}

func (r *TargetRegistry) Select(plan Plan) (TargetConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	required := targetLabels(plan.Metadata)
	size := workerSize(plan.Metadata, plan.Prompt)
	candidates := make([]TargetConfig, 0, len(r.targets))
	for _, target := range r.targets {
		if labelsMatch(target.Labels, required) && r.running[target.ID] < target.Capacity.MaxWorkers {
			candidates = append(candidates, target)
		}
	}
	if len(candidates) == 0 {
		return TargetConfig{}, fmt.Errorf("no execution target matches labels %v", required)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return r.scoreLocked(candidates[i], size) > r.scoreLocked(candidates[j], size)
	})
	return candidates[0], nil
}

func (r *TargetRegistry) Get(id string) (TargetConfig, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	target, ok := r.targets[id]
	return target, ok
}

func (r *TargetRegistry) Begin(targetID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.running[targetID]++
}

func (r *TargetRegistry) Finish(targetID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running[targetID] > 0 {
		r.running[targetID]--
	}
}

func (r *TargetRegistry) Snapshot() []core.TargetState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]core.TargetState, 0, len(r.targets))
	for _, target := range r.targets {
		out = append(out, core.TargetState{
			ID:        target.ID,
			Kind:      string(target.Kind),
			Labels:    target.Labels,
			Capacity:  core.TargetCapacity(target.Capacity),
			Running:   r.running[target.ID],
			Available: r.running[target.ID] < target.Capacity.MaxWorkers,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *TargetRegistry) scoreLocked(target TargetConfig, size string) float64 {
	running := float64(r.running[target.ID])
	score := target.Capacity.CPUWeight - running*2
	if target.Kind == TargetKindLocal {
		score += 0.5
	}
	switch size {
	case "large":
		score += target.Capacity.MemoryGB / 16
		score -= running * 2
	case "medium":
		score += target.Capacity.MemoryGB / 32
		score -= running
	}
	return score
}

func labelsMatch(labels map[string]string, required map[string]string) bool {
	for key, want := range required {
		if labels[key] != want {
			return false
		}
	}
	return true
}

func targetLabels(metadata map[string]any) map[string]string {
	out := map[string]string{}
	if metadata == nil {
		return out
	}
	switch value := metadata["targetLabels"].(type) {
	case map[string]string:
		return value
	case map[string]any:
		for key, item := range value {
			if text, ok := item.(string); ok {
				out[key] = text
			}
		}
	}
	return out
}

func workerSize(metadata map[string]any, prompt string) string {
	if metadata != nil {
		if size, ok := metadata["workerSize"].(string); ok && size != "" {
			return size
		}
	}
	lower := strings.ToLower(prompt)
	if strings.Contains(lower, "benchmark") || strings.Contains(lower, "profile") || strings.Contains(lower, "large") {
		return "large"
	}
	return "medium"
}

type RemoteExecutor interface {
	Run(ctx context.Context, argv []string) (string, error)
}
