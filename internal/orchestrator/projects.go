package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"aged/internal/core"
)

type ProjectRegistry struct {
	mu        sync.RWMutex
	projects  map[string]core.Project
	defaultID string
}

type ProjectsConfig struct {
	DefaultProjectID string         `json:"defaultProjectId,omitempty"`
	Projects         []core.Project `json:"projects"`
}

func NewProjectRegistry(projects []core.Project, defaultID string) (*ProjectRegistry, error) {
	if len(projects) == 0 {
		return nil, errors.New("at least one project is required")
	}
	registry := &ProjectRegistry{projects: map[string]core.Project{}, defaultID: strings.TrimSpace(defaultID)}
	for _, project := range projects {
		normalized, err := normalizeProject(project)
		if err != nil {
			return nil, err
		}
		if _, exists := registry.projects[normalized.ID]; exists {
			return nil, fmt.Errorf("duplicate project id %q", normalized.ID)
		}
		registry.projects[normalized.ID] = normalized
		if registry.defaultID == "" {
			registry.defaultID = normalized.ID
		}
	}
	if _, ok := registry.projects[registry.defaultID]; !ok {
		return nil, fmt.Errorf("default project %q is not configured", registry.defaultID)
	}
	return registry, nil
}

func NewDefaultProjectRegistry(workDir string) (*ProjectRegistry, error) {
	abs, err := filepath.Abs(workDir)
	if err != nil {
		return nil, err
	}
	project := core.Project{
		ID:          "default",
		Name:        filepath.Base(abs),
		LocalPath:   abs,
		VCS:         "auto",
		DefaultBase: "main",
	}
	if repo := detectGitHubRepo(context.Background(), abs); repo != "" {
		project.Repo = repo
	}
	return NewProjectRegistry([]core.Project{project}, project.ID)
}

func LoadProjectRegistry(path string, fallbackWorkDir string) (*ProjectRegistry, error) {
	if strings.TrimSpace(path) == "" {
		return NewDefaultProjectRegistry(fallbackWorkDir)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config ProjectsConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return NewProjectRegistry(config.Projects, config.DefaultProjectID)
}

func (r *ProjectRegistry) Snapshot() []core.Project {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]core.Project, 0, len(r.projects))
	for _, project := range r.projects {
		out = append(out, project)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].ID < out[i].ID {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func (r *ProjectRegistry) Default() core.Project {
	if r == nil {
		return core.Project{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.projects[r.defaultID]
}

func (r *ProjectRegistry) Get(id string) (core.Project, bool) {
	if r == nil {
		return core.Project{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	project, ok := r.projects[strings.TrimSpace(id)]
	return project, ok
}

func (r *ProjectRegistry) Add(project core.Project) (core.Project, error) {
	normalized, err := normalizeProject(project)
	if err != nil {
		return core.Project{}, err
	}
	if r == nil {
		return core.Project{}, errors.New("project registry is not configured")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.projects[normalized.ID]; exists {
		return core.Project{}, fmt.Errorf("project %q already exists", normalized.ID)
	}
	r.projects[normalized.ID] = normalized
	if r.defaultID == "" {
		r.defaultID = normalized.ID
	}
	return normalized, nil
}

func (r *ProjectRegistry) Resolve(req core.CreateTaskRequest) (core.Project, error) {
	if r == nil {
		return core.Project{}, errors.New("project registry is not configured")
	}
	if req.ProjectID != "" {
		project, ok := r.Get(req.ProjectID)
		if !ok {
			return core.Project{}, fmt.Errorf("unknown projectId %q", req.ProjectID)
		}
		return project, nil
	}
	if len(req.Metadata) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(req.Metadata, &metadata); err == nil {
			if value, ok := metadata["projectId"].(string); ok && strings.TrimSpace(value) != "" {
				project, found := r.Get(value)
				if !found {
					return core.Project{}, fmt.Errorf("unknown projectId %q", value)
				}
				return project, nil
			}
			if repo, ok := metadata["repo"].(string); ok && strings.TrimSpace(repo) != "" {
				if project, found := r.FindByRepo(repo); found {
					return project, nil
				}
			}
		}
	}
	return r.Default(), nil
}

func (r *ProjectRegistry) FindByRepo(repo string) (core.Project, bool) {
	repo = strings.TrimSpace(strings.ToLower(repo))
	if repo == "" || r == nil {
		return core.Project{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, project := range r.projects {
		if strings.ToLower(project.Repo) == repo {
			return project, true
		}
	}
	return core.Project{}, false
}

func normalizeProject(project core.Project) (core.Project, error) {
	project.ID = strings.TrimSpace(project.ID)
	project.Name = strings.TrimSpace(project.Name)
	project.LocalPath = strings.TrimSpace(project.LocalPath)
	project.Repo = strings.TrimSpace(project.Repo)
	project.VCS = strings.TrimSpace(project.VCS)
	project.DefaultBase = strings.TrimSpace(project.DefaultBase)
	project.WorkspaceRoot = strings.TrimSpace(project.WorkspaceRoot)
	if project.ID == "" {
		return core.Project{}, errors.New("project id is required")
	}
	if project.LocalPath == "" {
		return core.Project{}, fmt.Errorf("project %q localPath is required", project.ID)
	}
	abs, err := filepath.Abs(project.LocalPath)
	if err != nil {
		return core.Project{}, err
	}
	project.LocalPath = abs
	if project.Name == "" {
		project.Name = filepath.Base(abs)
	}
	if project.VCS == "" {
		project.VCS = "auto"
	}
	if project.DefaultBase == "" {
		project.DefaultBase = "main"
	}
	return project, nil
}

func detectGitHubRepo(ctx context.Context, dir string) string {
	out, err := runCommand(ctx, dir, "jj", "git", "remote", "list")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(out, "\n") {
		if repo := githubRepoFromRemote(line); repo != "" {
			return repo
		}
	}
	return ""
}

func githubRepoFromRemote(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	fields := strings.Fields(line)
	for _, field := range fields {
		field = strings.Trim(field, "()")
		if repo := githubRepoFromURL(field); repo != "" {
			return repo
		}
	}
	return ""
}

func githubRepoFromURL(value string) string {
	value = strings.TrimSuffix(value, ".git")
	if after, ok := strings.CutPrefix(value, "git@github.com:"); ok {
		return after
	}
	if index := strings.Index(value, "github.com/"); index >= 0 {
		return value[index+len("github.com/"):]
	}
	return ""
}
