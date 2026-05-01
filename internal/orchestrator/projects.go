package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"aged/internal/core"
	"aged/internal/eventstore"
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

func (r *ProjectRegistry) Update(project core.Project) (core.Project, error) {
	normalized, err := normalizeProject(project)
	if err != nil {
		return core.Project{}, err
	}
	if r == nil {
		return core.Project{}, errors.New("project registry is not configured")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.projects[normalized.ID]; !exists {
		return core.Project{}, eventstore.ErrNotFound
	}
	r.projects[normalized.ID] = normalized
	return normalized, nil
}

func (r *ProjectRegistry) Delete(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("project id is required")
	}
	if r == nil {
		return errors.New("project registry is not configured")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.projects[id]; !exists {
		return eventstore.ErrNotFound
	}
	if len(r.projects) <= 1 {
		return errors.New("cannot delete the last project")
	}
	delete(r.projects, id)
	if r.defaultID == id {
		projects := r.sortedProjectsLocked()
		r.defaultID = projects[0].ID
	}
	return nil
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
				if project, found := r.findByMetadataRepo(metadata, repo); found {
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

func (r *ProjectRegistry) FindByIssueRepo(repo string) (core.Project, bool) {
	repo = strings.TrimSpace(strings.ToLower(repo))
	if repo == "" || r == nil {
		return core.Project{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, project := range r.sortedProjectsLocked() {
		if strings.ToLower(project.UpstreamRepo) == repo {
			return project, true
		}
	}
	for _, project := range r.sortedProjectsLocked() {
		if strings.ToLower(project.Repo) == repo {
			return project, true
		}
	}
	return core.Project{}, false
}

func (r *ProjectRegistry) findByMetadataRepo(metadata map[string]any, repo string) (core.Project, bool) {
	if source, ok := metadata["source"].(string); ok && strings.TrimSpace(source) == "github-issue" {
		return r.FindByIssueRepo(repo)
	}
	return r.FindByRepo(repo)
}

func (r *ProjectRegistry) sortedProjectsLocked() []core.Project {
	out := make([]core.Project, 0, len(r.projects))
	for _, project := range r.projects {
		out = append(out, project)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func normalizeProject(project core.Project) (core.Project, error) {
	project.ID = strings.TrimSpace(project.ID)
	project.Name = strings.TrimSpace(project.Name)
	project.LocalPath = strings.TrimSpace(project.LocalPath)
	project.Repo = strings.TrimSpace(project.Repo)
	project.UpstreamRepo = strings.TrimSpace(project.UpstreamRepo)
	project.HeadRepoOwner = strings.TrimSpace(project.HeadRepoOwner)
	project.PushRemote = strings.TrimSpace(project.PushRemote)
	project.VCS = strings.TrimSpace(project.VCS)
	project.DefaultBase = strings.TrimSpace(project.DefaultBase)
	project.WorkspaceRoot = strings.TrimSpace(project.WorkspaceRoot)
	project.PullRequestPolicy.BranchPrefix = strings.TrimSpace(project.PullRequestPolicy.BranchPrefix)
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
	info, err := os.Stat(abs)
	if err != nil {
		return core.Project{}, fmt.Errorf("project %q localPath %q is not accessible: %w", project.ID, abs, err)
	}
	if !info.IsDir() {
		return core.Project{}, fmt.Errorf("project %q localPath %q is not a directory", project.ID, abs)
	}
	project.LocalPath = abs
	if project.Name == "" {
		project.Name = filepath.Base(abs)
	}
	if project.Repo == "" {
		project.Repo = detectGitHubRepo(context.Background(), abs)
	}
	if project.VCS == "" || strings.EqualFold(project.VCS, "auto") {
		if detected := detectProjectVCS(context.Background(), abs); detected != "" {
			project.VCS = detected
		}
	}
	if project.VCS == "" {
		project.VCS = "auto"
	}
	if project.DefaultBase == "" {
		project.DefaultBase = nonEmpty(detectDefaultBase(context.Background(), abs, project.Repo), "main")
	}
	if project.PullRequestPolicy.BranchPrefix == "" {
		project.PullRequestPolicy.BranchPrefix = "codex/aged-"
	}
	return project, nil
}

func repoOwner(repo string) string {
	owner, _, ok := strings.Cut(strings.TrimSpace(repo), "/")
	if !ok {
		return ""
	}
	return strings.TrimSpace(owner)
}

func detectGitHubRepo(ctx context.Context, dir string) string {
	out, err := runCommand(ctx, dir, "jj", "git", "remote", "list")
	if err == nil {
		for _, line := range strings.Split(out, "\n") {
			if repo := githubRepoFromRemote(line); repo != "" {
				return repo
			}
		}
	}
	out, err = runCommand(ctx, dir, "git", "remote", "-v")
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

func detectProjectVCS(ctx context.Context, dir string) string {
	if _, err := runCommand(ctx, dir, "jj", "root"); err == nil {
		return "jj"
	}
	if _, err := runCommand(ctx, dir, "git", "rev-parse", "--show-toplevel"); err == nil {
		return "git"
	}
	return ""
}

func detectDefaultBase(ctx context.Context, dir string, repo string) string {
	out, err := runCommand(ctx, dir, "git", "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
	if err == nil {
		branch := strings.TrimSpace(out)
		branch = strings.TrimPrefix(branch, "origin/")
		if branch != "" {
			return branch
		}
	}
	for _, branch := range []string{"main", "master", "trunk"} {
		if _, err := runCommand(ctx, dir, "git", "rev-parse", "--verify", "--quiet", "refs/heads/"+branch); err == nil {
			return branch
		}
		if _, err := runCommand(ctx, dir, "git", "rev-parse", "--verify", "--quiet", "refs/remotes/origin/"+branch); err == nil {
			return branch
		}
	}
	repo = strings.TrimSpace(repo)
	if repo != "" {
		out, err := runCommand(ctx, dir, "gh", "repo", "view", repo, "--json", "defaultBranchRef", "--jq", ".defaultBranchRef.name")
		if err == nil && strings.TrimSpace(out) != "" {
			return strings.TrimSpace(out)
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
