package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
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

const (
	defaultPromptSetID = "default"

	PromptTemplateSystem            = "system"
	PromptTemplatePlan              = "plan"
	PromptTemplateGitHubReview      = "github_review_request"
	PromptTemplateReplan            = "replan"
	PromptTemplateCodeReview        = "code_review"
	PromptTemplateCompletionReview  = "completion_review"
	PromptTemplatePublicationReview = "publication_review"
)

var promptTemplateFiles = map[string]string{
	PromptTemplateSystem:            "system.md",
	PromptTemplatePlan:              "plan.md",
	PromptTemplateGitHubReview:      "github_review_request.md",
	PromptTemplateReplan:            "replan.md",
	PromptTemplateCodeReview:        "code_review.md",
	PromptTemplateCompletionReview:  "completion_review.md",
	PromptTemplatePublicationReview: "publication_review.md",
}

type PromptSetRegistry struct {
	mu        sync.RWMutex
	sets      map[string]core.PromptSet
	defaultID string
}

type RenderedPrompt struct {
	Prompt      string
	PromptSetID string
	Template    string
	Hash        string
}

func NewPromptSetRegistry(promptSets []core.PromptSet, defaultID string) *PromptSetRegistry {
	registry := &PromptSetRegistry{sets: map[string]core.PromptSet{}, defaultID: strings.TrimSpace(defaultID)}
	if registry.defaultID == "" {
		registry.defaultID = defaultPromptSetID
	}
	registry.sets[defaultPromptSetID] = core.PromptSet{ID: defaultPromptSetID, Name: "Default", Description: "Built-in aged prompts.", BuiltIn: true}
	for _, promptSet := range promptSets {
		normalized, err := normalizePromptSet(promptSet)
		if err != nil {
			continue
		}
		if normalized.ID == defaultPromptSetID {
			if normalized.BuiltIn {
				normalized.Name = firstNonEmpty(normalized.Name, "Default")
				normalized.Description = firstNonEmpty(normalized.Description, "Built-in aged prompts.")
				registry.sets[defaultPromptSetID] = normalized
			}
		} else {
			registry.sets[normalized.ID] = normalized
		}
	}
	if _, ok := registry.sets[registry.defaultID]; !ok {
		registry.defaultID = defaultPromptSetID
	}
	return registry
}

func LoadDefaultPromptSet(dir string) (core.PromptSet, error) {
	return loadPromptSetFromDir(defaultPromptSetID, "Default", "Built-in aged prompts loaded from files.", dir, true)
}

func loadPromptSetFromDir(id, name, description, dir string, builtIn bool) (core.PromptSet, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return core.PromptSet{}, errors.New("prompt set directory is required")
	}
	templates := map[string]string{}
	for templateName, fileName := range promptTemplateFiles {
		path := filepath.Join(dir, fileName)
		content, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return core.PromptSet{}, fmt.Errorf("read prompt template %s: %w", path, err)
		}
		if value := strings.TrimSpace(string(content)); value != "" {
			templates[templateName] = value
		}
	}
	if len(templates) == 0 {
		return core.PromptSet{}, fmt.Errorf("no prompt templates found in %s", dir)
	}
	return core.PromptSet{
		ID:          id,
		Name:        name,
		Description: description,
		Templates:   templates,
		BuiltIn:     builtIn,
	}, nil
}

func normalizePromptSet(promptSet core.PromptSet) (core.PromptSet, error) {
	promptSet.ID = strings.TrimSpace(promptSet.ID)
	if promptSet.ID == "" {
		return core.PromptSet{}, errors.New("prompt set id is required")
	}
	promptSet.Name = strings.TrimSpace(promptSet.Name)
	if promptSet.Name == "" {
		promptSet.Name = promptSet.ID
	}
	if promptSet.Templates == nil {
		promptSet.Templates = map[string]string{}
	}
	templates := map[string]string{}
	for key, value := range promptSet.Templates {
		key = strings.TrimSpace(key)
		if key != "" && strings.TrimSpace(value) != "" {
			templates[key] = strings.TrimSpace(value)
		}
	}
	promptSet.Templates = templates
	return promptSet, nil
}

func (r *PromptSetRegistry) Snapshot() []core.PromptSet {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]core.PromptSet, 0, len(r.sets))
	for _, promptSet := range r.sets {
		cloned := clonePromptSet(promptSet)
		cloned.Default = cloned.ID == r.defaultID
		out = append(out, cloned)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BuiltIn != out[j].BuiltIn {
			return out[i].BuiltIn
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func clonePromptSet(promptSet core.PromptSet) core.PromptSet {
	if promptSet.Templates != nil {
		templates := make(map[string]string, len(promptSet.Templates))
		for key, value := range promptSet.Templates {
			templates[key] = value
		}
		promptSet.Templates = templates
	}
	return promptSet
}

func (r *PromptSetRegistry) Register(promptSet core.PromptSet, makeDefault bool) (core.PromptSet, error) {
	if r == nil {
		return core.PromptSet{}, errors.New("prompt set registry is not configured")
	}
	normalized, err := normalizePromptSet(promptSet)
	if err != nil {
		return core.PromptSet{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.sets[normalized.ID]; existing.BuiltIn {
		return core.PromptSet{}, errors.New("built-in prompt set cannot be replaced")
	}
	r.sets[normalized.ID] = normalized
	if makeDefault {
		r.defaultID = normalized.ID
	}
	normalized.Default = normalized.ID == r.defaultID
	return normalized, nil
}

func (r *PromptSetRegistry) Delete(id string) error {
	if r == nil {
		return errors.New("prompt set registry is not configured")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("prompt set id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	promptSet, ok := r.sets[id]
	if !ok {
		return eventstore.ErrNotFound
	}
	if promptSet.BuiltIn {
		return errors.New("built-in prompt set cannot be deleted")
	}
	delete(r.sets, id)
	if r.defaultID == id {
		r.defaultID = defaultPromptSetID
	}
	return nil
}

func (r *PromptSetRegistry) Render(task core.Task, templateName string, data map[string]any) (RenderedPrompt, bool) {
	if r == nil {
		return RenderedPrompt{}, false
	}
	id := promptSetIDFromTask(task)
	r.mu.RLock()
	if id == "" {
		id = r.defaultID
	}
	promptSet, ok := r.sets[id]
	if !ok {
		promptSet = r.sets[r.defaultID]
		id = r.defaultID
	}
	template := strings.TrimSpace(promptSet.Templates[templateName])
	systemTemplate := strings.TrimSpace(promptSet.Templates[PromptTemplateSystem])
	r.mu.RUnlock()
	if template == "" {
		return RenderedPrompt{}, false
	}
	if systemTemplate != "" {
		data = clonePromptData(data)
		data[PromptTemplateSystem] = systemTemplate
	}
	prompt := renderPromptTemplate(template, data)
	hash := sha256.Sum256([]byte(prompt))
	return RenderedPrompt{Prompt: prompt, PromptSetID: id, Template: templateName, Hash: hex.EncodeToString(hash[:])}, true
}

func clonePromptData(data map[string]any) map[string]any {
	cloned := make(map[string]any, len(data)+1)
	for key, value := range data {
		cloned[key] = value
	}
	return cloned
}

func renderPromptTemplate(template string, data map[string]any) string {
	for key, value := range data {
		var replacement string
		switch typed := value.(type) {
		case string:
			replacement = typed
		default:
			bytes, err := json.MarshalIndent(typed, "", "  ")
			if err != nil {
				replacement = fmt.Sprint(typed)
			} else {
				replacement = string(bytes)
			}
		}
		template = strings.ReplaceAll(template, "{{"+key+"}}", replacement)
	}
	return strings.TrimSpace(template)
}

func promptSetIDFromTask(task core.Task) string {
	var metadata map[string]any
	if len(task.Metadata) > 0 {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	return strings.TrimSpace(stringMetadataValue(metadata["promptSetId"]))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func promptMetadata(rendered RenderedPrompt, fallbackReason string) map[string]any {
	if strings.TrimSpace(rendered.PromptSetID) == "" {
		if strings.TrimSpace(fallbackReason) == "" {
			return map[string]any{}
		}
		return map[string]any{"customPromptFallbackReason": fallbackReason}
	}
	metadata := map[string]any{
		"promptSetId":    rendered.PromptSetID,
		"promptTemplate": rendered.Template,
		"promptHash":     rendered.Hash,
	}
	if strings.TrimSpace(fallbackReason) != "" {
		metadata["customPromptFallbackReason"] = fallbackReason
	}
	return metadata
}
