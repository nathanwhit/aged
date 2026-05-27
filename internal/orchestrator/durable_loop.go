package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/worker"
)

const (
	executionModeLoop     = "loop"
	defaultLoopInterval   = time.Minute
	defaultLoopWorkerKind = "codex"
	loopActionKind        = "durable_loop"
)

var errDurableLoopTaskTerminal = errors.New("durable loop task reached terminal state")

type durableLoopConfig struct {
	WorkerKind     string
	Prompt         string
	Role           string
	Reasoning      string
	Interval       time.Duration
	FreshWorkspace bool
}

func taskExecutionMode(task core.Task) string {
	return taskMetadataExecutionMode(taskMetadataMap(task))
}

func taskMetadataMap(task core.Task) map[string]any {
	metadata, _ := createTaskMetadataMap(task.Metadata)
	return metadata
}

func durableLoopConfigFromTask(task core.Task, runners map[string]worker.Runner) durableLoopConfig {
	metadata := taskMetadataMap(task)
	explicitWorkerKind := nonEmpty(
		stringMetadataValue(metadata["loopWorkerKind"]),
		stringMetadataValue(metadata["workerKind"]),
		stringMetadataValue(metadata["assistant"]),
	)
	workerKind := nonEmpty(explicitWorkerKind, defaultLoopWorkerKind)
	if runner := runners[workerKind]; runner == nil {
		if explicitWorkerKind == "" {
			workerKind = firstConfiguredRunnerKind(runners)
		}
	}
	interval := defaultLoopInterval
	if _, ok := metadata["loopIntervalSeconds"]; ok {
		interval = time.Duration(intMetadata(metadata, "loopIntervalSeconds")) * time.Second
		if interval < 0 {
			interval = 0
		}
	}
	prompt := strings.TrimSpace(stringMetadataValue(metadata["loopPrompt"]))
	if prompt == "" {
		prompt = task.Prompt
	}
	return durableLoopConfig{
		WorkerKind:     workerKind,
		Prompt:         prompt,
		Role:           nonEmpty(stringMetadataValue(metadata["loopRole"]), "worker_loop"),
		Reasoning:      stringMetadataValue(metadata["reasoningEffort"]),
		Interval:       interval,
		FreshWorkspace: boolMetadata(metadata, "loopFreshWorkspace"),
	}
}

func firstConfiguredRunnerKind(runners map[string]worker.Runner) string {
	candidates := make([]string, 0, len(runners))
	for candidate, runner := range runners {
		if runner != nil {
			candidates = append(candidates, candidate)
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

func (s *Service) runDurableLoopTask(ctx context.Context, task core.Task) {
	config := durableLoopConfigFromTask(task, s.runners)
	if strings.TrimSpace(config.WorkerKind) == "" || s.runners[config.WorkerKind] == nil {
		_ = s.failTask(ctx, task.ID, fmt.Errorf("loop worker kind %q is not configured", config.WorkerKind))
		return
	}
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return
	}
	if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveActive, "loop_running", "Durable agent loop is running."); err != nil {
		return
	}
	if err := s.recordTaskAction(ctx, task.ID, map[string]any{
		"kind":       loopActionKind,
		"status":     "started",
		"workerKind": config.WorkerKind,
		"role":       config.Role,
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}

	var previousWorkerID string
	nextIteration := 1
	waitBeforeNext := false
	if snapshot, err := s.store.Snapshot(ctx); err == nil {
		nextIteration, previousWorkerID, waitBeforeNext = durableLoopResumeState(snapshot, task.ID)
	}
	if waitBeforeNext {
		if err := s.waitDurableLoopInterval(ctx, task.ID, config.Interval); err != nil {
			return
		}
		config = s.latestDurableLoopConfig(ctx, task, config)
	}
	for iteration := nextIteration; ; iteration++ {
		if err := ctx.Err(); err != nil {
			return
		}
		result, err := s.runDurableLoopIteration(ctx, task, config, iteration, previousWorkerID)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			_ = s.recordTaskAction(context.Background(), task.ID, map[string]any{
				"kind":      loopActionKind,
				"status":    "iteration_failed",
				"iteration": iteration,
				"error":     err.Error(),
			})
		} else {
			if result.WorkerID != "" {
				previousWorkerID = result.WorkerID
			}
			if result.Status == core.WorkerWaiting {
				_ = s.recordTaskAction(ctx, task.ID, map[string]any{
					"kind":      loopActionKind,
					"status":    "waiting_for_input",
					"iteration": iteration,
					"workerId":  result.WorkerID,
					"summary":   result.Summary,
				})
				_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
				return
			}
			status := "iteration_completed"
			if result.Status == core.WorkerFailed || result.Status == core.WorkerCanceled {
				status = "iteration_" + string(result.Status)
			}
			_ = s.recordTaskAction(ctx, task.ID, map[string]any{
				"kind":      loopActionKind,
				"status":    status,
				"iteration": iteration,
				"workerId":  result.WorkerID,
				"summary":   result.Summary,
				"error":     result.Error,
			})
		}
		config = s.latestDurableLoopConfig(ctx, task, config)
		if err := s.waitDurableLoopInterval(ctx, task.ID, config.Interval); err != nil {
			return
		}
		config = s.latestDurableLoopConfig(ctx, task, config)
	}
}

func durableLoopResumeState(snapshot core.Snapshot, taskID string) (int, string, bool) {
	latestIteration := 0
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskPlanned {
			continue
		}
		var payload struct {
			Metadata map[string]any `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if taskMetadataExecutionMode(payload.Metadata) != executionModeLoop {
			continue
		}
		if iteration := intMetadata(payload.Metadata, "loopIteration"); iteration > latestIteration {
			latestIteration = iteration
		}
	}
	if latestIteration == 0 {
		return 1, "", false
	}
	previousWorkerID := ""
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID && isTerminalWorkerStatus(worker.Status) && worker.UpdatedAt.After(latestDurableLoopWorkerTime(snapshot, previousWorkerID)) {
			previousWorkerID = worker.ID
		}
	}
	return latestIteration + 1, previousWorkerID, previousWorkerID != ""
}

func latestDurableLoopWorkerTime(snapshot core.Snapshot, workerID string) time.Time {
	for _, worker := range snapshot.Workers {
		if worker.ID == workerID {
			return worker.UpdatedAt
		}
	}
	return time.Time{}
}

func (s *Service) latestDurableLoopConfig(ctx context.Context, task core.Task, fallback durableLoopConfig) durableLoopConfig {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return fallback
	}
	latest, ok := findTask(snapshot, task.ID)
	if !ok {
		return fallback
	}
	return durableLoopConfigFromTask(latest, s.runners)
}

func (s *Service) waitDurableLoopInterval(ctx context.Context, taskID string, interval time.Duration) error {
	if interval <= 0 {
		return nil
	}
	started := time.Now()
	latest := interval
	subscriptionID, events := s.Subscribe()
	defer s.Unsubscribe(subscriptionID)
	if task, ok, err := s.readDurableLoopTask(ctx, taskID); err == nil && ok {
		if isTerminalTaskStatus(task.Status) {
			return errDurableLoopTaskTerminal
		}
		latest = durableLoopConfigFromTask(task, s.runners).Interval
	}
	if latest <= 0 || time.Since(started) >= latest {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if latest <= 0 || time.Since(started) >= latest {
			return nil
		}
		remaining := latest - time.Since(started)
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case event, ok := <-events:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if !ok || event.TaskID != taskID || !durableLoopWaitEventCanChangeTaskState(event.Type) {
				continue
			}
			task, found, err := s.readDurableLoopTask(ctx, taskID)
			if err != nil || !found {
				continue
			}
			if isTerminalTaskStatus(task.Status) {
				return errDurableLoopTaskTerminal
			}
			latest = durableLoopConfigFromTask(task, s.runners).Interval
		case <-timer.C:
			return nil
		}
	}
}

func durableLoopWaitEventCanChangeTaskState(eventType core.EventType) bool {
	switch eventType {
	case core.EventTaskCreated, core.EventTaskUpdated, core.EventTaskStatus, core.EventTaskCleared:
		return true
	default:
		return false
	}
}

func (s *Service) readDurableLoopTask(ctx context.Context, taskID string) (core.Task, bool, error) {
	events, err := s.store.ListTaskEvents(ctx, taskID, 0)
	if err != nil {
		return core.Task{}, false, err
	}
	return durableLoopTaskFromEvents(events, taskID)
}

func durableLoopTaskFromEvents(events []core.Event, taskID string) (core.Task, bool, error) {
	var task core.Task
	found := false
	cleared := false
	for _, event := range events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskCreated:
			var payload struct {
				ProjectID string          `json:"projectId,omitempty"`
				Title     string          `json:"title"`
				Prompt    string          `json:"prompt"`
				Metadata  json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Task{}, false, fmt.Errorf("decode task.created: %w", err)
			}
			task = core.Task{
				ID:              event.TaskID,
				ProjectID:       payload.ProjectID,
				Title:           payload.Title,
				Prompt:          payload.Prompt,
				Status:          core.TaskQueued,
				ObjectiveStatus: core.ObjectiveActive,
				ObjectivePhase:  "queued",
				CreatedAt:       event.At,
				UpdatedAt:       event.At,
				Metadata:        payload.Metadata,
			}
			if metadata, err := createTaskMetadataMap(payload.Metadata); err == nil {
				if task.ProjectID == "" {
					task.ProjectID = stringMetadataValue(metadata["projectId"])
				}
				task.WorkstreamID = stringMetadataValue(metadata["workstreamId"])
			}
			found = true
			cleared = false
		case core.EventTaskUpdated:
			if !found {
				continue
			}
			var payload struct {
				Title         string          `json:"title,omitempty"`
				Prompt        string          `json:"prompt,omitempty"`
				MetadataPatch json.RawMessage `json:"metadataPatch,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Task{}, false, fmt.Errorf("decode task.updated: %w", err)
			}
			if payload.Title != "" {
				task.Title = payload.Title
			}
			if payload.Prompt != "" {
				task.Prompt = payload.Prompt
			}
			task.Metadata = mergeDurableLoopTaskMetadataPatch(task.Metadata, payload.MetadataPatch)
			if metadata, err := createTaskMetadataMap(task.Metadata); err == nil {
				task.WorkstreamID = stringMetadataValue(metadata["workstreamId"])
			}
			task.UpdatedAt = event.At
		case core.EventTaskStatus:
			if !found {
				continue
			}
			var payload struct {
				Status core.TaskStatus `json:"status"`
				Error  string          `json:"error,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Task{}, false, fmt.Errorf("decode task.status: %w", err)
			}
			task.Status = payload.Status
			task.Error = payload.Error
			task.UpdatedAt = event.At
		case core.EventTaskCleared:
			cleared = true
		}
	}
	if !found || cleared {
		return core.Task{}, false, nil
	}
	return task, true, nil
}

func mergeDurableLoopTaskMetadataPatch(base json.RawMessage, patch json.RawMessage) json.RawMessage {
	if len(patch) == 0 {
		return base
	}
	out := map[string]any{}
	if len(base) > 0 {
		_ = json.Unmarshal(base, &out)
	}
	var patchValues map[string]any
	if err := json.Unmarshal(patch, &patchValues); err != nil {
		return base
	}
	for key, value := range patchValues {
		out[key] = value
	}
	return core.MustJSON(out)
}

func (s *Service) runDurableLoopIteration(ctx context.Context, task core.Task, config durableLoopConfig, iteration int, previousWorkerID string) (WorkerTurnResult, error) {
	plan := Plan{
		WorkerKind:      config.WorkerKind,
		Prompt:          durableLoopPrompt(task, config, iteration, s.taskContextLedger(ctx, task.ID)),
		ReasoningEffort: config.Reasoning,
		Rationale:       "durable loop iteration",
		Metadata: map[string]any{
			"executionMode":  executionModeLoop,
			"loopIteration":  iteration,
			"loopRole":       config.Role,
			"loopWorkerKind": config.WorkerKind,
		},
	}
	if config.FreshWorkspace {
		plan.Metadata["workspaceReusePolicy"] = "fresh"
	} else if strings.TrimSpace(previousWorkerID) != "" {
		snapshot, err := s.store.Snapshot(ctx)
		if err == nil {
			plan = retryPlanWithResume(snapshot, plan, task.ID, previousWorkerID)
			plan.Metadata["retryContextKind"] = "durable_loop"
		}
	}
	if err := plan.Validate(); err != nil {
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		return WorkerTurnResult{}, err
	}
	return s.runPlannedWorker(ctx, task, plan)
}

func durableLoopPrompt(task core.Task, config durableLoopConfig, iteration int, ledger []ContextLedgerEntry) string {
	var builder strings.Builder
	builder.WriteString("# Durable Agent Loop\n\n")
	builder.WriteString("You are running iteration ")
	builder.WriteString(fmt.Sprint(iteration))
	builder.WriteString(" of a durable role loop. Keep the same objective and preserve useful context from the workspace and provider session. Make bounded progress, report what changed, and leave the workspace in an inspectable state.\n\n")
	builder.WriteString("Role: ")
	builder.WriteString(config.Role)
	builder.WriteString("\n\n")
	builder.WriteString("# Loop Playbook\n\n")
	builder.WriteString("- Inspect the current repository and workspace state before choosing the next work item.\n")
	builder.WriteString("- Check existing task artifacts and any open pull request context when available or obvious from the workspace, prior output, or task instructions.\n")
	builder.WriteString("- Prefer one bounded, coherent unit of progress for this iteration instead of starting several unrelated threads.\n")
	builder.WriteString("- Publish an intermediate pull request through the provided `aged-publish-pr` helper only when this iteration produced a real material change worth review; do not publish for analysis-only or no-op turns.\n")
	builder.WriteString("- After publishing, continue the durable objective in later iterations unless the loop is canceled or you are blocked.\n")
	builder.WriteString("- Ask for user input only for user-owned blockers such as missing credentials, permissions, ambiguous scope, or risky setup choices.\n\n")
	if rendered := renderContextLedgerForWorkerPrompt(ledger); rendered != "" {
		builder.WriteString(rendered)
	}
	builder.WriteString("# Task Objective\n\n")
	builder.WriteString(strings.TrimSpace(config.Prompt))
	return builder.String()
}
