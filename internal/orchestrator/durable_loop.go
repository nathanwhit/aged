package orchestrator

import (
	"context"
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

type durableLoopConfig struct {
	WorkerKind     string
	Prompt         string
	Role           string
	Reasoning      string
	Interval       time.Duration
	FreshWorkspace bool
}

func taskExecutionMode(task core.Task) string {
	metadata := taskMetadataMap(task)
	switch strings.ToLower(strings.TrimSpace(stringMetadataValue(metadata["executionMode"]))) {
	case "loop", "durable_loop", "agent_loop":
		return executionModeLoop
	default:
		return "orchestrated"
	}
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
	for iteration := 1; ; iteration++ {
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
		if config.Interval > 0 {
			timer := time.NewTimer(config.Interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (s *Service) runDurableLoopIteration(ctx context.Context, task core.Task, config durableLoopConfig, iteration int, previousWorkerID string) (WorkerTurnResult, error) {
	plan := Plan{
		WorkerKind:      config.WorkerKind,
		Prompt:          durableLoopPrompt(task, config, iteration),
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

func durableLoopPrompt(task core.Task, config durableLoopConfig, iteration int) string {
	var builder strings.Builder
	builder.WriteString("# Durable Agent Loop\n\n")
	builder.WriteString("You are running iteration ")
	builder.WriteString(fmt.Sprint(iteration))
	builder.WriteString(" of a durable role loop. Keep the same objective and preserve useful context from the workspace and provider session. Make bounded progress, report what changed, and leave the workspace in an inspectable state.\n\n")
	builder.WriteString("Role: ")
	builder.WriteString(config.Role)
	builder.WriteString("\n\n")
	builder.WriteString("# Task Objective\n\n")
	builder.WriteString(strings.TrimSpace(config.Prompt))
	return builder.String()
}
