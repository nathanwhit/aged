package orchestrator

import "strings"

func testWorkItemPlan(workerKind string, prompt string) Plan {
	return testWorkItemPlanWithID("main", "objective.implement", "Run the requested work.", prompt, workerKind)
}

func testWorkItemPlanWithID(id string, kind string, reason string, prompt string, workerKind string) Plan {
	return Plan{
		Rationale: "test work item plan",
		WorkItems: []WorkItemRequest{{
			ID:         id,
			Kind:       kind,
			Reason:     reason,
			Prompt:     prompt,
			TargetKind: "objective",
			WorkerKind: workerKind,
		}},
	}
}

func testPlanWithImplicitWorkItem(plan Plan) Plan {
	if len(plan.WorkItems) != 0 {
		return plan
	}
	items := make([]WorkItemRequest, 0, len(plan.Workers)+len(plan.Spawns)+1)
	if plan.WorkerKind != "" {
		items = append(items, WorkItemRequest{
			ID:              "main",
			Kind:            "objective.implement",
			Reason:          "Run the requested test work.",
			Prompt:          plan.Prompt,
			TargetKind:      "objective",
			WorkerKind:      plan.WorkerKind,
			ReasoningEffort: plan.ReasoningEffort,
		})
	}
	for _, worker := range plan.Workers {
		metadata := map[string]any{}
		if worker.Role != "" {
			metadata["role"] = worker.Role
		}
		items = append(items, WorkItemRequest{
			ID:              worker.ID,
			Kind:            testWorkItemKind(worker.ID, worker.Role),
			Reason:          worker.Reason,
			Prompt:          worker.Prompt,
			TargetKind:      "objective",
			WorkerKind:      worker.WorkerKind,
			ReasoningEffort: worker.ReasoningEffort,
			DependsOn:       worker.DependsOn,
			Metadata:        metadata,
		})
	}
	for _, spawn := range plan.Spawns {
		metadata := map[string]any{}
		if spawn.Role != "" {
			metadata["role"] = spawn.Role
		}
		items = append(items, WorkItemRequest{
			ID:              spawn.ID,
			Kind:            testWorkItemKind(spawn.ID, spawn.Role),
			Reason:          spawn.Reason,
			Prompt:          spawn.Reason,
			TargetKind:      "objective",
			WorkerKind:      spawn.WorkerKind,
			ReasoningEffort: spawn.ReasoningEffort,
			DependsOn:       spawn.DependsOn,
			Metadata:        metadata,
		})
	}
	if len(items) == 0 {
		return plan
	}
	plan.WorkItems = items
	plan.Workers = nil
	plan.Spawns = nil
	return plan
}

func testWorkItemKind(id string, role string) string {
	value := strings.ToLower(id + " " + role)
	switch {
	case strings.Contains(value, "review"), strings.Contains(value, "test"), strings.Contains(value, "validat"):
		return "objective.validate"
	case strings.Contains(value, "plan"):
		return "objective.plan"
	case strings.Contains(value, "compose"):
		return "objective.compose"
	case strings.Contains(value, "slice"):
		return "objective.slice"
	default:
		return "objective.implement"
	}
}

func testDecisionWithImplicitWorkItem(decision ReplanDecision) ReplanDecision {
	if decision.Plan != nil {
		plan := testPlanWithImplicitWorkItem(*decision.Plan)
		decision.Plan = &plan
	}
	return decision
}
