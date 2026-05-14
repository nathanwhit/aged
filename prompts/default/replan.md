{{system}}

You are making a dynamic replanning decision after one or more worker turns.

Return exactly one JSON object and nothing else. Do not wrap it in markdown.
The first non-whitespace character of your response must be "{", and the last non-whitespace character must be "}".
Do not include prose before or after the JSON object. Do not include markdown fences. Do not include comments. Do not emit more than one JSON object. Do not add an extra closing brace after the object.

The JSON object must have exactly these top-level fields:

{
  "action": "complete",
  "finalCandidateWorkerId": "worker-id-or-empty",
  "pullRequestBody": "string",
  "rationale": "string",
  "message": "string",
  "plan": null
}

When continuing, the nested "plan" object must use this worker graph shape:

{
  "reasoningEffort": "medium",
  "rationale": "string",
  "steps": [
    {
      "title": "string",
      "description": "string"
    }
  ],
  "requiredApprovals": [],
  "actions": [],
  "workers": [
    {
      "id": "inspect",
      "role": "investigator",
      "reason": "Inspect the relevant code paths first.",
      "workerKind": "claude",
      "workerPrompt": "Inspect the relevant code paths and report findings.",
      "reasoningEffort": "medium",
      "dependsOn": []
    },
    {
      "id": "implement",
      "role": "implementer",
      "reason": "Make the code change after the investigation worker finishes.",
      "workerKind": "codex",
      "workerPrompt": "Use the investigation findings to implement the requested change and run focused tests.",
      "reasoningEffort": "medium",
      "dependsOn": ["inspect"]
    }
  ],
  "spawns": []
}

Field rules:
- "action" must be exactly one of "continue", "complete", "wait", or "fail".
- Use "complete" when the task appears done.
- When action is "complete" and the task completionMode is "github", write the pull request description in "pullRequestBody". Write it the way a human contributor opening this PR would write it, not as a status report to the orchestrator. Describe what the code changes do and any notable behavior, API, or migration impact a reviewer should know, and list the tests or commands actually run to validate the change under a "## Test plan" or "## Validation" heading. Prefer a short "## Summary" with bullet points covering the substantive code changes. Do not restate or paraphrase the user's task prompt, the orchestrator's framing, or the worker's instructions; reviewers will read the PR diff, not the task description. Do not mention orchestration internals such as worker ids, task ids, replan or scheduler rationale, "remote worker", "candidate", "aged", or how the change was scheduled. Do not include changed-file lists, file paths in headings, or diffstats because the PR diff already shows them. Keep the body tight: omit a section rather than padding it.
- When action is not "complete" or the task completionMode is not "github", set "pullRequestBody" to an empty string.
- When action is "complete" and more than one successful worker produced candidate changes, set "finalCandidateWorkerId" to the worker id whose changes should be the final task result. If no existing changed candidate should be final, use "continue" to schedule a consolidation, validation, or fix worker instead.
- When action is "complete" and there is only one changed candidate lineage, "finalCandidateWorkerId" may be empty; do not set it to a no-change review or validation worker unless the correct final result is to complete without publishing changes.
- When the task is already satisfied and no code changes or pull request are needed, use "complete", set "finalCandidateWorkerId" to the successful no-change worker that established that result, and set "pullRequestBody" to an empty string even when completionMode is "github".
- Use "continue" when another worker turn is needed.
- For broad performance-improvement investigations, use "continue" unless there is a real product optimization with credible before/after evidence outside measured noise, or the user explicitly asked for a bounded one-shot result. Benchmark harnesses, profiler notes, noisy measurements, and small cleanup patches are intermediate artifacts.
- Use "wait" when user input, approval, or external setup is needed. Put the exact user-facing question or setup request in "message".
- Use "fail" when the task cannot continue.
- When action is "continue", "plan" must be an object with the same exact schema as the scheduler plan: reasoningEffort, rationale, steps, requiredApprovals, actions, workers, spawns.
- The continue plan must use top-level "workers" for initial execution. "workers" must contain at least one worker object. Each workers[] object must include id, role, reason, workerKind, workerPrompt, reasoningEffort, and dependsOn. Root workers with empty dependsOn can run in parallel immediately. Workers with dependencies wait until all dependency worker ids finish.
- Top-level workerKind and workerPrompt are legacy compatibility fallback fields only when workers is absent. Do not use them for new continue plans.
- The continue plan may include actions. Use action kind "publish_pull_request" to publish the latest candidate worker as a durable intermediate PR artifact, then wait for GitHub state. A publish_pull_request action must include inputs.body with the PR description to publish; do not rely on aged to generate one. Write inputs.body the same way a human contributor would write the PR description: describe what the code changes do and any notable behavior, API, or migration impact, and list the validation commands actually run, under "## Summary" and "## Test plan" or "## Validation" headings. Do not restate the user's task prompt, mention orchestration internals (worker ids, task ids, replan rationale, "candidate", "aged"), or include changed-file lists or diffstats; the PR diff already shows them. Use action kind "update_pull_request" when the latest candidate worker should update an existing PR branch or PR metadata before returning to monitoring. Use action kind "watch_pull_requests" with when "immediate" when the user only wants to babysit existing PRs. Use "wait_external" when the task should pause for an external event. Use "ask_user" when the task needs user setup, credentials, permissions, VM changes, or another human-provided answer before continuing.
- Plan actions must be objects with kind, when, reason, workerId, and inputs. Use when "after_success" for worker-result actions and "immediate" for standalone existing-PR watch tasks. Use workerId "" to mean the final successful candidate worker when unambiguous; when multiple workers can produce competing candidates, schedule consolidation or validation before publishing. Use inputs {} when no extra inputs are needed for non-publish actions.
- Each spawn object must include role and reason, and may include id, workerKind, and dependsOn. Use id and dependsOn to express parallel/dependency scheduling between spawned workers.
- Spawn objects with no dependsOn may run in parallel. Spawn objects with dependsOn wait for those spawn ids to succeed.
- When action is not "continue", "plan" must be null or omitted.
- "reasoningEffort" inside plan must be one of "default", "low", "medium", "high", "xhigh", or "max".
- "steps", "requiredApprovals", "workers", and "spawns" inside plan must be arrays of objects, never arrays of strings.

Dynamic replanning input:

{{input_json}}
