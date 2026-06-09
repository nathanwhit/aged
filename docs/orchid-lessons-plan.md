# Orchid Lessons Plan

## Thesis

Aged should move from a task-centered deterministic graph to a session/artifact-centered orchestrator.

Keep deterministic code for infrastructure: leases, cancellation, branch ownership, PR identity, persistence, event delivery, rate limits, and target accounting. Push semantic judgment back to agents: when to split work, how to title commits, whether a PR is coherent, whether to ask the user, and whether to continue or stop.

Orchid's lesson is not "less code." Its useful lesson is that its core object is closer to:

```
issue -> live session -> branch/PR artifacts -> relayed feedback
```

Aged's current core object is closer to:

```
task -> plan graph -> workers -> candidates -> implicit terminal artifact
```

The latter is where broad objectives keep getting wedged.

## Target Concepts

- `Objective`: the user's broad goal. Long-lived. Owns sessions, PRs, artifacts, questions, and work items.
- `Session`: one live agent execution on one target/worktree/branch. Has pane/log/current action/heartbeat.
- `WorkItem`: queued unit of attention: initial implementation, PR follow-up, CI repair, review reply, user steering, slice work, compose work.
- `PullRequest`: independent artifact with its own branch, status, feedback queue, and follow-up sessions.
- `Artifact`: benchmark result, patch, binary, note, scratch output, report.
- `Question`: explicit user input request with notification state.

`Task` remains the public API name for now, but it is the objective record. There is no separate compatibility layer that tries to preserve old task-completion semantics.

## Completion Mode Replacement

`completionMode=github/local` is the wrong abstraction. A task/objective may open zero, one, or many PRs. PRs are artifacts of the work, not the task's completion channel. Intermediate PRs do not complete or block the objective, and final task success does not publish a PR implicitly.

Replace completion mode with agent-emitted actions:

- `publish_pull_request`: create a new PR artifact from the current session branch.
- `update_pull_request`: update a specific existing PR artifact.
- `watch_pr`: start or continue babysitting a PR.
- `ask_user`: create a user question.
- `spawn_work`: enqueue more work items.
- `finish_objective`: mark objective done, with explanation and linked artifacts.

The daemon should validate hard safety only: correct branch, correct PR, no merged/closed PR update, no branch cross-contamination, no missing repo, no unowned PR.

## Pull Request Rules

- Opening an intermediate PR never changes objective status to waiting.
- Closing an intermediate PR means the artifact was rejected or abandoned, not that the objective is canceled.
- Merging an intermediate PR means the artifact landed, not necessarily that the objective is complete.
- PR follow-up work is scheduled from the PR feedback queue immediately, independent of whatever objective session is running.
- Each PR has a branch owner and update lease. A follow-up worker must run on that PR branch/worktree, not the objective's latest branch.

Before every PR update:

1. Refresh PR state from GitHub.
2. If merged or closed, do not push.
3. Verify branch head and PR head.
4. Apply only the follow-up session's diff against the PR branch.
5. Let the worker provide commit subject, body, and public comment text.
6. Push only if still current.

## Session-Centric UI

The UI should expose actual operational state:

- Objective summary.
- Active sessions with model, target, branch, worktree, pane tail/current action.
- Work queue: pending work items, pending PR feedback, pending steering, pending questions.
- PR artifacts with state, branch, feedback, last follow-up worker, next scheduled check.
- Scratch, artifacts, and memory entries.
- Event log as supporting detail, not the main state model.

Steering should target an objective, a specific session, a specific PR, a queued work item, or a specific worker.

## Work Queue

Schedulers enqueue durable work. Workers lease work. Completion emits artifacts/events and may enqueue more work.

Initial work item kinds:

- `objective.plan`
- `objective.implement`
- `objective.slice`
- `objective.compose`
- `pr.followup`
- `pr.ci_repair`
- `pr.review_reply`
- `user.steering`
- `user.question_answered`
- `session.recover`

Cancellation should be cheap:

- Cancel objective: mark objective canceled, revoke unleased work, request-kill active sessions.
- Cancel PR follow-up: cancel only that work item/session.
- Cancel session: kill pane/process, release lease, leave objective alive.

## Replanner Role

Keep dynamic replanning, but demote it from "brain of the whole task" to "planner for the next work items/actions."

- It receives bounded objective state, PR state, artifacts, memory, and active queue.
- It returns work items/actions, not final candidates.
- For broad objectives, there is no unproductive-turn terminal limit.
- Context overflow creates an `ask_user` or `objective.needs_compaction` work item, never fallback publication.
- A failed worker is just a result to incorporate, not automatically objective failure.

## Memory And Scratch

Use a layered model:

- Short-term: current context ledger, per objective.
- Medium-term: objective scratch notes/artifacts, writable by workers.
- Long-term: project-scoped memory repo or directory with markdown notes, provenance, and UI browsing.
- Shared scratch: project/objective-scoped directory for baseline binaries, benchmark outputs, build caches, and reports.

Scratch can hold bulky transient artifacts. Memory holds compact facts with provenance. The event log remains audit. Prompts get bounded retrieval, not raw scratch/event dumps.

## Wide Work

Wide objectives should be native:

- Planner creates `objective.slice` work items.
- Each slice session owns a file set or subsystem.
- Slice sessions can publish PRs independently.
- Compose sessions reconcile slices when needed.
- Validation sessions run against composed state.
- UI shows horizontal progress by slice, not just one vertical chain.

## Persistence Shape

Move active UI/scheduler paths toward tables:

- `objectives`
- `sessions`
- `work_items`
- `session_events`
- `pull_requests`
- `pull_request_feedback`
- `artifacts`
- `questions`
- `steering`
- `leases`
- `targets`
- `memory_entries`

The event log remains append-only audit. Tables are authoritative read/write state for scheduler and UI.

## Current Implementation Status

PR #396 is intentionally breaking and has moved the core model substantially toward this document:

- durable `workItems` are now the scheduler surface for objective work, PR follow-up, steering, and user questions
- PR follow-up is queued as background work and does not block broad objective work
- steering can target the objective, a session/worker, a work item, or a pull request
- user questions can be answered directly by question id and are recorded as `user.question_answered` work items
- intermediate PRs are explicit artifacts/actions; task completion no longer implicitly publishes a final candidate
- the legacy `OrchestrationGraph` read/UI surface has been removed
- objective scratch paths are surfaced in task detail from recorded shared workspace metadata
- GitHub PR `headRefOid` is retained in metadata, and code-changing PR updates fail before push when the worker base revision is stale or from the wrong PR head
- the planner contract is now work-item/action oriented: legacy `workers` and `spawns` plan shapes are no longer normalized by production code and fail validation instead
- pull request branch ownership and update leases are explicit read-model fields, projected from PR publish/update events and shown in the PR UI instead of only being buried in metadata
- session panes show current action, target/worktree/scratch context, targeted steering, and direct session cancellation controls
- `objective.slice`, `objective.compose`, and `objective.validate` work items are first-class planner outputs and the task detail UI shows horizontal wide-work progress by slice/compose/validation lane
- project-scoped memory entries are retrieved into replanning context and browsable in task detail alongside task memory, with scope metadata distinct from objective scratch
- scheduler/UI state now reads from normalized tables for tasks, workers, work items, sessions, PRs, feedback, artifacts, questions, steering, targets, memory, and session current-action state instead of requiring full event replay or JSON-row projection blobs on active paths

Remaining high-value gaps:

- none tracked in this document

## Migration Plan

1. Add durable `work_items`, initially mirrored from existing task/worker events.
2. Move PR follow-up scheduling to `work_items`; stop coupling it to task routine status.
3. Give every PR an owner branch/worktree/lease and enforce update identity before push.
4. Add session-centric UI panes and targeted steering.
5. Convert broad objective replanning to emit actions/work items instead of final candidates.
6. Add shared scratch and project memory surfaces.
7. Remove completion mode entirely; task completion records objective state and never publishes a PR implicitly.
8. Delete old final-candidate publication recovery paths instead of maintaining them beside explicit PR artifacts.
9. Collapse old execution graph paths once the new scheduler owns active work.

## Hard Invariants

- A closed intermediate PR cannot cancel an active objective.
- A merged PR cannot be force-pushed or updated.
- A PR follow-up worker can only update its target PR branch.
- Multiple PRs can be active for one objective.
- PR feedback queues spawn work immediately even if objective work is running.
- Cancel objective kills active sessions without scanning/rebuilding the world.
- User steering lands on the intended target.
- No final candidate fallback on broad objectives.
- UI task/detail reads do not require replaying the full event log.
