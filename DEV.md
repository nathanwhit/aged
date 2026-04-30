# Development Status

Last updated: 2026-04-29

## Current State

The initial local-first vertical slice is implemented.

- Go daemon with SQLite-backed append-only event log.
- Task and worker state reconstructed from events.
- Snapshot reconstruction pages through the complete event log instead of stopping at the public event-listing page size.
- HTTP API for health, snapshots, event history, task creation, steering, and cancellation.
- SSE event stream for live dashboard updates.
- Prompt-driven, Codex-backed, and API-backed orchestrator brain abstraction.
- Plugin manifests are first-class snapshot/API state. Built-in brain/runner/driver capabilities are projected by default, and `-plugins` / `AGED_PLUGINS` can add external plugin descriptors for drivers, runners, or integrations. Enabled `aged-plugin-v1` command plugins are probed at startup with `command... describe`.
- Project registry support for managing multiple local repositories from one daemon. Projects are persisted in SQLite; `-projects` / `AGED_PROJECTS` seeds an empty database, otherwise the daemon creates a default project from `-workdir`. Tasks can carry `projectId`, and external task metadata can map a `repo` to a configured project.
- Runtime project management is available through `POST /api/projects`, `PUT /api/projects/{id}`, `DELETE /api/projects/{id}`, `GET /api/projects/{id}/health`, and the dashboard Projects panel. Project registration validates `localPath`, discovers VCS kind / GitHub repo metadata from the checkout when possible, and fills `defaultBase` from local branch metadata when available.
- The daemon exposes a streamable HTTP MCP endpoint at `POST /mcp`, protected by the same auth middleware. MCP tools cover snapshot inspection, task/project listing and creation, PR publish/refresh/babysit, retry, steering, and cancellation; MCP resources expose the current snapshot and individual tasks/workers/PRs.
- Worker runners for `mock`, `codex`, `claude`, `shell`, and `benchmark_compare`.
- Scheduler plans can set per-worker `reasoningEffort` / thinking level (`default`, `low`, `medium`, `high`, `xhigh`, `max`). Codex workers receive it through `model_reasoning_effort` and Claude workers through `--effort`; unsupported/default values leave the runner default intact.
- React/Vite dashboard for creating tasks, assistant Q&A, PR publishing, viewing state/logs, steering, and cancellation.
- Dashboard has a phone-friendly responsive layout and a current-state summary with progress, active work, worker counts, target state, execution nodes, and timeline.
- Dashboard can clear finished tasks individually or in bulk. Clearing records `task.cleared` and hides the task, workers, and execution nodes from snapshots/UI without deleting event history.
- Worker cards show live activity from events: latest event text, command summary, and expandable recent worker logs/results/errors. Workers can be selected to drill into a detailed worker-scoped view with command, workspace, completion, target/node, and full worker event history.
- Worker event rendering recognizes Codex/Claude JSON stream shapes from persisted SQLite history: command executions render as shell cards with highlighted command/script, exit status, and truncated output; agent messages, file changes, usage, lifecycle, and completion events get dedicated compact views with raw JSON behind details.
- Worker detail and event views use compact responsive metadata strips, scroll-safe path rows, structured lifecycle cards, and concise timeline summaries instead of wrapping raw JSON/path fields into oversized cards.
- The dashboard hides Codex's benign `failed to record rollout items` session bookkeeping message from active worker progress, including older persisted events recorded before the parser downgrade.
- Workers that emit `needs_input` now create `approval.needed` events. Replanning brains can answer autonomously with a continuation plan; otherwise the task waits, and user/orchestrator feedback through task steering records `approval.decided` and resumes with a continuation worker.
- External drivers can use `POST /api/tasks` directly with optional `source`, `externalId`, and metadata. `source` plus `externalId` dedupes visible tasks, and `GET /api/tasks/lookup` resolves an external item back to its aged task.
- Built-in GitHub driver support is available with `-github-driver` / `AGED_GITHUB_DRIVER`. It polls configured GitHub issues through `gh`, creates idempotent `github-issue` tasks, auto-publishes PRs for succeeded GitHub issue tasks that opt into GitHub completion mode, refreshes known PR status, and starts babysitter tasks when checks fail, reviews request changes, or mergeability is blocked/dirty.
- Built-in Discord driver support is available with `-discord-driver` / `AGED_DISCORD_DRIVER`. It polls configured channels with a bot token, ignores startup history by default, answers messages through the configured assistant, supports structured `answer` / `list_projects` / `create_project` / `propose_task` / `create_task` decisions, carries `projectId` / `title` / `prompt` into created tasks, can add persisted projects through chat, runs chat turns in the selected project checkout for read-only code inspection, recovers pending proposals from persisted assistant events, and still supports `task: <prompt>` plus `do it` shortcuts.
- Worker workspace preparation, local worker cwd, source apply, and PR publishing now resolve through the task's project rather than an implicit daemon-wide checkout.
- Worker prompts are wrapped by the service with the prepared execution workspace before dispatch. In isolated mode the prompt explicitly tells workers to edit only the isolated workspace and not the source checkout, even if the scheduler's prompt mentions another local path.
- Lightweight assistant Q&A is available through `POST /api/assistant` and the dashboard Ask panel. It is now separate from the scheduler brain: `-assistant auto|codex|claude|brain|none` can use Codex or Claude directly even when scheduling runs with the prompt brain.
- Assistant conversations persist Codex/Claude provider session ids in assistant events and pass them back on later turns, so headless `codex exec resume` and `claude --resume` can provide real follow-up continuity.
- Task titles are optional. Blank titles are generated through the configured assistant with a 1-8 word title prompt, with a deterministic prompt-derived fallback if the assistant fails.
- Pull requests are first-class projected state in snapshots. `pull_request.published`, `pull_request.status_checked`, and `pull_request.babysitter_started` events reconstruct current PR state, including repo, number, branch, CI/check status, review status, merge status, and any babysitter task.
- Tasks now carry objective state separately from worker/task execution status. `task.objective_updated`, `task.milestone_reached`, and `task.artifact_recorded` events project `objectiveStatus`, `objectivePhase`, milestones, and artifacts onto each task so long-lived objectives can remain active or externally waiting after a worker graph finishes.
- Tasks can now complete with a first-class final candidate worker. The orchestrator records `task.final_candidate_selected`, snapshots expose `finalCandidateWorkerId`, and local mode exposes a task-level apply action so users apply the selected task result once instead of applying every worker.
- Final candidate selection is no longer an unconditional "latest worker wins" heuristic. Completion automatically selects only a single changed candidate or a single candidate leaf in a dependency lineage; if parallel competing candidate leaves remain, the task moves to `waiting` with an approval-needed event unless the replanning brain explicitly returns `finalCandidateWorkerId` or schedules a consolidation/validation turn.
- If the replanner selects a successful review/validation worker that has no changes, final-candidate resolution follows that worker's `baseWorkerID` lineage and applies the nearest changed ancestor instead of failing the task.
- Codex scheduler/replanner prompts now explicitly require a single bare JSON object with `{` as the first non-whitespace character and `}` as the last. The Codex brain also extracts the first balanced JSON object from otherwise-wrapped output, so accidental trailing braces/prose do not automatically fail parsing.
- Dynamic replan errors now degrade deterministically: if existing worker results have one unambiguous final candidate, aged records a fallback `task.replanned` decision and completes with that candidate; if candidates remain ambiguous, aged moves the task to `waiting` with an approval-needed event instead of failing spuriously.
- Retry is graph-aware for completed-graph orchestration failures. If a task failed after workers completed because replanning/parsing failed, final candidate selection was invalid, or a review/validation follow-up failed after a valid candidate existed, retry reconstructs the completed worker graph from events and reruns only the orchestration/final-selection step instead of starting the last worker again.
- Task creation supports `metadata.completionMode`: `local` means review/apply the final candidate through aged, while `github` means completion publishes the final candidate as a PR, records the PR as a task artifact, moves the task to `waiting` / `waiting_external`, and treats merge as the apply/satisfaction point.
- Completed tasks or tasks with a selected final candidate can be published as GitHub PRs with `POST /api/tasks/{id}/pull-request`, GitHub completion mode, or the dashboard PR panel. The service defaults to the final candidate worker, applies it first when needed, creates/pushes a branch, opens a PR with `gh`, records the published PR, and records a `pr_opened` milestone.
- Pull request follow-up can be driven by `POST /api/pull-requests/{id}/refresh`, `POST /api/pull-requests/{id}/babysit`, or the GitHub driver monitor loop.
- Pull request refresh updates the task's PR artifact and objective phase. Merged PRs record a `pr_merged` milestone and satisfy the task; closed unmerged PRs abandon/cancel the task.
- Google OAuth can protect the dashboard/API for public exposure. `-auth google` requires Google client credentials, an allowed-email list, and uses signed HTTP-only session cookies.
- Built-in Codex/Claude workers no longer hold stdin open for steering, avoiding `codex exec` waiting forever for appended stdin input.
- Daemon startup recovery marks stale local nonterminal workers as canceled when their process handles are no longer recoverable.
- Built dashboard can be served directly by the Go daemon from `web/dist`.
- Dev control server in `cmd/aged-dev` can rebuild the daemon, rebuild the UI, and restart the managed daemon through a local HTTP trigger.
- Dev control can be launched with `-daemon-addr 0.0.0.0:8787` so the dashboard is reachable from other devices on the local network.
- Frontend toolchain is on Vite 8 with `@vitejs/plugin-react` 6.
- Scheduling is recorded as `task.planned` before worker creation.
- Workspace preflight is recorded as `worker.workspace_prepared` before worker creation.
- Workspace allocation is VCS-pluggable: `auto`, `jj`, or `git`.
- Workspace cleanup policy is configurable: `retain`, `delete_on_success`, or `delete_on_terminal`.
- Worker output is normalized into log/result/error/needs-input events, with raw JSON preserved for Codex and Claude streams.
- Worker completion records derived `summary`, `error`, `needsInput`, `logCount`, changed files, and workspace diffstat/status fields.
- Worker execution is projected as first-class `executionNodes` snapshot state from `execution.node_planned` events, with node id, worker id, worker kind, spawn id, dependencies, role/reason, and status.
- Per-task orchestration graphs are projected from durable execution events into snapshots/UI, including nodes, parent/dependency edges, target placement, and aggregate status counts.
- Execution target pools are configurable with `-targets` / `AGED_TARGETS`. The default target is local; SSH targets use detached tmux sessions and remote status/log files so work can outlive the SSH connection.
- Target placement is service-owned. Scheduler-provided target labels are ignored and annotated; task metadata target labels take precedence over project target labels.
- SSH targets write remote VCS change artifacts (`jj` or Git summaries plus `diff.patch`) into the run directory; the orchestrator reads them back into `worker.completed.workspaceChanges` for normal review/projection.
- Remote worker patches can be reviewed/applied through the normal worker apply endpoint; the current implementation applies the collected `diff.patch` to the task project's local checkout and records `worker.changes_applied`.
- Target scheduling scores matching targets by labels, capacity, current running workers, worker size, and memory/CPU hints.
- Retained isolated worker changes can be reviewed and applied through HTTP/UI; jj apply creates a merge revision and records `worker.changes_applied`.
- Task-level apply policy recommendations are available through `POST /api/tasks/{id}/apply-policy`; when the orchestrator selected a final candidate, the recommendation is `apply_final` / `already_applied`, with manual selection remaining as a fallback for unresolved competing candidates.
- Follow-up workers inherit candidate state from the latest changed successful dependency. For jj workspaces this uses the candidate workspace revision; for Git workspaces this copies the base workspace patch into a fresh worktree before running the follow-up worker.
- Active task steering is delivered to currently running workers through `worker.Spec.Steering` for runners that support mid-run steering. Codex and Claude exec adapters intentionally do not hold stdin open because those CLIs treat piped stdin as extra prompt input and may wait indefinitely.
- `benchmark_compare` provides a reusable primitive for explicit numeric before/after benchmark comparison.
- Repeated worker apply attempts are blocked in the service and already-applied workers render as disabled `Applied` actions in the dashboard.
- Failed or canceled tasks can be retried through `POST /api/tasks/{id}/retry` and the dashboard. Retry reuses the persisted plan on the same task id, records a new `task.planned`, and runs normal follow-up/replan handling again; this supports picking work back up after daemon restart recovery marks local workers canceled.
- Scheduler `spawns` now run as dependency-aware follow-up worker turns after the plan's primary worker succeeds; independent spawns run in parallel and dependent spawns wait for prerequisite spawn ids. This applies to both initial plans and dynamic `continue` replans.
- Follow-up worker prompts include the original request, follow-up role/reason, prior worker summaries/errors, and changed files.
- Failed review/validation follow-up workers are preserved in the orchestration state and sent to the replanner instead of immediately failing the whole task. That lets the orchestrator retry with another worker, continue from a valid candidate, or ask for steering.
- Dynamic replanning is available for brains that implement `Replan`: after follow-up workers, the brain can return `continue`, `complete`, `wait`, or `fail`; `continue` schedules another worker turn and records `task.replanned`.
- Dynamic replanning carries workers created during `continue` turns forward into final completion, so a replanner-selected final candidate can be a worker that did not exist before the replan loop began.
- Codex parser treats `agent_message` as the useful result summary, leaves `turn.completed` usage records as logs, and downgrades Codex's benign `failed to record rollout items` shutdown/session-recording message so successful workers do not surface it as their completion error.

## Verified

- `go test ./...`
- `npm run build`
- Rebuild endpoint smoke: `GET /rebuild` successfully rebuilt daemon/UI and restarted the managed daemon on `0.0.0.0:8787`; `GET /api/health` returned ok.
- SQLite event history inspection against `aged.db` confirmed existing worker output shapes are mostly `command_execution`, `agent_message`, `file_change`, lifecycle, and usage events.
- Clear-task tests verify `task.cleared` hides tasks/workers/execution nodes while preserving events, and the bulk clear HTTP endpoint hides terminal tasks.
- Auth tests verify Google auth protects API/static routes, leaves health public, and creates a session through a fake OAuth callback for an allowed Google account.
- MCP tests verify initialize, tool listing, task creation through `tools/call`, resource listing/reading, and auth protection.
- Worker tests verify the default Codex runner does not advertise stdin steering; recovery tests verify stale local workers become canceled on daemon startup.
- Worker-question tests verify autonomous replanning answers and user feedback both resume waiting tasks.
- Assistant tests verify Q&A records durable question/answer events.
- CLI assistant tests verify Codex JSON and Claude stream output are converted into Ask responses.
- Title-generation tests verify blank task titles use the generator and fall back locally when generation fails.
- Pull request tests verify single-worker auto-apply before publish, PR projection, status refresh, and babysitter task scheduling.
- GitHub driver tests verify issue polling dedupes tasks, GitHub-mode succeeded issue tasks are auto-published as PRs, and PRs needing attention are refreshed and babysat.
- Discord driver tests verify startup history skipping, direct task creation, assistant-suggested `do it` task creation, and fallback task creation when the assistant cannot answer conversationally.
- Project tests verify SQLite persistence, startup seeding/loading, runtime API creation, explicit `projectId` routing, external repo-to-project mapping, and PR publishing defaults from project repo/base settings.
- Retry and workspace-guard tests verify failed/canceled tasks rerun from persisted plans and workers receive the prepared workspace path instead of relying on scheduler-generated paths.
- `npm ls vite @vitejs/plugin-react`
- Scheduler tests:
  - Codex brain parses Codex `agent_message` plans.
  - Codex brain falls back on invalid output.
  - API brain parses structured plans.
  - API brain falls back on invalid output.
  - Service uses the brain-selected worker.
  - Service runs a spawned follow-up review worker after the initial worker succeeds.
  - Service runs independent spawned follow-up workers in parallel.
  - Service honors spawned worker dependencies before starting dependent follow-ups.
  - Follow-up worker prompts include prior worker result context.
  - Service dynamically replans after follow-up output and can schedule an incorporation worker.
  - Service runs spawned workers from dynamic replans before asking the brain for the next decision.
  - Service can complete with a final candidate worker created during a dynamic replan turn.
- Service emits durable execution graph nodes into snapshots.
- Snapshot projection includes per-task orchestration graphs with dependency edges.
- Plugin registry tests verify built-in/configured plugin descriptors and executable `describe` probing.
- Service schedules workers onto local or SSH execution targets and records target/session metadata on execution nodes.
  - SSH runner tests verify remote change artifact and patch collection/parsing.
  - Service tests verify remote worker patch artifacts route through the normal apply flow.
  - Service delivers task steering to compatible running workers.
  - Service records final task candidates, applies task results through the selected candidate, and publishes GitHub-mode completions through that final candidate.
  - Service waits on ambiguous parallel candidate completion without explicit final selection, accepts replanner-selected `finalCandidateWorkerId`, and resolves selected review/validation workers to their changed candidate ancestor.
  - Service continues to dynamic replanning after a follow-up worker fails, preserving the failed result as context instead of failing the task immediately.
  - Service falls back from malformed replanner output to deterministic final-candidate completion when unambiguous, or waits for steering when ambiguous.
  - Service retries dynamic-replan, final-candidate-selection, and follow-up-worker failures from completed worker events without creating another worker when a recoverable candidate graph already exists.
  - Service recommends final-candidate apply when one has been selected and falls back to manual apply selection when changed worker branches are unresolved.
  - Unknown brain-selected workers fail the task cleanly.
  - HTTP API rejects user-supplied worker selection.
- Worker runner integration tests:
  - stdout/stderr streaming
  - non-zero exits
  - cancellation
  - large lines
  - command-not-found
  - Codex/Claude JSONL normalization
  - Codex `turn.completed` does not overwrite the final agent-message summary
  - Benchmark comparison primitive emits an improvement verdict for explicit before/after inputs
- Workspace tests:
  - service passes the prepared cwd into the runner
  - service emits `worker.workspace_prepared`
  - service records worker completion summaries
  - service records terminal workspace changes on `worker.completed`
  - service applies retained worker workspace changes back to the source checkout
  - `needs_input` worker events move the task to `waiting` and retain the workspace
  - `JJWorkspaceManager` captures the current repo root, `jj @` change, status, mode, and dirty flag
  - jj and Git changed-file summaries are parsed into normalized file/status records
  - auto workspace selection uses the current repo VCS
  - cleanup decisions honor retain/delete-on-success/delete-on-terminal policies
- Real CLI smoke tests:
  - `codex exec --json` in `/tmp/aged-real-cli-smoke` emitted `thread.started`, `turn.started`, `item.completed`, and `turn.completed`.
  - `claude --print --output-format stream-json` in `/tmp/aged-real-cli-smoke` emitted `system`, `rate_limit_event`, `assistant`, and `result`.
  - Claude reported `total_cost_usd: 0.058204` for the smoke run.
- API smoke test:
  - `GET /api/health`
  - `POST /api/tasks`; the orchestrator selected the configured fallback worker
  - `GET /api/snapshot` showed the mock task reaching `succeeded`
  - After a daemon restart, `GET /api/snapshot` correctly replayed the self-apply task past event 200 and showed task/worker `succeeded`.
  - Firefox Developer Edition loaded `http://127.0.0.1:8787`, showed the self-apply task/worker as `Succeeded`, and showed the applied worker action as disabled `Applied`.
- Dev control smoke test:
  - Ran `cmd/aged-dev` on `127.0.0.1:8791` managing a test daemon on `127.0.0.1:8789`.
  - `GET /rebuild` built `./cmd/aged`, ran `npm run build`, started the rebuilt daemon, and `GET /api/health` on the managed daemon returned ok.
  - Stopping the dev control server stopped its managed daemon child.
- Codex brain smoke test:
  - Ran daemon with `-brain codex -worker mock -workspace-mode shared` on `127.0.0.1:8788`.
  - Initial run fell back because the scheduler prompt did not explicitly require object-shaped `steps`.
  - Tightened `prompts/scheduler.md` with the exact output object schema and field rules.
  - Re-run produced `metadata.brain: codex`, selected `workerKind: mock`, emitted object-shaped `steps`, and the mock worker reached `succeeded`.
- Self-bootstrap smoke test:
  - Started a retained Codex worker in an isolated jj workspace.
  - Worker refined a focused completion-summary test and reported package-test sandbox limitations.
  - The run exposed and drove a parser fix for Codex `turn.completed` result overwrite.
- Self-apply smoke test:
  - Started retained Codex worker `cc89cf1f-82e8-4a0b-855e-6296128bc915` in isolated jj workspace `aged-cc89cf1f-82e`.
  - Worker added `TestNativeWorkspaceApplyResultsPreserveMethodAndFiles` in `internal/orchestrator/workspace_test.go`.
  - `POST /api/workers/cc89cf1f-82e8-4a0b-855e-6296128bc915/apply` applied the worker revision with method `jj_new_merge`.
  - Current source workspace `@` is a merge revision with parents `rswvvtss` and worker revision `krozmvrk`, so the worker change was integrated with jj merge semantics rather than a squash.
  - Focused worker test passed; full worker-local package test hit the known sandbox limit when jj attempted to write parent repo objects.

## Running Locally

Current daemon command:

```sh
GOPATH=/tmp/aged-gopath GOMODCACHE=/tmp/aged-gopath/pkg/mod GOCACHE=/tmp/aged-go-build go run ./cmd/aged -addr 127.0.0.1:8787 -db aged.db -worker codex -workdir . -workspace-cleanup retain
```

Dashboard URL:

```text
http://127.0.0.1:8787
```

## Known Notes

- `aged.db` is local runtime state and is ignored.
- `web/dist` is generated by `npm run build` and is ignored.
- `.aged/dev/` contains the dev-control-built daemon binary and logs and is ignored through `.aged/`.
- The `mock` worker is best for cheap orchestration/UI validation; `codex` is now usable for retained local self-bootstrap probes.
- Users do not choose workers per task. Scheduling is owned by the orchestrator brain/provider.
- The orchestrator must support long-lived, multi-turn tasks: it should be able to decompose large refactors, schedule implementation/review/follow-up worker turns, inspect outputs, incorporate feedback, request approvals, and decide when to continue, revise, merge, or stop.
- Performance-improvement tasks currently rely on prompt conventions: scheduler guidance should decompose work into investigation, profiling/benchmark analysis, implementation, and validation turns; workers should report stable markdown sections including benchmark commands and before/after numbers when applicable.
- Codex-backed scheduling is available with `AGED_BRAIN=codex`.
- API-backed scheduling is available with `AGED_BRAIN=api`, `AGED_BRAIN_MODEL`, and `AGED_BRAIN_API_KEY`.
- Codex and Claude runners are wired as subprocess adapters; Codex has now completed retained self-bootstrap and self-apply tasks end to end.
- Worker output normalization has been tuned against one real Claude smoke stream and retained Codex self-bootstrap/self-apply runs; broader task streams still need coverage.
- Claude smoke used `--no-session-persistence` to avoid durable state from a disposable parser probe. The default runner does not hard-code that policy.
- Isolated workspace mode is the default.
- Jujutsu isolated workspaces are created with `jj workspace add -r @`.
- Git isolated workspaces are implemented with `git worktree add --detach HEAD`, but require a clean source working tree because Git worktrees cannot safely carry uncommitted source changes.
- Tests that run real `jj` preflight may need permission to let `jj` snapshot `.git/objects` in the sandbox.
- User steering is recorded as events and delivered to compatible active runners through `worker.Spec.Steering`; Codex/Claude exec adapters need an out-of-band resume/session mechanism before they can support mid-run steering safely.
- Multi-turn orchestration executes initial and dynamically replanned `spawns` as dependency graphs. Dynamic replanning still has a bounded maximum turn count.
- SSH target execution exists for pre-provisioned machines and now collects remote VCS summaries/patch artifacts and can apply the patch back locally. Conflict handling is currently delegated to `git apply` errors.
- GitHub PR publishing and the built-in GitHub driver currently depend on local `gh` authentication and repo remotes being configured.
- Apply is task-level by default: in local mode the user applies the final selected candidate after the task is terminal; in GitHub mode the task publishes the final candidate as a PR, waits on the external PR lifecycle, and merge is the apply/satisfaction step.
- Project creation/editing/deletion is DB-backed through the API/UI. Project health checks cover local path accessibility, detected VCS, configured GitHub repo/auth readiness, default base status, and target-label matchability.
- Assistant Q&A is currently one-shot CLI execution. It records conversation ids, but does not yet resume durable Codex/Claude sessions across asks.
- Discord conversational quality uses the configured assistant and now benefits from persisted Codex/Claude session resume. MCP remains the cleaner path when an external agent runtime is available.
- Task retry is currently plan-level retry, not a durable Codex/Claude session resume or first-class graph resume from an arbitrary failed execution node.

## Next Work

- Add more tests for event replay and state reconstruction.
- Add richer artifact semantics beyond PRs and changed source files.
- Add structured benchmark/profiler artifact semantics when prompt-parsed text stops being reliable enough for comparison, UI display, or audit trails.
- Extend benchmark comparison beyond explicit numeric prompt fields if the orchestrator needs to enforce same-command before/after runs, repeated samples, thresholds, or anti-cherry-picking rules.
- Add UI affordances for workspace location and cleanup status.
- Exercise the Codex/API brains against real code-editing tasks and continue tuning `prompts/scheduler.md`.
- Exercise dynamic replanning with a real Codex brain on a retained self-editing task.
- Extend the executable plugin process protocol beyond startup `describe` probing into long-running driver lifecycle management.
- Improve task cancellation so task-scoped worker indexing survives daemon restart.
- Add approval request/decision flow in the UI and orchestrator.
- Add richer remote apply conflict handling and optional remote artifact download/storage beyond the current patch application path.
- Add live SSH resource probes for CPU/memory/load rather than relying only on configured capacity and assigned worker count.
- Ingest GitHub review comments/check logs as richer structured context for babysitter tasks.
- Add configurable PR publish policy for branch naming, base branch detection, draft-vs-ready default, repo selection, merge permissions, and whether aged should ever merge automatically.
- Finish project CRUD and health checks in the UI/API, including editing/deletion, repo path validation, VCS detection, default branch discovery, and per-project runner/target policy.
