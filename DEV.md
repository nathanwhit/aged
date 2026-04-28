# Development Status

Last updated: 2026-04-28

## Current State

The initial local-first vertical slice is implemented.

- Go daemon with SQLite-backed append-only event log.
- Task and worker state reconstructed from events.
- Snapshot reconstruction pages through the complete event log instead of stopping at the public event-listing page size.
- HTTP API for health, snapshots, event history, task creation, steering, and cancellation.
- SSE event stream for live dashboard updates.
- Prompt-driven, Codex-backed, and API-backed orchestrator brain abstraction.
- Worker runners for `mock`, `codex`, `claude`, `shell`, and `benchmark_compare`.
- React/Vite dashboard for creating tasks, viewing state/logs, steering, and cancellation.
- Dashboard has a phone-friendly responsive layout and a current-state summary with progress, active work, worker counts, target state, execution nodes, and timeline.
- Dashboard can clear finished tasks individually or in bulk. Clearing records `task.cleared` and hides the task, workers, and execution nodes from snapshots/UI without deleting event history.
- Google OAuth can protect the dashboard/API for public exposure. `-auth google` requires Google client credentials, an allowed-email list, and uses signed HTTP-only session cookies.
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
- Execution target pools are configurable with `-targets` / `AGED_TARGETS`. The default target is local; SSH targets use detached tmux sessions and remote status/log files so work can outlive the SSH connection.
- Target scheduling scores matching targets by labels, capacity, current running workers, worker size, and memory/CPU hints.
- Retained isolated worker changes can be reviewed and applied through HTTP/UI; jj apply creates a merge revision and records `worker.changes_applied`.
- Task-level apply policy recommendations are available through `POST /api/tasks/{id}/apply-policy`; multiple competing changed workers produce a `manual_select` recommendation instead of pretending there is a safe automatic merge order.
- Active task steering is delivered to currently running workers through `worker.Spec.Steering` for runners that support mid-run steering. Codex and Claude command adapters now forward steering messages to subprocess stdin and record delivery log events.
- `benchmark_compare` provides a reusable primitive for explicit numeric before/after benchmark comparison.
- Repeated worker apply attempts are blocked in the service and already-applied workers render as disabled `Applied` actions in the dashboard.
- Scheduler `spawns` now run as dependency-aware follow-up worker turns after the plan's primary worker succeeds; independent spawns run in parallel and dependent spawns wait for prerequisite spawn ids. This applies to both initial plans and dynamic `continue` replans.
- Follow-up worker prompts include the original request, follow-up role/reason, prior worker summaries/errors, and changed files.
- Dynamic replanning is available for brains that implement `Replan`: after follow-up workers, the brain can return `continue`, `complete`, `wait`, or `fail`; `continue` schedules another worker turn and records `task.replanned`.
- Codex parser treats `agent_message` as the useful result summary and leaves `turn.completed` usage records as logs.

## Verified

- `go test ./...`
- `npm run build`
- Clear-task tests verify `task.cleared` hides tasks/workers/execution nodes while preserving events, and the bulk clear HTTP endpoint hides terminal tasks.
- Auth tests verify Google auth protects API/static routes, leaves health public, and creates a session through a fake OAuth callback for an allowed Google account.
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
- Service emits durable execution graph nodes into snapshots.
- Service schedules workers onto local or SSH execution targets and records target/session metadata on execution nodes.
  - Service delivers task steering to compatible running workers.
  - Service recommends manual apply selection when multiple changed worker branches compete.
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
- User steering is recorded as events and delivered to compatible active runners through `worker.Spec.Steering`; Codex/Claude subprocess adapters forward steering to stdin, but behavior still depends on whether the underlying CLI reads stdin during that run.
- Multi-turn orchestration executes initial and dynamically replanned `spawns` as dependency graphs. Dynamic replanning still has a bounded maximum turn count.
- SSH target execution exists for pre-provisioned machines; richer remote workspace synchronization and patch collection are still open.

## Next Work

- Add more tests for event replay and state reconstruction.
- Add richer artifact semantics beyond changed source files.
- Add structured benchmark/profiler artifact semantics when prompt-parsed text stops being reliable enough for comparison, UI display, or audit trails.
- Extend benchmark comparison beyond explicit numeric prompt fields if the orchestrator needs to enforce same-command before/after runs, repeated samples, thresholds, or anti-cherry-picking rules.
- Add UI affordances for workspace location and cleanup status.
- Exercise the Codex/API brains against real code-editing tasks and continue tuning `prompts/scheduler.md`.
- Exercise dynamic replanning with a real Codex brain on a retained self-editing task.
- Decide the exact plugin process protocol for external worker/plugins.
- Improve task cancellation so task-scoped worker indexing survives daemon restart.
- Add approval request/decision flow in the UI and orchestrator.
- Add richer remote workspace synchronization and remote patch/apply collection for SSH targets.
- Add live SSH resource probes for CPU/memory/load rather than relying only on configured capacity and assigned worker count.
