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
- Worker runners for `mock`, `codex`, `claude`, and `shell`.
- React/Vite dashboard for creating tasks, viewing state/logs, steering, and cancellation.
- Built dashboard can be served directly by the Go daemon from `web/dist`.
- Frontend toolchain is on Vite 8 with `@vitejs/plugin-react` 6.
- Scheduling is recorded as `task.planned` before worker creation.
- Workspace preflight is recorded as `worker.workspace_prepared` before worker creation.
- Workspace allocation is VCS-pluggable: `auto`, `jj`, or `git`.
- Workspace cleanup policy is configurable: `retain`, `delete_on_success`, or `delete_on_terminal`.
- Worker output is normalized into log/result/error/needs-input events, with raw JSON preserved for Codex and Claude streams.
- Worker completion records derived `summary`, `error`, `needsInput`, `logCount`, changed files, and workspace diffstat/status fields.
- Retained isolated worker changes can be reviewed and applied through HTTP/UI; jj apply creates a merge revision and records `worker.changes_applied`.
- Repeated worker apply attempts are blocked in the service and already-applied workers render as disabled `Applied` actions in the dashboard.
- Codex parser treats `agent_message` as the useful result summary and leaves `turn.completed` usage records as logs.

## Verified

- `go test ./...`
- `npm run build`
- `npm ls vite @vitejs/plugin-react`
- Scheduler tests:
  - Codex brain parses Codex `agent_message` plans.
  - Codex brain falls back on invalid output.
  - API brain parses structured plans.
  - API brain falls back on invalid output.
  - Service uses the brain-selected worker.
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
- The `mock` worker is best for cheap orchestration/UI validation; `codex` is now usable for retained local self-bootstrap probes.
- Users do not choose workers per task. Scheduling is owned by the orchestrator brain/provider.
- Codex-backed scheduling is available with `AGED_BRAIN=codex`.
- API-backed scheduling is available with `AGED_BRAIN=api`, `AGED_BRAIN_MODEL`, and `AGED_BRAIN_API_KEY`.
- Codex and Claude runners are wired as subprocess adapters; Codex has now completed retained self-bootstrap and self-apply tasks end to end.
- Worker output normalization has been tuned against one real Claude smoke stream and retained Codex self-bootstrap/self-apply runs; broader task streams still need coverage.
- Claude smoke used `--no-session-persistence` to avoid durable state from a disposable parser probe. The default runner does not hard-code that policy.
- Isolated workspace mode is the default.
- Jujutsu isolated workspaces are created with `jj workspace add -r @`.
- Git isolated workspaces are implemented with `git worktree add --detach HEAD`, but require a clean source working tree because Git worktrees cannot safely carry uncommitted source changes.
- Tests that run real `jj` preflight may need permission to let `jj` snapshot `.git/objects` in the sandbox.
- User steering is recorded as events, but active workers do not yet consume steering messages mid-run.
- Remote VM execution is intentionally not implemented yet; the runner interface is the extension point.

## Next Work

- Add tests for event replay and state reconstruction.
- Add richer artifact semantics beyond changed source files.
- Add UI affordances for workspace location and cleanup status.
- Exercise the Codex/API brains against real tasks and tune `prompts/scheduler.md`.
- Decide the exact plugin process protocol for external worker/plugins.
- Improve task cancellation so task-scoped worker indexing survives daemon restart.
- Add approval request/decision flow in the UI and orchestrator.
- Add remote runner design once local runner behavior is stable.
