# aged

`aged` is a local-first agent orchestrator for autonomous development work.

The current implementation is an initial vertical slice:

- Go daemon with SQLite event persistence
- Prompt-driven and API-backed orchestrator brain providers
- Mock, Codex, Claude, and shell worker runner adapters selected by the orchestrator
- Normalized worker events for logs, results, errors, and worker requests for input
- VCS-pluggable workspace preflight before workers start
- HTTP API and SSE event stream
- React/Vite dashboard for task creation, steering, cancellation, and live state

## Run the daemon

```sh
go run ./cmd/aged
```

The daemon listens on `http://127.0.0.1:8787` by default and writes state to `aged.db`.

Useful flags:

```sh
go run ./cmd/aged -addr 127.0.0.1:8787 -db aged.db -worker mock -workdir .
```

Scheduler behavior:

- `-worker mock` sets the orchestrator's fallback runner when the prompt brain does not choose a different runner.
- Users do not choose workers per task; task creation only supplies the work request.
- Available runner adapters include `mock`, `codex`, `claude`, and `shell`.
- Each task records a `task.planned` event with the orchestrator's selected `workerKind`, `workerPrompt`, rationale, steps, approvals, and future spawn hints.
- Before a worker is created, the orchestrator records `worker.workspace_prepared` with the prepared cwd, source root, current change, dirty status, VCS type, and workspace mode.
- Workspace backends are selected with `-workspace-vcs auto|jj|git`; `auto` prefers `jj` when `.jj` is present and otherwise supports Git repos.
- Isolated mode is the default. Jujutsu repos use `jj workspace add -r @`; Git repos use `git worktree add --detach HEAD` and require a clean source working tree.
- Workspace cleanup is selected with `-workspace-cleanup retain|delete_on_success|delete_on_terminal`. Cleanup emits `worker.workspace_cleaned` and does not hide the worker's terminal status.
- Worker subprocess output is normalized into `worker.output` events. Plain output becomes log/error events; Codex and Claude JSONL output preserves raw payloads and is classified as `log`, `result`, `error`, or `needs_input`.
- `worker.completed` includes derived run semantics: `summary`, `error`, `needsInput`, `logCount`, and `workspaceChanges` when available. Workspace changes include dirty status, diffstat, and changed files. Workers that emit `needs_input` move the task to `waiting` and retain the workspace.
- Retained isolated workspace changes can be reviewed with `GET /api/workers/{id}/changes` and applied back to the source checkout with `POST /api/workers/{id}/apply`. Jujutsu apply creates a new merge revision with source `@` and the worker workspace revision as parents; Git apply commits the worker worktree and merges that commit. Apply records `worker.changes_applied`.

API-backed scheduling can be enabled with:

```sh
AGED_BRAIN=api AGED_BRAIN_MODEL=<model> AGED_BRAIN_API_KEY=<key> go run ./cmd/aged
```

The API brain uses an OpenAI-compatible chat completions endpoint. Override it with `AGED_BRAIN_ENDPOINT` or `-brain-endpoint`. If the API brain is not configured or returns invalid structured output, the daemon falls back to the local prompt brain.

## Run the dashboard

```sh
cd web
npm install
npm run dev
```

Vite proxies `/api` requests to the daemon. Open `http://127.0.0.1:5173`.

## Build

```sh
go test ./...
cd web && npm run build
```

After `web/dist` exists, the Go daemon serves it from the same origin.

## API sketch

- `GET /api/health`
- `GET /api/snapshot`
- `GET /api/events?after=<id>`
- `GET /api/events/stream?after=<id>`
- `POST /api/tasks`
- `POST /api/tasks/{id}/steer`
- `POST /api/tasks/{id}/cancel`
- `GET /api/workers/{id}/changes`
- `POST /api/workers/{id}/apply`
- `POST /api/workers/{id}/cancel`
