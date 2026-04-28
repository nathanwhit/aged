# aged - agent daemon like a fine wine

`aged` is a local-first agent orchestrator for autonomous development work.

The current implementation is an initial vertical slice:

- Go daemon with SQLite event persistence
- Prompt-driven, Codex-backed, and API-backed orchestrator brain providers
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

Remote execution targets are configured with `-targets` / `AGED_TARGETS` using JSON:

```json
{
  "targets": [
    {
      "id": "perf-1",
      "kind": "ssh",
      "host": "perf-1.internal",
      "user": "aged",
      "identityFile": "/Users/me/.ssh/id_ed25519",
      "insecureIgnoreHostKey": false,
      "workDir": "/srv/aged/repo",
      "workRoot": "/srv/aged/runs",
      "labels": { "role": "benchmark" },
      "capacity": { "maxWorkers": 2, "cpuWeight": 8, "memoryGB": 32 }
    }
  ]
}
```

SSH targets expect `ssh` and `tmux` to be available. The daemon starts a detached tmux session per worker, writes logs/status under `workRoot`, polls those files back into the normal event stream, and can resume polling after a daemon restart from execution node metadata.

Scheduler behavior:

- `-worker mock` sets the orchestrator's fallback runner when the prompt brain does not choose a different runner.
- Users do not choose workers per task; task creation only supplies the work request.
- Available runner adapters include `mock`, `codex`, `claude`, `shell`, and `benchmark_compare`.
- Each task records a `task.planned` event with the orchestrator's selected `workerKind`, `workerPrompt`, rationale, steps, approvals, and future spawn hints.
- Worker execution is also represented as first-class `execution.node_planned` state in snapshots, including node id, worker id, worker kind, spawn id, dependencies, role, and status.
- The orchestrator chooses an execution target for each worker. Plans can request labels through `metadata.targetLabels`, and the target registry scores matching VMs by capacity, current running workers, worker size, and target labels.
- The scheduler is expected to support long-lived work by planning bounded worker turns, then using later turns for review, validation, feedback incorporation, or follow-up implementation.
- `spawns` from initial and dynamically replanned plans are executed as a dependency graph after the plan's primary worker succeeds. Independent spawns run in parallel; spawns with `dependsOn` wait for their prerequisite spawn ids. Follow-up prompts include the original request plus prior worker summaries, errors, and changed files.
- Brains that implement dynamic replanning can inspect completed worker turns after follow-ups and return `continue`, `complete`, `wait`, or `fail`. `continue` schedules another worker turn, runs that plan's follow-up graph, and records `task.replanned`.
- Before a worker is created, the orchestrator records `worker.workspace_prepared` with the prepared cwd, source root, current change, dirty status, VCS type, and workspace mode.
- Workspace backends are selected with `-workspace-vcs auto|jj|git`; `auto` prefers `jj` when `.jj` is present and otherwise supports Git repos.
- Isolated mode is the default. Jujutsu repos use `jj workspace add -r @`; Git repos use `git worktree add --detach HEAD` and require a clean source working tree.
- Workspace cleanup is selected with `-workspace-cleanup retain|delete_on_success|delete_on_terminal`. Cleanup emits `worker.workspace_cleaned` and does not hide the worker's terminal status.
- Worker subprocess output is normalized into `worker.output` events. Plain output becomes log/error events; Codex and Claude JSONL output preserves raw payloads and is classified as `log`, `result`, `error`, or `needs_input`.
- `worker.completed` includes derived run semantics: `summary`, `error`, `needsInput`, `logCount`, and `workspaceChanges` when available. Workspace changes include dirty status, diffstat, and changed files. Workers that emit `needs_input` move the task to `waiting` and retain the workspace.
- Retained isolated workspace changes can be reviewed with `GET /api/workers/{id}/changes` and applied back to the source checkout with `POST /api/workers/{id}/apply`. Jujutsu apply creates a new merge revision with source `@` and the worker workspace revision as parents; Git apply commits the worker worktree and merges that commit. Apply records `worker.changes_applied`.
- Apply policy recommendations can be requested with `POST /api/tasks/{id}/apply-policy`; the service reports whether there are no candidates, exactly one candidate, or multiple competing candidates requiring manual selection or review.
- Running workers receive task steering through `worker.Spec.Steering` when a runner supports it. The Codex and Claude command adapters forward steering messages to the subprocess stdin and record a delivery log event; CLI behavior still depends on whether the underlying tool reads stdin during that run.
- `benchmark_compare` compares explicit `baseline`, `candidate`, `threshold_percent`, and `higher_is_better` prompt fields and emits a benchmark comparison report.

API-backed scheduling can be enabled with:

```sh
AGED_BRAIN=api AGED_BRAIN_MODEL=<model> AGED_BRAIN_API_KEY=<key> go run ./cmd/aged
```

The API brain uses an OpenAI-compatible chat completions endpoint. Override it with `AGED_BRAIN_ENDPOINT` or `-brain-endpoint`. If the API brain is not configured or returns invalid structured output, the daemon falls back to the local prompt brain.

Codex-backed scheduling can be enabled with:

```sh
AGED_BRAIN=codex go run ./cmd/aged
```

The Codex brain runs `codex exec --json` against `prompts/scheduler.md`, extracts the final agent message as the scheduler plan JSON, validates it, and falls back to the local prompt brain on command or validation failures. Override the binary with `AGED_CODEX_PATH` or `-codex-path`.

## Google authentication

Local development is unauthenticated by default. To expose the daemon on the web, enable Google OAuth and allowlist the Google account that should have access:

```sh
AGED_AUTH=google \
AGED_GOOGLE_CLIENT_ID=<client-id> \
AGED_GOOGLE_CLIENT_SECRET=<client-secret> \
AGED_AUTH_ALLOWED_EMAILS=you@example.com \
AGED_AUTH_SESSION_KEY="$(openssl rand -base64 32)" \
AGED_AUTH_REDIRECT_URL=https://aged.example.com/auth/callback \
go run ./cmd/aged -addr 0.0.0.0:8787
```

Create the OAuth client in Google Cloud Console as a web application and add the same callback URL as an authorized redirect URI:

```text
https://aged.example.com/auth/callback
```

Useful flags and env vars:

- `-auth` / `AGED_AUTH`: `none` or `google`
- `-google-client-id` / `AGED_GOOGLE_CLIENT_ID`
- `-google-client-secret` / `AGED_GOOGLE_CLIENT_SECRET`
- `-auth-allowed-emails` / `AGED_AUTH_ALLOWED_EMAILS`: comma-separated allowlist
- `-auth-session-key` / `AGED_AUTH_SESSION_KEY`: stable random key, at least 32 bytes
- `-auth-redirect-url` / `AGED_AUTH_REDIRECT_URL`: public callback URL when running behind a tunnel or reverse proxy

When auth is enabled, `/api/health`, `/auth/login`, `/auth/callback`, `/auth/logout`, and `/api/auth/me` remain public enough to complete login/logout. Dashboard pages and operational APIs require a signed session cookie. If `AGED_AUTH_SESSION_KEY` is omitted, the daemon generates an ephemeral key and existing sessions are invalidated on restart.

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

## Dev control server

For self-iteration, run the local dev control server:

```sh
go run ./cmd/aged-dev
```

It listens on `http://127.0.0.1:8790` by default and manages the daemon at `http://127.0.0.1:8787`.

To view the dashboard from a phone or another device on the same network, bind both listeners to all interfaces:

```sh
go run ./cmd/aged-dev -addr 0.0.0.0:8790 -daemon-addr 0.0.0.0:8787
```

Then open `http://<your-lan-ip>:8787` from the device.

- `GET /health` checks the control server.
- `GET /status` returns the last rebuild/restart result.
- `GET /rebuild` or `POST /rebuild` stops the previously managed daemon, kills any process listening on the daemon port, rebuilds `./cmd/aged`, runs `npm run build` in `web`, and starts the rebuilt daemon.

The rebuilt daemon binary and logs live under `.aged/dev/`.

## API sketch

- `GET /api/health`
- `GET /api/auth/me`
- `GET /api/snapshot`
- `GET /api/events?after=<id>`
- `GET /api/events/stream?after=<id>`
- `POST /api/tasks`
- `POST /api/tasks/clear-terminal`
- `POST /api/tasks/{id}/clear`
- `POST /api/tasks/{id}/steer`
- `POST /api/tasks/{id}/cancel`
- `GET /api/workers/{id}/changes`
- `POST /api/workers/{id}/apply`
- `POST /api/workers/{id}/cancel`
