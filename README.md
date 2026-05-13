# aged

`aged` is a durable, local-first orchestrator for autonomous development work. It runs bounded agent turns under long-lived task objectives, stores state in SQLite, and can keep tasks moving through local workspaces, SSH targets, GitHub PR feedback, and user steering.

## Current State

- SQLite-backed daemon with HTTP API, SSE, MCP, and a React/Vite dashboard
- Prompt, Codex, API, and static scheduler brains
- Codex, Claude, shell, mock, benchmark, and plugin-backed workers
- `jj` and Git workspace support, with isolated workspaces by default
- Local and SSH/tmux execution targets
- Durable task objectives with milestones, artifacts, final candidates, PR state, retries, and orchestration graphs
- Built-in GitHub and Discord drivers
- Dynamic project, target, and plugin registries persisted in SQLite

## Run

```sh
go run ./cmd/aged
```

Defaults:

- address: `http://127.0.0.1:8787`
- database: `aged.db`
- worker: `mock`
- workspace mode: `isolated`
- auth: `none`

Common local command:

```sh
go run ./cmd/aged -addr 127.0.0.1:8787 -db aged.db -worker codex -workdir . -auth none
```

Useful scheduler/assistant settings:

```sh
AGED_BRAIN=codex go run ./cmd/aged
AGED_BRAIN=api AGED_BRAIN_MODEL=<model> AGED_BRAIN_API_KEY=<key> go run ./cmd/aged
AGED_ASSISTANT=codex go run ./cmd/aged
AGED_ASSISTANT=claude go run ./cmd/aged
```

The Ask panel is configured separately from the scheduler with `-assistant` / `AGED_ASSISTANT`: `auto`, `codex`, `claude`, `brain`, or `none`.

## Projects

Projects are persisted in SQLite. On an empty database, `-projects` / `AGED_PROJECTS` seeds them; otherwise aged creates one default project from `-workdir`.

```json
{
  "defaultProjectId": "aged",
  "projects": [
    {
      "id": "aged",
      "name": "aged",
      "localPath": "/Users/me/Documents/Code/aged",
      "repo": "owner/aged",
      "upstreamRepo": "owner/aged",
      "headRepoOwner": "fork-owner",
      "pushRemote": "fork",
      "defaultBase": "main",
      "workspaceRoot": "/tmp/aged-workspaces",
      "targetLabels": { "role": "default" },
      "remoteCheckouts": { "perf-1": "/srv/aged/custom/aged" },
      "pullRequestPolicy": {
        "branchPrefix": "codex/aged-",
        "draft": false,
        "allowMerge": false,
        "autoMerge": false,
        "monitorPullRequests": true
      }
    }
  ]
}
```

Tasks can set `projectId`. External tasks can also include `metadata.repo`; if it matches a configured project, aged routes the task there.

For fork workflows:

- `repo`: local checkout/fork repository
- `upstreamRepo`: issue and PR target repository
- `headRepoOwner`: fork owner for PR heads
- `pushRemote`: remote used for pushed branches/bookmarks

Runtime project management is available through `/api/projects`, `/api/projects/{id}`, `/api/projects/{id}/health`, MCP, Discord project creation, and the dashboard.

## Workspaces And Targets

Workspace flags:

- `-workspace-vcs auto|jj|git`
- `-workspace-mode isolated|shared`
- `-workspace-root <dir>`
- `-workspace-cleanup retain|delete_on_success|delete_on_terminal`

If `-workspace-root` is empty, isolated workspaces default to `~/.aged/workspaces`. Relative roots are resolved inside the source checkout.

Retained workspace artifact cleanup runs on startup by default:

- `-workspace-artifact-cleanup`
- `-workspace-artifact-cleanup-dry-run`
- `-workspace-artifact-cleanup-min-age 24h`

The current artifact allowlist removes stale terminal worker `target/` directories from retained local `jj` and Git workspaces when they are safe to clean.

SSH targets are configured with `-targets` / `AGED_TARGETS` or through `/api/targets`:

```json
{
  "targets": [
    {
      "id": "perf-1",
      "kind": "ssh",
      "host": "perf-1.internal",
      "user": "aged",
      "checkoutRoot": "/srv/aged/checkouts",
      "workRoot": "/srv/aged/runs",
      "labels": { "role": "benchmark" },
      "capacity": { "maxWorkers": 2, "cpuWeight": 8, "memoryGB": 32 }
    }
  ]
}
```

SSH targets need `ssh` and `tmux`. `identityFile` is optional; leave it unset to use normal OpenSSH behavior. Remote workers run in detached tmux sessions, publish logs/status back into the event stream, and write VCS summaries plus `diff.patch` for review/apply.

## Task Model

A task is not just a worker run. It has execution status plus objective state.

- `task.planned` records scheduler plans, worker kind, reasoning effort, steps, spawns, and actions.
- `execution.node_planned` records the durable worker graph.
- `task.objective_updated`, `task.milestone_reached`, and `task.artifact_recorded` track long-lived workflow state.
- `task.final_candidate_selected` records the worker whose changes satisfy the task.
- `worker.completed` records summary, error, needs-input state, log count, and workspace changes.

Scheduler plans can:

- run a primary worker plus dependency-aware `spawns`
- dynamically replan with `continue`, `complete`, `wait`, or `fail`
- publish intermediate PRs with `publish_pull_request`
- watch existing PRs with `watch_pull_requests`
- wait on external state with `wait_external`
- ask the user with `ask_user`

Dynamic replanning has no fixed turn cap. Durable objectives keep taking bounded turns until they wait, complete, fail, or are canceled.

### Durable Loop Mode

Set `metadata.executionMode: "loop"` to bypass scheduler planning and run one role worker repeatedly.

Loop metadata:

- `loopWorkerKind`
- `loopRole`
- `loopPrompt`
- `loopIntervalSeconds`
- `loopFreshWorkspace`
- `reasoningEffort`

`PUT /api/tasks/{id}/loop-config` updates the interval or prompt while the loop is waiting between turns. Retry can restart a succeeded loop task on the same task id.

### Completion Modes

Task metadata can set `completionMode`:

- `local`: select/apply the final candidate through aged
- `github`: publish the final candidate as a PR and treat merge as task satisfaction

If omitted, aged infers GitHub completion for issue/PR-shaped tasks and local completion otherwise.

## Pull Requests

PRs are first-class snapshot state.

- `POST /api/tasks/{id}/pull-request` publishes a selected or unambiguous candidate.
- `POST /api/tasks/{id}/watch-pull-requests` imports existing PRs into a task.
- `POST /api/pull-requests/{id}/refresh` refreshes checks, reviews, merge state, and unresolved feedback.
- `POST /api/pull-requests/{id}/babysit` marks the same task as waiting on the PR.

The GitHub monitor steers the original task when checks fail, reviews request changes, mergeability blocks, or new external PR feedback appears. Merged PRs satisfy related tasks; closed unmerged PRs abandon/cancel them. Aged does not merge PRs automatically today.

## Drivers And Plugins

GitHub driver:

```sh
go run ./cmd/aged -github-driver github-driver.json
```

It uses local `gh` auth, creates idempotent issue and mention tasks, publishes GitHub-completion task PRs, refreshes PRs, and starts same-task follow-up when PRs need work. Mention tasks use local completion, so review-request mentions can be satisfied by a GitHub review/comment without opening a new PR. The same config can be read or hot-swapped while the daemon is running:

```sh
curl http://localhost:8787/api/drivers/github
curl -X PUT http://localhost:8787/api/drivers/github \
  -H 'content-type: application/json' \
  -d '{"enabled":true,"issues":[{"repo":"owner/repo","labels":["aged"]}],"mentions":{"enabled":true,"repos":["owner/repo"]}}'
```

Projects can also opt into issue polling directly with `githubIssues`; the driver uses the project's `upstreamRepo` when present, otherwise `repo`, and routes created issue tasks back to that project:

```json
{
  "id": "repo",
  "repo": "fork-owner/repo",
  "upstreamRepo": "owner/repo",
  "githubIssues": { "enabled": true, "labels": ["aged"], "issueLimit": 20 }
}
```

Discord driver:

```sh
go run ./cmd/aged -discord-driver discord-driver.json
```

It polls configured bot channels, answers through the assistant, can propose or create tasks, supports `task: <prompt>` and `do it`, and can create projects from chat. The Discord driver is managed by the same runtime driver registry as the GitHub driver and can be read or hot-swapped with `GET` / `PUT /api/drivers/discord`; state responses redact the bot token.

Plugins use `-plugins` / `AGED_PLUGINS`. Enabled `aged-plugin-v1` command plugins are probed with `command... describe`; driver plugins may be supervised with `command... serve`, and runner plugins become worker kinds.

## Dashboard And Dev Server

Run the dashboard in dev mode:

```sh
cd web
npm install
npm run dev
```

Vite proxies `/api` to the daemon. Open `http://127.0.0.1:5173`.

For self-iteration:

```sh
go run ./cmd/aged-dev
```

The dev server listens on `http://127.0.0.1:8790` and manages a daemon on `http://127.0.0.1:8787`.

- `GET /health`: control server health
- `GET /status`: last rebuild/restart result
- `GET /rebuild` or `POST /rebuild`: rebuild daemon/UI and restart the managed daemon

The rebuilt binary and logs live under `.aged/dev/`.

## Auth

Local development is unauthenticated by default. For web exposure:

```sh
AGED_AUTH=google \
AGED_GOOGLE_CLIENT_ID=<client-id> \
AGED_GOOGLE_CLIENT_SECRET=<client-secret> \
AGED_AUTH_ALLOWED_EMAILS=you@example.com \
AGED_AUTH_SESSION_KEY="$(openssl rand -base64 32)" \
AGED_AUTH_REDIRECT_URL=https://aged.example.com/auth/callback \
go run ./cmd/aged -addr 0.0.0.0:8787
```

Auth flags:

- `-auth` / `AGED_AUTH`: `none` or `google`
- `-google-client-id` / `AGED_GOOGLE_CLIENT_ID`
- `-google-client-secret` / `AGED_GOOGLE_CLIENT_SECRET`
- `-auth-allowed-emails` / `AGED_AUTH_ALLOWED_EMAILS`
- `-auth-session-key` / `AGED_AUTH_SESSION_KEY`
- `-auth-redirect-url` / `AGED_AUTH_REDIRECT_URL`

When auth is enabled, login/logout routes and `/api/health` remain public enough to complete auth. Dashboard pages and operational APIs require a signed session cookie.

## API Sketch

Main routes:

- State/events: `GET /api/snapshot`, `GET /api/snapshot?events=none`, `GET /api/events`, `GET /api/events/stream`
- Registries: `/api/projects`, `/api/targets`, `/api/plugins`
- Tasks: `POST /api/tasks`, `POST /api/tasks/{id}/steer`, `retry`, `cancel`, `apply`, `apply-policy`, `clear`, `pull-request`, `watch-pull-requests`
- Workers: `GET /api/workers/{id}/changes`, `POST /api/workers/{id}/apply`, `POST /api/workers/{id}/cancel`
- PRs: `POST /api/pull-requests/{id}/refresh`, `POST /api/pull-requests/{id}/babysit`
- MCP: `POST /mcp`, with snapshot/task/project/target/plugin/PR tools and `aged://...` resources

External drivers should use `source` plus `externalId` for idempotency:

```sh
curl -X POST http://127.0.0.1:8787/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "projectId": "aged",
    "title": "GitHub issue owner/repo#123",
    "prompt": "Fix the issue described at owner/repo#123...",
    "source": "github-issue",
    "externalId": "owner/repo#123",
    "metadata": {
      "repo": "owner/repo",
      "issue": 123,
      "url": "https://github.com/owner/repo/issues/123"
    }
  }'
```

## Durable Loop Evals

```sh
go run ./cmd/aged-loop-eval
```

The eval runner creates a loop task from `evals/durable-loop-pr-producer.md`, watches it for a finite `-horizon`, optionally cancels it, and writes a JSON scorecard. `-repeat` and `-feedback-on-fail` turn it into a local feedback loop that creates follow-up aged tasks on failed checks.

## Build

```sh
go test ./...
cd web && npm run build
```

After `web/dist` exists, the Go daemon serves it from the same origin.
