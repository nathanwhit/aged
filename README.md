# aged

`aged` is a local-first agent orchestrator for autonomous development work.

The current implementation is an initial vertical slice:

- Go daemon with SQLite event persistence
- Prompt-driven orchestrator brain interface
- Mock, Codex, Claude, and shell worker runner adapters selected by the orchestrator
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

Default scheduler fallback:

- `-worker mock` sets the orchestrator's fallback runner when the prompt brain does not choose a different runner.
- Users do not choose workers per task; task creation only supplies the work request.
- Available runner adapters include `mock`, `codex`, `claude`, and `shell`.

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
- `POST /api/workers/{id}/cancel`
