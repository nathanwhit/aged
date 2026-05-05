# Durable Loop PR Producer Eval

This eval target checks whether durable loop mode can act like a useful long-running engineering loop instead of a one-shot task runner. It is intentionally a concrete workload, not a synthetic microbenchmark.

## Target

Run a durable loop against this repository with one objective: repeatedly produce small, useful maintenance PRs while babysitting existing PRs first.

## Task Prompt

```text
You are running as a durable maintenance loop for aged.

Each iteration:
1. Inspect the repo and current open PRs.
2. If there is already an open PR from a prior iteration, inspect its CI/review state first and fix it before starting new work.
3. Find exactly one small reliability, orchestration, or developer-experience improvement.
4. Implement it as a narrow, reviewable change.
5. Run the relevant tests.
6. Commit, push, and open a PR if there is a real change.
7. Do not mark the task complete just because one PR was opened. Continue looking for the next useful improvement until explicitly canceled or blocked on user input.

Prefer improvements around:
- durable loop behavior
- stuck or hung worker detection
- PR publishing quality
- worker prompt visibility
- reducing hard-coded orchestration logic

Ask for input only when genuinely blocked.
```

## Metadata

```json
{
  "executionMode": "loop",
  "loopWorkerKind": "codex",
  "loopRole": "maintenance_pr_loop",
  "loopIntervalSeconds": 300,
  "completionMode": "local"
}
```

## Example API Request

```sh
curl -sS http://127.0.0.1:8787/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "title": "Durable loop PR producer eval",
    "prompt": "You are running as a durable maintenance loop for aged.\n\nEach iteration:\n1. Inspect the repo and current open PRs.\n2. If there is already an open PR from a prior iteration, inspect its CI/review state first and fix it before starting new work.\n3. Find exactly one small reliability, orchestration, or developer-experience improvement.\n4. Implement it as a narrow, reviewable change.\n5. Run the relevant tests.\n6. Commit, push, and open a PR if there is a real change.\n7. Do not mark the task complete just because one PR was opened. Continue looking for the next useful improvement until explicitly canceled or blocked on user input.\n\nPrefer improvements around:\n- durable loop behavior\n- stuck or hung worker detection\n- PR publishing quality\n- worker prompt visibility\n- reducing hard-coded orchestration logic\n\nAsk for input only when genuinely blocked.",
    "metadata": {
      "executionMode": "loop",
      "loopWorkerKind": "codex",
      "loopRole": "maintenance_pr_loop",
      "loopIntervalSeconds": 300,
      "completionMode": "local"
    }
  }'
```

## Eval Runner

Use `cmd/aged-loop-eval` to run the target for a finite external observation window. The product loop remains unbounded; the eval runner owns the horizon, cancels the task at the end, and writes a JSON scorecard.

```sh
go run ./cmd/aged-loop-eval \
  -addr http://127.0.0.1:8787 \
  -horizon 90m \
  -poll 10s \
  -steer-after 30m
```

For a local smoke test that exercises the durable-loop plumbing without creating real PRs, run aged with a mock worker and override the eval metadata:

```sh
go run ./cmd/aged \
  -addr 127.0.0.1:8787 \
  -db /tmp/aged-loop-eval-smoke.db \
  -worker mock \
  -brain static \
  -workdir /path/to/a/jj-or-git-checkout \
  -workspace-mode shared

go run ./cmd/aged-loop-eval \
  -addr http://127.0.0.1:8787 \
  -horizon 10s \
  -poll 1s \
  -worker-kind mock \
  -loop-interval-seconds 1 \
  -out /tmp/aged-loop-eval-smoke.json
```

The smoke run should pass loop mechanics checks such as no self-completion, no iteration failures, cancelation behavior, and loop progress. PR-quality checks are expected to fail under the mock worker because it does not inspect GitHub or open PRs.

To turn the eval into an automated feedback loop, run it repeatedly with feedback enabled:

```sh
go run ./cmd/aged-loop-eval \
  -addr http://127.0.0.1:8787 \
  -horizon 90m \
  -poll 10s \
  -steer-after 30m \
  -repeat 24h \
  -max-runs 0 \
  -feedback-on-fail
```

Each run still has a finite external horizon. When a scorecard contains failing checks, the runner creates a follow-up aged task with the failed checks, scorecard path, and metrics so aged can make one narrow improvement PR. Keep this behind a local scheduler, launchd job, cron job, or dedicated always-on machine; it is intentionally not a GitHub Actions workflow because real runs need local Codex/Claude credentials and can create PRs.

## Pass Criteria

- The task stays active after opening or updating a PR.
- The loop fixes existing PR CI/review problems before starting unrelated new work.
- Each new PR contains a real, narrow change with a repo-appropriate description.
- The loop does not open empty, no-op, duplicate, or purely cosmetic PRs.
- Cancelation stops the active worker and the task without calling the eval complete.
- Steering is applied to the next loop turn while preserving the retained workspace and provider session when supported.
- The loop asks for input only when blocked on credentials, permissions, ambiguous product direction, or another user-owned decision.

## Fail Criteria

- The task completes after a single useful PR.
- The loop opens a PR with no material diff.
- The loop ignores an existing failing or reviewed PR and starts unrelated new work.
- A canceled or failed worker is treated as successful task completion.
- Steering disappears, restarts from scratch unnecessarily, or is not visible in the resumed worker prompt.
- The loop repeatedly performs the same repository inspection without producing a decision, a PR fix, a new PR, or a user question.

## Metrics To Record

- Iterations run.
- PRs opened.
- PRs fixed after CI or review feedback.
- Empty or rejected PRs.
- Times the task entered waiting state.
- Time from steering to a resumed worker turn.
- Time since last worker output when a worker is running.
