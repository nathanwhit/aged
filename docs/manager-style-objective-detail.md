# Manager-Style Objective Detail UI

## Goal

Make the objective detail screen read as a manager console first and a low-level orchestration debugger second. The primary path should let a manager scan active assignments, inspect the live session, review pull request output, and steer the objective without opening backend internals by default.

## Default Detail Order

1. Assignments
2. Live Session
3. Pull Requests
4. Objective Brief
5. Durable loop settings and objective steering, when applicable
6. Debug

Worker, work-item, session queue, PR feedback queue, event, and progress internals should stay available under Debug or be visually demoted below the manager flow.

## Assignment Rows

Rows are derived from the existing hydrated task snapshot and assignment data. This first pass must not require new backend API fields.

Include rows for:

- Active sessions.
- Queued, running, and failed work items.
- Pull request records.
- Pending pull request feedback.
- Pending questions and approvals.
- Artifacts, including pull request artifacts.
- Task failures and terminal task lifecycle actions.
- Debug entries such as orphan workers and queued steering.

Rows should be dense, scan-friendly, sorted with attention states above routine work, and stable across refreshes by using source-prefixed IDs such as `session:<id>`, `work:<id>`, `pr:<id>`, `question:<id>`, and `artifact:<id>`.

## Actions

Use existing frontend/backend capabilities only:

- Inspect live session.
- Open pull request or artifact URL.
- Cancel sessions, workers, and work items.
- Answer questions through the existing approval form.
- Retry or clear terminal tasks.
- Publish, watch, refresh, babysit, and steer pull requests.
- Steer the objective and selected session.

## Selection

Clicking an assignment row should select the most relevant detail context when data exists:

- Session rows select the live session terminal.
- Pull request and feedback rows select the pull request summary.
- Question rows select the matching approval card.
- Work item and artifact rows remain selectable metadata anchors until richer detail views exist.

## Live Session

Render the selected session as a terminal-like view using existing session, worker, node, and event data. Show:

- Provider and model when present in metadata.
- Target, branch, worktree, run directory, scratch path, and command.
- Current action and latest output.
- Changed files from worker completion payloads.
- Recent event tail.
- Targeted steering labels that make the session and worker target explicit.

## Backend Follow-Up Gaps

This UI continues to derive display rows locally. A later backend contract should provide typed assignment display rows with titles, subtitles, tone, actions, branch/model details, PR IDs, question IDs, and PR metadata so the UI can stop rebuilding the same view from several snapshot arrays.
