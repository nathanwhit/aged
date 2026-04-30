# SPEC Gap Tasks

## Objective And Workflow Reliability

- [x] Make GitHub PR workflows a flexible durable objective loop, including PRs as intermediate artifacts from any task source, automatic refresh, review/check ingestion, same-task follow-up worker scheduling, and terminal satisfaction when merged.
- [x] Add configurable PR policy for draft/ready state, branch naming, base detection, repo/fork selection, merge permissions, and whether aged may merge automatically.
- [x] Add first-class recovery from arbitrary failed graph nodes, including durable Codex/Claude worker session resume when provider sessions are available.
- [x] Improve approval UX and policy so user decisions, autonomous decisions, and worker questions are visible and actionable in one place.

## Projects

- [x] Validate project `localPath` during project registration.
- [x] Discover project checkout metadata on registration when possible: VCS kind and GitHub repo.
- [x] Add project edit/delete APIs and UI.
- [x] Add project health checks in API/UI, including path accessibility, VCS status, GitHub auth readiness, default branch, and target policy.
- [x] Discover default branch/base more robustly from checkout or GitHub metadata.

## Remote Targets

- [ ] Add live target probes for SSH targets: reachable, tmux available, repo path present, disk, CPU/load, and memory.
- [ ] Incorporate live target health/resource data into target scheduling, not just configured capacity and assigned worker count.
- [ ] Improve remote apply conflict handling beyond raw patch application failures.
- [ ] Store and expose remote artifacts beyond `diff.patch`, such as logs, benchmark output, and profiler reports.

## Plugins And Drivers

- [ ] Turn plugin descriptors into supervised long-running driver lifecycles.
- [ ] Add driver lifecycle status, logs, restart policy, and failure visibility in snapshots/UI.
- [ ] Define a runner plugin protocol beyond built-in subprocess adapters.

## Artifacts And Evaluation

- [ ] Add structured benchmark/profiler artifacts when prompt-parsed text is not enough for comparison, UI display, or auditability.
- [ ] Extend benchmark comparison to enforce same-command before/after runs, repeated samples, thresholds, and anti-cherry-picking rules.
- [ ] Add richer task artifacts for deployments, packages, test reports, CI runs, and review comments.
