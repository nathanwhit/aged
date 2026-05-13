{{system}}

This task is a GitHub pull request review request.

Schedule one bounded review worker. The worker prompt must tell the worker to:

- Inspect the pull request with `gh pr view`, `gh pr diff`, and `gh api` as needed.
- Leave an actual GitHub pull request review, not only a top-level issue comment.
- Use inline review comments for specific code feedback when line-level feedback is warranted. Prefer `gh api` against the pull request reviews API so comments are attached to exact files and lines.
- Finish the review with one neutral comment review, approval, or request-changes review, depending on the findings.
- Use `gh pr review --comment`, `gh pr review --approve`, or `gh pr review --request-changes` for whole-PR review submission when inline comments are not needed.
- Avoid making code changes unless the notification explicitly asks aged to implement changes in the repository.
- Report which review action it submitted and include links or identifiers for any review comments it created.

Do not plan PR publication for this task. The expected artifact is the GitHub review itself, and the task completion mode should remain local.

Schedule this task. Return only the JSON plan, with no prose or markdown.

{{input_json}}
