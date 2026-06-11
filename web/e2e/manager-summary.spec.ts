import { expect, test, type Page } from "@playwright/test";

const taskId = "task-manager-summary";
const now = "2026-06-10T12:00:00Z";

const task = {
  id: taskId,
  projectId: "proj-a",
  title: "Manager summary validation",
  prompt: "Validate the manager summary dashboard path.",
  status: "running",
  objectiveStatus: "waiting_user",
  objectivePhase: "review",
  createdAt: now,
  updatedAt: now,
  metadata: { objectiveMode: "broad" },
};

const snapshot = {
  tasks: [task],
  workers: [
    {
      id: "worker-manager-summary",
      taskId,
      kind: "codex",
      status: "running",
      createdAt: now,
      updatedAt: now,
      metadata: { model: "gpt-5" },
    },
  ],
  executionNodes: [],
  workItems: [
    {
      id: "work-manager-summary",
      taskId,
      kind: "objective.implement",
      status: "queued",
      reason: "Implement manager summary UI consumption",
      createdAt: now,
      updatedAt: now,
    },
  ],
  artifacts: [],
  memoryEntries: [],
  questions: [
    {
      id: "question-manager-summary",
      taskId,
      workerId: "worker-manager-summary",
      reason: "approval",
      question: "Approve the manager summary follow-up?",
      decided: false,
      createdAt: now,
      updatedAt: now,
    },
  ],
  sessions: [
    {
      id: "session-manager-summary",
      taskId,
      workerId: "worker-manager-summary",
      workerKind: "codex",
      role: "implementer",
      status: "running",
      targetId: "vm-1",
      targetKind: "ssh",
      remoteSession: "tmux-manager",
      workspaceName: "workspace",
      currentAction: "Running validation checks for manager summary",
      currentActionLabel: "Validation",
      currentActionAt: now,
      createdAt: now,
      startedAt: now,
      updatedAt: now,
    },
  ],
  targets: [],
  plugins: [],
  promptSets: [],
  projects: [],
  pullRequests: [
    {
      id: "owner/repo#42",
      taskId,
      repo: "owner/repo",
      number: 42,
      url: "https://github.com/owner/repo/pull/42",
      branch: "manager-summary",
      base: "main",
      title: "Manager summary slice",
      state: "OPEN",
      checksStatus: "PENDING",
      createdAt: now,
      updatedAt: now,
    },
  ],
  pullRequestFeedback: [
    {
      id: "feedback-manager-summary",
      taskId,
      pullRequestId: "owner/repo#42",
      status: "pending",
      reason: "review_feedback",
      repo: "owner/repo",
      number: 42,
      url: "https://github.com/owner/repo/pull/42",
      branch: "manager-summary",
      feedbackBody: "Reviewer requested clearer attention badges.",
      prompt: "Address review feedback.",
      createdAt: now,
      updatedAt: now,
    },
  ],
  steering: [],
  managerSummary: [
    {
      taskId,
      activeSignals: 5,
      attentionCount: 3,
      pendingApprovals: 1,
      pendingFeedback: 1,
      activeSessions: 1,
      activeWorkers: 1,
      activeWorkItems: 1,
      pullRequests: 1,
      artifacts: 0,
      latestAction: "Running validation checks for manager summary",
      latestActionAt: now,
      latestActionLabel: "Validation",
      tone: "warning",
      updatedAt: now,
    },
  ],
  lastEventId: 1,
  events: [],
};

async function mockApi(page: Page) {
  await page.route("**/api/snapshot?**", (route) => route.fulfill({ json: snapshot }));
  await page.route(`**/api/tasks/${taskId}`, (route) => route.fulfill({ json: snapshot }));
  await page.route(`**/api/tasks/${taskId}/assignments`, (route) => route.fulfill({ json: { taskId, assignments: [] } }));
  await page.route(`**/api/tasks/${taskId}/events?**`, (route) => route.fulfill({ json: [] }));
  await page.route("**/api/sessions/session-manager-summary/tail?**", (route) =>
    route.fulfill({
      json: {
        sessionId: "session-manager-summary",
        workerId: "worker-manager-summary",
        taskId,
        status: "running",
        lastEventId: 1,
        events: [],
        currentAction: { label: "Validation", text: "Running validation checks for manager summary", at: now, eventId: 1 },
      },
    }),
  );
  await page.route("**/api/events/stream?**", (route) =>
    route.fulfill({
      status: 200,
      headers: { "content-type": "text/event-stream" },
      body: "",
    }),
  );
}

async function expectNoHorizontalOverflow(page: Page) {
  const metrics = await page.evaluate(() => ({
    viewport: window.innerWidth,
    documentWidth: document.documentElement.scrollWidth,
    bodyWidth: document.body.scrollWidth,
  }));
  expect(metrics.documentWidth, JSON.stringify(metrics)).toBeLessThanOrEqual(metrics.viewport);
  expect(metrics.bodyWidth, JSON.stringify(metrics)).toBeLessThanOrEqual(metrics.viewport);
}

test("manager summary badges render through dashboard and task detail without horizontal overflow", async ({ page }) => {
  await mockApi(page);
  await page.setViewportSize({ width: 1366, height: 900 });
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Manager summary validation" })).toBeVisible();
  await expect(page.getByText("Needs Attention")).toBeVisible();
  const managerRow = page.locator(".manager-objective-row", { hasText: "Manager summary validation" });
  await expect(managerRow).toBeVisible();
  await expect(managerRow).toContainText("Project proj-a");
  await expect(managerRow.getByTitle("Active agents")).toHaveText("1");
  await expect(managerRow.getByTitle("Open pull requests")).toHaveText("1");
  await expect(managerRow.getByTitle("Latest state")).toHaveText("Review");
  await expect(managerRow.getByTitle("3 need attention")).toHaveText("3 attn");
  await expect(managerRow.getByTitle("1 approvals pending")).toHaveText("1 ask");
  await expect(managerRow.getByTitle("1 PR feedback items pending")).toHaveText("1 fb");
  await managerRow.getByRole("button").click();
  await expect(page.getByText("Running validation checks for manager summary").first()).toBeVisible();
  await expect(page.getByText("5 active signals")).toBeVisible();
  await expect(page.getByText("3 need attention")).toBeVisible();
  await expect(page.getByText("1 approval pending")).toBeVisible();
  await expect(page.getByText("1 PR feedback item pending")).toBeVisible();
  await expectNoHorizontalOverflow(page);

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("heading", { name: "Manager summary validation" })).toBeVisible();
  await expect(page.getByText("Assignments")).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("selected task defaults to manager console without legacy backend panes", async ({ page }) => {
  await mockApi(page);
  await page.setViewportSize({ width: 1366, height: 900 });
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Manager summary validation" })).toBeVisible();
  await expect(page.getByText("Pull Requests", { exact: true })).toHaveCount(1);
  await expect(page.getByRole("heading", { name: "Current State" })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Orchestration" })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Worker Detail" })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Timeline" })).toHaveCount(0);
  await expect(page.locator(".debug-backend-internals")).toHaveCount(0);
  await expect(page.locator(".debug-pane summary").getByText("Debug")).toBeVisible();
});
