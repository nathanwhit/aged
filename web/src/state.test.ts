import assert from "node:assert/strict";
import test from "node:test";
import { applyTaskHistoryEvents, emptySnapshot, reduceEvent } from "./state";
import type { AppSnapshot } from "./state";
import type { EventRecord, Task } from "./types";

const task: Task = {
  id: "task-1",
  title: "Task",
  prompt: "Do work",
  status: "running",
  objectiveStatus: "active",
  objectivePhase: "running",
  createdAt: "2026-05-02T00:00:00Z",
  updatedAt: "2026-05-02T00:00:10Z",
};

test("selected task history projects events newer than the compact snapshot handoff", () => {
  const snapshot: AppSnapshot = {
    ...emptySnapshot,
    tasks: [task],
    lastEventId: 10,
    snapshotEventId: 10,
  };
  const oldQueued = taskStatusEvent(9, "queued");
  const newSucceeded = taskStatusEvent(11, "succeeded");

  const next = applyTaskHistoryEvents(snapshot, [oldQueued, newSucceeded]);

  assert.equal(next.events.map((event) => event.id).join(","), "9,11");
  assert.equal(next.lastEventId, 11);
  assert.equal(next.snapshotEventId, 10);
  assert.equal(next.tasks[0].status, "succeeded");
  assert.equal(next.tasks[0].objectiveStatus, "satisfied");
});

test("SSE duplicate after selected task history does not undo the projected update", () => {
  const snapshot: AppSnapshot = {
    ...emptySnapshot,
    tasks: [task],
    lastEventId: 10,
    snapshotEventId: 10,
  };
  const newSucceeded = taskStatusEvent(11, "succeeded");

  const withHistory = applyTaskHistoryEvents(snapshot, [newSucceeded]);
  const afterDuplicateSSE = reduceEvent(withHistory, newSucceeded);

  assert.equal(afterDuplicateSSE.tasks[0].status, "succeeded");
  assert.equal(afterDuplicateSSE.events.length, 1);
});

test("worker created events project prompt fields", () => {
  const snapshot: AppSnapshot = {
    ...emptySnapshot,
    tasks: [task],
  };

  const next = reduceEvent(snapshot, {
    id: 12,
    at: "2026-05-02T00:00:12Z",
    type: "worker.created",
    taskId: task.id,
    workerId: "worker-1",
    payload: {
      kind: "codex",
      command: ["codex", "exec", "-"],
      prompt: "Inspect the workspace.\nReturn a concise report.",
      promptPath: "/tmp/prompt.txt",
    },
  });

  assert.equal(next.workers.length, 1);
  assert.equal(next.workers[0].prompt, "Inspect the workspace.\nReturn a concise report.");
  assert.equal(next.workers[0].promptPath, "/tmp/prompt.txt");
});

function taskStatusEvent(id: number, status: Task["status"]): EventRecord {
  return {
    id,
    at: `2026-05-02T00:00:${String(id).padStart(2, "0")}Z`,
    type: "task.status",
    taskId: task.id,
    payload: { status },
  };
}
