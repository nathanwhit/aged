import { expect, it } from "vitest";
import { selectSessions, selectWorkItems } from "./assignments";
import type { Session, TaskAssignment, WorkItem } from "./types";

const baseTimestamp = "2025-01-01T00:00:00Z";

function workItem(overrides: Partial<WorkItem> & { id: string; taskId: string }): WorkItem {
  return {
    kind: "objective.slice",
    status: "queued",
    createdAt: baseTimestamp,
    updatedAt: baseTimestamp,
    ...overrides,
  };
}

function session(overrides: Partial<Session> & { id: string; taskId: string; workerId: string }): Session {
  return {
    status: "running" as Session["status"],
    createdAt: baseTimestamp,
    updatedAt: baseTimestamp,
    ...overrides,
  };
}

function assignment(overrides: Partial<TaskAssignment> & { id: string; sourceKind: string; sourceId: string; taskId: string }): TaskAssignment {
  return {
    createdAt: baseTimestamp,
    updatedAt: baseTimestamp,
    ...overrides,
  };
}

it("selectWorkItems falls back to snapshot when no backend assignments", () => {
  const items = [workItem({ id: "w1", taskId: "t" }), workItem({ id: "w2", taskId: "t" })];
  expect(selectWorkItems(items, null)).toEqual(items);
  expect(selectWorkItems(items, undefined)).toEqual(items);
});

it("selectWorkItems prefers backend order and reuses snapshot data", () => {
  const snapshotItems = [
    workItem({ id: "w1", taskId: "t", reason: "first" }),
    workItem({ id: "w2", taskId: "t", reason: "second" }),
  ];
  const assignments = [
    assignment({ id: "work_item:w2", sourceKind: "work_item", sourceId: "w2", taskId: "t" }),
    assignment({ id: "session:s1", sourceKind: "session", sourceId: "s1", taskId: "t" }),
    assignment({ id: "work_item:w1", sourceKind: "work_item", sourceId: "w1", taskId: "t" }),
  ];

  const result = selectWorkItems(snapshotItems, assignments);
  expect(result.map((item) => item.id)).toEqual(["w2", "w1"]);
  expect(result[0].reason).toEqual("second");
  expect(result[1].reason).toEqual("first");
});

it("selectWorkItems synthesizes when snapshot row is missing", () => {
  const assignments = [
    assignment({
      id: "work_item:w99",
      sourceKind: "work_item",
      sourceId: "w99",
      taskId: "t",
      kind: "objective.compose",
      status: "running",
      reason: "remote-only",
      workerId: "worker-1",
      targetKind: "ssh",
      targetId: "host-a",
    }),
  ];
  const result = selectWorkItems([], assignments);
  expect(result.length).toEqual(1);
  expect(result[0].id).toEqual("w99");
  expect(result[0].kind).toEqual("objective.compose");
  expect(result[0].status).toEqual("running");
  expect(result[0].reason).toEqual("remote-only");
  expect(result[0].workerId).toEqual("worker-1");
  expect(result[0].targetKind).toEqual("ssh");
  expect(result[0].targetId).toEqual("host-a");
});

it("selectWorkItems deduplicates repeated assignment ids", () => {
  const items = [workItem({ id: "w1", taskId: "t" })];
  const assignments = [
    assignment({ id: "work_item:w1", sourceKind: "work_item", sourceId: "w1", taskId: "t" }),
    assignment({ id: "work_item:w1-dup", sourceKind: "work_item", sourceId: "w1", taskId: "t" }),
  ];
  expect(selectWorkItems(items, assignments).map((item) => item.id)).toEqual(["w1"]);
});

it("selectWorkItems returns empty array when backend lists no work_item rows", () => {
  const items = [workItem({ id: "w1", taskId: "t" })];
  const assignments = [
    assignment({ id: "session:s1", sourceKind: "session", sourceId: "s1", taskId: "t" }),
  ];
  expect(selectWorkItems(items, assignments)).toEqual([]);
});

it("selectSessions falls back to snapshot when no backend assignments", () => {
  const sessions = [session({ id: "s1", taskId: "t", workerId: "w1" })];
  expect(selectSessions(sessions, null)).toEqual(sessions);
});

it("selectSessions prefers backend order and reuses snapshot data", () => {
  const sessions = [
    session({ id: "s1", taskId: "t", workerId: "w1", currentAction: "alpha" }),
    session({ id: "s2", taskId: "t", workerId: "w2", currentAction: "beta" }),
  ];
  const assignments = [
    assignment({ id: "session:s2", sourceKind: "session", sourceId: "s2", taskId: "t" }),
    assignment({ id: "session:s1", sourceKind: "session", sourceId: "s1", taskId: "t" }),
  ];
  const result = selectSessions(sessions, assignments);
  expect(result.map((item) => item.id)).toEqual(["s2", "s1"]);
  expect(result[0].currentAction).toEqual("beta");
  expect(result[1].currentAction).toEqual("alpha");
});

it("selectSessions synthesizes when snapshot row is missing", () => {
  const assignments = [
    assignment({
      id: "session:s9",
      sourceKind: "session",
      sourceId: "s9",
      taskId: "t",
      status: "queued",
      workerId: "worker-9",
      workerKind: "claude",
      nodeId: "node-9",
      targetKind: "ssh",
      targetId: "host-b",
      currentActionLabel: "Waiting",
    }),
  ];
  const result = selectSessions([], assignments);
  expect(result.length).toEqual(1);
  expect(result[0].id).toEqual("s9");
  expect(result[0].workerId).toEqual("worker-9");
  expect(result[0].workerKind).toEqual("claude");
  expect(result[0].nodeId).toEqual("node-9");
  expect(result[0].targetKind).toEqual("ssh");
  expect(result[0].targetId).toEqual("host-b");
  expect(result[0].status).toEqual("queued");
  expect(result[0].currentActionLabel).toEqual("Waiting");
});
