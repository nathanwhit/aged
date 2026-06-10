/// <reference lib="deno.ns" />
import { assertEquals } from "jsr:@std/assert@^1";
import { selectSessions, selectWorkItems } from "./assignments.ts";
import type { Session, TaskAssignment, WorkItem } from "./types.ts";

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

Deno.test("selectWorkItems falls back to snapshot when no backend assignments", () => {
  const items = [workItem({ id: "w1", taskId: "t" }), workItem({ id: "w2", taskId: "t" })];
  assertEquals(selectWorkItems(items, null), items);
  assertEquals(selectWorkItems(items, undefined), items);
});

Deno.test("selectWorkItems prefers backend order and reuses snapshot data", () => {
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
  assertEquals(result.map((item) => item.id), ["w2", "w1"]);
  assertEquals(result[0].reason, "second");
  assertEquals(result[1].reason, "first");
});

Deno.test("selectWorkItems synthesizes when snapshot row is missing", () => {
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
  assertEquals(result.length, 1);
  assertEquals(result[0].id, "w99");
  assertEquals(result[0].kind, "objective.compose");
  assertEquals(result[0].status, "running");
  assertEquals(result[0].reason, "remote-only");
  assertEquals(result[0].workerId, "worker-1");
  assertEquals(result[0].targetKind, "ssh");
  assertEquals(result[0].targetId, "host-a");
});

Deno.test("selectWorkItems deduplicates repeated assignment ids", () => {
  const items = [workItem({ id: "w1", taskId: "t" })];
  const assignments = [
    assignment({ id: "work_item:w1", sourceKind: "work_item", sourceId: "w1", taskId: "t" }),
    assignment({ id: "work_item:w1-dup", sourceKind: "work_item", sourceId: "w1", taskId: "t" }),
  ];
  assertEquals(selectWorkItems(items, assignments).map((item) => item.id), ["w1"]);
});

Deno.test("selectWorkItems returns empty array when backend lists no work_item rows", () => {
  const items = [workItem({ id: "w1", taskId: "t" })];
  const assignments = [
    assignment({ id: "session:s1", sourceKind: "session", sourceId: "s1", taskId: "t" }),
  ];
  assertEquals(selectWorkItems(items, assignments), []);
});

Deno.test("selectSessions falls back to snapshot when no backend assignments", () => {
  const sessions = [session({ id: "s1", taskId: "t", workerId: "w1" })];
  assertEquals(selectSessions(sessions, null), sessions);
});

Deno.test("selectSessions prefers backend order and reuses snapshot data", () => {
  const sessions = [
    session({ id: "s1", taskId: "t", workerId: "w1", currentAction: "alpha" }),
    session({ id: "s2", taskId: "t", workerId: "w2", currentAction: "beta" }),
  ];
  const assignments = [
    assignment({ id: "session:s2", sourceKind: "session", sourceId: "s2", taskId: "t" }),
    assignment({ id: "session:s1", sourceKind: "session", sourceId: "s1", taskId: "t" }),
  ];
  const result = selectSessions(sessions, assignments);
  assertEquals(result.map((item) => item.id), ["s2", "s1"]);
  assertEquals(result[0].currentAction, "beta");
  assertEquals(result[1].currentAction, "alpha");
});

Deno.test("selectSessions synthesizes when snapshot row is missing", () => {
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
  assertEquals(result.length, 1);
  assertEquals(result[0].id, "s9");
  assertEquals(result[0].workerId, "worker-9");
  assertEquals(result[0].workerKind, "claude");
  assertEquals(result[0].nodeId, "node-9");
  assertEquals(result[0].targetKind, "ssh");
  assertEquals(result[0].targetId, "host-b");
  assertEquals(result[0].status, "queued");
  assertEquals(result[0].currentActionLabel, "Waiting");
});
