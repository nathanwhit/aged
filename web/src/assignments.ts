import type { Session, TaskAssignment, WorkItem } from "./types";

export function selectWorkItems(
  snapshotItems: WorkItem[],
  assignments: TaskAssignment[] | null | undefined,
): WorkItem[] {
  if (!assignments) return snapshotItems;
  const byId = new Map(snapshotItems.map((item) => [item.id, item]));
  const result: WorkItem[] = [];
  const seen = new Set<string>();
  for (const assignment of assignments) {
    if (assignment.sourceKind !== "work_item") continue;
    if (seen.has(assignment.sourceId)) continue;
    seen.add(assignment.sourceId);
    const existing = byId.get(assignment.sourceId);
    result.push(existing ?? synthesizeWorkItem(assignment));
  }
  return result;
}

export function selectSessions(
  snapshotSessions: Session[],
  assignments: TaskAssignment[] | null | undefined,
): Session[] {
  if (!assignments) return snapshotSessions;
  const byId = new Map(snapshotSessions.map((session) => [session.id, session]));
  const result: Session[] = [];
  const seen = new Set<string>();
  for (const assignment of assignments) {
    if (assignment.sourceKind !== "session") continue;
    if (seen.has(assignment.sourceId)) continue;
    seen.add(assignment.sourceId);
    const existing = byId.get(assignment.sourceId);
    result.push(existing ?? synthesizeSession(assignment));
  }
  return result;
}

function synthesizeWorkItem(assignment: TaskAssignment): WorkItem {
  return {
    id: assignment.sourceId,
    taskId: assignment.taskId,
    kind: assignment.kind ?? "work_item",
    status: assignment.status ?? "queued",
    targetKind: assignment.targetKind,
    targetId: assignment.targetId,
    reason: assignment.reason,
    workerId: assignment.workerId,
    createdAt: assignment.createdAt,
    updatedAt: assignment.updatedAt,
  };
}

function synthesizeSession(assignment: TaskAssignment): Session {
  return {
    id: assignment.sourceId,
    taskId: assignment.taskId,
    workerId: assignment.workerId ?? "",
    nodeId: assignment.nodeId,
    workerKind: assignment.workerKind,
    role: assignment.role,
    spawnId: assignment.spawnId,
    status: (assignment.status ?? "queued") as Session["status"],
    targetKind: assignment.targetKind,
    targetId: assignment.targetId,
    currentAction: assignment.currentAction,
    currentActionLabel: assignment.currentActionLabel,
    createdAt: assignment.createdAt,
    startedAt: assignment.startedAt,
    updatedAt: assignment.updatedAt,
    completedAt: assignment.completedAt,
  };
}
