import type { EventRecord, ExecutionNode, OrchestrationGraph, Plugin, Project, PullRequestState, Snapshot, TargetState, Task, Worker, WorkerStatus } from "./types";

export type AppSnapshot = {
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  targets: TargetState[];
  plugins: Plugin[];
  projects: Project[];
  pullRequests: PullRequestState[];
  orchestrationGraphs: OrchestrationGraph[];
  lastEventId: number;
  snapshotEventId: number;
  events: EventRecord[];
};

export const emptySnapshot: AppSnapshot = {
  tasks: [],
  workers: [],
  executionNodes: [],
  targets: [],
  plugins: [],
  projects: [],
  pullRequests: [],
  orchestrationGraphs: [],
  lastEventId: 0,
  snapshotEventId: 0,
  events: [],
};

export function normalizeSnapshot(snapshot: Snapshot): AppSnapshot {
  const executionNodes = snapshot.executionNodes ?? [];
  const tasks = snapshot.tasks ?? [];
  const lastEventId = snapshot.lastEventId ?? snapshot.events?.at(-1)?.id ?? 0;
  return {
    tasks,
    workers: snapshot.workers ?? [],
    executionNodes,
    targets: snapshot.targets ?? [],
    plugins: snapshot.plugins ?? [],
    projects: snapshot.projects ?? [],
    pullRequests: snapshot.pullRequests ?? [],
    orchestrationGraphs: snapshot.orchestrationGraphs ?? deriveOrchestrationGraphs(tasks, executionNodes),
    lastEventId,
    snapshotEventId: lastEventId,
    events: snapshot.events ?? [],
  };
}

export function upsertTask(snapshot: AppSnapshot, task: Task): AppSnapshot {
  const tasks = snapshot.tasks.some((candidate) => candidate.id === task.id)
    ? snapshot.tasks.map((candidate) => (candidate.id === task.id ? task : candidate))
    : [...snapshot.tasks, task];
  return { ...snapshot, tasks };
}

export function reduceEvent(snapshot: AppSnapshot, event: EventRecord): AppSnapshot {
  if (snapshot.events.some((existing) => existing.id === event.id)) {
    return snapshot;
  }
  return applyProjectionEvent({
    ...snapshot,
    events: mergeEvents(snapshot.events, [event]),
    lastEventId: Math.max(snapshot.lastEventId, event.id),
  }, event);
}

export function applyTaskHistoryEvents(snapshot: AppSnapshot, events: EventRecord[]): AppSnapshot {
  const existingEventIds = new Set(snapshot.events.map((event) => event.id));
  let next = {
    ...snapshot,
    events: mergeEvents(snapshot.events, events),
    lastEventId: Math.max(snapshot.lastEventId, maxEventId(events)),
  };
  for (const event of [...events].sort((left, right) => left.id - right.id)) {
    if (event.id <= snapshot.snapshotEventId || existingEventIds.has(event.id)) {
      continue;
    }
    next = applyProjectionEvent(next, event);
  }
  return next;
}

export function mergeEvents(current: EventRecord[], next: EventRecord[]): EventRecord[] {
  const byId = new Map<number, EventRecord>();
  for (const event of current) byId.set(event.id, event);
  for (const event of next) byId.set(event.id, event);
  return [...byId.values()].sort((left, right) => left.id - right.id);
}

function maxEventId(events: EventRecord[]): number {
  return events.reduce((max, event) => Math.max(max, event.id), 0);
}

function applyProjectionEvent(snapshot: AppSnapshot, event: EventRecord): AppSnapshot {
  const payload = asRecord(event.payload);
  if (event.type === "task.created" && event.taskId) {
    const task: Task = {
      id: event.taskId,
      projectId: String(payload.projectId ?? "") || (isRecord(payload.metadata) ? String(payload.metadata.projectId ?? "") : undefined),
      title: String(payload.title ?? "Untitled task"),
      prompt: String(payload.prompt ?? ""),
      status: "queued",
      objectiveStatus: "active",
      objectivePhase: "queued",
      createdAt: event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
    };
    return { ...snapshot, tasks: upsertById(snapshot.tasks, task) };
  }
  if (event.type === "task.status" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    const status = String(payload.status) as Task["status"];
    const objective = taskObjectiveForStatus(task.objectiveStatus, task.objectivePhase, status);
    return {
      ...snapshot,
      tasks: upsertById(snapshot.tasks, {
        ...task,
        status,
        error: payloadValue(payload.error) || undefined,
        objectiveStatus: objective.status,
        objectivePhase: objective.phase,
        updatedAt: event.at,
      }),
    };
  }
  if (event.type === "task.final_candidate_selected" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, finalCandidateWorkerId: String(payload.workerId ?? "") || undefined, updatedAt: event.at }) } : snapshot;
  }
  if (event.type === "task.objective_updated" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, objectiveStatus: String(payload.status ?? task.objectiveStatus) as Task["objectiveStatus"], objectivePhase: String(payload.phase ?? task.objectivePhase ?? ""), updatedAt: event.at }) } : snapshot;
  }
  if (event.type === "task.milestone_reached" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    return {
      ...snapshot,
      tasks: upsertById(snapshot.tasks, {
        ...task,
        milestones: [...(task.milestones ?? []), {
          name: String(payload.name ?? ""),
          phase: String(payload.phase ?? "") || undefined,
          summary: String(payload.summary ?? "") || undefined,
          at: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
        }],
        updatedAt: event.at,
      }),
    };
  }
  if (event.type === "task.artifact_recorded" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    return {
      ...snapshot,
      tasks: upsertById(snapshot.tasks, {
        ...task,
        artifacts: upsertTaskArtifactClient(task.artifacts ?? [], {
          id: String(payload.id ?? ""),
          kind: String(payload.kind ?? ""),
          name: String(payload.name ?? "") || undefined,
          url: String(payload.url ?? "") || undefined,
          ref: String(payload.ref ?? "") || undefined,
          createdAt: event.at,
          updatedAt: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
        }),
        updatedAt: event.at,
      }),
    };
  }
  if (event.type === "task.cleared" && event.taskId) {
    return {
      ...snapshot,
      tasks: snapshot.tasks.filter((task) => task.id !== event.taskId),
      workers: snapshot.workers.filter((worker) => worker.taskId !== event.taskId),
      executionNodes: snapshot.executionNodes.filter((node) => node.taskId !== event.taskId),
      pullRequests: snapshot.pullRequests.filter((pr) => pr.taskId !== event.taskId),
      orchestrationGraphs: snapshot.orchestrationGraphs.filter((graph) => graph.taskId !== event.taskId),
    };
  }
  if (event.type === "execution.node_planned" && event.taskId) {
    const nodeId = String(payload.nodeId ?? "");
    if (!nodeId) return snapshot;
    const executionNodes = upsertById(snapshot.executionNodes, {
      id: nodeId,
      taskId: event.taskId,
      workerId: String(payload.workerId ?? event.workerId ?? "") || undefined,
      workerKind: String(payload.workerKind ?? "unknown"),
      status: "queued",
      planId: String(payload.planId ?? "") || undefined,
      parentNodeId: String(payload.parentNodeId ?? "") || undefined,
      spawnId: String(payload.spawnId ?? "") || undefined,
      role: String(payload.role ?? "") || undefined,
      reason: String(payload.reason ?? "") || undefined,
      targetId: String(payload.targetId ?? "") || undefined,
      targetKind: String(payload.targetKind ?? "") || undefined,
      remoteSession: String(payload.remoteSession ?? "") || undefined,
      remoteRunDir: String(payload.remoteRunDir ?? "") || undefined,
      remoteWorkDir: String(payload.remoteWorkDir ?? "") || undefined,
      dependsOn: Array.isArray(payload.dependsOn) ? payload.dependsOn.map(String) : undefined,
      createdAt: event.at,
      updatedAt: event.at,
    });
    return { ...snapshot, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "execution.node_status") {
    const nodeId = String(payload.nodeId ?? "");
    const executionNodes = snapshot.executionNodes.map((node) => node.id === nodeId ? { ...node, status: String(payload.status) as WorkerStatus, updatedAt: event.at } : node);
    return { ...snapshot, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "worker.created" && event.workerId && event.taskId) {
    return {
      ...snapshot,
      workers: upsertById(snapshot.workers, {
        id: event.workerId,
        taskId: event.taskId,
        kind: String(payload.kind ?? "unknown"),
        status: "queued",
        command: Array.isArray(payload.command) ? payload.command.map(String) : undefined,
        prompt: payloadValue(payload.prompt) || undefined,
        promptPath: payloadValue(payload.promptPath) || undefined,
        promptError: payloadValue(payload.promptError) || undefined,
        createdAt: event.at,
        updatedAt: event.at,
        metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
      }),
    };
  }
  if (event.type === "worker.workspace_prepared" && event.workerId) {
    return {
      ...snapshot,
      workers: snapshot.workers.map((worker) => worker.id === event.workerId ? {
        ...worker,
        metadata: { ...(worker.metadata ?? {}), ...payload },
        updatedAt: event.at,
      } : worker),
    };
  }
  if ((event.type === "worker.started" || event.type === "worker.completed") && event.workerId) {
    const status = event.type === "worker.started" ? "running" : String(payload.status) as WorkerStatus;
    const workers = snapshot.workers.map((worker) => worker.id === event.workerId ? { ...worker, status, updatedAt: event.at } : worker);
    const executionNodes = snapshot.executionNodes.map((node) => node.workerId === event.workerId ? { ...node, status, updatedAt: event.at } : node);
    return { ...snapshot, workers, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "worker.changes_applied" && event.taskId && event.workerId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, appliedWorkerId: event.workerId, updatedAt: event.at }) } : snapshot;
  }
  if (event.type === "pull_request.published" && event.taskId) {
    const id = String(payload.id ?? "") || `${String(payload.repo ?? "")}#${String(payload.number ?? "")}`;
    if (!id) return snapshot;
    return {
      ...snapshot,
      pullRequests: upsertById(snapshot.pullRequests, {
        id,
        taskId: event.taskId,
        repo: String(payload.repo ?? ""),
        number: typeof payload.number === "number" ? payload.number : undefined,
        url: String(payload.url ?? ""),
        branch: String(payload.branch ?? ""),
        base: String(payload.base ?? ""),
        title: String(payload.title ?? ""),
        state: String(payload.state ?? "") || undefined,
        draft: Boolean(payload.draft),
        checksStatus: String(payload.checksStatus ?? "") || undefined,
        mergeStatus: String(payload.mergeStatus ?? "") || undefined,
        reviewStatus: String(payload.reviewStatus ?? "") || undefined,
        createdAt: event.at,
        updatedAt: event.at,
        metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
      }),
    };
  }
  if (event.type === "pull_request.status_checked") {
    const id = String(payload.id ?? "");
    const pr = snapshot.pullRequests.find((candidate) => candidate.id === id);
    return pr ? { ...snapshot, pullRequests: upsertById(snapshot.pullRequests, { ...pr, state: String(payload.state ?? "") || pr.state, draft: Boolean(payload.draft), checksStatus: String(payload.checksStatus ?? "") || pr.checksStatus, mergeStatus: String(payload.mergeStatus ?? "") || pr.mergeStatus, reviewStatus: String(payload.reviewStatus ?? "") || pr.reviewStatus, updatedAt: event.at, metadata: isRecord(payload.metadata) ? payload.metadata : pr.metadata }) } : snapshot;
  }
  if (event.type === "pull_request.babysitter_started") {
    const id = String(payload.id ?? "");
    const pr = snapshot.pullRequests.find((candidate) => candidate.id === id);
    return pr ? { ...snapshot, pullRequests: upsertById(snapshot.pullRequests, { ...pr, babysitterTaskId: String(payload.babysitterTaskId ?? "") || pr.babysitterTaskId, updatedAt: event.at }) } : snapshot;
  }
  return snapshot;
}

function upsertById<T extends { id: string }>(items: T[], next: T): T[] {
  return items.some((item) => item.id === next.id)
    ? items.map((item) => (item.id === next.id ? next : item))
    : [...items, next];
}

function objectiveStatusForTaskStatus(status: Task["status"]): Task["objectiveStatus"] {
  if (status === "succeeded") return "satisfied";
  if (status === "failed" || status === "canceled") return "abandoned";
  if (status === "waiting") return "waiting_user";
  return "active";
}

function taskObjectiveForStatus(currentStatus: Task["objectiveStatus"], currentPhase: string | undefined, status: Task["status"]): { status: Task["objectiveStatus"]; phase: string } {
  if (status === "succeeded" || status === "failed" || status === "canceled") {
    return { status: objectiveStatusForTaskStatus(status), phase: objectivePhaseForTaskStatus(status) };
  }
  if (status === "waiting" && (!currentStatus || currentStatus === "active")) {
    return { status: "waiting_user", phase: objectivePhaseForTaskStatus(status) };
  }
  return {
    status: currentStatus || objectiveStatusForTaskStatus(status),
    phase: currentPhase || objectivePhaseForTaskStatus(status),
  };
}

function objectivePhaseForTaskStatus(status: Task["status"]): string {
  if (status === "succeeded") return "satisfied";
  return status;
}

function upsertTaskArtifactClient(items: NonNullable<Task["artifacts"]>, next: NonNullable<Task["artifacts"]>[number]): NonNullable<Task["artifacts"]> {
  if (!next.id) return [...items, next];
  return items.some((item) => item.id === next.id)
    ? items.map((item) => (item.id === next.id ? { ...next, createdAt: next.createdAt || item.createdAt } : item))
    : [...items, next];
}

function deriveOrchestrationGraphs(tasks: Task[], nodes: ExecutionNode[]): OrchestrationGraph[] {
  const tasksById = new Map(tasks.map((task) => [task.id, task]));
  const byTask = new Map<string, ExecutionNode[]>();
  for (const node of nodes) {
    byTask.set(node.taskId, [...(byTask.get(node.taskId) ?? []), node]);
  }
  return [...byTask.entries()].map(([taskId, taskNodes]) => {
    const spawnToNode = new Map(taskNodes.filter((node) => node.spawnId).map((node) => [node.spawnId!, node.id]));
    const edges = taskNodes.flatMap((node) => {
      const items = [];
      if (node.parentNodeId) items.push({ from: node.parentNodeId, to: node.id, reason: "parent" });
      for (const dep of node.dependsOn ?? []) {
        const from = spawnToNode.get(dep);
        if (from) items.push({ from, to: node.id, reason: `depends_on:${dep}` });
      }
      return items;
    });
    const summary = {
      total: taskNodes.length,
      running: taskNodes.filter((node) => node.status === "running").length,
      waiting: taskNodes.filter((node) => node.status === "waiting" || node.status === "queued").length,
      done: taskNodes.filter((node) => node.status === "succeeded").length,
      failed: taskNodes.filter((node) => node.status === "failed").length,
      canceled: taskNodes.filter((node) => node.status === "canceled").length,
    };
    return {
      taskId,
      status: tasksById.get(taskId)?.status ?? "queued",
      nodes: taskNodes.map((node) => ({
        id: node.id,
        workerId: node.workerId,
        workerKind: node.workerKind,
        status: node.status,
        role: node.role,
        reason: node.reason,
        spawnId: node.spawnId,
        targetId: node.targetId,
        targetKind: node.targetKind,
      })),
      edges,
      summary,
      updatedAt: taskNodes.map((node) => node.updatedAt).sort().at(-1) ?? "",
    };
  });
}

function payloadValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) return value.map(payloadValue).filter(Boolean).join(", ");
  if (typeof value === "object") return prettyPayload(value);
  return String(value);
}

function prettyPayload(value: unknown): string {
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value, null, 2);
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
