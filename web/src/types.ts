export type TaskStatus =
  | "queued"
  | "planning"
  | "running"
  | "waiting"
  | "succeeded"
  | "failed"
  | "canceled";

export type WorkerStatus =
  | "queued"
  | "running"
  | "waiting"
  | "succeeded"
  | "failed"
  | "canceled";

export type EventRecord = {
  id: number;
  at: string;
  type: string;
  taskId?: string;
  workerId?: string;
  payload: unknown;
};

export type Task = {
  id: string;
  title: string;
  prompt: string;
  status: TaskStatus;
  createdAt: string;
  updatedAt: string;
};

export type Worker = {
  id: string;
  taskId: string;
  kind: string;
  status: WorkerStatus;
  command?: string[];
  createdAt: string;
  updatedAt: string;
};

export type WorkspaceChangedFile = {
  path: string;
  status: string;
};

export type WorkspaceChanges = {
  root: string;
  cwd: string;
  workspaceName: string;
  mode: string;
  vcsType: string;
  status: string;
  diffStat: string;
  diff?: string;
  changedFiles: WorkspaceChangedFile[];
  dirty: boolean;
  error?: string;
};

export type WorkerChangesReview = {
  workerId: string;
  workspace: {
    root: string;
    cwd: string;
    sourceRoot: string;
    workspaceName: string;
    mode: string;
    vcsType: string;
  };
  changes: WorkspaceChanges;
};

export type ExecutionNode = {
  id: string;
  taskId: string;
  workerId?: string;
  workerKind: string;
  status: WorkerStatus;
  planId?: string;
  parentNodeId?: string;
  spawnId?: string;
  role?: string;
  reason?: string;
  targetId?: string;
  targetKind?: string;
  remoteSession?: string;
  remoteRunDir?: string;
  remoteWorkDir?: string;
  dependsOn?: string[];
  createdAt: string;
  updatedAt: string;
};

export type TargetState = {
  id: string;
  kind: string;
  labels?: Record<string, string>;
  capacity: {
    maxWorkers: number;
    cpuWeight?: number;
    memoryGB?: number;
  };
  running: number;
  available: boolean;
};

export type Snapshot = {
  tasks: Task[] | null;
  workers: Worker[] | null;
  executionNodes?: ExecutionNode[] | null;
  targets?: TargetState[] | null;
  events: EventRecord[] | null;
};
