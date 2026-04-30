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
  projectId?: string;
  title: string;
  prompt: string;
  status: TaskStatus;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
  finalCandidateWorkerId?: string;
  appliedWorkerId?: string;
};

export type Project = {
  id: string;
  name: string;
  localPath: string;
  repo?: string;
  upstreamRepo?: string;
  headRepoOwner?: string;
  pushRemote?: string;
  vcs?: string;
  defaultBase?: string;
  workspaceRoot?: string;
  targetLabels?: Record<string, string>;
};

export type Plugin = {
  id: string;
  name: string;
  kind: string;
  protocol?: string;
  enabled: boolean;
  status?: string;
  error?: string;
  command?: string[];
  endpoint?: string;
  capabilities?: string[];
  config?: Record<string, string>;
};

export type Worker = {
  id: string;
  taskId: string;
  kind: string;
  status: WorkerStatus;
  command?: string[];
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
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

export type OrchestrationGraph = {
  taskId: string;
  status: TaskStatus;
  nodes: OrchestrationGraphNode[];
  edges: OrchestrationGraphEdge[];
  summary: {
    total: number;
    running: number;
    waiting: number;
    done: number;
    failed: number;
    canceled: number;
  };
  updatedAt: string;
};

export type OrchestrationGraphNode = {
  id: string;
  workerId?: string;
  workerKind: string;
  status: WorkerStatus;
  role?: string;
  reason?: string;
  spawnId?: string;
  targetId?: string;
  targetKind?: string;
};

export type OrchestrationGraphEdge = {
  from: string;
  to: string;
  reason?: string;
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

export type PullRequestState = {
  id: string;
  taskId: string;
  repo: string;
  number?: number;
  url: string;
  branch: string;
  base: string;
  title: string;
  state?: string;
  draft?: boolean;
  checksStatus?: string;
  mergeStatus?: string;
  reviewStatus?: string;
  babysitterTaskId?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
};

export type Snapshot = {
  tasks: Task[] | null;
  workers: Worker[] | null;
  executionNodes?: ExecutionNode[] | null;
  targets?: TargetState[] | null;
  plugins?: Plugin[] | null;
  projects?: Project[] | null;
  pullRequests?: PullRequestState[] | null;
  orchestrationGraphs?: OrchestrationGraph[] | null;
  events: EventRecord[] | null;
};
