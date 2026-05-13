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

export type ObjectiveStatus =
  | "active"
  | "waiting_external"
  | "waiting_user"
  | "satisfied"
  | "abandoned";

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
  error?: string;
  objectiveStatus?: ObjectiveStatus;
  objectivePhase?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
  finalCandidateWorkerId?: string;
  appliedWorkerId?: string;
  milestones?: TaskMilestone[];
  artifacts?: TaskArtifact[];
};

export type TaskMilestone = {
  name: string;
  phase?: string;
  summary?: string;
  at: string;
  metadata?: Record<string, unknown>;
};

export type TaskArtifact = {
  id: string;
  kind: string;
  name?: string;
  url?: string;
  ref?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
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
  remoteCheckouts?: Record<string, string>;
  githubIssues?: GitHubIssuePolicy;
  githubMentions?: GitHubMentionPolicy;
  pullRequestPolicy?: PullRequestPolicy;
};

export type ProjectInput = {
  id: string;
  name?: string;
  localPath: string;
  repo?: string;
  upstreamRepo?: string;
  headRepoOwner?: string;
  pushRemote?: string;
  vcs?: string;
  defaultBase?: string;
  workspaceRoot?: string;
  targetLabels?: Record<string, string>;
  remoteCheckouts?: Record<string, string>;
  githubIssues?: GitHubIssuePolicy;
  githubMentions?: GitHubMentionPolicy;
  pullRequestPolicy?: PullRequestPolicy;
};

export type GitHubIssuePolicy = {
  enabled?: boolean;
  labels?: string[];
  issueLimit?: number;
  autoPublish?: boolean;
};

export type GitHubMentionPolicy = {
  enabled?: boolean;
  reasons?: string[];
  limit?: number;
};

export type PullRequestPolicy = {
  branchPrefix?: string;
  draft?: boolean;
  allowMerge?: boolean;
  autoMerge?: boolean;
  mergeMethod?: "squash" | "merge" | "rebase";
  monitorPullRequests?: boolean;
};

export type ProjectHealth = {
  projectId: string;
  ok: boolean;
  pathStatus: string;
  vcsStatus: string;
  githubStatus?: string;
  defaultBaseStatus?: string;
  targetStatus?: string;
  detectedVcs?: string;
  detectedRepo?: string;
  detectedBase?: string;
  errors?: string[];
  checkedAt: string;
};

export type Plugin = {
  id: string;
  name: string;
  kind: string;
  protocol?: string;
  enabled: boolean;
  builtIn?: boolean;
  status?: string;
  error?: string;
  command?: string[];
  endpoint?: string;
  capabilities?: string[];
  config?: Record<string, string>;
  driver?: {
    managed?: boolean;
    pid?: number;
    startedAt?: string;
    lastExitAt?: string;
    restartCount?: number;
    restartPolicy?: string;
    logTail?: string[];
  };
};

export type PromptSet = {
  id: string;
  name: string;
  description?: string;
  templates?: Record<string, string>;
  builtIn?: boolean;
  default?: boolean;
};

export type GitHubDriverConfig = {
  enabled: boolean;
  intervalSeconds?: number;
  issueLimit?: number;
  issues?: {
    repo: string;
    labels?: string[];
    projectId?: string;
    enabled?: boolean;
    issueLimit?: number;
    autoPublish?: boolean;
  }[];
  mentions?: {
    enabled?: boolean;
    repos?: string[];
    reasons?: string[];
    limit?: number;
  };
  pullRequests?: {
    enabled?: boolean;
    repos?: string[];
    autoPublish?: boolean;
    autoBabysit?: boolean;
    draft?: boolean;
  };
};

export type GitHubDriverState = {
  config: GitHubDriverConfig;
  running: boolean;
  startedAt?: string;
  updatedAt?: string;
  lastRunAt?: string;
  lastError?: string;
};

export type DiscordDriverConfig = {
  enabled: boolean;
  token?: string;
  intervalSeconds?: number;
  messageLimit?: number;
  processHistory?: boolean;
  assistantProjectId?: string;
  channels?: {
    id: string;
    projectId?: string;
    defaultProjectId?: string;
    allowedUserIds?: string[];
    requireMention?: boolean;
    taskPrefix?: string;
  }[];
};

export type DiscordDriverState = {
  config: DiscordDriverConfig;
  running: boolean;
  startedAt?: string;
  updatedAt?: string;
  lastRunAt?: string;
  lastError?: string;
};

export type Worker = {
  id: string;
  taskId: string;
  kind: string;
  status: WorkerStatus;
  command?: string[];
  prompt?: string;
  promptPath?: string;
  promptError?: string;
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
  host?: string;
  user?: string;
  port?: number;
  identityFile?: string;
  insecureIgnoreHostKey?: boolean;
  checkoutRoot?: string;
  workDir?: string;
  workRoot?: string;
  labels?: Record<string, string>;
  capacity: {
    maxWorkers: number;
    cpuWeight?: number;
    memoryGB?: number;
  };
  running: number;
  available: boolean;
  health?: {
    status?: string;
    error?: string;
    checkedAt?: string;
    reachable?: boolean;
    tmux?: boolean;
    repoPresent?: boolean;
    tools?: Record<string, boolean>;
  };
  resources?: {
    load1?: number;
    cpuCount?: number;
    memoryTotalMb?: number;
    memoryAvailableMb?: number;
    diskAvailableMb?: number;
    diskUsedPercent?: number;
  };
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
  checksConclusion?: string;
  mergeStatus?: string;
  mergeable?: string;
  reviewStatus?: string;
  babysitterTaskId?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
};

export type WatchPullRequestsInput = {
  repo?: string;
  number?: number;
  url?: string;
  state?: string;
  author?: string;
  headBranch?: string;
  limit?: number;
};

export type Snapshot = {
  tasks: Task[] | null;
  workers: Worker[] | null;
  executionNodes?: ExecutionNode[] | null;
  targets?: TargetState[] | null;
  plugins?: Plugin[] | null;
  promptSets?: PromptSet[] | null;
  projects?: Project[] | null;
  pullRequests?: PullRequestState[] | null;
  orchestrationGraphs?: OrchestrationGraph[] | null;
  lastEventId?: number;
  events: EventRecord[] | null;
};
