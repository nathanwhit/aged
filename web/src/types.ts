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
  workstreamId?: string;
  title: string;
  prompt: string;
  status: TaskStatus;
  error?: string;
  objectiveStatus?: ObjectiveStatus;
  objectivePhase?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
  appliedWorkerId?: string;
  milestones?: TaskMilestone[];
  workPlan?: WorkPlan;
  artifacts?: TaskArtifact[];
};

export type WorkPlan = {
  summary?: string;
  workstreams?: WorkPlanItem[];
  validation?: WorkPlanItem[];
  risks?: string[];
};

export type WorkPlanItem = {
  id: string;
  goal: string;
  status?: string;
  doneWhen?: string;
  dependsOn?: string[];
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

export type Artifact = TaskArtifact & {
  taskId: string;
};

export type MemoryEntry = {
  id: string;
  projectId?: string;
  taskId?: string;
  kind: string;
  sourceEventId?: number;
  sourceEvent?: string;
  workerId?: string;
  summary: string;
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
  requirements?: ProjectRequirements;
  remoteCheckouts?: Record<string, string>;
  githubIssues?: GitHubIssuePolicy;
  githubMentions?: GitHubMentionPolicy;
  reviewPolicy?: ReviewPolicy;
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
  requirements?: ProjectRequirements;
  remoteCheckouts?: Record<string, string>;
  githubIssues?: GitHubIssuePolicy;
  githubMentions?: GitHubMentionPolicy;
  reviewPolicy?: ReviewPolicy;
  pullRequestPolicy?: PullRequestPolicy;
};

export type ProjectRequirements = {
  memoryMb?: number;
  storageMb?: number;
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

export type ReviewPolicy = {
  enabled?: boolean;
  beforeCompletionPr?: boolean;
  beforeIntermediatePr?: boolean;
  blockingSeverities?: string[];
  reviewerKinds?: string[];
  promptSetId?: string;
  maxAttempts?: number;
  instructions?: string;
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
  branchOwner?: string;
  branchOwnerDir?: string;
  branchHead?: string;
  updateLeaseOwner?: string;
  updateLeaseDir?: string;
  updateBaseHead?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
};

export type PullRequestFeedback = {
  id: string;
  taskId: string;
  pullRequestId: string;
  eventId?: number;
  attempt?: number;
  status?: string;
  reason?: string;
  repo?: string;
  number?: number;
  url?: string;
  branch?: string;
  base?: string;
  state?: string;
  checksStatus?: string;
  mergeStatus?: string;
  reviewStatus?: string;
  feedbackSignature?: string;
  feedbackBody?: string;
  prompt?: string;
  createdAt: string;
  updatedAt: string;
  handledAt?: string;
  metadata?: Record<string, unknown>;
};

export type SteeringItem = {
  id: string;
  taskId: string;
  workerId?: string;
  nodeId?: string;
  workerKind?: string;
  role?: string;
  spawnId?: string;
  candidateWorkerId?: string;
  reviewPhase?: string;
  targetKind?: string;
  targetId?: string;
  status?: string;
  reason?: string;
  message: string;
  createdAt: string;
  updatedAt: string;
  appliedAt?: string;
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

export type WorkItem = {
  id: string;
  taskId: string;
  kind: string;
  status: string;
  targetKind?: string;
  targetId?: string;
  reason?: string;
  prompt?: string;
  workerId?: string;
  leaseOwner?: string;
  leaseUntil?: string;
  attempt?: number;
  error?: string;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
};

export type Question = {
  id: string;
  taskId: string;
  workerId?: string;
  reason?: string;
  question: string;
  answer?: string;
  decided: boolean;
  approved?: boolean;
  createdAt: string;
  updatedAt: string;
  metadata?: Record<string, unknown>;
};

export type Session = {
  id: string;
  taskId: string;
  workerId: string;
  nodeId?: string;
  workerKind?: string;
  role?: string;
  spawnId?: string;
  status: WorkerStatus;
  targetId?: string;
  targetKind?: string;
  remoteSession?: string;
  remoteRunDir?: string;
  remoteWorkDir?: string;
  workspaceRoot?: string;
  workspaceCwd?: string;
  sourceRoot?: string;
  workspaceName?: string;
  workspaceMode?: string;
  vcsType?: string;
  sharedRoot?: string;
  sharedArtifactsDir?: string;
  sharedWorkerDir?: string;
  providerSessionId?: string;
  currentAction?: string;
  currentActionLabel?: string;
  currentActionAt?: string;
  currentActionEvent?: number;
  createdAt: string;
  startedAt?: string;
  updatedAt: string;
  completedAt?: string;
  metadata?: Record<string, unknown>;
};

export type Snapshot = {
  tasks: Task[] | null;
  workers: Worker[] | null;
  executionNodes?: ExecutionNode[] | null;
  workItems?: WorkItem[] | null;
  artifacts?: Artifact[] | null;
  memoryEntries?: MemoryEntry[] | null;
  questions?: Question[] | null;
  sessions?: Session[] | null;
  targets?: TargetState[] | null;
  plugins?: Plugin[] | null;
  promptSets?: PromptSet[] | null;
  projects?: Project[] | null;
  pullRequests?: PullRequestState[] | null;
  pullRequestFeedback?: PullRequestFeedback[] | null;
  steering?: SteeringItem[] | null;
  lastEventId?: number;
  events: EventRecord[] | null;
};

export type TaskAssignment = {
  id: string;
  taskId: string;
  sourceKind: string;
  sourceId: string;
  status?: string;
  kind?: string;
  role?: string;
  workerId?: string;
  workerKind?: string;
  workItemId?: string;
  nodeId?: string;
  sessionId?: string;
  targetKind?: string;
  targetId?: string;
  parentNodeId?: string;
  spawnId?: string;
  dependsOn?: string[];
  reason?: string;
  currentAction?: string;
  currentActionLabel?: string;
  createdAt: string;
  startedAt?: string;
  updatedAt: string;
  completedAt?: string;
};

export type TaskAssignmentsResponse = {
  taskId: string;
  assignments: TaskAssignment[];
};
