import type { DiscordDriverConfig, DiscordDriverState, EventRecord, GitHubDriverConfig, GitHubDriverState, Plugin, Project, ProjectHealth, PullRequestState, Snapshot, TargetState, Task, WatchPullRequestsInput, WorkerChangesReview } from "./types";

async function request(url: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(url, init);
  if (!response.ok) throw new Error(await errorMessage(response));
  return response;
}

async function requestJSON<T = any>(url: string, init?: RequestInit): Promise<T> {
  return (await request(url, init)).json();
}

async function requestVoid(url: string, init?: RequestInit): Promise<void> {
  await request(url, init);
}

function jsonInit(method: "POST" | "PUT", body: unknown): RequestInit {
  return {
    method,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  };
}

export async function getSnapshot(options: { events?: "all" | "none" } = {}): Promise<Snapshot> {
  const query = options.events === "none" ? "?events=none" : "";
  return requestJSON(`/api/snapshot${query}`);
}

export async function getTaskEvents(taskId: string, options: { limit?: number } = {}): Promise<EventRecord[]> {
  const query = options.limit ? `?limit=${encodeURIComponent(String(options.limit))}` : "";
  return requestJSON(`/api/tasks/${encodeURIComponent(taskId)}/events${query}`);
}

export async function createTask(input: {
  projectId?: string;
  title: string;
  prompt: string;
  source?: string;
  externalId?: string;
  metadata?: Record<string, unknown>;
}): Promise<Task> {
  return requestJSON("/api/tasks", jsonInit("POST", input));
}

export async function updateTaskLoopConfig(taskId: string, input: {
  loopIntervalSeconds?: number;
  loopPrompt?: string;
}): Promise<Task> {
  return requestJSON(`/api/tasks/${encodeURIComponent(taskId)}/loop-config`, jsonInit("PUT", input));
}

export async function createProject(input: {
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
  githubIssues?: {
    enabled?: boolean;
    labels?: string[];
    issueLimit?: number;
    autoPublish?: boolean;
  };
  githubMentions?: {
    enabled?: boolean;
    reasons?: string[];
    limit?: number;
  };
  pullRequestPolicy?: {
    branchPrefix?: string;
    draft?: boolean;
    allowMerge?: boolean;
    autoMerge?: boolean;
    monitorPullRequests?: boolean;
  };
}): Promise<Project> {
  return requestJSON("/api/projects", jsonInit("POST", input));
}

export async function updateProject(id: string, input: {
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
  githubIssues?: {
    enabled?: boolean;
    labels?: string[];
    issueLimit?: number;
    autoPublish?: boolean;
  };
  githubMentions?: {
    enabled?: boolean;
    reasons?: string[];
    limit?: number;
  };
  pullRequestPolicy?: {
    branchPrefix?: string;
    draft?: boolean;
    allowMerge?: boolean;
    autoMerge?: boolean;
    monitorPullRequests?: boolean;
  };
}): Promise<Project> {
  return requestJSON(`/api/projects/${encodeURIComponent(id)}`, jsonInit("PUT", input));
}

export async function deleteProject(id: string): Promise<void> {
  return requestVoid(`/api/projects/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export async function registerPlugin(input: Plugin): Promise<Plugin> {
  return requestJSON("/api/plugins", jsonInit("POST", input));
}

export async function updatePlugin(id: string, input: Plugin): Promise<Plugin> {
  return requestJSON(`/api/plugins/${encodeURIComponent(id)}`, jsonInit("PUT", input));
}

export async function deletePlugin(id: string): Promise<void> {
  return requestVoid(`/api/plugins/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export async function getGitHubDriver(): Promise<GitHubDriverState> {
  return requestJSON("/api/drivers/github");
}

export async function updateGitHubDriver(input: GitHubDriverConfig): Promise<GitHubDriverState> {
  return requestJSON("/api/drivers/github", jsonInit("PUT", input));
}

export async function getDiscordDriver(): Promise<DiscordDriverState> {
  return requestJSON("/api/drivers/discord");
}

export async function updateDiscordDriver(input: DiscordDriverConfig): Promise<DiscordDriverState> {
  return requestJSON("/api/drivers/discord", jsonInit("PUT", input));
}

export type TargetInput = {
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
  capacity?: {
    maxWorkers?: number;
    cpuWeight?: number;
    memoryGB?: number;
  };
};

export async function createTarget(input: TargetInput): Promise<TargetState> {
  return requestJSON("/api/targets", jsonInit("POST", input));
}

export async function updateTarget(id: string, input: TargetInput): Promise<TargetState> {
  return requestJSON(`/api/targets/${encodeURIComponent(id)}`, jsonInit("PUT", input));
}

export async function deleteTarget(id: string): Promise<void> {
  return requestVoid(`/api/targets/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export async function refreshTargetHealth(id: string): Promise<TargetState> {
  return requestJSON(`/api/targets/${encodeURIComponent(id)}/health`, { method: "POST" });
}

export async function getProjectHealth(id: string): Promise<ProjectHealth> {
  return requestJSON(`/api/projects/${encodeURIComponent(id)}/health`);
}

export async function askAssistant(input: {
  conversationId?: string;
  message: string;
  context?: Record<string, unknown>;
}): Promise<{ conversationId: string; message: string }> {
  return requestJSON("/api/assistant", jsonInit("POST", input));
}

export async function steerTask(taskId: string, message: string): Promise<void> {
  return requestVoid(`/api/tasks/${taskId}/steer`, jsonInit("POST", { message }));
}

export async function retryTask(taskId: string) {
  return requestJSON(`/api/tasks/${taskId}/retry`, { method: "POST" });
}

export async function cancelTask(taskId: string): Promise<void> {
  return requestVoid(`/api/tasks/${taskId}/cancel`, { method: "POST" });
}

export async function clearTask(taskId: string): Promise<void> {
  return requestVoid(`/api/tasks/${taskId}/clear`, { method: "POST" });
}

export async function clearFinishedTasks() {
  return requestJSON("/api/tasks/clear-terminal", { method: "POST" });
}

export async function cancelWorker(workerId: string): Promise<void> {
  return requestVoid(`/api/workers/${workerId}/cancel`, { method: "POST" });
}

export async function getWorkerChanges(workerId: string): Promise<WorkerChangesReview> {
  return requestJSON(`/api/workers/${workerId}/changes`);
}

export async function applyWorkerChanges(workerId: string) {
  return requestJSON(`/api/workers/${workerId}/apply`, { method: "POST" });
}

export async function applyTaskResult(taskId: string) {
  return requestJSON(`/api/tasks/${taskId}/apply`, { method: "POST" });
}

export async function publishTaskPullRequest(taskId: string, input: {
  workerId?: string;
  repo?: string;
  base?: string;
  branch?: string;
  title?: string;
  body?: string;
  draft?: boolean;
} = {}): Promise<PullRequestState> {
  return requestJSON(`/api/tasks/${taskId}/pull-request`, jsonInit("POST", input));
}

export async function watchTaskPullRequests(taskId: string, input: WatchPullRequestsInput): Promise<PullRequestState[]> {
  return requestJSON(`/api/tasks/${encodeURIComponent(taskId)}/watch-pull-requests`, jsonInit("POST", input));
}

export async function refreshPullRequest(id: string): Promise<PullRequestState> {
  return requestJSON(`/api/pull-requests/${encodeURIComponent(id)}/refresh`, { method: "POST" });
}

export async function babysitPullRequest(id: string) {
  return requestJSON(`/api/pull-requests/${encodeURIComponent(id)}/babysit`, { method: "POST" });
}

async function errorMessage(response: Response): Promise<string> {
  try {
    const body = await response.json();
    return body.error ?? response.statusText;
  } catch {
    return response.statusText;
  }
}
