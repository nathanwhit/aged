import type { Project, PullRequestState, Snapshot, Task, WorkerChangesReview } from "./types";

export async function getSnapshot(): Promise<Snapshot> {
  const response = await fetch("/api/snapshot");
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function createTask(input: {
  projectId?: string;
  title: string;
  prompt: string;
  source?: string;
  externalId?: string;
  metadata?: Record<string, unknown>;
}): Promise<Task> {
  const response = await fetch("/api/tasks", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
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
}): Promise<Project> {
  const response = await fetch("/api/projects", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function askAssistant(input: {
  conversationId?: string;
  message: string;
  context?: Record<string, unknown>;
}): Promise<{ conversationId: string; message: string }> {
  const response = await fetch("/api/assistant", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function steerTask(taskId: string, message: string) {
  const response = await fetch(`/api/tasks/${taskId}/steer`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message }),
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
}

export async function retryTask(taskId: string) {
  const response = await fetch(`/api/tasks/${taskId}/retry`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function cancelTask(taskId: string) {
  const response = await fetch(`/api/tasks/${taskId}/cancel`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
}

export async function clearTask(taskId: string) {
  const response = await fetch(`/api/tasks/${taskId}/clear`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
}

export async function clearFinishedTasks() {
  const response = await fetch("/api/tasks/clear-terminal", {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function cancelWorker(workerId: string) {
  const response = await fetch(`/api/workers/${workerId}/cancel`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
}

export async function getWorkerChanges(workerId: string): Promise<WorkerChangesReview> {
  const response = await fetch(`/api/workers/${workerId}/changes`);
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function applyWorkerChanges(workerId: string) {
  const response = await fetch(`/api/workers/${workerId}/apply`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function applyTaskResult(taskId: string) {
  const response = await fetch(`/api/tasks/${taskId}/apply`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
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
  const response = await fetch(`/api/tasks/${taskId}/pull-request`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function refreshPullRequest(id: string): Promise<PullRequestState> {
  const response = await fetch(`/api/pull-requests/${id}/refresh`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function babysitPullRequest(id: string) {
  const response = await fetch(`/api/pull-requests/${id}/babysit`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

async function errorMessage(response: Response): Promise<string> {
  try {
    const body = await response.json();
    return body.error ?? response.statusText;
  } catch {
    return response.statusText;
  }
}
