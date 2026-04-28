import type { Snapshot } from "./types";

export async function getSnapshot(): Promise<Snapshot> {
  const response = await fetch("/api/snapshot");
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
  return response.json();
}

export async function createTask(input: {
  title: string;
  prompt: string;
}) {
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

export async function cancelTask(taskId: string) {
  const response = await fetch(`/api/tasks/${taskId}/cancel`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
}

export async function cancelWorker(workerId: string) {
  const response = await fetch(`/api/workers/${workerId}/cancel`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(await errorMessage(response));
  }
}

async function errorMessage(response: Response): Promise<string> {
  try {
    const body = await response.json();
    return body.error ?? response.statusText;
  } catch {
    return response.statusText;
  }
}
