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

export type Snapshot = {
  tasks: Task[] | null;
  workers: Worker[] | null;
  events: EventRecord[] | null;
};
