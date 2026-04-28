import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bot,
  Check,
  CircleStop,
  MessageSquarePlus,
  Play,
  RefreshCw,
  Send,
  Terminal,
} from "lucide-react";
import { applyWorkerChanges, cancelTask, cancelWorker, createTask, getSnapshot, steerTask } from "./api";
import type { EventRecord, ExecutionNode, Snapshot, Task, Worker } from "./types";
import "./styles.css";

type AppSnapshot = {
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  events: EventRecord[];
};

const emptySnapshot: AppSnapshot = {
  tasks: [],
  workers: [],
  executionNodes: [],
  events: [],
};

function App() {
  const [snapshot, setSnapshot] = useState<AppSnapshot>(emptySnapshot);
  const [selectedTaskId, setSelectedTaskId] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [connected, setConnected] = useState(false);

  async function refresh() {
    const next = normalizeSnapshot(await getSnapshot());
    setSnapshot(next);
    setSelectedTaskId((current) => current || next.tasks.at(-1)?.id || "");
  }

  useEffect(() => {
    refresh().catch((err: Error) => setError(err.message));
  }, []);

  useEffect(() => {
    const lastID = snapshot.events.at(-1)?.id ?? 0;
    const source = new EventSource(`/api/events/stream?after=${lastID}`);
    source.addEventListener("open", () => setConnected(true));
    source.addEventListener("error", () => setConnected(false));
    source.addEventListener("event", (message) => {
      const event = JSON.parse((message as MessageEvent).data) as EventRecord;
      setSnapshot((current) => reduceEvent(current, event));
    });
    return () => source.close();
  }, []);

  const selectedTask = useMemo(
    () => snapshot.tasks.find((task) => task.id === selectedTaskId) ?? snapshot.tasks.at(-1),
    [selectedTaskId, snapshot.tasks],
  );
  const selectedWorkers = snapshot.workers.filter((worker) => worker.taskId === selectedTask?.id);
  const selectedNodes = snapshot.executionNodes.filter((node) => node.taskId === selectedTask?.id);
  const selectedEvents = snapshot.events.filter((event) => event.taskId === selectedTask?.id);

  return (
    <main className="app">
      <header className="topbar">
        <div>
          <h1>aged</h1>
          <p>Local agent orchestration</p>
        </div>
        <div className="topbar-actions">
          <span className={connected ? "pill ok" : "pill"}>{connected ? "Live" : "Offline"}</span>
          <button className="icon-button" onClick={() => refresh().catch((err: Error) => setError(err.message))} title="Refresh">
            <RefreshCw size={18} />
          </button>
        </div>
      </header>

      {error && (
        <div className="notice" role="alert">
          {error}
          <button onClick={() => setError("")}>Dismiss</button>
        </div>
      )}

      <section className="layout">
        <TaskComposer
          onCreate={async (input) => {
            setError("");
            const task = await createTask(input);
            setSelectedTaskId(task.id);
          }}
          onError={setError}
        />

        <section className="panel task-list">
          <div className="panel-title">
            <Activity size={18} />
            <h2>Tasks</h2>
          </div>
          {snapshot.tasks.length === 0 ? (
            <p className="empty">No tasks yet.</p>
          ) : (
            snapshot.tasks.map((task) => (
              <button
                key={task.id}
                className={task.id === selectedTask?.id ? "task-row selected" : "task-row"}
                onClick={() => setSelectedTaskId(task.id)}
              >
                <span>
                  <strong>{task.title}</strong>
                  <small>{task.id.slice(0, 8)}</small>
                </span>
                <Status value={task.status} />
              </button>
            ))
          )}
        </section>

        <section className="workspace">
          {selectedTask ? (
            <>
              <TaskDetail task={selectedTask} onCancel={cancelTask} onSteer={steerTask} onError={setError} />
              <ExecutionGraph nodes={selectedNodes} />
              <WorkerList workers={selectedWorkers} events={selectedEvents} onApply={applyWorkerChanges} onApplied={refresh} onCancel={cancelWorker} onError={setError} />
              <EventLog events={selectedEvents} />
            </>
          ) : (
            <div className="panel empty-state">Create a task to start orchestration.</div>
          )}
        </section>
      </section>
    </main>
  );
}

function TaskComposer({
  onCreate,
  onError,
}: {
  onCreate: (input: { title: string; prompt: string }) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [title, setTitle] = useState("");
  const [prompt, setPrompt] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      await onCreate({ title, prompt });
      setTitle("");
      setPrompt("");
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="panel composer" onSubmit={submit}>
      <div className="panel-title">
        <MessageSquarePlus size={18} />
        <h2>Start Work</h2>
      </div>
      <label>
        Title
        <input value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Implement parser retry path" required />
      </label>
      <label>
        Prompt
        <textarea value={prompt} onChange={(event) => setPrompt(event.target.value)} placeholder="Describe the development task..." required />
      </label>
      <button className="primary" disabled={busy}>
        <Play size={16} />
        {busy ? "Starting" : "Start"}
      </button>
    </form>
  );
}

function TaskDetail({
  task,
  onCancel,
  onSteer,
  onError,
}: {
  task: Task;
  onCancel: (id: string) => Promise<void>;
  onSteer: (id: string, message: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [message, setMessage] = useState("");

  async function steer(event: React.FormEvent) {
    event.preventDefault();
    try {
      await onSteer(task.id, message);
      setMessage("");
    } catch (err) {
      onError((err as Error).message);
    }
  }

  return (
    <section className="panel detail">
      <div className="detail-heading">
        <div>
          <h2>{task.title}</h2>
          <p>{task.prompt}</p>
        </div>
        <div className="detail-actions">
          <Status value={task.status} />
          <button className="icon-button danger" onClick={() => onCancel(task.id).catch((err: Error) => onError(err.message))} title="Cancel task">
            <CircleStop size={18} />
          </button>
        </div>
      </div>
      <form className="steer" onSubmit={steer}>
        <input value={message} onChange={(event) => setMessage(event.target.value)} placeholder="Steer this task..." required />
        <button className="icon-button" title="Send steering">
          <Send size={18} />
        </button>
      </form>
    </section>
  );
}

function WorkerList({
  workers,
  events,
  onApply,
  onApplied,
  onCancel,
  onError,
}: {
  workers: Worker[];
  events: EventRecord[];
  onApply: (id: string) => Promise<void>;
  onApplied: () => Promise<void>;
  onCancel: (id: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [applying, setApplying] = useState<string>("");

  async function apply(workerId: string) {
    setApplying(workerId);
    try {
      await onApply(workerId);
      await onApplied();
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setApplying("");
    }
  }

  return (
    <section className="panel">
      <div className="panel-title">
        <Bot size={18} />
        <h2>Workers</h2>
      </div>
      {workers.length === 0 ? (
        <p className="empty">No workers have been spawned.</p>
      ) : (
        <div className="worker-grid">
          {workers.map((worker) => {
            const completion = latestWorkerCompletion(events, worker.id);
            const changes = completion.changedFiles ?? completion.workspaceChanges?.changedFiles ?? [];
            const applied = workerChangesApplied(events, worker.id);
            return (
              <article key={worker.id} className="worker-card">
                <div>
                  <strong>{worker.kind}</strong>
                  <small>{worker.id.slice(0, 8)}</small>
                </div>
                <Status value={worker.status} />
                <button className="icon-button danger" onClick={() => onCancel(worker.id).catch((err: Error) => onError(err.message))} title="Cancel worker">
                  <CircleStop size={16} />
                </button>
                {changes.length > 0 && (
                  <div className="worker-review">
                    <details>
                      <summary>{changes.length} changed files</summary>
                      <ul>
                        {changes.slice(0, 8).map((file) => (
                          <li key={`${file.status}-${file.path}`}>
                            <code>{file.status ?? "changed"}</code>
                            <span>{file.path}</span>
                          </li>
                        ))}
                      </ul>
                    </details>
                    <button className="primary compact" disabled={applied || applying === worker.id} onClick={() => apply(worker.id)} title={applied ? "Worker changes already applied" : "Apply worker changes"}>
                      <Check size={16} />
                      {applied ? "Applied" : applying === worker.id ? "Applying" : "Apply"}
                    </button>
                  </div>
                )}
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}

function ExecutionGraph({ nodes }: { nodes: ExecutionNode[] }) {
  if (nodes.length === 0) {
    return null;
  }
  return (
    <section className="panel">
      <div className="panel-title">
        <Activity size={18} />
        <h2>Execution</h2>
      </div>
      <div className="node-grid">
        {nodes.map((node) => (
          <article key={node.id} className="node-card">
            <div>
              <strong>{node.role || node.workerKind}</strong>
              <small>{node.id.slice(0, 8)}</small>
            </div>
            <Status value={node.status} />
            <small>{node.workerKind}</small>
            {(node.spawnId || node.dependsOn?.length) && (
              <p>
                {node.spawnId ? `spawn ${node.spawnId}` : ""}
                {node.dependsOn?.length ? ` depends on ${node.dependsOn.join(", ")}` : ""}
              </p>
            )}
          </article>
        ))}
      </div>
    </section>
  );
}

type WorkerCompletionPayload = {
  changedFiles?: { path: string; status?: string }[];
  workspaceChanges?: { changedFiles?: { path: string; status?: string }[] };
};

function latestWorkerCompletion(events: EventRecord[], workerId: string): WorkerCompletionPayload {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event.type === "worker.completed" && event.workerId === workerId) {
      return event.payload as WorkerCompletionPayload;
    }
  }
  return {};
}

function workerChangesApplied(events: EventRecord[], workerId: string): boolean {
  return events.some((event) => event.type === "worker.changes_applied" && event.workerId === workerId);
}

function EventLog({ events }: { events: EventRecord[] }) {
  return (
    <section className="panel log-panel">
      <div className="panel-title">
        <Terminal size={18} />
        <h2>Timeline</h2>
      </div>
      <div className="event-log">
        {events.length === 0 ? (
          <p className="empty">No events for this task.</p>
        ) : (
          events.map((event) => <EventLine key={event.id} event={event} />)
        )}
      </div>
    </section>
  );
}

function EventLine({ event }: { event: EventRecord }) {
  const payload = event.payload as {
    text?: string;
    stream?: string;
    status?: string;
    message?: string;
    error?: string;
    summary?: string;
    reason?: string;
    cleaned?: boolean;
    changedFiles?: { path: string; status?: string }[];
    workspaceChanges?: { changedFiles?: { path: string; status?: string }[]; dirty?: boolean };
  };
  const changedFiles = payload.changedFiles ?? payload.workspaceChanges?.changedFiles ?? [];
  const changeText =
    changedFiles.length > 0
      ? `${changedFiles.length} changed: ${changedFiles
          .slice(0, 4)
          .map((file) => file.path)
          .join(", ")}${changedFiles.length > 4 ? "..." : ""}`
      : undefined;
  const primaryText =
    payload.text ??
    payload.summary ??
    payload.message ??
    payload.error ??
    payload.reason ??
    payload.status ??
    (typeof payload.cleaned === "boolean" ? `cleaned: ${payload.cleaned}` : undefined);
  const text = primaryText
    ? changeText
      ? `${primaryText} | ${changeText}`
      : primaryText
    : (changeText ?? JSON.stringify(event.payload));
  return (
    <div className="event-line">
      <time>{new Date(event.at).toLocaleTimeString()}</time>
      <code>{event.type}</code>
      <span>{text}</span>
    </div>
  );
}

function Status({ value }: { value: string }) {
  return <span className={`status ${value}`}>{value}</span>;
}

function normalizeSnapshot(snapshot: Snapshot): AppSnapshot {
  return {
    tasks: snapshot.tasks ?? [],
    workers: snapshot.workers ?? [],
    executionNodes: snapshot.executionNodes ?? [],
    events: snapshot.events ?? [],
  };
}

function reduceEvent(snapshot: AppSnapshot, event: EventRecord): AppSnapshot {
  if (snapshot.events.some((existing) => existing.id === event.id)) {
    return snapshot;
  }
  return rebuildSnapshot({ ...snapshot, events: [...snapshot.events, event] });
}

function rebuildSnapshot(snapshot: AppSnapshot): AppSnapshot {
  const tasks = new Map<string, Task>();
  const workers = new Map<string, Worker>();
  const executionNodes = new Map<string, ExecutionNode>();

  for (const event of snapshot.events) {
    const payload = event.payload as Record<string, unknown>;
    if (event.type === "task.created" && event.taskId) {
      tasks.set(event.taskId, {
        id: event.taskId,
        title: String(payload.title ?? "Untitled task"),
        prompt: String(payload.prompt ?? ""),
        status: "queued",
        createdAt: event.at,
        updatedAt: event.at,
      });
    }
    if (event.type === "task.status" && event.taskId) {
      const task = tasks.get(event.taskId);
      if (task) {
        tasks.set(event.taskId, { ...task, status: String(payload.status) as Task["status"], updatedAt: event.at });
      }
    }
    if (event.type === "execution.node_planned" && event.taskId) {
      const nodeId = String(payload.nodeId ?? "");
      if (nodeId) {
        executionNodes.set(nodeId, {
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
          dependsOn: Array.isArray(payload.dependsOn) ? payload.dependsOn.map(String) : undefined,
          createdAt: event.at,
          updatedAt: event.at,
        });
      }
    }
    if (event.type === "execution.node_status") {
      const nodeId = String(payload.nodeId ?? "");
      const node = executionNodes.get(nodeId);
      if (node) {
        executionNodes.set(nodeId, { ...node, status: String(payload.status) as Worker["status"], updatedAt: event.at });
      }
    }
    if (event.type === "worker.created" && event.workerId && event.taskId) {
      workers.set(event.workerId, {
        id: event.workerId,
        taskId: event.taskId,
        kind: String(payload.kind ?? "unknown"),
        status: "queued",
        command: Array.isArray(payload.command) ? payload.command.map(String) : undefined,
        createdAt: event.at,
        updatedAt: event.at,
      });
    }
    if (event.type === "worker.started" && event.workerId) {
      const worker = workers.get(event.workerId);
      if (worker) workers.set(event.workerId, { ...worker, status: "running", updatedAt: event.at });
      const node = [...executionNodes.values()].find((candidate) => candidate.workerId === event.workerId);
      if (node) executionNodes.set(node.id, { ...node, status: "running", updatedAt: event.at });
    }
    if (event.type === "worker.completed" && event.workerId) {
      const worker = workers.get(event.workerId);
      if (worker) workers.set(event.workerId, { ...worker, status: String(payload.status) as Worker["status"], updatedAt: event.at });
      const node = [...executionNodes.values()].find((candidate) => candidate.workerId === event.workerId);
      if (node) executionNodes.set(node.id, { ...node, status: String(payload.status) as Worker["status"], updatedAt: event.at });
    }
  }

  return {
    tasks: [...tasks.values()],
    workers: [...workers.values()],
    executionNodes: [...executionNodes.values()],
    events: snapshot.events,
  };
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
