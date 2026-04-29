import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bot,
  Check,
  CircleStop,
  Eye,
  FileText,
  MessageSquarePlus,
  Play,
  RefreshCw,
  Send,
  Terminal,
  Trash2,
} from "lucide-react";
import { applyWorkerChanges, cancelTask, cancelWorker, clearFinishedTasks, clearTask, createTask, getSnapshot, getWorkerChanges, steerTask } from "./api";
import type { EventRecord, ExecutionNode, Snapshot, TargetState, Task, Worker, WorkerChangesReview } from "./types";
import "./styles.css";

type AppSnapshot = {
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  targets: TargetState[];
  events: EventRecord[];
};

const emptySnapshot: AppSnapshot = {
  tasks: [],
  workers: [],
  executionNodes: [],
  targets: [],
  events: [],
};

function App() {
  const [snapshot, setSnapshot] = useState<AppSnapshot>(emptySnapshot);
  const [selectedTaskId, setSelectedTaskId] = useState<string>("");
  const [selectedWorkerId, setSelectedWorkerId] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [connected, setConnected] = useState(false);

  async function refresh() {
    const next = normalizeSnapshot(await getSnapshot());
    setSnapshot(next);
    setSelectedTaskId((current) => (next.tasks.some((task) => task.id === current) ? current : next.tasks.at(-1)?.id || ""));
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
  const selectedWorker = selectedWorkers.find((worker) => worker.id === selectedWorkerId);
  const selectedWorkerNode = selectedNodes.find((node) => node.workerId === selectedWorker?.id);
  const selectedWorkerEvents = selectedEvents.filter((event) => event.workerId === selectedWorker?.id);
  const progress = workProgress(selectedTask, selectedWorkers, selectedNodes);
  const hasTerminalTasks = snapshot.tasks.some(isTerminalTask);

  async function handleClearTask(taskId: string) {
    try {
      setError("");
      await clearTask(taskId);
      await refresh();
    } catch (err) {
      setError((err as Error).message);
    }
  }

  async function handleClearFinished() {
    try {
      setError("");
      await clearFinishedTasks();
      await refresh();
    } catch (err) {
      setError((err as Error).message);
    }
  }

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
          <div className="panel-title split-title">
            <span>
              <Activity size={18} />
              <h2>Tasks</h2>
            </span>
            <button className="icon-button ghost" disabled={!hasTerminalTasks} onClick={handleClearFinished} title="Clear finished tasks">
              <Trash2 size={16} />
            </button>
          </div>
          {snapshot.tasks.length === 0 ? (
            <p className="empty">No tasks yet.</p>
          ) : (
            snapshot.tasks.map((task) => (
              <div
                key={task.id}
                className={task.id === selectedTask?.id ? "task-row selected" : "task-row"}
              >
                <button className="task-row-main" onClick={() => setSelectedTaskId(task.id)}>
                  <span>
                    <strong>{task.title}</strong>
                    <small>{task.id.slice(0, 8)}</small>
                  </span>
                  <Status value={task.status} />
                </button>
                {isTerminalTask(task) && (
                  <button className="icon-button ghost danger-text task-clear" onClick={() => handleClearTask(task.id)} title="Clear task">
                    <Trash2 size={16} />
                  </button>
                )}
              </div>
            ))
          )}
        </section>

        <section className="workspace">
          {selectedTask ? (
            <>
              <TaskDetail task={selectedTask} onCancel={cancelTask} onSteer={steerTask} onError={setError} />
              <WorkSummary progress={progress} nodes={selectedNodes} workers={selectedWorkers} />
              <TargetPanel targets={snapshot.targets} />
              <ExecutionGraph nodes={selectedNodes} />
              <WorkerList
                workers={selectedWorkers}
                events={selectedEvents}
                selectedWorkerId={selectedWorkerId}
                onSelect={setSelectedWorkerId}
                onReview={getWorkerChanges}
                onApply={applyWorkerChanges}
                onApplied={refresh}
                onCancel={cancelWorker}
                onError={setError}
              />
              {selectedWorker && <WorkerDetail worker={selectedWorker} node={selectedWorkerNode} events={selectedWorkerEvents} />}
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

type WorkProgress = {
  total: number;
  done: number;
  running: number;
  waiting: number;
  failed: number;
  percent: number;
};

function workProgress(task: Task | undefined, workers: Worker[], nodes: ExecutionNode[]): WorkProgress {
  const items = nodes.length > 0 ? nodes.map((node) => node.status) : workers.map((worker) => worker.status);
  const total = items.length || (task ? 1 : 0);
  const done = items.filter((status) => status === "succeeded").length;
  const running = items.filter((status) => status === "running").length + (task?.status === "planning" ? 1 : 0);
  const waiting = items.filter((status) => status === "waiting" || status === "queued").length;
  const failed = items.filter((status) => status === "failed" || status === "canceled").length;
  const terminalTaskDone = task?.status === "succeeded" && total === 1 ? 1 : done;
  return {
    total,
    done: terminalTaskDone,
    running,
    waiting,
    failed,
    percent: total > 0 ? Math.round((terminalTaskDone / total) * 100) : 0,
  };
}

function isTerminalTask(task: Task): boolean {
  return task.status === "succeeded" || task.status === "failed" || task.status === "canceled";
}

function WorkSummary({ progress, nodes, workers }: { progress: WorkProgress; nodes: ExecutionNode[]; workers: Worker[] }) {
  const activeNodes = nodes.filter((node) => node.status === "running" || node.status === "queued" || node.status === "waiting");
  const activeWorkers = workers.filter((worker) => worker.status === "running" || worker.status === "queued" || worker.status === "waiting");
  const activeCount = activeNodes.length || activeWorkers.length;
  return (
    <section className="panel summary-panel">
      <div className="panel-title">
        <Activity size={18} />
        <h2>Current State</h2>
      </div>
      <div className="summary-grid">
        <Metric label="Progress" value={`${progress.percent}%`} />
        <Metric label="Done" value={`${progress.done}/${progress.total}`} />
        <Metric label="Running" value={String(progress.running)} />
        <Metric label="Waiting" value={String(progress.waiting)} />
        <Metric label="Failed" value={String(progress.failed)} />
      </div>
      <div className="progress-track" aria-label={`Progress ${progress.percent}%`}>
        <div style={{ width: `${progress.percent}%` }} />
      </div>
      <div className="active-work">
        <strong>{activeCount} active</strong>
        {(activeNodes.length > 0 ? activeNodes : activeWorkers).slice(0, 4).map((item) => (
          <span key={item.id}>
            {"workerKind" in item ? (item.role || item.workerKind) : item.kind} <Status value={item.status} />
          </span>
        ))}
      </div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
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
  selectedWorkerId,
  onSelect,
  onReview,
  onApply,
  onApplied,
  onCancel,
  onError,
}: {
  workers: Worker[];
  events: EventRecord[];
  selectedWorkerId: string;
  onSelect: (id: string) => void;
  onReview: (id: string) => Promise<WorkerChangesReview>;
  onApply: (id: string) => Promise<void>;
  onApplied: () => Promise<void>;
  onCancel: (id: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [applying, setApplying] = useState<string>("");
  const [diffs, setDiffs] = useState<Record<string, DiffReviewState>>({});

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

  async function toggleDiff(workerId: string) {
    const current = diffs[workerId];
    if (current?.open) {
      setDiffs((items) => ({ ...items, [workerId]: { ...current, open: false } }));
      return;
    }
    if (current?.loaded) {
      setDiffs((items) => ({ ...items, [workerId]: { ...current, open: true } }));
      return;
    }
    setDiffs((items) => ({ ...items, [workerId]: { open: true, loading: true, loaded: false, diff: "" } }));
    try {
      const review = await onReview(workerId);
      setDiffs((items) => ({
        ...items,
        [workerId]: {
          open: true,
          loading: false,
          loaded: true,
          diff: review.changes.diff ?? "",
          error: review.changes.error,
        },
      }));
    } catch (err) {
      const message = (err as Error).message;
      setDiffs((items) => ({
        ...items,
        [workerId]: { open: true, loading: false, loaded: true, diff: "", error: message },
      }));
      onError(message);
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
            const workerEvents = events.filter((event) => event.workerId === worker.id);
            const latestEvent = latestInspectableWorkerEvent(workerEvents);
            const diff = diffs[worker.id];
            return (
              <article key={worker.id} className={worker.id === selectedWorkerId ? "worker-card selected" : "worker-card"}>
                <div>
                  <strong>{worker.kind}</strong>
                  <small>{worker.id.slice(0, 8)}</small>
                </div>
                <Status value={worker.status} />
                <button className="icon-button ghost" onClick={() => onSelect(worker.id)} title="Inspect worker">
                  <Eye size={16} />
                </button>
                <button className="icon-button danger" onClick={() => onCancel(worker.id).catch((err: Error) => onError(err.message))} title="Cancel worker">
                  <CircleStop size={16} />
                </button>
                <div className="worker-current">
                  <span>Latest</span>
                  <p>{latestEvent ? eventDisplayText(latestEvent) : "No worker events yet."}</p>
                </div>
                <WorkerActivity events={workerEvents} defaultOpen={worker.status === "running" || worker.status === "waiting"} />
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
                    <div className="worker-review-actions">
                      <button className="secondary compact" disabled={diff?.loading} onClick={() => toggleDiff(worker.id)} title={diff?.open ? "Hide worker diff" : "Show worker diff"}>
                        <FileText size={16} />
                        {diff?.loading ? "Loading" : diff?.open ? "Hide Diff" : "Diff"}
                      </button>
                      <button className="primary compact" disabled={applied || applying === worker.id} onClick={() => apply(worker.id)} title={applied ? "Worker changes already applied" : "Apply worker changes"}>
                        <Check size={16} />
                        {applied ? "Applied" : applying === worker.id ? "Applying" : "Apply"}
                      </button>
                    </div>
                    {diff?.open && <DiffViewer state={diff} />}
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

type DiffReviewState = {
  open: boolean;
  loading: boolean;
  loaded: boolean;
  diff: string;
  error?: string;
};

function DiffViewer({ state }: { state: DiffReviewState }) {
  if (state.loading) {
    return <div className="worker-diff loading">Loading diff...</div>;
  }
  if (state.error) {
    return <div className="worker-diff error">{state.error}</div>;
  }
  if (!state.diff) {
    return <div className="worker-diff empty">No diff content available.</div>;
  }
  return (
    <pre className="worker-diff" aria-label="Worker diff">
      {state.diff.split("\n").map((line, index) => (
        <span key={index} className={diffLineClass(line)}>
          {line || " "}
        </span>
      ))}
    </pre>
  );
}

function diffLineClass(line: string): string {
  if (line.startsWith("+") && !line.startsWith("+++")) return "diff-add";
  if (line.startsWith("-") && !line.startsWith("---")) return "diff-remove";
  if (line.startsWith("@@")) return "diff-hunk";
  if (line.startsWith("diff ") || line.startsWith("index ") || line.startsWith("+++ ") || line.startsWith("--- ")) return "diff-meta";
  return "diff-context";
}

function WorkerDetail({ worker, node, events }: { worker: Worker; node: ExecutionNode | undefined; events: EventRecord[] }) {
  const created = events.find((event) => event.type === "worker.created");
  const workspace = events.find((event) => event.type === "worker.workspace_prepared");
  const completed = [...events].reverse().find((event) => event.type === "worker.completed");
  return (
    <section className="panel worker-detail-panel">
      <div className="panel-title split-title">
        <span>
          <Eye size={18} />
          <h2>Worker Detail</h2>
        </span>
        <Status value={worker.status} />
      </div>
      <div className="worker-detail-grid">
        <DetailItem label="Worker" value={`${worker.kind} ${worker.id.slice(0, 8)}`} />
        <DetailItem label="Node" value={node?.id.slice(0, 8) ?? "none"} />
        <DetailItem label="Target" value={node?.targetId ? `${node.targetKind ?? "target"}:${node.targetId}` : "local"} />
        <DetailItem label="Updated" value={new Date(worker.updatedAt).toLocaleString()} />
      </div>
      {created && <FullCommand event={created} />}
      {workspace && <WorkspaceSummary event={workspace} />}
      {completed && <CompletionSummary event={completed} />}
      <div className="worker-detail-events">
        <h3>Worker Events</h3>
        <div className="worker-event-list full">
          {events.filter(isInspectableWorkerEvent).length === 0 ? (
            <p className="empty">No worker events yet.</p>
          ) : (
            events.filter(isInspectableWorkerEvent).slice().reverse().map((event) => <WorkerEventLine key={event.id} event={event} />)
          )}
        </div>
      </div>
    </section>
  );
}

function DetailItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="detail-item">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function WorkerActivity({ events, defaultOpen }: { events: EventRecord[]; defaultOpen: boolean }) {
  const inspectable = events.filter(isInspectableWorkerEvent);
  const recent = inspectable.slice(-8).reverse();
  const command = events.find((event) => event.type === "worker.created");
  return (
    <details className="worker-activity" open={defaultOpen}>
      <summary>{inspectable.length} events</summary>
      {command && <CommandLine event={command} />}
      <div className="worker-event-list">
        {recent.length === 0 ? (
          <p className="empty">No worker activity yet.</p>
        ) : (
          recent.map((event) => <WorkerEventLine key={event.id} event={event} />)
        )}
      </div>
    </details>
  );
}

function FullCommand({ event }: { event: EventRecord }) {
  const payload = event.payload as { command?: string[] };
  if (!Array.isArray(payload.command) || payload.command.length === 0) {
    return null;
  }
  return (
    <details className="worker-detail-section">
      <summary>Command</summary>
      <pre>{payload.command.join(" ")}</pre>
    </details>
  );
}

function CommandLine({ event }: { event: EventRecord }) {
  const payload = event.payload as { command?: string[] };
  if (!Array.isArray(payload.command) || payload.command.length === 0) {
    return null;
  }
  const command = payload.command
    .slice(0, 5)
    .map((part) => (part.length > 80 ? `${part.slice(0, 80)}...` : part))
    .join(" ");
  return (
    <div className="worker-command">
      <span>Command</span>
      <code>{command}</code>
    </div>
  );
}

function WorkspaceSummary({ event }: { event: EventRecord }) {
  const payload = event.payload as DisplayPayload;
  return (
    <details className="worker-detail-section" open>
      <summary>Workspace</summary>
      <div className="worker-detail-grid">
        <DetailItem label="Mode" value={payload.mode ?? "unknown"} />
        <DetailItem label="VCS" value={payload.vcsType ?? "unknown"} />
        <DetailItem label="CWD" value={payload.cwd ?? payload.root ?? "unknown"} />
        <DetailItem label="Dirty" value={String(Boolean(payload.dirty ?? payload.workspaceChanges?.dirty ?? false))} />
      </div>
      {payload.root && <pre>{payload.root}</pre>}
    </details>
  );
}

function CompletionSummary({ event }: { event: EventRecord }) {
  const text = eventDisplayText(event);
  return (
    <details className="worker-detail-section" open>
      <summary>Completion</summary>
      <pre>{text}</pre>
    </details>
  );
}

function WorkerEventLine({ event }: { event: EventRecord }) {
  return (
    <div className="worker-event-line">
      <div className="worker-event-meta">
        <time>{new Date(event.at).toLocaleTimeString()}</time>
        <code>{workerEventLabel(event)}</code>
      </div>
      <EventPayload event={event} />
    </div>
  );
}

function latestInspectableWorkerEvent(events: EventRecord[]): EventRecord | undefined {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    if (isInspectableWorkerEvent(events[index])) {
      return events[index];
    }
  }
  return undefined;
}

function isInspectableWorkerEvent(event: EventRecord): boolean {
  return event.type !== "worker.started";
}

function workerEventLabel(event: EventRecord): string {
  if (event.type === "worker.output") {
    const payload = event.payload as { kind?: string; stream?: string };
    return [payload.kind, payload.stream].filter(Boolean).join(":") || "output";
  }
  return event.type.replace("worker.", "");
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
            {node.targetId && <small>{node.targetKind ?? "target"}: {node.targetId}</small>}
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

function TargetPanel({ targets }: { targets: TargetState[] }) {
  if (targets.length === 0) return null;
  return (
    <section className="panel">
      <div className="panel-title">
        <Activity size={18} />
        <h2>Targets</h2>
      </div>
      <div className="node-grid">
        {targets.map((target) => (
          <article key={target.id} className="node-card">
            <div>
              <strong>{target.id}</strong>
              <small>{target.kind}</small>
            </div>
            <Status value={target.available ? "running" : "waiting"} />
            <p>
              {target.running}/{target.capacity.maxWorkers} workers
              {target.capacity.memoryGB ? ` | ${target.capacity.memoryGB} GB` : ""}
            </p>
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

type DisplayPayload = {
  text?: string;
  stream?: string;
  kind?: string;
  status?: string;
  message?: string;
  error?: string;
  summary?: string;
  reason?: string;
  question?: string;
  answer?: string;
  approved?: boolean;
  cleaned?: boolean;
  logCount?: number;
  needsInput?: boolean;
  root?: string;
  cwd?: string;
  sourceRoot?: string;
  workspaceName?: string;
  change?: string;
  baseChange?: string;
  mode?: string;
  vcsType?: string;
  dirty?: boolean;
  sourceDirty?: boolean;
  cleanupPolicy?: string;
  policy?: string;
  result?: string;
  command?: string[];
  raw?: unknown;
  rawResult?: unknown;
  changedFiles?: { path: string; status?: string }[];
  workspaceChanges?: { changedFiles?: { path: string; status?: string }[]; dirty?: boolean; diffStat?: string };
};

type DetailField = {
  label: string;
  value: string;
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
  return (
    <div className="event-line">
      <time>{new Date(event.at).toLocaleTimeString()}</time>
      <code>{event.type}</code>
      <span>{eventDisplayText(event)}</span>
    </div>
  );
}

function EventPayload({ event }: { event: EventRecord }) {
  const payload = asRecord(event.payload);
  const display = eventDisplayText(event);
  const fields = eventDetailFields(payload);
  const command = Array.isArray(payload.command) ? payload.command.map(String) : [];
  const changedFiles = eventChangedFiles(payload);
  const rawPayload = payload.raw ?? payload.rawResult;

  return (
    <div className="event-payload">
      {display && <p>{display}</p>}
      {fields.length > 0 && (
        <dl className="event-fields">
          {fields.map((field) => (
            <div key={field.label}>
              <dt>{field.label}</dt>
              <dd>{field.value}</dd>
            </div>
          ))}
        </dl>
      )}
      {command.length > 0 && (
        <div className="event-command">
          <span>Command</span>
          <code>{command.join(" ")}</code>
        </div>
      )}
      {changedFiles.length > 0 && (
        <details className="event-files">
          <summary>{changedFiles.length} changed files</summary>
          <ul>
            {changedFiles.map((file) => (
              <li key={`${file.status ?? "changed"}-${file.path}`}>
                <code>{file.status ?? "changed"}</code>
                <span>{file.path}</span>
              </li>
            ))}
          </ul>
        </details>
      )}
      {rawPayload !== undefined && (
        <details className="event-raw">
          <summary>Raw payload</summary>
          <pre>{prettyPayload(rawPayload)}</pre>
        </details>
      )}
    </div>
  );
}

function eventDisplayText(event: EventRecord): string {
  const payload = event.payload as DisplayPayload;
  const changedFiles = payload.changedFiles ?? payload.workspaceChanges?.changedFiles ?? [];
  const changeText =
    changedFiles.length > 0
      ? `${changedFiles.length} changed: ${changedFiles
          .slice(0, 4)
          .map((file) => file.path)
          .join(", ")}${changedFiles.length > 4 ? "..." : ""}`
      : undefined;
  const workspaceText = payload.cwd || payload.root ? `${payload.mode ?? "workspace"} ${payload.cwd ?? payload.root}` : undefined;
  const primaryText =
    payload.text ??
    payload.summary ??
    payload.message ??
    payload.error ??
    payload.reason ??
    payload.question ??
    payload.answer ??
    payload.workspaceChanges?.diffStat ??
    workspaceText ??
    payload.status ??
    (typeof payload.approved === "boolean" ? `approved: ${payload.approved}` : undefined) ??
    (typeof payload.cleaned === "boolean" ? `cleaned: ${payload.cleaned}` : undefined);
  return primaryText
    ? changeText
      ? `${primaryText} | ${changeText}`
      : primaryText
    : (changeText ?? payloadSummary(event.payload));
}

function eventDetailFields(payload: Record<string, unknown>): DetailField[] {
  const fieldKeys: [string, string][] = [
    ["Kind", "kind"],
    ["Stream", "stream"],
    ["Status", "status"],
    ["Log count", "logCount"],
    ["Needs input", "needsInput"],
    ["Mode", "mode"],
    ["VCS", "vcsType"],
    ["Workspace", "workspaceName"],
    ["CWD", "cwd"],
    ["Root", "root"],
    ["Source", "sourceRoot"],
    ["Change", "change"],
    ["Base change", "baseChange"],
    ["Dirty", "dirty"],
    ["Source dirty", "sourceDirty"],
    ["Cleanup policy", "cleanupPolicy"],
    ["Policy", "policy"],
    ["Result", "result"],
    ["Cleaned", "cleaned"],
    ["Reason", "reason"],
  ];
  return fieldKeys
    .map(([label, key]) => ({ label, value: payloadValue(payload[key]) }))
    .filter((field) => field.value !== "");
}

function eventChangedFiles(payload: Record<string, unknown>): { path: string; status?: string }[] {
  const direct = changedFileList(payload.changedFiles);
  if (direct.length > 0) return direct;
  const workspaceChanges = asRecord(payload.workspaceChanges);
  return changedFileList(workspaceChanges.changedFiles);
}

function changedFileList(value: unknown): { path: string; status?: string }[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) => {
    const file = asRecord(item);
    const path = payloadValue(file.path);
    if (!path) return [];
    const status = payloadValue(file.status) || undefined;
    return [{ path, status }];
  });
}

function payloadSummary(value: unknown): string {
  const payload = asRecord(value);
  const entries = Object.entries(payload)
    .filter(([key]) => !["raw", "rawResult", "command", "changedFiles", "workspaceChanges"].includes(key))
    .map(([key, item]) => `${humanizeKey(key)}: ${payloadValue(item)}`)
    .filter((item) => !item.endsWith(": "))
    .slice(0, 5);
  if (entries.length > 0) return entries.join(" | ");
  return prettyPayload(value);
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

function humanizeKey(key: string): string {
  return key
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/^./, (char) => char.toUpperCase());
}

function Status({ value }: { value: string }) {
  return <span className={`status ${value}`}>{value}</span>;
}

function normalizeSnapshot(snapshot: Snapshot): AppSnapshot {
  return {
    tasks: snapshot.tasks ?? [],
    workers: snapshot.workers ?? [],
    executionNodes: snapshot.executionNodes ?? [],
    targets: snapshot.targets ?? [],
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
  const clearedTasks = new Set<string>();

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
    if (event.type === "task.cleared" && event.taskId) {
      clearedTasks.add(event.taskId);
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
          targetId: String(payload.targetId ?? "") || undefined,
          targetKind: String(payload.targetKind ?? "") || undefined,
          remoteSession: String(payload.remoteSession ?? "") || undefined,
          remoteRunDir: String(payload.remoteRunDir ?? "") || undefined,
          remoteWorkDir: String(payload.remoteWorkDir ?? "") || undefined,
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
    tasks: [...tasks.values()].filter((task) => !clearedTasks.has(task.id)),
    workers: [...workers.values()].filter((worker) => !clearedTasks.has(worker.taskId)),
    executionNodes: [...executionNodes.values()].filter((node) => !clearedTasks.has(node.taskId)),
    targets: snapshot.targets,
    events: snapshot.events,
  };
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
