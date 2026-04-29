import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bot,
  Check,
  CircleStop,
  Eye,
  FileText,
  GitPullRequest,
  MessageSquarePlus,
  Play,
  RefreshCw,
  Send,
  Terminal,
  Trash2,
} from "lucide-react";
import { applyWorkerChanges, askAssistant, babysitPullRequest, cancelTask, cancelWorker, clearFinishedTasks, clearTask, createTask, getSnapshot, getWorkerChanges, publishTaskPullRequest, refreshPullRequest, steerTask } from "./api";
import type { EventRecord, ExecutionNode, Project, PullRequestState, Snapshot, TargetState, Task, Worker, WorkerChangesReview } from "./types";
import "./styles.css";

type AppSnapshot = {
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  targets: TargetState[];
  projects: Project[];
  pullRequests: PullRequestState[];
  events: EventRecord[];
};

const emptySnapshot: AppSnapshot = {
  tasks: [],
  workers: [],
  executionNodes: [],
  targets: [],
  projects: [],
  pullRequests: [],
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
  const selectedPullRequests = snapshot.pullRequests.filter((pr) => pr.taskId === selectedTask?.id);
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
        <section className="left-stack">
          <TaskComposer
            onCreate={async (input) => {
              setError("");
              const task = await createTask(input);
              setSelectedTaskId(task.id);
            }}
            onError={setError}
            projects={snapshot.projects}
          />
          <AssistantPanel onError={setError} />
        </section>

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
              <PullRequestPanel
                task={selectedTask}
                pullRequests={selectedPullRequests}
                onPublish={publishTaskPullRequest}
                onRefresh={refreshPullRequest}
                onBabysit={babysitPullRequest}
                onDone={refresh}
                onError={setError}
              />
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
  projects,
}: {
  onCreate: (input: { projectId?: string; title: string; prompt: string }) => Promise<void>;
  onError: (message: string) => void;
  projects: Project[];
}) {
  const [projectId, setProjectId] = useState("");
  const [title, setTitle] = useState("");
  const [prompt, setPrompt] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      await onCreate({ projectId: projectId || undefined, title, prompt });
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
        Project
        <select value={projectId} onChange={(event) => setProjectId(event.target.value)}>
          <option value="">Default project</option>
          {projects.map((project) => (
            <option key={project.id} value={project.id}>
              {project.name}
              {project.repo ? ` (${project.repo})` : ""}
            </option>
          ))}
        </select>
      </label>
      <label>
        Title
        <input value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Auto-generated if blank" />
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

function AssistantPanel({ onError }: { onError: (message: string) => void }) {
  const [message, setMessage] = useState("");
  const [conversationId, setConversationId] = useState("");
  const [answer, setAnswer] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      const response = await askAssistant({ conversationId: conversationId || undefined, message });
      setConversationId(response.conversationId);
      setAnswer(response.message);
      setMessage("");
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="panel assistant-panel" onSubmit={submit}>
      <div className="panel-title">
        <Bot size={18} />
        <h2>Ask</h2>
      </div>
      <textarea value={message} onChange={(event) => setMessage(event.target.value)} placeholder="Ask about the system or repo..." required />
      <button className="secondary" disabled={busy}>
        <Send size={16} />
        {busy ? "Asking" : "Ask"}
      </button>
      {answer && <pre className="assistant-answer">{answer}</pre>}
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
          {task.projectId && <small>Project {task.projectId}</small>}
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

function PullRequestPanel({
  task,
  pullRequests,
  onPublish,
  onRefresh,
  onBabysit,
  onDone,
  onError,
}: {
  task: Task;
  pullRequests: PullRequestState[];
  onPublish: (taskId: string) => Promise<PullRequestState>;
  onRefresh: (id: string) => Promise<PullRequestState>;
  onBabysit: (id: string) => Promise<unknown>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
  const [busy, setBusy] = useState("");
  const canPublish = isTerminalTask(task) && pullRequests.length === 0;

  async function run(action: string, fn: () => Promise<unknown>) {
    setBusy(action);
    try {
      await fn();
      await onDone();
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setBusy("");
    }
  }

  return (
    <section className="panel pr-panel">
      <div className="panel-title split-title">
        <span>
          <GitPullRequest size={18} />
          <h2>Pull Requests</h2>
        </span>
        <button className="secondary compact" disabled={!canPublish || busy === "publish"} onClick={() => run("publish", () => onPublish(task.id))}>
          <GitPullRequest size={16} />
          {busy === "publish" ? "Opening" : "Open PR"}
        </button>
      </div>
      {pullRequests.length === 0 ? (
        <p className="empty">No pull request has been opened for this task.</p>
      ) : (
        <div className="pr-list">
          {pullRequests.map((pr) => (
            <article key={pr.id} className="pr-card">
              <div className="pr-main">
                <a href={pr.url} target="_blank" rel="noreferrer">
                  {pr.repo}
                  {pr.number ? `#${pr.number}` : ""}
                </a>
                <small>{pr.title}</small>
              </div>
              <div className="pr-statuses">
                <Status value={pr.state?.toLowerCase() || "waiting"} />
                {pr.checksStatus && <span className="pill">{pr.checksStatus}</span>}
                {pr.reviewStatus && <span className="pill">{pr.reviewStatus.toLowerCase()}</span>}
              </div>
              <div className="pr-actions">
                <button className="secondary compact" disabled={busy === `refresh:${pr.id}`} onClick={() => run(`refresh:${pr.id}`, () => onRefresh(pr.id))}>
                  <RefreshCw size={16} />
                  Refresh
                </button>
                <button className="secondary compact" disabled={Boolean(pr.babysitterTaskId) || busy === `babysit:${pr.id}`} onClick={() => run(`babysit:${pr.id}`, () => onBabysit(pr.id))}>
                  <Bot size={16} />
                  {pr.babysitterTaskId ? "Babysitting" : "Babysit"}
                </button>
              </div>
            </article>
          ))}
        </div>
      )}
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
  const target = node?.targetId ? `${node.targetKind ?? "target"}:${node.targetId}` : "local";
  return (
    <section className="panel worker-detail-panel">
      <div className="worker-detail-hero">
        <div className="worker-detail-title">
          <Eye size={18} />
          <div>
            <h2>{worker.kind} worker</h2>
            <p>{worker.id}</p>
          </div>
        </div>
        <Status value={worker.status} />
      </div>
      <div className="worker-meta-strip">
        <WorkerMetaItem label="Kind" value={worker.kind} />
        <WorkerMetaItem label="Worker" value={worker.id.slice(0, 8)} />
        <WorkerMetaItem label="Node" value={node?.id.slice(0, 8) ?? "none"} />
        <WorkerMetaItem label="Target" value={target} />
        <WorkerMetaItem label="Updated" value={new Date(worker.updatedAt).toLocaleString()} />
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

function WorkerMetaItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="worker-meta-item">
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
    <section className="worker-section-card">
      <div className="section-title-row">
        <strong>Command</strong>
        <span className="tool-status neutral">{payload.command.length} parts</span>
      </div>
      <CodeBlock label="command" value={payload.command.join(" ")} className="shell-script" />
    </section>
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
  const dirty = Boolean(payload.dirty ?? payload.workspaceChanges?.dirty ?? false);
  return (
    <section className="worker-section-card">
      <div className="section-title-row">
        <strong>Workspace</strong>
        <span className="tool-status neutral">{payload.mode ?? "unknown"}</span>
        <span className="tool-status neutral">{payload.vcsType ?? "vcs unknown"}</span>
        <span className={dirty ? "tool-status warning" : "tool-status"}>{dirty ? "dirty" : "clean"}</span>
      </div>
      <div className="path-list">
        <PathRow label="CWD" value={payload.cwd ?? payload.root ?? "unknown"} />
        {payload.root && payload.root !== payload.cwd && <PathRow label="Root" value={payload.root} />}
        {payload.sourceRoot && <PathRow label="Source" value={payload.sourceRoot} />}
        {payload.workspaceName && <PathRow label="Workspace" value={payload.workspaceName} />}
      </div>
    </section>
  );
}

function CompletionSummary({ event }: { event: EventRecord }) {
  const text = eventDisplayText(event);
  const payload = asRecord(event.payload);
  const changedFiles = eventChangedFiles(payload);
  return (
    <section className="worker-section-card">
      <div className="section-title-row">
        <strong>Completion</strong>
        {payloadValue(payload.status) && <span className={payload.status === "succeeded" ? "tool-status" : "tool-status failed"}>{payloadValue(payload.status)}</span>}
      </div>
      {text && <TruncatedBlock label="Summary" value={text} className="agent-message-body" limit={1000} />}
      {changedFiles.length > 0 && <ChangedFilesList files={changedFiles} />}
    </section>
  );
}

function PathRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="path-row">
      <span>{label}</span>
      <code title={value}>{value}</code>
    </div>
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
  return event.type.startsWith("worker.");
}

function workerEventLabel(event: EventRecord): string {
  if (event.type === "worker.output") {
    const payload = event.payload as { kind?: string; stream?: string; raw?: unknown };
    const raw = asRecord(payload.raw);
    const item = asRecord(raw.item);
    if (item.type === "command_execution") return `command:${payload.kind ?? "log"}`;
    if (item.type === "agent_message") return `message:${payload.kind ?? "result"}`;
    if (item.type === "file_change") return `file:${String(item.status ?? payload.kind ?? "log")}`;
    if (raw.type === "turn.completed") return "usage";
    if (raw.type === "thread.started") return "thread";
    if (raw.type === "turn.started") return "turn";
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
  id?: string;
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
  repo?: string;
  number?: number;
  url?: string;
  branch?: string;
  base?: string;
  title?: string;
  state?: string;
  draft?: boolean;
  checksStatus?: string;
  mergeStatus?: string;
  reviewStatus?: string;
  babysitterTaskId?: string;
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
  const structured = structuredWorkerEvent(event);
  if (structured) return structured;

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

function structuredWorkerEvent(event: EventRecord): React.ReactNode {
  if (!event.type.startsWith("worker.")) {
    return null;
  }
  const payload = asRecord(event.payload);
  const raw = asRecord(payload.raw ?? payload.rawResult);
  const item = asRecord(raw.item);

  if (event.type === "worker.created") {
    return <WorkerCreatedCard payload={payload} />;
  }
  if (event.type === "worker.started") {
    return <WorkerLifecycleCard title="Started" subtitle="Worker process is running" />;
  }
  if (event.type === "worker.workspace_prepared") {
    return <WorkerWorkspaceCard payload={payload} />;
  }
  if (event.type === "worker.workspace_cleaned") {
    return <WorkerCleanupCard payload={payload} />;
  }
  if (event.type === "worker.completed") {
    return <WorkerCompletedCard payload={payload} />;
  }
  if (event.type !== "worker.output") {
    return null;
  }
  if (item.type === "command_execution") {
    return <CommandExecutionCard payload={payload} item={item} raw={raw} />;
  }
  if (item.type === "agent_message") {
    return <AgentMessageCard payload={payload} item={item} raw={raw} />;
  }
  if (item.type === "file_change") {
    return <FileChangeCard payload={payload} item={item} raw={raw} />;
  }
  if (raw.type === "turn.completed") {
    return <UsageCard raw={raw} />;
  }
  if (raw.type === "thread.started" || raw.type === "turn.started") {
    return <LifecycleCard raw={raw} />;
  }
  return null;
}

function WorkerCreatedCard({ payload }: { payload: Record<string, unknown> }) {
  const metadata = asRecord(payload.metadata);
  const steps = Array.isArray(metadata.steps) ? metadata.steps : [];
  const command = Array.isArray(payload.command) ? payload.command.map(String) : [];
  return (
    <div className="lifecycle-card">
      <div className="tool-card-header">
        <strong>Worker Created</strong>
        {payloadValue(payload.kind) && <span className="tool-status neutral">{payloadValue(payload.kind)}</span>}
        {payloadValue(metadata.brain) && <span className="tool-status neutral">{payloadValue(metadata.brain)}</span>}
        {steps.length > 0 && <span className="tool-status neutral">{steps.length} steps</span>}
      </div>
      {payloadValue(metadata.rationale) && <p>{payloadValue(metadata.rationale)}</p>}
      {command.length > 0 && <CodeBlock label="command" value={command.join(" ")} className="shell-script" />}
      <MetadataPreview metadata={metadata} />
    </div>
  );
}

function WorkerLifecycleCard({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="lifecycle-card compact-card">
      <div className="tool-card-header">
        <strong>{title}</strong>
        <span className="tool-status">running</span>
      </div>
      <p>{subtitle}</p>
    </div>
  );
}

function WorkerWorkspaceCard({ payload }: { payload: Record<string, unknown> }) {
  const dirty = payload.dirty === true || asRecord(payload.workspaceChanges).dirty === true;
  return (
    <div className="lifecycle-card">
      <div className="tool-card-header">
        <strong>Workspace Ready</strong>
        {payloadValue(payload.mode) && <span className="tool-status neutral">{payloadValue(payload.mode)}</span>}
        {payloadValue(payload.vcsType) && <span className="tool-status neutral">{payloadValue(payload.vcsType)}</span>}
        <span className={dirty ? "tool-status warning" : "tool-status"}>{dirty ? "dirty" : "clean"}</span>
      </div>
      <div className="path-list">
        <PathRow label="CWD" value={payloadValue(payload.cwd || payload.root) || "unknown"} />
        {payloadValue(payload.root) && payload.root !== payload.cwd && <PathRow label="Root" value={payloadValue(payload.root)} />}
        {payloadValue(payload.sourceRoot) && <PathRow label="Source" value={payloadValue(payload.sourceRoot)} />}
        {payloadValue(payload.workspaceName) && <PathRow label="Workspace" value={payloadValue(payload.workspaceName)} />}
      </div>
    </div>
  );
}

function WorkerCleanupCard({ payload }: { payload: Record<string, unknown> }) {
  const cleaned = payload.cleaned === true;
  return (
    <div className="lifecycle-card compact-card">
      <div className="tool-card-header">
        <strong>Workspace Cleanup</strong>
        {payloadValue(payload.cleanupPolicy || payload.policy) && <span className="tool-status neutral">{payloadValue(payload.cleanupPolicy || payload.policy)}</span>}
        <span className={cleaned ? "tool-status" : "tool-status neutral"}>{cleaned ? "cleaned" : "retained"}</span>
      </div>
      {payloadValue(payload.result) && <p>{payloadValue(payload.result)}</p>}
      {payloadValue(payload.root) && <PathRow label="Root" value={payloadValue(payload.root)} />}
    </div>
  );
}

function MetadataPreview({ metadata }: { metadata: Record<string, unknown> }) {
  const steps = Array.isArray(metadata.steps) ? metadata.steps.map(asRecord).slice(0, 4) : [];
  if (steps.length === 0) {
    return <RawPayloadDetails value={metadata} />;
  }
  return (
    <details className="event-files metadata-preview">
      <summary>Plan steps</summary>
      <ul>
        {steps.map((step, index) => (
          <li key={index}>
            <code>{payloadValue(step.workerKind || step.kind || index + 1)}</code>
            <span>{payloadValue(step.title || step.description || step.prompt)}</span>
          </li>
        ))}
      </ul>
    </details>
  );
}

function CommandExecutionCard({ payload, item, raw }: { payload: Record<string, unknown>; item: Record<string, unknown>; raw: Record<string, unknown> }) {
  const command = payloadValue(item.command);
  const script = shellScriptFromCommand(command);
  const output = payloadValue(item.aggregated_output);
  const status = payloadValue(item.status) || payloadValue(payload.kind);
  const exitCode = payloadValue(item.exit_code);
  const failed = status === "failed" || (exitCode !== "" && exitCode !== "0");
  return (
    <div className={failed ? "tool-card failed" : "tool-card"}>
      <div className="tool-card-header">
        <strong>Shell</strong>
        <span className={failed ? "tool-status failed" : "tool-status"}>{status || "running"}</span>
        {exitCode && <span className={failed ? "tool-status failed" : "tool-status"}>exit {exitCode}</span>}
      </div>
      {script ? (
        <CodeBlock label="bash" value={script} className="shell-script" />
      ) : (
        <CodeBlock label="command" value={command} className="shell-script" />
      )}
      {output ? <TruncatedBlock label="Output" value={output} className={failed ? "tool-output failed" : "tool-output"} /> : <p className="empty">No output yet.</p>}
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function AgentMessageCard({ payload, item, raw }: { payload: Record<string, unknown>; item: Record<string, unknown>; raw: Record<string, unknown> }) {
  const text = payloadValue(item.text) || payloadValue(payload.text);
  return (
    <div className="agent-message-card">
      <div className="tool-card-header">
        <strong>Agent Message</strong>
        <span className="tool-status">{payloadValue(payload.kind) || "result"}</span>
      </div>
      <TruncatedBlock label="Message" value={text} className="agent-message-body" limit={1600} />
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function FileChangeCard({ payload, item, raw }: { payload: Record<string, unknown>; item: Record<string, unknown>; raw: Record<string, unknown> }) {
  const path = payloadValue(item.path) || payloadValue(item.file) || payloadValue(payload.text);
  return (
    <div className="file-change-card">
      <div className="tool-card-header">
        <strong>File Change</strong>
        <span className="tool-status">{payloadValue(item.status) || payloadValue(payload.kind) || "changed"}</span>
      </div>
      <code>{path || eventDisplayText({ id: 0, at: "", type: "worker.output", payload })}</code>
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function UsageCard({ raw }: { raw: Record<string, unknown> }) {
  const usage = asRecord(raw.usage);
  return (
    <div className="usage-card">
      <div className="tool-card-header">
        <strong>Usage</strong>
        <span className="tool-status">turn completed</span>
      </div>
      <dl className="event-fields">
        {Object.entries(usage).map(([key, value]) => (
          <div key={key}>
            <dt>{humanizeKey(key)}</dt>
            <dd>{payloadValue(value)}</dd>
          </div>
        ))}
      </dl>
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function LifecycleCard({ raw }: { raw: Record<string, unknown> }) {
  return (
    <div className="lifecycle-card">
      <strong>{payloadValue(raw.type)}</strong>
      {payloadValue(raw.thread_id) && <code>{payloadValue(raw.thread_id)}</code>}
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function WorkerCompletedCard({ payload }: { payload: Record<string, unknown> }) {
  const changedFiles = eventChangedFiles(payload);
  return (
    <div className="completion-card">
      <div className="tool-card-header">
        <strong>Completed</strong>
        <span className={payload.status === "succeeded" ? "tool-status" : "tool-status failed"}>{payloadValue(payload.status)}</span>
        {payloadValue(payload.logCount) && <span className="tool-status">{payloadValue(payload.logCount)} logs</span>}
      </div>
      {payloadValue(payload.summary) && <TruncatedBlock label="Summary" value={payloadValue(payload.summary)} className="agent-message-body" limit={1600} />}
      {payloadValue(payload.error) && <TruncatedBlock label="Error" value={payloadValue(payload.error)} className="tool-output failed" limit={1000} />}
      {changedFiles.length > 0 && <ChangedFilesList files={changedFiles} />}
      <RawPayloadDetails value={payload.rawResult ?? payload.workspaceChanges} />
    </div>
  );
}

function ChangedFilesList({ files }: { files: { path: string; status?: string }[] }) {
  return (
    <details className="event-files" open>
      <summary>{files.length} changed files</summary>
      <ul>
        {files.map((file) => (
          <li key={`${file.status ?? "changed"}-${file.path}`}>
            <code>{file.status ?? "changed"}</code>
            <span>{file.path}</span>
          </li>
        ))}
      </ul>
    </details>
  );
}

function CodeBlock({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <div className="code-block">
      <span>{label}</span>
      <pre className={className}>{highlightShell(value)}</pre>
    </div>
  );
}

function TruncatedBlock({ label, value, className, limit = 2400 }: { label: string; value: string; className?: string; limit?: number }) {
  const truncated = value.length > limit;
  const visible = truncated ? `${value.slice(0, limit).trimEnd()}\n... truncated ${value.length - limit} chars` : value;
  return (
    <div className="truncated-block">
      <span>{label}</span>
      <pre className={className}>{visible || " "}</pre>
      {truncated && (
        <details>
          <summary>Full output</summary>
          <pre className={className}>{value}</pre>
        </details>
      )}
    </div>
  );
}

function RawPayloadDetails({ value }: { value: unknown }) {
  if (value === undefined || value === null || prettyPayload(value) === "{}") return null;
  return (
    <details className="event-raw compact">
      <summary>Raw payload</summary>
      <pre>{prettyPayload(value)}</pre>
    </details>
  );
}

function shellScriptFromCommand(command: string): string {
  const marker = " -lc ";
  const index = command.indexOf(marker);
  if (index < 0) return "";
  const script = command.slice(index + marker.length).trim();
  if ((script.startsWith("'") && script.endsWith("'")) || (script.startsWith('"') && script.endsWith('"'))) {
    return script.slice(1, -1);
  }
  return script;
}

function highlightShell(value: string): React.ReactNode {
  const lines = value.split("\n");
  return lines.map((line, lineIndex) => (
    <React.Fragment key={lineIndex}>
      {line.split(/(\s+|&&|\|\||;)/).map((part, index) => {
        const cls = shellTokenClass(part);
        return cls ? <span key={index} className={cls}>{part}</span> : <React.Fragment key={index}>{part}</React.Fragment>;
      })}
      {lineIndex < lines.length - 1 ? "\n" : ""}
    </React.Fragment>
  ));
}

function shellTokenClass(token: string): string {
  if (/^(jj|git|npm|go|curl|sqlite3|sed|rg|cat|ls|cd|mkdir|rm|cp|mv|test)$/.test(token)) return "shell-command-token";
  if (/^(-{1,2}[\w-]+)/.test(token)) return "shell-flag-token";
  if (/^(&&|\|\||;)$/.test(token)) return "shell-operator-token";
  return "";
}

function eventDisplayText(event: EventRecord): string {
  const compact = compactEventDisplay(event);
  if (compact) return compact;

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
    payload.title ??
    payload.url ??
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

function compactEventDisplay(event: EventRecord): string {
  const payload = asRecord(event.payload);
  const metadata = asRecord(payload.metadata);
  if (event.type === "task.created") {
    return payloadValue(payload.title || payload.prompt) || "Task created";
  }
  if (event.type === "task.planned") {
    const worker = payloadValue(payload.workerKind) || "worker";
    const rationale = payloadValue(payload.rationale || metadata.rationale);
    return rationale ? `Planned ${worker}: ${rationale}` : `Planned ${worker}`;
  }
  if (event.type === "execution.node_planned") {
    const worker = payloadValue(payload.workerKind) || "worker";
    const node = payloadValue(payload.nodeId || metadata.nodeID);
    const target = payloadValue(payload.targetId || metadata.targetID);
    const targetKind = payloadValue(payload.targetKind || metadata.targetKind) || "target";
    return [`${worker} node`, node ? node.slice(0, 8) : "", target ? `on ${targetKind}:${target}` : ""].filter(Boolean).join(" ");
  }
  if (event.type === "execution.node_status") {
    return payloadValue(payload.status) || "Node status changed";
  }
  if (event.type === "worker.created") {
    const brain = payloadValue(metadata.brain);
    const kind = payloadValue(payload.kind) || "worker";
    return brain ? `${kind} worker created by ${brain}` : `${kind} worker created`;
  }
  if (event.type === "worker.started") {
    return "Worker started";
  }
  if (event.type === "worker.workspace_prepared") {
    const mode = payloadValue(payload.mode) || "workspace";
    const vcs = payloadValue(payload.vcsType);
    return [mode, vcs, "workspace ready"].filter(Boolean).join(" ");
  }
  if (event.type === "worker.workspace_cleaned") {
    return payloadValue(payload.result) || payloadValue(payload.cleanupPolicy || payload.policy) || "Workspace cleanup recorded";
  }
  if (event.type === "worker.output") {
    const raw = asRecord(payload.raw ?? payload.rawResult);
    const item = asRecord(raw.item);
    if (item.type === "command_execution") {
      return `Shell ${payloadValue(item.status || payload.kind) || "event"}`;
    }
    if (item.type === "agent_message") {
      return payloadValue(item.text || payload.text) || "Agent message";
    }
    if (item.type === "file_change") {
      return payloadValue(item.path || item.file || payload.text) || "File changed";
    }
    if (raw.type === "thread.started") return "Thread started";
    if (raw.type === "turn.started") return "Turn started";
    if (raw.type === "turn.completed") return "Turn completed";
  }
  return "";
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
    ["Repo", "repo"],
    ["Number", "number"],
    ["URL", "url"],
    ["Branch", "branch"],
    ["Base", "base"],
    ["Checks", "checksStatus"],
    ["Merge", "mergeStatus"],
    ["Review", "reviewStatus"],
    ["Babysitter task", "babysitterTaskId"],
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
    projects: snapshot.projects ?? [],
    pullRequests: snapshot.pullRequests ?? [],
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
  const pullRequests = new Map<string, PullRequestState>();
  const clearedTasks = new Set<string>();

  for (const event of snapshot.events) {
    const payload = event.payload as Record<string, unknown>;
    if (event.type === "task.created" && event.taskId) {
      tasks.set(event.taskId, {
        id: event.taskId,
        projectId: String(payload.projectId ?? "") || (isRecord(payload.metadata) ? String(payload.metadata.projectId ?? "") : undefined),
        title: String(payload.title ?? "Untitled task"),
        prompt: String(payload.prompt ?? ""),
        status: "queued",
        createdAt: event.at,
        updatedAt: event.at,
        metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
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
    if (event.type === "pull_request.published" && event.taskId) {
      const prId = String(payload.id ?? "");
      if (prId) {
        pullRequests.set(prId, {
          id: prId,
          taskId: event.taskId,
          repo: String(payload.repo ?? ""),
          number: typeof payload.number === "number" ? payload.number : undefined,
          url: String(payload.url ?? ""),
          branch: String(payload.branch ?? ""),
          base: String(payload.base ?? ""),
          title: String(payload.title ?? ""),
          state: String(payload.state ?? "") || undefined,
          draft: Boolean(payload.draft),
          checksStatus: String(payload.checksStatus ?? "") || undefined,
          mergeStatus: String(payload.mergeStatus ?? "") || undefined,
          reviewStatus: String(payload.reviewStatus ?? "") || undefined,
          createdAt: event.at,
          updatedAt: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
        });
      }
    }
    if (event.type === "pull_request.status_checked") {
      const prId = String(payload.id ?? "");
      const pr = pullRequests.get(prId);
      if (pr) {
        pullRequests.set(prId, {
          ...pr,
          state: String(payload.state ?? "") || pr.state,
          draft: Boolean(payload.draft),
          checksStatus: String(payload.checksStatus ?? "") || pr.checksStatus,
          mergeStatus: String(payload.mergeStatus ?? "") || pr.mergeStatus,
          reviewStatus: String(payload.reviewStatus ?? "") || pr.reviewStatus,
          updatedAt: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : pr.metadata,
        });
      }
    }
    if (event.type === "pull_request.babysitter_started") {
      const prId = String(payload.id ?? "");
      const pr = pullRequests.get(prId);
      if (pr) {
        pullRequests.set(prId, {
          ...pr,
          babysitterTaskId: String(payload.babysitterTaskId ?? "") || pr.babysitterTaskId,
          updatedAt: event.at,
        });
      }
    }
  }

  return {
    tasks: [...tasks.values()].filter((task) => !clearedTasks.has(task.id)),
    workers: [...workers.values()].filter((worker) => !clearedTasks.has(worker.taskId)),
    executionNodes: [...executionNodes.values()].filter((node) => !clearedTasks.has(node.taskId)),
    projects: snapshot.projects,
    pullRequests: [...pullRequests.values()].filter((pr) => !clearedTasks.has(pr.taskId)),
    targets: snapshot.targets,
    events: snapshot.events,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
