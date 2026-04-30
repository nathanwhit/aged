import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bot,
  Check,
  CircleStop,
  Eye,
  FileText,
  FolderPlus,
  GitPullRequest,
  GripVertical,
  LoaderCircle,
  Maximize2,
  Minimize2,
  MessageSquarePlus,
  Play,
  Puzzle,
  RefreshCw,
  RotateCcw,
  Send,
  Terminal,
  Trash2,
} from "lucide-react";
import { applyTaskResult, applyWorkerChanges, askAssistant, babysitPullRequest, cancelTask, cancelWorker, clearFinishedTasks, clearTask, createProject, createTask, deletePlugin, deleteProject, getProjectHealth, getSnapshot, getWorkerChanges, publishTaskPullRequest, refreshPullRequest, registerPlugin, retryTask, steerTask, updatePlugin, updateProject, watchTaskPullRequests } from "./api";
import type { EventRecord, ExecutionNode, OrchestrationGraph, Plugin, Project, ProjectHealth, PullRequestState, Snapshot, TargetState, Task, WatchPullRequestsInput, Worker, WorkerChangesReview, WorkerStatus } from "./types";
import "./styles.css";

type AppSnapshot = {
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  targets: TargetState[];
  plugins: Plugin[];
  projects: Project[];
  pullRequests: PullRequestState[];
  orchestrationGraphs: OrchestrationGraph[];
  events: EventRecord[];
};

type TaskStartInput = {
  projectId?: string;
  title: string;
  prompt: string;
  metadata?: Record<string, unknown>;
};

type InitialSnapshotStatus = "loading" | "ready" | "error";

const emptySnapshot: AppSnapshot = {
  tasks: [],
  workers: [],
  executionNodes: [],
  targets: [],
  plugins: [],
  projects: [],
  pullRequests: [],
  orchestrationGraphs: [],
  events: [],
};

type DashboardPaneId =
  | "task-detail"
  | "pull-requests"
  | "current-state"
  | "targets"
  | "plugins"
  | "workers"
  | "worker-detail"
  | "timeline";

type DashboardPaneLayout = {
  id: DashboardPaneId;
  span: number;
  minHeight: number;
};

type DashboardPane = {
  id: DashboardPaneId;
  title: string;
  element: React.ReactNode;
};

const DASHBOARD_LAYOUT_STORAGE_KEY = "aged.dashboard.layout.v1";
const DASHBOARD_MIN_SPAN = 4;
const DASHBOARD_MAX_SPAN = 12;
const DASHBOARD_MIN_HEIGHT = 0;
const DASHBOARD_MAX_HEIGHT = 900;
const DASHBOARD_HEIGHT_STEP = 48;
const DEFAULT_DASHBOARD_LAYOUT: DashboardPaneLayout[] = [
  { id: "task-detail", span: 12, minHeight: 0 },
  { id: "pull-requests", span: 6, minHeight: 0 },
  { id: "current-state", span: 6, minHeight: 0 },
  { id: "workers", span: 12, minHeight: 0 },
  { id: "worker-detail", span: 8, minHeight: 0 },
  { id: "targets", span: 4, minHeight: 0 },
  { id: "plugins", span: 4, minHeight: 0 },
  { id: "timeline", span: 12, minHeight: 360 },
];

function App() {
  const [snapshot, setSnapshot] = useState<AppSnapshot>(emptySnapshot);
  const [selectedTaskId, setSelectedTaskId] = useState<string>("");
  const [selectedWorkerId, setSelectedWorkerId] = useState<string>("");
  const [pendingTask, setPendingTask] = useState<TaskStartInput | null>(null);
  const [error, setError] = useState<string>("");
  const [connected, setConnected] = useState(false);
  const [retryingTaskId, setRetryingTaskId] = useState("");
  const [initialSnapshotStatus, setInitialSnapshotStatus] = useState<InitialSnapshotStatus>("loading");

  async function refresh() {
    const next = normalizeSnapshot(await getSnapshot());
    setSnapshot(next);
    setInitialSnapshotStatus("ready");
    setSelectedTaskId((current) => (next.tasks.some((task) => task.id === current) ? current : next.tasks.at(-1)?.id || ""));
  }

  useEffect(() => {
    refresh().catch((err: Error) => {
      setError(err.message);
      setInitialSnapshotStatus((current) => (current === "loading" ? "error" : current));
    });
  }, []);

  useEffect(() => {
    if (initialSnapshotStatus !== "ready") {
      return;
    }
    const lastID = snapshot.events.at(-1)?.id ?? 0;
    const source = new EventSource(`/api/events/stream?after=${lastID}`);
    source.addEventListener("open", () => setConnected(true));
    source.addEventListener("error", () => setConnected(false));
    source.addEventListener("event", (message) => {
      const event = JSON.parse((message as MessageEvent).data) as EventRecord;
      setSnapshot((current) => reduceEvent(current, event));
    });
    return () => source.close();
  }, [initialSnapshotStatus]);

  const selectedTask = useMemo(
    () => snapshot.tasks.find((task) => task.id === selectedTaskId) ?? snapshot.tasks.at(-1),
    [selectedTaskId, snapshot.tasks],
  );
  const selectedWorkers = snapshot.workers.filter((worker) => worker.taskId === selectedTask?.id);
  const selectedNodes = snapshot.executionNodes.filter((node) => node.taskId === selectedTask?.id);
  const selectedGraph = snapshot.orchestrationGraphs.find((graph) => graph.taskId === selectedTask?.id);
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

  async function handleRetryTask(taskId: string) {
    setRetryingTaskId(taskId);
    try {
      setError("");
      await retryTask(taskId);
      await refresh();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setRetryingTaskId("");
    }
  }

  const pluginPane: DashboardPane = {
    id: "plugins",
    title: "Plugins",
    element: (
      <PluginPanel
        plugins={snapshot.plugins}
        onRegister={async (plugin) => {
          setError("");
          await registerPlugin(plugin);
          await refresh();
        }}
        onUpdate={async (id, plugin) => {
          setError("");
          await updatePlugin(id, plugin);
          await refresh();
        }}
        onDelete={async (id) => {
          setError("");
          await deletePlugin(id);
          await refresh();
        }}
        onError={setError}
      />
    ),
  };
  const targetPanes: DashboardPane[] = snapshot.targets.length > 0
    ? [{ id: "targets", title: "Targets", element: <TargetPanel targets={snapshot.targets} /> }]
    : [];
  const taskPanes: DashboardPane[] = selectedTask
    ? [
        {
          id: "task-detail",
          title: "Task",
          element: <TaskDetail task={selectedTask} workers={selectedWorkers} nodes={selectedNodes} events={selectedEvents} onCancel={cancelTask} onRetry={handleRetryTask} onReview={getWorkerChanges} onApply={applyTaskResult} onApplied={refresh} onSteer={steerTask} retrying={retryingTaskId === selectedTask.id} onError={setError} />,
        },
        {
          id: "pull-requests",
          title: "Pull Requests",
          element: (
            <PullRequestPanel
              task={selectedTask}
              pullRequests={selectedPullRequests}
              onPublish={publishTaskPullRequest}
              onWatch={watchTaskPullRequests}
              onRefresh={refreshPullRequest}
              onBabysit={babysitPullRequest}
              onDone={refresh}
              onError={setError}
            />
          ),
        },
        {
          id: "current-state",
          title: "Current State",
          element: <WorkSummary progress={progress} nodes={selectedNodes} workers={selectedWorkers} />,
        },
        {
          id: "workers",
          title: "Orchestration",
          element: (
            <WorkerList
              workers={selectedWorkers}
              nodes={selectedNodes}
              graph={selectedGraph}
              progress={progress}
              task={selectedTask}
              events={selectedEvents}
              selectedWorkerId={selectedWorkerId}
              onSelect={setSelectedWorkerId}
              onReview={getWorkerChanges}
              onApply={applyWorkerChanges}
              onApplied={refresh}
              onCancel={cancelWorker}
              onError={setError}
            />
          ),
        },
        ...(selectedWorker
          ? [
              {
                id: "worker-detail" as const,
                title: "Worker Detail",
                element: <WorkerDetail worker={selectedWorker} node={selectedWorkerNode} events={selectedWorkerEvents} />,
              },
            ]
          : []),
        {
          id: "timeline",
          title: "Timeline",
          element: <EventLog events={selectedEvents} />,
        },
      ]
    : [];
  const dashboardPanes: DashboardPane[] = [...taskPanes, ...targetPanes, pluginPane];

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
              setSnapshot((current) => upsertTask(current, task));
              setSelectedTaskId(task.id);
              refresh().catch((err: Error) => setError(err.message));
              return task;
            }}
            onStartPending={(input) => {
              setError("");
              setPendingTask(input);
            }}
            onStartSettled={() => setPendingTask(null)}
            onError={setError}
            projects={snapshot.projects}
          />
          <ProjectPanel
            projects={snapshot.projects}
            onCreate={async (input) => {
              setError("");
              await createProject(input);
              await refresh();
            }}
            onUpdate={async (id, input) => {
              setError("");
              await updateProject(id, input);
              await refresh();
            }}
            onDelete={async (id) => {
              setError("");
              await deleteProject(id);
              await refresh();
            }}
            onHealth={getProjectHealth}
            onError={setError}
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
          {initialSnapshotStatus === "loading" ? (
            <TaskListLoading />
          ) : initialSnapshotStatus === "error" && snapshot.tasks.length === 0 && !pendingTask ? (
            <p className="empty">Unable to load tasks.</p>
          ) : snapshot.tasks.length === 0 && !pendingTask ? (
            <p className="empty">No tasks yet.</p>
          ) : (
            <>
              {snapshot.tasks.map((task) => (
                <div
                  key={task.id}
                  className={task.id === selectedTask?.id ? "task-row selected" : "task-row"}
                >
                  <button className="task-row-main" onClick={() => setSelectedTaskId(task.id)}>
                    <span>
                      <strong>{task.title}</strong>
                      <small>{task.id.slice(0, 8)}</small>
                    </span>
                    <span className="task-row-status">
                      <Status value={task.status} />
                      {task.objectivePhase && task.objectivePhase !== task.status && <span className="pill subtle">{humanizeKey(task.objectivePhase)}</span>}
                    </span>
                  </button>
                  <div className="task-row-actions">
                    {isRetryableTask(task) && (
                      <button className="icon-button ghost task-action" disabled={retryingTaskId === task.id} onClick={() => handleRetryTask(task.id)} title="Retry task">
                        <RefreshCw size={16} />
                      </button>
                    )}
                    {isTerminalTask(task) && (
                      <button className="icon-button ghost danger-text task-action" onClick={() => handleClearTask(task.id)} title="Clear task">
                        <Trash2 size={16} />
                      </button>
                    )}
                  </div>
                </div>
              ))}
              {pendingTask && <PendingTaskRow task={pendingTask} />}
            </>
          )}
        </section>

        {initialSnapshotStatus === "loading" ? (
          <section className="workspace">
            <div className="panel empty-state loading-state">
              <LoaderCircle className="spin" size={18} />
              <span>Loading tasks...</span>
            </div>
          </section>
        ) : dashboardPanes.length > 0 ? (
          <DashboardGrid panes={dashboardPanes} />
        ) : (
          <section className="workspace">
            <div className="panel empty-state">Create a task to start orchestration.</div>
          </section>
        )}
      </section>
    </main>
  );
}

function TaskListLoading() {
  return (
    <div className="task-list-loading" role="status" aria-live="polite">
      <LoaderCircle className="spin" size={16} />
      <span>Loading tasks...</span>
    </div>
  );
}

function DashboardGrid({ panes }: { panes: DashboardPane[] }) {
  const [layout, setLayout] = useDashboardLayout();
  const [draggingId, setDraggingId] = useState<DashboardPaneId | null>(null);
  const [dragOverId, setDragOverId] = useState<DashboardPaneId | null>(null);
  const [resizing, setResizing] = useState<{
    id: DashboardPaneId;
    startX: number;
    startY: number;
    startSpan: number;
    startMinHeight: number;
  } | null>(null);
  const gridRef = useRef<HTMLDivElement | null>(null);
  const paneById = useMemo(() => new Map(panes.map((pane) => [pane.id, pane])), [panes]);

  const orderedPanes = useMemo(() => {
    const ordered = layout.map((item) => paneById.get(item.id)).filter((pane): pane is DashboardPane => Boolean(pane));
    const orderedIds = new Set(ordered.map((pane) => pane.id));
    return [...ordered, ...panes.filter((pane) => !orderedIds.has(pane.id))];
  }, [layout, paneById, panes]);

  useEffect(() => {
    if (!resizing) return;
    const activeResize = resizing;
    function handlePointerMove(event: PointerEvent) {
      const gridWidth = gridRef.current?.getBoundingClientRect().width ?? 1200;
      const columnWidth = Math.max(64, gridWidth / DASHBOARD_MAX_SPAN);
      const nextSpan = clamp(activeResize.startSpan + Math.round((event.clientX - activeResize.startX) / columnWidth), DASHBOARD_MIN_SPAN, DASHBOARD_MAX_SPAN);
      const nextMinHeight = clamp(activeResize.startMinHeight + Math.round((event.clientY - activeResize.startY) / DASHBOARD_HEIGHT_STEP) * DASHBOARD_HEIGHT_STEP, DASHBOARD_MIN_HEIGHT, DASHBOARD_MAX_HEIGHT);
      updatePaneLayout(setLayout, activeResize.id, { span: nextSpan, minHeight: nextMinHeight });
    }
    function handlePointerUp() {
      setResizing(null);
    }
    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", handlePointerUp, { once: true });
    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", handlePointerUp);
    };
  }, [resizing, setLayout]);

  function movePane(sourceId: DashboardPaneId, targetId: DashboardPaneId) {
    if (sourceId === targetId) return;
    setLayout((current) => {
      const next = [...current];
      const from = next.findIndex((item) => item.id === sourceId);
      const to = next.findIndex((item) => item.id === targetId);
      if (from === -1 || to === -1) return current;
      const [item] = next.splice(from, 1);
      next.splice(to, 0, item);
      return next;
    });
  }

  function movePaneByOffset(sourceId: DashboardPaneId, offset: number) {
    setLayout((current) => {
      const next = [...current];
      const from = next.findIndex((item) => item.id === sourceId);
      if (from === -1) return current;
      const to = clamp(from + offset, 0, next.length - 1);
      if (from === to) return current;
      const [item] = next.splice(from, 1);
      next.splice(to, 0, item);
      return next;
    });
  }

  function paneLayout(id: DashboardPaneId): DashboardPaneLayout {
    return layout.find((item) => item.id === id) ?? DEFAULT_DASHBOARD_LAYOUT.find((item) => item.id === id)!;
  }

  return (
    <section className="workspace" aria-label="Dashboard panes">
      <div className="dashboard-toolbar">
        <span>{orderedPanes.length} panes</span>
        <button className="icon-button ghost" onClick={() => setLayout(defaultDashboardLayout())} title="Reset dashboard layout">
          <RotateCcw size={16} />
        </button>
      </div>
      <div className="dashboard-grid" ref={gridRef}>
        {orderedPanes.map((pane) => {
          const item = paneLayout(pane.id);
          return (
            <div
              key={pane.id}
              className={[
                "dashboard-pane",
                draggingId === pane.id ? "dragging" : "",
                dragOverId === pane.id && draggingId !== pane.id ? "drag-over" : "",
                resizing?.id === pane.id ? "resizing" : "",
              ].filter(Boolean).join(" ")}
              style={
                {
                  "--pane-span": item.span,
                  "--pane-min-height": `${item.minHeight}px`,
                } as React.CSSProperties
              }
              onDragOver={(event) => {
                event.preventDefault();
                setDragOverId(pane.id);
              }}
              onDragLeave={() => setDragOverId((current) => (current === pane.id ? null : current))}
              onDrop={(event) => {
                event.preventDefault();
                const sourceId = event.dataTransfer.getData("text/plain") as DashboardPaneId;
                movePane(sourceId, pane.id);
                setDraggingId(null);
                setDragOverId(null);
              }}
            >
              <div className="dashboard-pane-chrome">
                <div
                  className="dashboard-pane-grip"
                  draggable
                  title={`Drag ${pane.title}`}
                  aria-label={`Drag ${pane.title}`}
                  role="button"
                  tabIndex={0}
                  onDragStart={(event) => {
                    event.dataTransfer.effectAllowed = "move";
                    event.dataTransfer.setData("text/plain", pane.id);
                    setDraggingId(pane.id);
                  }}
                  onDragEnd={() => {
                    setDraggingId(null);
                    setDragOverId(null);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "ArrowUp" || event.key === "ArrowLeft") {
                      event.preventDefault();
                      movePaneByOffset(pane.id, -1);
                    }
                    if (event.key === "ArrowDown" || event.key === "ArrowRight") {
                      event.preventDefault();
                      movePaneByOffset(pane.id, 1);
                    }
                  }}
                >
                  <GripVertical size={16} />
                  <span>{pane.title}</span>
                </div>
                <div className="dashboard-pane-actions">
                  <button className="icon-button ghost" onClick={() => updatePaneLayout(setLayout, pane.id, { span: item.span - 2 })} disabled={item.span <= DASHBOARD_MIN_SPAN} title={`Make ${pane.title} narrower`}>
                    <Minimize2 size={14} />
                  </button>
                  <button className="icon-button ghost" onClick={() => updatePaneLayout(setLayout, pane.id, { span: item.span + 2 })} disabled={item.span >= DASHBOARD_MAX_SPAN} title={`Make ${pane.title} wider`}>
                    <Maximize2 size={14} />
                  </button>
                </div>
              </div>
              {pane.element}
              <button
                className="dashboard-pane-resize"
                aria-label={`Resize ${pane.title}`}
                title={`Resize ${pane.title}`}
                onPointerDown={(event) => {
                  event.preventDefault();
                  setResizing({
                    id: pane.id,
                    startX: event.clientX,
                    startY: event.clientY,
                    startSpan: item.span,
                    startMinHeight: item.minHeight,
                  });
                }}
              />
            </div>
          );
        })}
      </div>
    </section>
  );
}

function useDashboardLayout() {
  const [layout, setLayout] = useState<DashboardPaneLayout[]>(() => {
    if (typeof window === "undefined") return defaultDashboardLayout();
    try {
      return normalizeDashboardLayout(JSON.parse(window.localStorage.getItem(DASHBOARD_LAYOUT_STORAGE_KEY) || "null"));
    } catch {
      return defaultDashboardLayout();
    }
  });

  useEffect(() => {
    try {
      window.localStorage.setItem(DASHBOARD_LAYOUT_STORAGE_KEY, JSON.stringify(layout));
    } catch {
      // Browsers can reject storage in privacy modes; layout customization still works for the session.
    }
  }, [layout]);

  return [layout, setLayout] as const;
}

function normalizeDashboardLayout(value: unknown): DashboardPaneLayout[] {
  if (!Array.isArray(value)) return defaultDashboardLayout();
  const defaults = new Map(defaultDashboardLayout().map((item) => [item.id, item]));
  const normalized: DashboardPaneLayout[] = [];
  for (const entry of value) {
    if (!isRecord(entry) || typeof entry.id !== "string" || !defaults.has(entry.id as DashboardPaneId)) continue;
    const defaultsForPane = defaults.get(entry.id as DashboardPaneId)!;
    normalized.push({
      id: entry.id as DashboardPaneId,
      span: clampNumber(entry.span, defaultsForPane.span, DASHBOARD_MIN_SPAN, DASHBOARD_MAX_SPAN),
      minHeight: clampNumber(entry.minHeight, defaultsForPane.minHeight, DASHBOARD_MIN_HEIGHT, DASHBOARD_MAX_HEIGHT),
    });
    defaults.delete(entry.id as DashboardPaneId);
  }
  return [...normalized, ...defaults.values()];
}

function updatePaneLayout(
  setLayout: React.Dispatch<React.SetStateAction<DashboardPaneLayout[]>>,
  id: DashboardPaneId,
  values: Partial<Pick<DashboardPaneLayout, "span" | "minHeight">>,
) {
  setLayout((current) =>
    current.map((item) =>
      item.id === id
        ? {
            ...item,
            span: values.span === undefined ? item.span : clamp(values.span, DASHBOARD_MIN_SPAN, DASHBOARD_MAX_SPAN),
            minHeight: values.minHeight === undefined ? item.minHeight : clamp(values.minHeight, DASHBOARD_MIN_HEIGHT, DASHBOARD_MAX_HEIGHT),
          }
        : item,
    ),
  );
}

function defaultDashboardLayout(): DashboardPaneLayout[] {
  return DEFAULT_DASHBOARD_LAYOUT.map((item) => ({ ...item }));
}

function clampNumber(value: unknown, fallback: number, min: number, max: number): number {
  return typeof value === "number" && Number.isFinite(value) ? clamp(value, min, max) : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
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

function isRetryableTask(task: Task): boolean {
  return task.status === "failed" || task.status === "canceled";
}

function canPublishPullRequest(task: Task): boolean {
  return isTerminalTask(task) || Boolean(task.finalCandidateWorkerId);
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
  onStartPending,
  onStartSettled,
  onError,
  projects,
}: {
  onCreate: (input: TaskStartInput) => Promise<Task>;
  onStartPending: (input: TaskStartInput) => void;
  onStartSettled: () => void;
  onError: (message: string) => void;
  projects: Project[];
}) {
  const [projectId, setProjectId] = useState("");
  const [title, setTitle] = useState("");
  const [prompt, setPrompt] = useState("");
  const [completionMode, setCompletionMode] = useState<"local" | "github">("local");
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const input = { projectId: projectId || undefined, title, prompt, metadata: { completionMode } };
    setBusy(true);
    onStartPending(input);
    try {
      await onCreate(input);
      setTitle("");
      setPrompt("");
      setCompletionMode("local");
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setBusy(false);
      onStartSettled();
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
      <label>
        Completion
        <select value={completionMode} onChange={(event) => setCompletionMode(event.target.value as "local" | "github")}>
          <option value="local">Local: review diff here and apply result</option>
          <option value="github">GitHub: open PR when complete</option>
        </select>
      </label>
      <button className={busy ? "primary is-busy" : "primary"} disabled={busy} aria-busy={busy}>
        {busy ? <LoaderCircle className="spin" size={16} /> : <Play size={16} />}
        {busy ? "Starting task" : "Start"}
      </button>
      {busy && (
        <div className="task-start-progress" role="status" aria-live="polite">
          <LoaderCircle className="spin" size={16} />
          <span>Scheduling task and waiting for the first status event...</span>
        </div>
      )}
    </form>
  );
}

function PendingTaskRow({ task }: { task: TaskStartInput }) {
  const title = task.title.trim() || "Generating task title...";
  return (
    <div className="task-row pending-start" aria-busy="true" aria-live="polite">
      <div className="task-row-main pending-task-main">
        <span>
          <strong>{title}</strong>
          <small>Start request in progress</small>
        </span>
        <span className="status starting">
          <LoaderCircle className="spin" size={12} />
          Starting
        </span>
      </div>
    </div>
  );
}

function ProjectPanel({
  projects,
  onCreate,
  onUpdate,
  onDelete,
  onHealth,
  onError,
}: {
  projects: Project[];
  onCreate: (input: { id: string; name?: string; localPath: string; repo?: string; upstreamRepo?: string; headRepoOwner?: string; pushRemote?: string; vcs?: string; defaultBase?: string; workspaceRoot?: string }) => Promise<void>;
  onUpdate: (id: string, input: { id: string; name?: string; localPath: string; repo?: string; upstreamRepo?: string; headRepoOwner?: string; pushRemote?: string; vcs?: string; defaultBase?: string; workspaceRoot?: string }) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  onHealth: (id: string) => Promise<ProjectHealth>;
  onError: (message: string) => void;
}) {
  const [editingId, setEditingId] = useState("");
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [localPath, setLocalPath] = useState("");
  const [repo, setRepo] = useState("");
  const [upstreamRepo, setUpstreamRepo] = useState("");
  const [headRepoOwner, setHeadRepoOwner] = useState("");
  const [pushRemote, setPushRemote] = useState("");
  const [defaultBase, setDefaultBase] = useState("main");
  const [branchPrefix, setBranchPrefix] = useState("codex/aged-");
  const [draftPRs, setDraftPRs] = useState(false);
  const [allowMerge, setAllowMerge] = useState(false);
  const [autoMerge, setAutoMerge] = useState(false);
  const [busy, setBusy] = useState(false);
  const [health, setHealth] = useState<Record<string, ProjectHealth>>({});
  const [healthBusy, setHealthBusy] = useState("");

  function loadProject(project: Project) {
    setEditingId(project.id);
    setId(project.id);
    setName(project.name ?? "");
    setLocalPath(project.localPath);
    setRepo(project.repo ?? "");
    setUpstreamRepo(project.upstreamRepo ?? "");
    setHeadRepoOwner(project.headRepoOwner ?? "");
    setPushRemote(project.pushRemote ?? "");
    setDefaultBase(project.defaultBase ?? "main");
    setBranchPrefix(project.pullRequestPolicy?.branchPrefix ?? "codex/aged-");
    setDraftPRs(Boolean(project.pullRequestPolicy?.draft));
    setAllowMerge(Boolean(project.pullRequestPolicy?.allowMerge));
    setAutoMerge(Boolean(project.pullRequestPolicy?.autoMerge));
  }

  function resetForm() {
    setEditingId("");
    setId("");
    setName("");
    setLocalPath("");
    setRepo("");
    setUpstreamRepo("");
    setHeadRepoOwner("");
    setPushRemote("");
    setDefaultBase("main");
    setBranchPrefix("codex/aged-");
    setDraftPRs(false);
    setAllowMerge(false);
    setAutoMerge(false);
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      const input = {
        id,
        name: name || undefined,
        localPath,
        repo: repo || undefined,
        upstreamRepo: upstreamRepo || undefined,
        headRepoOwner: headRepoOwner || undefined,
        pushRemote: pushRemote || undefined,
        vcs: "auto",
        defaultBase: defaultBase || undefined,
        pullRequestPolicy: {
          branchPrefix: branchPrefix || undefined,
          draft: draftPRs,
          allowMerge,
          autoMerge,
        },
      };
      if (editingId) {
        await onUpdate(editingId, input);
      } else {
        await onCreate(input);
      }
      resetForm();
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function checkHealth(projectId: string) {
    setHealthBusy(projectId);
    try {
      const result = await onHealth(projectId);
      setHealth((current) => ({ ...current, [projectId]: result }));
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setHealthBusy("");
    }
  }

  async function removeProject(projectId: string) {
    try {
      await onDelete(projectId);
      if (editingId === projectId) resetForm();
    } catch (err) {
      onError((err as Error).message);
    }
  }

  return (
    <section className="panel project-panel">
      <div className="panel-title">
        <FolderPlus size={18} />
        <h2>Projects</h2>
      </div>
      <div className="project-list">
        {projects.map((project) => (
          <div className="project-chip" key={project.id}>
            <strong>{project.name}</strong>
            <span>{project.id}</span>
            <small>{project.localPath}</small>
            <div className="project-chip-actions">
              <button className="secondary compact" onClick={() => loadProject(project)} type="button">Edit</button>
              <button className="secondary compact" disabled={healthBusy === project.id} onClick={() => checkHealth(project.id)} type="button">
                {healthBusy === project.id ? "Checking" : "Health"}
              </button>
              <button className="secondary compact danger-text" onClick={() => removeProject(project.id)} type="button">Delete</button>
            </div>
            {health[project.id] && <ProjectHealthSummary health={health[project.id]} />}
          </div>
        ))}
      </div>
      <form className="project-form" onSubmit={submit}>
        <label>
          ID
          <input value={id} onChange={(event) => setId(event.target.value)} placeholder="nodejs" required disabled={Boolean(editingId)} />
        </label>
        <label>
          Name
          <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Node.js" />
        </label>
        <label>
          Local path
          <input value={localPath} onChange={(event) => setLocalPath(event.target.value)} placeholder="/Users/nathanwhit/Documents/Code/node" required />
        </label>
        <label>
          Repo
          <input value={repo} onChange={(event) => setRepo(event.target.value)} placeholder="fork-owner/repo" />
        </label>
        <label>
          Upstream repo
          <input value={upstreamRepo} onChange={(event) => setUpstreamRepo(event.target.value)} placeholder="owner/repo" />
        </label>
        <label>
          Head owner
          <input value={headRepoOwner} onChange={(event) => setHeadRepoOwner(event.target.value)} placeholder="fork-owner" />
        </label>
        <label>
          Push remote
          <input value={pushRemote} onChange={(event) => setPushRemote(event.target.value)} placeholder="origin" />
        </label>
        <label>
          Base
          <input value={defaultBase} onChange={(event) => setDefaultBase(event.target.value)} placeholder="main" />
        </label>
        <label>
          PR branch prefix
          <input value={branchPrefix} onChange={(event) => setBranchPrefix(event.target.value)} placeholder="codex/aged-" />
        </label>
        <label className="checkbox-label">
          <input type="checkbox" checked={draftPRs} onChange={(event) => setDraftPRs(event.target.checked)} />
          Draft PRs by default
        </label>
        <label className="checkbox-label">
          <input type="checkbox" checked={allowMerge} onChange={(event) => setAllowMerge(event.target.checked)} />
          Allow aged to merge
        </label>
        <label className="checkbox-label">
          <input type="checkbox" checked={autoMerge} onChange={(event) => setAutoMerge(event.target.checked)} />
          Auto-merge when policy allows
        </label>
        <button disabled={busy}>
          <FolderPlus size={16} />
          {busy ? "Saving" : editingId ? "Save Project" : "Add Project"}
        </button>
        {editingId && <button type="button" className="secondary" onClick={resetForm}>Cancel Edit</button>}
      </form>
    </section>
  );
}

function ProjectHealthSummary({ health }: { health: ProjectHealth }) {
  return (
    <div className={health.ok ? "project-health ok" : "project-health issue"}>
      <strong>{health.ok ? "Healthy" : "Needs attention"}</strong>
      <span>path {health.pathStatus}</span>
      <span>vcs {health.vcsStatus}{health.detectedVcs ? `:${health.detectedVcs}` : ""}</span>
      {health.githubStatus && <span>github {health.githubStatus}</span>}
      {health.defaultBaseStatus && <span>base {health.defaultBaseStatus}{health.detectedBase ? `:${health.detectedBase}` : ""}</span>}
      {health.targetStatus && <span>target {health.targetStatus}</span>}
      {(health.errors ?? []).slice(0, 2).map((error) => <small key={error}>{error}</small>)}
    </div>
  );
}

function AssistantPanel({ onError }: { onError: (message: string) => void }) {
  const [message, setMessage] = useState("");
  const [conversationId, setConversationId] = useState("");
  const [answer, setAnswer] = useState("");
  const [busy, setBusy] = useState(false);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);

  useEffect(() => {
    if (!busy) {
      setElapsedSeconds(0);
      return;
    }
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      setElapsedSeconds(Math.max(1, Math.floor((Date.now() - startedAt) / 1000)));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [busy]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setAnswer("");
    onError("");
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
    <form className="panel assistant-panel" onSubmit={submit} aria-busy={busy}>
      <div className="panel-title">
        <Bot size={18} />
        <h2>Ask</h2>
      </div>
      <textarea
        value={message}
        onChange={(event) => setMessage(event.target.value)}
        placeholder="Ask about the system or repo..."
        disabled={busy}
        aria-describedby={busy ? "assistant-progress" : undefined}
        required
      />
      <button className="secondary" disabled={busy}>
        {busy ? <LoaderCircle className="spin" size={16} /> : <Send size={16} />}
        {busy ? "Asking" : "Ask"}
      </button>
      {busy && (
        <div className="assistant-progress">
          <div id="assistant-progress" className="assistant-progress-status" role="status" aria-live="polite">
            <LoaderCircle className="spin" size={16} aria-hidden="true" />
            <span>{assistantProgressLabel(elapsedSeconds)}</span>
          </div>
          {elapsedSeconds > 0 && <small aria-hidden="true">{elapsedSeconds}s elapsed</small>}
        </div>
      )}
      {answer && <pre className="assistant-answer">{answer}</pre>}
    </form>
  );
}

function assistantProgressLabel(elapsedSeconds: number): string {
  if (elapsedSeconds < 2) return "Sending question...";
  if (elapsedSeconds < 8) return "Waiting for assistant response...";
  return "Still working on the answer...";
}

function TaskDetail({
  task,
  workers,
  nodes,
  events,
  onCancel,
  onRetry,
  onReview,
  onApply,
  onApplied,
  onSteer,
  retrying,
  onError,
}: {
  task: Task;
  workers: Worker[];
  nodes: ExecutionNode[];
  events: EventRecord[];
  onCancel: (id: string) => Promise<void>;
  onRetry: (id: string) => Promise<void>;
  onReview: (id: string) => Promise<WorkerChangesReview>;
  onApply: (id: string) => Promise<void>;
  onApplied: () => Promise<void>;
  onSteer: (id: string, message: string) => Promise<void>;
  retrying: boolean;
  onError: (message: string) => void;
}) {
  const [message, setMessage] = useState("");
  const [applying, setApplying] = useState(false);
  const [diff, setDiff] = useState<DiffReviewState | undefined>();
  const completionMode = String(task.metadata?.completionMode ?? "local");
  const finalWorkerId = task.finalCandidateWorkerId ?? "";
  const finalWorkerApplied = finalWorkerId !== "" && (task.appliedWorkerId === finalWorkerId || workerChangesApplied(events, finalWorkerId));
  const canApplyResult = completionMode !== "github" && isTerminalTask(task) && finalWorkerId !== "" && !finalWorkerApplied;
  const finalCompletion = finalWorkerId ? latestWorkerCompletion(events, finalWorkerId) : {};
  const finalChangedFiles = finalCompletion.changedFiles ?? finalCompletion.workspaceChanges?.changedFiles ?? [];
  const workerUpdate = currentWorkerUpdate(workers, nodes, events);
  const approvals = approvalStates(events);

  useEffect(() => {
    setDiff(undefined);
  }, [finalWorkerId]);

  async function steer(event: React.FormEvent) {
    event.preventDefault();
    try {
      await onSteer(task.id, message);
      setMessage("");
    } catch (err) {
      onError((err as Error).message);
    }
  }

  async function applyResult() {
    setApplying(true);
    try {
      await onApply(task.id);
      await onApplied();
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setApplying(false);
    }
  }

  async function toggleFinalDiff() {
    if (!finalWorkerId) return;
    if (diff?.open) {
      setDiff({ ...diff, open: false });
      return;
    }
    if (diff?.loaded) {
      setDiff({ ...diff, open: true });
      return;
    }
    setDiff({ open: true, loading: true, loaded: false, diff: "" });
    try {
      const review = await onReview(finalWorkerId);
      setDiff({
        open: true,
        loading: false,
        loaded: true,
        diff: review.changes.diff ?? "",
        error: review.changes.error,
      });
    } catch (err) {
      const error = (err as Error).message;
      setDiff({ open: true, loading: false, loaded: true, diff: "", error });
      onError(error);
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
          {task.objectiveStatus && <Status value={task.objectiveStatus} />}
          {task.objectivePhase && <span className="pill">{humanizeKey(task.objectivePhase)}</span>}
          {completionMode === "github" && <span className="pill">GitHub mode</span>}
          {canApplyResult && (
            <button className="primary compact" disabled={applying} onClick={applyResult} title="Apply final task result locally">
              <Check size={16} />
              {applying ? "Applying" : "Apply Result"}
            </button>
          )}
          {(task.appliedWorkerId || finalWorkerApplied) && <span className="pill">Applied</span>}
          {isRetryableTask(task) && (
            <button className="icon-button ghost" disabled={retrying} onClick={() => onRetry(task.id)} title="Retry task">
              <RefreshCw size={18} />
            </button>
          )}
          <button className="icon-button danger" onClick={() => onCancel(task.id).catch((err: Error) => onError(err.message))} title="Cancel task">
            <CircleStop size={18} />
          </button>
        </div>
      </div>
      {(task.artifacts?.length || task.milestones?.length) && (
        <TaskObjectiveStrip task={task} />
      )}
      {approvals.length > 0 && <ApprovalPanel approvals={approvals} onUseMessage={setMessage} />}
      <WorkerProgressSpotlight update={workerUpdate} />
      <form className="steer" onSubmit={steer}>
        <input value={message} onChange={(event) => setMessage(event.target.value)} placeholder="Steer this task..." required />
        <button className="icon-button" title="Send steering">
          <Send size={18} />
        </button>
      </form>
      {finalWorkerId && finalChangedFiles.length > 0 && (
        <div className="worker-review final-result-review">
          <details>
            <summary>{finalChangedFiles.length} changed files</summary>
            <ul>
              {finalChangedFiles.slice(0, 8).map((file) => (
                <li key={`${file.status}-${file.path}`}>
                  <code>{file.status ?? "changed"}</code>
                  <span>{file.path}</span>
                </li>
              ))}
            </ul>
          </details>
          <div className="worker-review-actions">
            <button className="secondary compact" disabled={diff?.loading} onClick={toggleFinalDiff} title={diff?.open ? "Hide final result diff" : "Show final result diff"}>
              <FileText size={16} />
              {diff?.loading ? "Loading" : diff?.open ? "Hide Diff" : "Diff"}
            </button>
          </div>
          {diff?.open && <DiffViewer state={diff} />}
        </div>
      )}
    </section>
  );
}

function TaskObjectiveStrip({ task }: { task: Task }) {
  const artifacts = task.artifacts ?? [];
  const milestones = task.milestones ?? [];
  const latestMilestone = milestones[milestones.length - 1];
  return (
    <section className="objective-strip">
      {latestMilestone && (
        <div className="objective-item">
          <small>Latest milestone</small>
          <strong>{humanizeKey(latestMilestone.name)}</strong>
          {latestMilestone.summary && <span>{latestMilestone.summary}</span>}
        </div>
      )}
      {artifacts.slice(-3).map((artifact) => (
        <div key={artifact.id || `${artifact.kind}:${artifact.ref}`} className="objective-item">
          <small>{humanizeKey(artifact.kind)}</small>
          {artifact.url ? (
            <a href={artifact.url} target="_blank" rel="noreferrer">
              {artifact.name || artifact.ref || artifact.url}
            </a>
          ) : (
            <strong>{artifact.name || artifact.ref || artifact.id}</strong>
          )}
          {artifact.ref && <span>{artifact.ref}</span>}
        </div>
      ))}
    </section>
  );
}

type ApprovalState = {
  id: number;
  at: string;
  question: string;
  reason: string;
  workerId?: string;
  decided: boolean;
  answer?: string;
};

function ApprovalPanel({ approvals, onUseMessage }: { approvals: ApprovalState[]; onUseMessage: (message: string) => void }) {
  return (
    <section className="approval-panel">
      <div className="approval-title">
        <strong>Approvals</strong>
        <span>{approvals.filter((approval) => !approval.decided).length} pending</span>
      </div>
      {approvals.slice(0, 4).map((approval) => (
        <div className={approval.decided ? "approval-card decided" : "approval-card"} key={approval.id}>
          <div>
            <small>{new Date(approval.at).toLocaleTimeString()} · {humanizeKey(approval.reason || "approval")}</small>
            <p>{approval.question}</p>
            {approval.answer && <span>Answer: {approval.answer}</span>}
          </div>
          {!approval.decided && (
            <button className="secondary compact" type="button" onClick={() => onUseMessage(`Answer approval: ${approval.question}\n\n`)}>
              Respond
            </button>
          )}
        </div>
      ))}
    </section>
  );
}

type WorkerProgressUpdate = {
  workerId: string;
  title: string;
  status: WorkerStatus;
  at?: string;
  label?: string;
  text: string;
  source: "output" | "lifecycle" | "waiting";
};

function WorkerProgressSpotlight({ update }: { update: WorkerProgressUpdate | undefined }) {
  if (!update) {
    return null;
  }
  return (
    <section className={`worker-progress-spotlight ${update.source}`} aria-live="polite">
      <div className="worker-progress-heading">
        <div>
          <span className="worker-progress-eyebrow">Active worker update</span>
          <strong>{update.title}</strong>
        </div>
        <div className="worker-progress-meta">
          <Status value={update.status} />
          {update.label && <span className="pill">{update.label}</span>}
          {update.at && <time>{new Date(update.at).toLocaleTimeString()}</time>}
        </div>
      </div>
      <p>{update.text}</p>
      <small title="Worker output may be a progress summary, log message, tool event, or final message rather than private model thinking.">
        {update.source === "output"
          ? "Latest worker output."
          : update.source === "waiting"
            ? "Waiting for worker output."
            : "Worker lifecycle update."}
      </small>
    </section>
  );
}

function PullRequestPanel({
  task,
  pullRequests,
  onPublish,
  onWatch,
  onRefresh,
  onBabysit,
  onDone,
  onError,
}: {
  task: Task;
  pullRequests: PullRequestState[];
  onPublish: (taskId: string) => Promise<PullRequestState>;
  onWatch: (taskId: string, input: WatchPullRequestsInput) => Promise<PullRequestState[]>;
  onRefresh: (id: string) => Promise<PullRequestState>;
  onBabysit: (id: string) => Promise<unknown>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
  const [busy, setBusy] = useState("");
  const [watchRepo, setWatchRepo] = useState("");
  const [watchNumber, setWatchNumber] = useState("");
  const [watchUrl, setWatchUrl] = useState("");
  const canPublish = canPublishPullRequest(task) && pullRequests.length === 0;

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
      <form
        className="pr-watch-form"
        onSubmit={(event) => {
          event.preventDefault();
          const input: WatchPullRequestsInput = {
            repo: watchRepo.trim() || undefined,
            url: watchUrl.trim() || undefined,
            number: watchNumber.trim() ? Number(watchNumber) : undefined,
            state: "open",
          };
          run("watch", () => onWatch(task.id, input));
        }}
      >
        <input value={watchRepo} onChange={(event) => setWatchRepo(event.target.value)} placeholder="owner/repo" />
        <input value={watchNumber} onChange={(event) => setWatchNumber(event.target.value)} placeholder="PR #" inputMode="numeric" />
        <input value={watchUrl} onChange={(event) => setWatchUrl(event.target.value)} placeholder="or PR URL" />
        <button className="secondary compact" disabled={busy === "watch" || (!watchRepo.trim() && !watchUrl.trim())}>
          <Eye size={16} />
          {busy === "watch" ? "Watching" : "Watch Existing"}
        </button>
      </form>
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
  task,
  workers,
  nodes,
  graph,
  progress,
  events,
  selectedWorkerId,
  onSelect,
  onReview,
  onApply,
  onApplied,
  onCancel,
  onError,
}: {
  task: Task;
  workers: Worker[];
  nodes: ExecutionNode[];
  graph: OrchestrationGraph | undefined;
  progress: WorkProgress;
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
    <section className="panel orchestration-panel">
      <div className="panel-title split-title">
        <span>
          <Bot size={18} />
          <h2>Orchestration</h2>
        </span>
        <span className="pill">{workers.length || nodes.length} workers</span>
      </div>
      <OrchestrationOverview progress={progress} graph={graph} nodes={nodes} workers={workers} />
      {workers.length === 0 && nodes.length === 0 ? (
        <p className="empty">No workers have been spawned.</p>
      ) : (
        <div className="worker-grid">
          {orchestrationRows(workers, nodes, graph).map(({ worker, node, graphNode }) => {
            const rowId = worker?.id ?? node?.id ?? graphNode?.id ?? "";
            const status = worker?.status ?? node?.status ?? graphNode?.status ?? "queued";
            const workerId = worker?.id ?? node?.workerId ?? graphNode?.workerId ?? "";
            const kind = worker?.kind ?? node?.workerKind ?? graphNode?.workerKind ?? "worker";
            const completion = workerId ? latestWorkerCompletion(events, workerId) : {};
            const changes = completion.changedFiles ?? completion.workspaceChanges?.changedFiles ?? [];
            const applied = workerId ? workerChangesApplied(events, workerId) : false;
            const isFinalCandidate = task.finalCandidateWorkerId === workerId;
            const workerEvents = workerId ? events.filter((event) => event.workerId === workerId) : [];
            const latestEvent = latestWorkerProgressEvent(workerEvents) ?? latestInspectableWorkerEvent(workerEvents);
            const diff = workerId ? diffs[workerId] : undefined;
            const dependencies = node?.dependsOn ?? graph?.edges.filter((edge) => edge.to === (graphNode?.id ?? node?.id)).map((edge) => edge.from) ?? [];
            const blockers = dependencies.filter((dependencyId) => {
              const dependency = nodes.find((candidate) => candidate.id === dependencyId) ?? graph?.nodes.find((candidate) => candidate.id === dependencyId);
              return dependency && dependency.status !== "succeeded";
            });
            const duration = worker ? formatDuration(worker.createdAt, worker.updatedAt) : node ? formatDuration(node.createdAt, node.updatedAt) : "";
            return (
              <article key={rowId} className={workerId === selectedWorkerId ? "worker-card selected" : "worker-card"}>
                <div>
                  <strong>{node?.role || graphNode?.role || kind}</strong>
                  <small>{workerId ? workerId.slice(0, 8) : rowId.slice(0, 8)}</small>
                </div>
                <Status value={status} />
                <button className="icon-button ghost" disabled={!workerId} onClick={() => onSelect(workerId)} title="Inspect worker">
                  <Eye size={16} />
                </button>
                <button className="icon-button danger" disabled={!workerId || isTerminalWorkerStatus(status)} onClick={() => onCancel(workerId).catch((err: Error) => onError(err.message))} title="Cancel worker">
                  <CircleStop size={16} />
                </button>
                <div className="worker-context">
                  <WorkerContextItem label="Kind" value={kind} />
                  <WorkerContextItem label="Node" value={node?.id.slice(0, 8) ?? graphNode?.id.slice(0, 8) ?? "none"} />
                  <WorkerContextItem label="Target" value={targetLabel(node, graphNode)} />
                  <WorkerContextItem label="Updated" value={worker ? new Date(worker.updatedAt).toLocaleTimeString() : node ? new Date(node.updatedAt).toLocaleTimeString() : ""} />
                  {duration && <WorkerContextItem label="Duration" value={duration} />}
                  {node?.spawnId || graphNode?.spawnId ? <WorkerContextItem label="Spawn" value={node?.spawnId ?? graphNode?.spawnId ?? ""} /> : null}
                </div>
                {(dependencies.length > 0 || blockers.length > 0 || node?.reason || graphNode?.reason) && (
                  <div className="worker-graph-context">
                    {dependencies.length > 0 && <span>Depends on {dependencies.map((id) => id.slice(0, 8)).join(", ")}</span>}
                    {blockers.length > 0 && <span className="warning">Blocked by {blockers.map((id) => id.slice(0, 8)).join(", ")}</span>}
                    {(node?.reason || graphNode?.reason) && <p>{node?.reason ?? graphNode?.reason}</p>}
                  </div>
                )}
                <div className="worker-current">
                  <span>Latest</span>
                  <p>{latestEvent ? eventDisplayText(latestEvent) : "No worker events yet."}</p>
                </div>
                <WorkerActivity events={workerEvents} defaultOpen={status === "failed"} />
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
                      <button className="secondary compact" disabled={!workerId || diff?.loading} onClick={() => toggleDiff(workerId)} title={diff?.open ? "Hide worker diff" : "Show worker diff"}>
                        <FileText size={16} />
                        {diff?.loading ? "Loading" : diff?.open ? "Hide Diff" : "Diff"}
                      </button>
                      <button className="secondary compact" disabled={!workerId || applied || applying === workerId || isFinalCandidate} onClick={() => apply(workerId)} title={isFinalCandidate ? "Use Apply Result on the task" : applied ? "Worker changes already applied" : "Manual worker apply"}>
                        <Check size={16} />
                        {isFinalCandidate ? "Final" : applied ? "Applied" : applying === workerId ? "Applying" : "Manual Apply"}
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

function OrchestrationOverview({
  progress,
  graph,
  nodes,
  workers,
}: {
  progress: WorkProgress;
  graph: OrchestrationGraph | undefined;
  nodes: ExecutionNode[];
  workers: Worker[];
}) {
  const edgeCount = graph?.edges.length ?? nodes.reduce((total, node) => total + (node.dependsOn?.length ?? 0), 0);
  const failed = graph ? graph.summary.failed + graph.summary.canceled : progress.failed;
  const waiting = graph ? graph.summary.waiting : progress.waiting;
  const running = graph ? graph.summary.running : progress.running;
  return (
    <div className="orchestration-overview">
      <div className="summary-grid compact">
        <Metric label="Progress" value={`${progress.percent}%`} />
        <Metric label="Done" value={`${progress.done}/${progress.total}`} />
        <Metric label="Running" value={String(running)} />
        <Metric label="Waiting" value={String(waiting)} />
        <Metric label="Failed" value={String(failed)} />
      </div>
      <div className="progress-track" aria-label={`Progress ${progress.percent}%`}>
        <div style={{ width: `${progress.percent}%` }} />
      </div>
      <div className="orchestration-meta">
        <span>{nodes.length || workers.length} execution nodes</span>
        <span>{edgeCount} dependencies</span>
        {graph?.updatedAt && <span>Updated {new Date(graph.updatedAt).toLocaleTimeString()}</span>}
      </div>
    </div>
  );
}

type OrchestrationRow = {
  worker?: Worker;
  node?: ExecutionNode;
  graphNode?: OrchestrationGraph["nodes"][number];
};

function orchestrationRows(workers: Worker[], nodes: ExecutionNode[], graph: OrchestrationGraph | undefined): OrchestrationRow[] {
  const rows = new Map<string, OrchestrationRow>();
  for (const node of nodes) {
    rows.set(node.workerId ?? node.id, { node });
  }
  for (const graphNode of graph?.nodes ?? []) {
    const key = graphNode.workerId ?? graphNode.id;
    rows.set(key, { ...rows.get(key), graphNode });
  }
  for (const worker of workers) {
    rows.set(worker.id, { ...rows.get(worker.id), worker });
  }
  return [...rows.values()];
}

function WorkerContextItem({ label, value }: { label: string; value: string }) {
  if (!value) return null;
  return (
    <span>
      <strong>{label}</strong>
      {value}
    </span>
  );
}

function targetLabel(node: ExecutionNode | undefined, graphNode: OrchestrationGraph["nodes"][number] | undefined): string {
  const targetId = node?.targetId ?? graphNode?.targetId;
  if (!targetId) return "local";
  return `${node?.targetKind ?? graphNode?.targetKind ?? "target"}:${targetId}`;
}

function isTerminalWorkerStatus(status: Worker["status"]): boolean {
  return status === "succeeded" || status === "failed" || status === "canceled";
}

function formatDuration(start: string, end: string): string {
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return "";
  const seconds = Math.max(0, Math.round((endMs - startMs) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const minuteRemainder = minutes % 60;
  return minuteRemainder ? `${hours}h ${minuteRemainder}m` : `${hours}h`;
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
  const recent = inspectable.slice(-6).reverse();
  const command = events.find((event) => event.type === "worker.created");
  const latest = recent[0];
  return (
    <details className="worker-activity" open={defaultOpen}>
      <summary>{inspectable.length} events{latest ? `, latest: ${eventDisplayText(latest)}` : ""}</summary>
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
  const label = workerEventLabel(event);
  const display = eventDisplayText(event);
  const lowerDisplay = display.toLowerCase();
  const defaultOpen = event.type === "worker.completed" || lowerDisplay.includes("error") || lowerDisplay.includes("failed");
  return (
    <div className="worker-event-line">
      <div className="worker-event-meta">
        <time>{new Date(event.at).toLocaleTimeString()}</time>
        <code>{label}</code>
      </div>
      <details className="worker-event-details" open={defaultOpen}>
        <summary>{display || label}</summary>
        <EventPayload event={event} />
      </details>
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

function latestWorkerProgressEvent(events: EventRecord[]): EventRecord | undefined {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    if (isWorkerProgressEvent(events[index])) {
      return events[index];
    }
  }
  return undefined;
}

function isInspectableWorkerEvent(event: EventRecord): boolean {
  if (isBenignCodexRolloutRecordEvent(event)) {
    return false;
  }
  return event.type.startsWith("worker.");
}

function isWorkerProgressEvent(event: EventRecord): boolean {
  if (event.type !== "worker.output") {
    return false;
  }
  if (isBenignCodexRolloutRecordEvent(event)) {
    return false;
  }
  const display = eventDisplayText(event).trim();
  if (!display) {
    return false;
  }
  const payload = asRecord(event.payload);
  const raw = asRecord(payload.raw ?? payload.rawResult);
  const item = asRecord(raw.item);
  if (raw.type === "thread.started" || raw.type === "turn.started" || raw.type === "turn.completed") {
    return false;
  }
  return Boolean(payload.text || item.type === "agent_message" || item.type === "command_execution" || item.type === "file_change");
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
            {target.health?.status && (
              <div className="target-health">
                <span className={target.health.status === "ok" ? "health-dot ok" : "health-dot warn"} />
                <span>{target.health.status}</span>
                {target.resources?.load1 !== undefined && target.resources?.cpuCount ? (
                  <span>load {target.resources.load1.toFixed(2)}/{target.resources.cpuCount}</span>
                ) : null}
                {target.resources?.memoryAvailableMb ? <span>{formatMB(target.resources.memoryAvailableMb)} free</span> : null}
                {target.resources?.diskAvailableMb ? <span>{formatMB(target.resources.diskAvailableMb)} disk</span> : null}
              </div>
            )}
            {target.health?.error && <p className="plugin-error">{target.health.error}</p>}
          </article>
        ))}
      </div>
    </section>
  );
}

function formatMB(value: number): string {
  if (value >= 1024) return `${(value / 1024).toFixed(value >= 10240 ? 0 : 1)} GB`;
  return `${Math.round(value)} MB`;
}

function PluginPanel({
  plugins,
  onRegister,
  onUpdate,
  onDelete,
  onError,
}: {
  plugins: Plugin[];
  onRegister: (plugin: Plugin) => Promise<void>;
  onUpdate: (id: string, plugin: Plugin) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const enabled = plugins.filter((plugin) => plugin.enabled).length;
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [kind, setKind] = useState("runner");
  const [protocol, setProtocol] = useState("aged-runner-v1");
  const [command, setCommand] = useState("");
  const [enabledValue, setEnabledValue] = useState(true);
  const [config, setConfig] = useState("{}");
  const [editingId, setEditingId] = useState("");
  const [busy, setBusy] = useState(false);

  const reset = () => {
    setId("");
    setName("");
    setKind("runner");
    setProtocol("aged-runner-v1");
    setCommand("");
    setEnabledValue(true);
    setConfig("{}");
    setEditingId("");
  };

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      setBusy(true);
      const parsedConfig = config.trim() ? JSON.parse(config) : {};
      const plugin: Plugin = {
        id: id.trim(),
        name: name.trim() || id.trim(),
        kind,
        protocol,
        enabled: enabledValue,
        command: command.trim() ? command.trim().split(/\s+/) : undefined,
        config: parsedConfig,
      };
      if (editingId) {
        await onUpdate(editingId, plugin);
      } else {
        await onRegister(plugin);
      }
      reset();
    } catch (err) {
      onError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const edit = (plugin: Plugin) => {
    setEditingId(plugin.id);
    setId(plugin.id);
    setName(plugin.name ?? "");
    setKind(plugin.kind || "runner");
    setProtocol(plugin.protocol || (plugin.kind === "runner" ? "aged-runner-v1" : "aged-plugin-v1"));
    setCommand(plugin.command?.join(" ") ?? "");
    setEnabledValue(plugin.enabled);
    setConfig(JSON.stringify(plugin.config ?? {}, null, 2));
  };

  return (
    <section className="panel">
      <div className="panel-title split-title">
        <span>
          <Puzzle size={18} />
          <h2>Plugins</h2>
        </span>
        <span className="pill">{enabled}/{plugins.length} enabled</span>
      </div>
      <form className="plugin-register" onSubmit={submit}>
        <input value={id} onChange={(event) => setId(event.target.value)} placeholder="runner:lint" required />
        <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Display name" />
        <select value={kind} onChange={(event) => {
          const next = event.target.value;
          setKind(next);
          setProtocol(next === "runner" ? "aged-runner-v1" : "aged-plugin-v1");
        }}>
          <option value="runner">Runner</option>
          <option value="driver">Driver</option>
          <option value="brain">Brain</option>
          <option value="external">External</option>
        </select>
        <input value={protocol} onChange={(event) => setProtocol(event.target.value)} placeholder="protocol" />
        <input value={command} onChange={(event) => setCommand(event.target.value)} placeholder="command arg..." />
        <textarea value={config} onChange={(event) => setConfig(event.target.value)} rows={3} spellCheck={false} />
        <label className="checkbox-label">
          <input type="checkbox" checked={enabledValue} onChange={(event) => setEnabledValue(event.target.checked)} />
          Enabled
        </label>
        <div className="plugin-form-actions">
          <button type="submit" disabled={busy}>{editingId ? "Update" : "Register"}</button>
          {editingId && <button type="button" className="secondary" onClick={reset}>Cancel</button>}
        </div>
      </form>
      <div className="plugin-grid">
        {plugins.map((plugin) => (
          <article key={plugin.id} className={plugin.enabled ? "plugin-card" : "plugin-card disabled"}>
            <div>
              <strong>{plugin.name}</strong>
              <small>{plugin.id}</small>
            </div>
            <div className="plugin-status-row">
              <span className={plugin.enabled ? "tool-status" : "tool-status neutral"}>{plugin.kind}</span>
              {plugin.status && <span className={plugin.status === "error" ? "tool-status failed" : "tool-status neutral"}>{plugin.status}</span>}
            </div>
            {plugin.capabilities && plugin.capabilities.length > 0 && (
              <div className="plugin-capabilities">
                {plugin.capabilities.slice(0, 5).map((capability) => (
                  <span key={capability}>{capability}</span>
                ))}
              </div>
            )}
            {plugin.driver?.managed && (
              <div className="driver-runtime">
                {plugin.driver.pid ? <span>pid {plugin.driver.pid}</span> : <span>not running</span>}
                {plugin.driver.restartPolicy && <span>{plugin.driver.restartPolicy}</span>}
                {plugin.driver.restartCount ? <span>{plugin.driver.restartCount} restarts</span> : null}
              </div>
            )}
            {plugin.driver?.logTail && plugin.driver.logTail.length > 0 && (
              <details className="driver-log">
                <summary>{plugin.driver.logTail.length} log lines</summary>
                <pre>{plugin.driver.logTail.slice(-8).join("\n")}</pre>
              </details>
            )}
            {plugin.error && <p className="plugin-error">{plugin.error}</p>}
            <div className="plugin-card-actions">
              <button className="secondary" onClick={() => edit(plugin)}>Edit</button>
              <button
                className="secondary danger-text"
                onClick={() => onDelete(plugin.id).catch((err: Error) => onError(err.message))}
              >
                <Trash2 size={14} />
              </button>
            </div>
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

function approvalStates(events: EventRecord[]): ApprovalState[] {
  const decisions = events
    .filter((event) => event.type === "approval.decided")
    .map((event) => ({ event, payload: asRecord(event.payload) }));
  return events
    .filter((event) => event.type === "approval.needed")
    .map((event) => {
      const payload = asRecord(event.payload);
      const workerId = event.workerId || payloadValue(payload.workerId);
      const reason = payloadValue(payload.reason);
      const question = payloadValue(payload.question || payload.error || payload.summary) || "Approval needed.";
      const decision = decisions.find(({ event: decidedEvent, payload: decisionPayload }) => {
        if (decidedEvent.id < event.id) return false;
        const decidedWorker = decidedEvent.workerId || payloadValue(decisionPayload.workerId);
        if (workerId && decidedWorker && workerId !== decidedWorker) return false;
        const decidedReason = payloadValue(decisionPayload.reason);
        return !reason || !decidedReason || reason === decidedReason || decidedReason === "user_feedback" || decidedReason === "autonomous_replan";
      });
      return {
        id: event.id,
        at: event.at,
        question,
        reason,
        workerId,
        decided: Boolean(decision),
        answer: decision ? payloadValue(decision.payload.answer || decision.payload.message) : undefined,
      };
    })
    .reverse();
}

function currentWorkerUpdate(workers: Worker[], nodes: ExecutionNode[], events: EventRecord[]): WorkerProgressUpdate | undefined {
  if (workers.length === 0 && nodes.length === 0) {
    return undefined;
  }

  const nodesByWorkerId = new Map(nodes.filter((node) => node.workerId).map((node) => [node.workerId!, node]));
  const activeWorkers = workers.filter((worker) => !isTerminalWorkerStatus(worker.status));
  const candidates = (activeWorkers.length > 0 ? activeWorkers : [...workers])
    .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
  const progressCandidates = candidates
    .map((worker) => {
      const progressEvent = latestWorkerProgressEvent(events.filter((event) => event.workerId === worker.id));
      return { worker, progressEvent };
    })
    .filter((candidate): candidate is { worker: Worker; progressEvent: EventRecord } => Boolean(candidate.progressEvent))
    .sort((left, right) => Date.parse(right.progressEvent.at) - Date.parse(left.progressEvent.at));

  const latestProgressCandidate = progressCandidates[0];
  if (latestProgressCandidate) {
    const { worker, progressEvent } = latestProgressCandidate;
    return {
      workerId: worker.id,
      title: workerProgressTitle(worker, nodesByWorkerId.get(worker.id)),
      status: worker.status,
      at: progressEvent.at,
      label: workerEventLabel(progressEvent),
      text: eventDisplayText(progressEvent),
      source: "output",
    };
  }

  for (const worker of candidates) {
    const workerEvents = events.filter((event) => event.workerId === worker.id);
    const latestEvent = latestInspectableWorkerEvent(workerEvents);
    if (latestEvent) {
      return {
        workerId: worker.id,
        title: workerProgressTitle(worker, nodesByWorkerId.get(worker.id)),
        status: worker.status,
        at: latestEvent.at,
        label: workerEventLabel(latestEvent),
        text: eventDisplayText(latestEvent),
        source: "lifecycle",
      };
    }
  }

  const activeNodeWithoutWorker = nodes
    .filter((node) => !node.workerId && !isTerminalWorkerStatus(node.status))
    .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))[0];
  if (activeNodeWithoutWorker) {
    return {
      workerId: activeNodeWithoutWorker.id,
      title: activeNodeWithoutWorker.role || activeNodeWithoutWorker.workerKind,
      status: activeNodeWithoutWorker.status,
      text: activeNodeWithoutWorker.reason || "Worker is queued or waiting for execution.",
      source: "waiting",
    };
  }

  const latestWorker = candidates[0];
  if (!latestWorker) {
    return undefined;
  }
  return {
    workerId: latestWorker.id,
    title: workerProgressTitle(latestWorker, nodesByWorkerId.get(latestWorker.id)),
    status: latestWorker.status,
    text: "No worker output has been reported yet.",
    source: "waiting",
  };
}

function workerProgressTitle(worker: Worker, node: ExecutionNode | undefined): string {
  return `${node?.role || worker.kind} (${worker.id.slice(0, 8)})`;
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
    <details className="event-files">
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
  if (isBenignCodexRolloutRecordEvent(event)) {
    return "";
  }
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

function isBenignCodexRolloutRecordEvent(event: EventRecord): boolean {
  if (event.type !== "worker.output") {
    return false;
  }
  const payload = asRecord(event.payload);
  const text = payloadValue(payload.text);
  return text.includes("failed to record rollout items: thread") && text.includes("codex_core::session");
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
  if (event.type === "approval.needed") {
    return payloadValue(payload.question || payload.error || payload.reason) || "Approval needed";
  }
  if (event.type === "approval.decided") {
    return payloadValue(payload.answer || payload.message || payload.reason) || "Approval decided";
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
  const executionNodes = snapshot.executionNodes ?? [];
  const tasks = snapshot.tasks ?? [];
  return {
    tasks,
    workers: snapshot.workers ?? [],
    executionNodes,
    targets: snapshot.targets ?? [],
    plugins: snapshot.plugins ?? [],
    projects: snapshot.projects ?? [],
    pullRequests: snapshot.pullRequests ?? [],
    orchestrationGraphs: snapshot.orchestrationGraphs ?? deriveOrchestrationGraphs(tasks, executionNodes),
    events: snapshot.events ?? [],
  };
}

function upsertTask(snapshot: AppSnapshot, task: Task): AppSnapshot {
  const tasks = snapshot.tasks.some((candidate) => candidate.id === task.id)
    ? snapshot.tasks.map((candidate) => (candidate.id === task.id ? task : candidate))
    : [...snapshot.tasks, task];
  return { ...snapshot, tasks };
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
    if (event.type === "task.final_candidate_selected" && event.taskId) {
      const task = tasks.get(event.taskId);
      if (task) {
        tasks.set(event.taskId, { ...task, finalCandidateWorkerId: String(payload.workerId ?? "") || undefined, updatedAt: event.at });
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
        metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
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
    if (event.type === "worker.changes_applied" && event.taskId && event.workerId) {
      const task = tasks.get(event.taskId);
      if (task) {
        tasks.set(event.taskId, { ...task, appliedWorkerId: event.workerId, updatedAt: event.at });
      }
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
    orchestrationGraphs: deriveOrchestrationGraphs(
      [...tasks.values()].filter((task) => !clearedTasks.has(task.id)),
      [...executionNodes.values()].filter((node) => !clearedTasks.has(node.taskId)),
    ),
    projects: snapshot.projects,
    plugins: snapshot.plugins,
    pullRequests: [...pullRequests.values()].filter((pr) => !clearedTasks.has(pr.taskId)),
    targets: snapshot.targets,
    events: snapshot.events,
  };
}

function deriveOrchestrationGraphs(tasks: Task[], nodes: ExecutionNode[]): OrchestrationGraph[] {
  const tasksById = new Map(tasks.map((task) => [task.id, task]));
  const byTask = new Map<string, ExecutionNode[]>();
  for (const node of nodes) {
    byTask.set(node.taskId, [...(byTask.get(node.taskId) ?? []), node]);
  }
  return [...byTask.entries()].map(([taskId, taskNodes]) => {
    const spawnToNode = new Map(taskNodes.filter((node) => node.spawnId).map((node) => [node.spawnId!, node.id]));
    const edges = taskNodes.flatMap((node) => {
      const items = [];
      if (node.parentNodeId) items.push({ from: node.parentNodeId, to: node.id, reason: "parent" });
      for (const dep of node.dependsOn ?? []) {
        const from = spawnToNode.get(dep);
        if (from) items.push({ from, to: node.id, reason: `depends_on:${dep}` });
      }
      return items;
    });
    const summary = {
      total: taskNodes.length,
      running: taskNodes.filter((node) => node.status === "running").length,
      waiting: taskNodes.filter((node) => node.status === "waiting" || node.status === "queued").length,
      done: taskNodes.filter((node) => node.status === "succeeded").length,
      failed: taskNodes.filter((node) => node.status === "failed").length,
      canceled: taskNodes.filter((node) => node.status === "canceled").length,
    };
    return {
      taskId,
      status: tasksById.get(taskId)?.status ?? "queued",
      nodes: taskNodes.map((node) => ({
        id: node.id,
        workerId: node.workerId,
        workerKind: node.workerKind,
        status: node.status,
        role: node.role,
        reason: node.reason,
        spawnId: node.spawnId,
        targetId: node.targetId,
        targetKind: node.targetKind,
      })),
      edges,
      summary,
      updatedAt: taskNodes.map((node) => node.updatedAt).sort().at(-1) ?? "",
    };
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
