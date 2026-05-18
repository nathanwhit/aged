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
import { applyTaskResult, applyWorkerChanges, askAssistant, babysitPullRequest, cancelTask, cancelWorker, clearFinishedTasks, clearTask, createCampaign, createProject, createTarget, createTask, deletePlugin, deleteProject, deletePromptSet, deleteTarget, getProjectHealth, getSnapshot, getTaskEvents, getTaskSnapshot, getWorkerChanges, publishTaskPullRequest, refreshPullRequest, refreshTargetHealth, registerPlugin, registerPromptSet, retryTask, steerTask, updatePlugin, updateProject, updatePromptSet, updateTarget, updateTaskLoopConfig, watchTaskPullRequests } from "./api";
import type { TargetInput } from "./api";
import type { Campaign, EventRecord, ExecutionNode, OrchestrationGraph, Plugin, Project, ProjectHealth, ProjectInput, PromptSet, PullRequestPolicy, PullRequestState, Snapshot, TargetState, Task, WatchPullRequestsInput, Worker, WorkerChangesReview, WorkerStatus } from "./types";
import "./styles.css";

type AppSnapshot = {
  campaigns: Campaign[];
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  targets: TargetState[];
  plugins: Plugin[];
  promptSets: PromptSet[];
  projects: Project[];
  pullRequests: PullRequestState[];
  orchestrationGraphs: OrchestrationGraph[];
  lastEventId: number;
  snapshotEventId: number;
  events: EventRecord[];
};

type TaskStartInput = {
  runMode?: RunMode;
  projectId?: string;
  title: string;
  prompt: string;
  metadata?: Record<string, unknown>;
};

type RunMode = "one-shot" | "campaign" | "loop";

type InitialSnapshotStatus = "loading" | "ready" | "error";

const emptySnapshot: AppSnapshot = {
  campaigns: [],
  tasks: [],
  workers: [],
  executionNodes: [],
  targets: [],
  plugins: [],
  promptSets: [],
  projects: [],
  pullRequests: [],
  orchestrationGraphs: [],
  lastEventId: 0,
  snapshotEventId: 0,
  events: [],
};

type DashboardPaneId =
  | "task-detail"
  | "pull-requests"
  | "current-state"
  | "projects"
  | "assistant"
  | "targets"
  | "plugins"
  | "prompt-sets"
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

const DASHBOARD_LAYOUT_STORAGE_KEY = "aged.dashboard.layout.v3";
const DASHBOARD_MIN_SPAN = 4;
const DASHBOARD_MAX_SPAN = 12;
const DASHBOARD_MIN_HEIGHT = 0;
const DASHBOARD_MAX_HEIGHT = 900;
const DASHBOARD_HEIGHT_STEP = 48;
const SELECTED_TASK_OUTPUT_EVENT_LIMIT = 250;
const DEFAULT_DASHBOARD_LAYOUT: DashboardPaneLayout[] = [
  { id: "task-detail", span: 12, minHeight: 0 },
  { id: "current-state", span: 4, minHeight: 0 },
  { id: "pull-requests", span: 8, minHeight: 0 },
  { id: "workers", span: 12, minHeight: 0 },
  { id: "worker-detail", span: 8, minHeight: 0 },
  { id: "timeline", span: 12, minHeight: 320 },
  { id: "targets", span: 4, minHeight: 0 },
  { id: "projects", span: 4, minHeight: 0 },
  { id: "prompt-sets", span: 4, minHeight: 0 },
  { id: "assistant", span: 4, minHeight: 0 },
];

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function App() {
  const [snapshot, setSnapshot] = useState<AppSnapshot>(emptySnapshot);
  const [selectedTaskId, setSelectedTaskId] = useState<string>("");
  const [selectedWorkerId, setSelectedWorkerId] = useState<string>("");
  const [pendingTask, setPendingTask] = useState<TaskStartInput | null>(null);
  const [error, setError] = useState<string>("");
  const [connected, setConnected] = useState(false);
  const [retryingTaskId, setRetryingTaskId] = useState("");
  const [hydratedTaskIds, setHydratedTaskIds] = useState<Set<string>>(() => new Set());
  const [initialSnapshotStatus, setInitialSnapshotStatus] = useState<InitialSnapshotStatus>("loading");
  const [showCompletedTasks, setShowCompletedTasks] = useState(false);

  async function refresh() {
    const next = normalizeSnapshot(await getSnapshot({ events: "none", tasks: "cards" }));
    setSnapshot(next);
    setHydratedTaskIds(new Set(next.tasks.filter((task) => !isTerminalTask(task)).map((task) => task.id)));
    setInitialSnapshotStatus("ready");
    setSelectedTaskId((current) => (next.tasks.some((task) => task.id === current) ? current : preferredTask(next.tasks)?.id || ""));
  }

  useEffect(() => {
    refresh().catch((err) => {
      setError(errorMessage(err));
      setInitialSnapshotStatus((current) => (current === "loading" ? "error" : current));
    });
  }, []);

  useEffect(() => {
    if (initialSnapshotStatus !== "ready") {
      return;
    }
    const lastID = snapshot.lastEventId || snapshot.events.at(-1)?.id || 0;
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
    () => snapshot.tasks.find((task) => task.id === selectedTaskId) ?? preferredTask(snapshot.tasks),
    [selectedTaskId, snapshot.tasks],
  );

  useEffect(() => {
    if (!selectedTask?.id || initialSnapshotStatus !== "ready") {
      return;
    }
    if (isTerminalTask(selectedTask) && !hydratedTaskIds.has(selectedTask.id)) {
      let active = true;
      getTaskSnapshot(selectedTask.id)
        .then((taskSnapshot) => {
          if (!active) return;
          setSnapshot((current) => mergeTaskSnapshot(current, normalizeSnapshot(taskSnapshot)));
          setHydratedTaskIds((current) => new Set(current).add(selectedTask.id));
        })
        .catch((err) => {
          if (active) setError(errorMessage(err));
        });
      return () => {
        active = false;
      };
    }
    let active = true;
    getTaskEvents(selectedTask.id, { limit: SELECTED_TASK_OUTPUT_EVENT_LIMIT })
      .then((events) => {
        if (active) setSnapshot((current) => applyTaskHistoryEvents(current, events));
      })
      .catch((err) => {
        if (active) setError(errorMessage(err));
      });
    return () => {
      active = false;
    };
  }, [hydratedTaskIds, initialSnapshotStatus, selectedTask?.id, selectedTask?.status, snapshot.snapshotEventId]);
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
  const activeTasks = snapshot.tasks.filter((task) => !isTerminalTask(task));
  const completedTasks = tasksByNewestCompletion(snapshot.tasks.filter(isTerminalTask));
  const activeCampaigns = campaignsByNewestUpdate(snapshot.campaigns.filter((campaign) => !isTerminalCampaign(campaign)));
  const taskById = new Map(snapshot.tasks.map((task) => [task.id, task]));

  async function handleClearTask(taskId: string) {
    try {
      setError("");
      await clearTask(taskId);
      await refresh();
    } catch (err) { setError(errorMessage(err)); }
  }

  async function handleClearFinished() {
    try {
      setError("");
      await clearFinishedTasks();
      await refresh();
    } catch (err) { setError(errorMessage(err)); }
  }

  async function handleRetryTask(taskId: string) {
    setRetryingTaskId(taskId);
    try {
      setError("");
      await retryTask(taskId);
      await refresh();
    } catch (err) { setError(errorMessage(err)); } finally {
      setRetryingTaskId("");
    }
  }

  const projectPane: DashboardPane = {
    id: "projects",
    title: "Projects",
    element: (
      <ProjectPanel
        projects={snapshot.projects}
        promptSets={snapshot.promptSets}
        onCreate={async (input) => {
          setError("");
          const project = await createProject(input);
          setSnapshot((current) => upsertProject(current, project));
          return project;
        }}
        onUpdate={async (id, input) => {
          setError("");
          const project = await updateProject(id, input);
          setSnapshot((current) => upsertProject(current, project));
          return project;
        }}
        onDelete={async (id) => {
          setError("");
          await deleteProject(id);
          setSnapshot((current) => removeProjectFromSnapshot(current, id));
        }}
        onHealth={getProjectHealth}
        onError={setError}
      />
    ),
  };
  const assistantPane: DashboardPane = {
    id: "assistant",
    title: "Ask",
    element: <AssistantPanel onError={setError} />,
  };
  const promptSetPane: DashboardPane = {
    id: "prompt-sets",
    title: "Prompts",
    element: (
      <PromptSetPanel
        promptSets={snapshot.promptSets}
        onRegister={async (promptSet) => {
          setError("");
          await registerPromptSet(promptSet);
          await refresh();
        }}
        onUpdate={async (id, promptSet) => {
          setError("");
          await updatePromptSet(id, promptSet);
          await refresh();
        }}
        onDelete={async (id) => {
          setError("");
          await deletePromptSet(id);
          await refresh();
        }}
        onError={setError}
      />
    ),
  };
  const targetPanes: DashboardPane[] = snapshot.targets.length > 0
    ? [{
      id: "targets",
      title: "Targets",
      element: (
        <TargetPanel
          targets={snapshot.targets}
          onRegister={async (target) => {
            setError("");
            await createTarget(target);
            await refresh();
          }}
          onUpdate={async (id, target) => {
            setError("");
            await updateTarget(id, target);
            await refresh();
          }}
          onDelete={async (id) => {
            setError("");
            await deleteTarget(id);
            await refresh();
          }}
          onProbe={async (id) => {
            setError("");
            await refreshTargetHealth(id);
            await refresh();
          }}
          onError={setError}
        />
      ),
    }]
    : [];
  const taskPanes: DashboardPane[] = selectedTask
    ? [
        {
          id: "task-detail",
          title: "Task",
          element: <TaskDetail task={selectedTask} workers={selectedWorkers} nodes={selectedNodes} targets={snapshot.targets} events={selectedEvents} onCancel={cancelTask} onRetry={handleRetryTask} onReview={getWorkerChanges} onApply={applyTaskResult} onApplied={refresh} onSteer={steerTask} onUpdateLoopConfig={updateTaskLoopConfig} onLoopConfigUpdated={refresh} retrying={retryingTaskId === selectedTask.id} onError={setError} />,
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
  const dashboardPanes: DashboardPane[] = [...taskPanes, ...targetPanes, projectPane, promptSetPane, assistantPane];

  return (
    <main className="app">
      <header className="topbar">
        <div>
          <h1>aged</h1>
          <p>Agent orchestration</p>
        </div>
        <div className="topbar-actions">
          <span className={connected ? "pill ok" : "pill"}>{connected ? "Live" : "Offline"}</span>
          <button className="icon-button" onClick={() => refresh().catch((err) => setError(errorMessage(err)))} title="Refresh">
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

      <DashboardOverview
        tasks={snapshot.tasks}
        workers={snapshot.workers}
        selectedTask={selectedTask}
        progress={progress}
      />

      <section className="layout">
        <section className="left-rail">
          <section className="panel campaign-list">
            <div className="panel-title split-title">
              <span>
                <FolderPlus size={18} />
                <h2>Campaigns</h2>
              </span>
              <span className="pill subtle">{activeCampaigns.length} active</span>
            </div>
            {initialSnapshotStatus === "loading" ? (
              <TaskListLoading label="Loading campaigns..." />
            ) : activeCampaigns.length === 0 ? (
              <p className="empty">No active campaigns.</p>
            ) : (
              <div className="campaign-row-list">
                {activeCampaigns.map((campaign) => (
                  <CampaignRow
                    key={campaign.id}
                    campaign={campaign}
                    rootTask={campaign.rootTaskId ? taskById.get(campaign.rootTaskId) : undefined}
                    selectedTaskId={selectedTask?.id ?? ""}
                    onSelectTask={setSelectedTaskId}
                  />
                ))}
              </div>
            )}
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
                {activeTasks.length === 0 && !pendingTask ? (
                  <p className="empty">No active tasks.</p>
                ) : (
                  activeTasks.map((task) => (
                    <TaskRow
                      key={task.id}
                      task={task}
                      selected={task.id === selectedTask?.id}
                      retrying={retryingTaskId === task.id}
                      onSelect={setSelectedTaskId}
                      onRetry={handleRetryTask}
                      onClear={handleClearTask}
                    />
                  ))
                )}
                {pendingTask && <PendingTaskRow task={pendingTask} />}
                {completedTasks.length > 0 && (
                  <div className="completed-task-group">
                    <button className="secondary compact completed-toggle" onClick={() => setShowCompletedTasks((value) => !value)}>
                      <Check size={14} />
                      {showCompletedTasks ? "Hide completed" : `Show completed (${completedTasks.length})`}
                    </button>
                    {showCompletedTasks && (
                      <div className="completed-task-list">
                        {completedTasks.map((task) => (
                          <TaskRow
                            key={task.id}
                            task={task}
                            selected={task.id === selectedTask?.id}
                            retrying={retryingTaskId === task.id}
                            onSelect={setSelectedTaskId}
                            onRetry={handleRetryTask}
                            onClear={handleClearTask}
                          />
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </>
            )}
          </section>

          <TaskComposer
            onCreate={async (input) => {
              setError("");
              const { runMode, ...request } = input;
              if (runMode === "campaign") {
                const campaign = await createCampaign(request);
                setSnapshot((current) => upsertCampaign(current, campaign));
                if (campaign.rootTaskId) {
                  setSelectedTaskId(campaign.rootTaskId);
                }
                refresh().catch((err) => setError(errorMessage(err)));
                return campaign;
              }
              const task = await createTask(request);
              setSnapshot((current) => upsertTask(current, task));
              setSelectedTaskId(task.id);
              refresh().catch((err) => setError(errorMessage(err)));
              return task;
            }}
            onStartPending={(input) => {
              setError("");
              setPendingTask(input);
            }}
            onStartSettled={() => setPendingTask(null)}
            onError={setError}
            projects={snapshot.projects}
            promptSets={snapshot.promptSets}
            targets={snapshot.targets}
          />
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

function DashboardOverview({
  tasks,
  workers,
  selectedTask,
  progress,
}: {
  tasks: Task[];
  workers: Worker[];
  selectedTask: Task | undefined;
  progress: WorkProgress;
}) {
  const activeTasks = tasks.filter((task) => !isTerminalTask(task));
  const runningWorkers = workers.filter((worker) => worker.status === "running").length;
  const waitingWorkers = workers.filter((worker) => worker.status === "waiting" || worker.status === "queued").length;
  const failedTasks = tasks.filter((task) => task.status === "failed" || task.status === "canceled").length;
  return (
    <section className="overview-strip" aria-label="Dashboard overview">
      <div className="overview-primary">
        <span>Selected task</span>
        <strong>{selectedTask?.title ?? "No task selected"}</strong>
        <small>
          {selectedTask
            ? [selectedTask.projectId && `Project ${selectedTask.projectId}`, selectedTask.id.slice(0, 8), `${progress.percent}%`].filter(Boolean).join(" · ")
            : "Create or select a task to inspect it."}
        </small>
      </div>
      <OverviewMetric label="Active" value={String(activeTasks.length)} />
      <OverviewMetric label="Running" value={String(runningWorkers || progress.running)} />
      <OverviewMetric label="Waiting" value={String(waitingWorkers || progress.waiting)} />
      <OverviewMetric label="Failed" value={String(failedTasks)} tone={failedTasks ? "bad" : undefined} />
    </section>
  );
}

function OverviewMetric({ label, value, tone }: { label: string; value: string; tone?: "bad" }) {
  return (
    <div className={tone ? `overview-metric ${tone}` : "overview-metric"}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function TaskListLoading({ label = "Loading tasks..." }: { label?: string }) {
  return (
    <div className="task-list-loading" role="status" aria-live="polite">
      <LoaderCircle className="spin" size={16} />
      <span>{label}</span>
    </div>
  );
}

function CampaignRow({
  campaign,
  rootTask,
  selectedTaskId,
  onSelectTask,
}: {
  campaign: Campaign;
  rootTask?: Task;
  selectedTaskId: string;
  onSelectTask: (id: string) => void;
}) {
  const childTaskIds = campaign.childTaskIds ?? [];
  const implementationChildren = Math.max(0, childTaskIds.length - (campaign.rootTaskId ? 1 : 0));
  const selected = selectedTaskId !== "" && (selectedTaskId === campaign.rootTaskId || childTaskIds.includes(selectedTaskId));
  const canSelectRoot = Boolean(campaign.rootTaskId);
  return (
    <div className={selected ? "campaign-row selected" : "campaign-row"}>
      <button
        className="campaign-row-main"
        disabled={!canSelectRoot}
        onClick={() => campaign.rootTaskId && onSelectTask(campaign.rootTaskId)}
        type="button"
        aria-current={selected ? "true" : undefined}
        title={canSelectRoot ? "Open campaign coordinator" : "Campaign coordinator unavailable"}
      >
        <span className="campaign-row-copy">
          <strong>{campaign.title}</strong>
          <small className="task-row-meta">
            {[campaign.projectId && `Project ${campaign.projectId}`, campaign.id.slice(0, 8), rootTask?.status && `Coordinator ${humanizeKey(rootTask.status)}`].filter(Boolean).join(" · ")}
          </small>
        </span>
        <span className="task-row-status">
          <Status value={campaign.status} />
          {campaign.objectivePhase && campaign.objectivePhase !== campaign.status && <span className="pill subtle">{humanizeKey(campaign.objectivePhase)}</span>}
          <span className="pill subtle">{implementationChildren === 1 ? "1 child" : `${implementationChildren} children`}</span>
        </span>
      </button>
    </div>
  );
}

function TaskRow({
  task,
  selected,
  retrying,
  onSelect,
  onRetry,
  onClear,
}: {
  task: Task;
  selected: boolean;
  retrying: boolean;
  onSelect: (id: string) => void;
  onRetry: (id: string) => void;
  onClear: (id: string) => void;
}) {
  return (
    <div className={selected ? "task-row selected" : "task-row"}>
      <button className="task-row-main" onClick={() => onSelect(task.id)} type="button" aria-current={selected ? "true" : undefined}>
        <span className="task-row-copy">
          <strong>{task.title}</strong>
          <small className="task-row-meta">
            {[task.projectId && `Project ${task.projectId}`, task.id.slice(0, 8)].filter(Boolean).join(" · ")}
          </small>
          {task.error && <small className="task-row-error">{task.error}</small>}
        </span>
        <span className="task-row-status">
          {isDurableLoopMetadata(task.metadata) && <span className="pill subtle">Loop</span>}
          <Status value={task.status} />
          {task.objectiveStatus && String(task.objectiveStatus) !== task.status && <span className="pill subtle">{humanizeKey(task.objectiveStatus)}</span>}
          {task.objectivePhase && task.objectivePhase !== task.status && <span className="pill subtle">{humanizeKey(task.objectivePhase)}</span>}
        </span>
      </button>
      <div className="task-row-actions">
        {isRetryableTask(task) && (
          <button className="icon-button ghost task-action" disabled={retrying} onClick={() => onRetry(task.id)} title="Retry task">
            <RefreshCw size={16} />
          </button>
        )}
        {isTerminalTask(task) && (
          <button className="icon-button ghost danger-text task-action" onClick={() => onClear(task.id)} title="Clear task">
            <Trash2 size={16} />
          </button>
        )}
      </div>
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
  const hasTaskPane = orderedPanes.some((pane) => pane.id === "task-detail" || pane.id === "workers" || pane.id === "timeline");
  const customizable = hasTaskPane && orderedPanes.length > 2;

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
      {customizable && (
        <div className="dashboard-toolbar">
          <span>{orderedPanes.length} panes</span>
          <button className="icon-button ghost" onClick={() => setLayout(defaultDashboardLayout())} title="Reset dashboard layout">
            <RotateCcw size={16} />
          </button>
        </div>
      )}
      <div className="dashboard-grid" ref={gridRef}>
        {orderedPanes.map((pane) => {
          const item = paneLayout(pane.id);
          return (
            <div
              key={pane.id}
              className={[
                "dashboard-pane",
                customizable ? "" : "plain",
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
              onDragOver={customizable
                ? (event) => {
                    event.preventDefault();
                    setDragOverId(pane.id);
                  }
                : undefined}
              onDragLeave={customizable ? () => setDragOverId((current) => (current === pane.id ? null : current)) : undefined}
              onDrop={customizable
                ? (event) => {
                    event.preventDefault();
                    const sourceId = event.dataTransfer.getData("text/plain") as DashboardPaneId;
                    movePane(sourceId, pane.id);
                    setDraggingId(null);
                    setDragOverId(null);
                  }
                : undefined}
            >
              {customizable && (
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
              )}
              {pane.element}
              {customizable && (
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
              )}
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

function isTerminalCampaign(campaign: Campaign): boolean {
  return campaign.status === "succeeded" || campaign.status === "failed" || campaign.status === "canceled";
}

function tasksByNewestCompletion(tasks: Task[]): Task[] {
  return [...tasks].sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
}

function campaignsByNewestUpdate(campaigns: Campaign[]): Campaign[] {
  return [...campaigns].sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
}

function preferredTask(tasks: Task[]): Task | undefined {
  return [...tasks].reverse().find((task) => !isTerminalTask(task)) ?? tasks.at(-1);
}

function isRetryableTask(task: Task): boolean {
  return task.status === "failed" || task.status === "canceled";
}

function isDurableLoopMetadata(metadata: Record<string, unknown> | undefined): boolean {
  const mode = String(metadata?.executionMode ?? "").trim().toLowerCase();
  return mode === "loop" || mode === "durable_loop" || mode === "agent_loop";
}

function durableLoopIntervalSeconds(metadata: Record<string, unknown> | undefined): number {
  const value = Number(metadata?.loopIntervalSeconds ?? 60);
  return Number.isFinite(value) && value >= 0 ? Math.floor(value) : 60;
}

function durableLoopPromptValue(task: Task): string {
  const prompt = String(task.metadata?.loopPrompt ?? "").trim();
  return prompt || task.prompt;
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
        {(activeNodes.length > 0 ? activeNodes : activeWorkers).slice(0, 4).map((item) => {
          const idle = formatWorkerIdle(item.status, item.updatedAt);
          return (
            <span key={item.id}>
              {"workerKind" in item ? (item.role || item.workerKind) : item.kind} <Status value={item.status} />
              {idle ? ` idle ${idle}` : ""}
            </span>
          );
        })}
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
  promptSets,
  targets,
}: {
  onCreate: (input: TaskStartInput) => Promise<Task | Campaign>;
  onStartPending: (input: TaskStartInput) => void;
  onStartSettled: () => void;
  onError: (message: string) => void;
  projects: Project[];
  promptSets: PromptSet[];
  targets: TargetState[];
}) {
  const [projectId, setProjectId] = useState("");
  const [promptSetId, setPromptSetId] = useState("");
  const [requiredTargetID, setRequiredTargetID] = useState("");
  const [title, setTitle] = useState("");
  const [prompt, setPrompt] = useState("");
  const [runMode, setRunMode] = useState<RunMode>("one-shot");
  const [completionMode, setCompletionMode] = useState<"local" | "github">("github");
  const [loopWorkerKind, setLoopWorkerKind] = useState("codex");
  const [loopRole, setLoopRole] = useState("maintenance_pr_loop");
  const [loopIntervalSeconds, setLoopIntervalSeconds] = useState("300");
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const interval = Math.max(0, Number.parseInt(loopIntervalSeconds, 10) || 0);
    const metadata: Record<string, unknown> = runMode === "loop"
      ? {
          executionMode: "loop",
          loopWorkerKind: loopWorkerKind.trim() || "codex",
          loopRole: loopRole.trim() || "maintenance_pr_loop",
          loopIntervalSeconds: interval,
        }
      : runMode === "campaign"
        ? {}
        : { completionMode };
    if (promptSetId) {
      metadata.promptSetId = promptSetId;
    }
    if (requiredTargetID) {
      metadata.requiredTargetID = requiredTargetID;
    }
    const input = { runMode, projectId: projectId || undefined, title, prompt, metadata };
    setBusy(true);
    onStartPending(input);
    try {
      await onCreate(input);
      setTitle("");
      setPrompt("");
      setPromptSetId("");
      setRequiredTargetID("");
      setRunMode("one-shot");
      setCompletionMode("github");
      setLoopWorkerKind("codex");
      setLoopRole("maintenance_pr_loop");
      setLoopIntervalSeconds("300");
    } catch (err) { onError(errorMessage(err)); } finally {
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
        Prompt set
        <select value={promptSetId} onChange={(event) => setPromptSetId(event.target.value)}>
          <option value="">Default prompt set</option>
          {promptSets.filter((promptSet) => !promptSet.builtIn).map((promptSet) => (
            <option key={promptSet.id} value={promptSet.id}>
              {promptSet.name}{promptSet.default ? " (default)" : ""}
            </option>
          ))}
        </select>
      </label>
      <TargetPinSelect value={requiredTargetID} targets={targets} onChange={setRequiredTargetID} />
      <label>
        Prompt
        <textarea value={prompt} onChange={(event) => setPrompt(event.target.value)} placeholder="Describe the development task..." required />
      </label>
      <fieldset className="run-mode-control">
        <legend>Run mode</legend>
        <label className={runMode === "one-shot" ? "run-mode-option selected" : "run-mode-option"}>
          <input type="radio" name="run-mode" value="one-shot" checked={runMode === "one-shot"} onChange={() => setRunMode("one-shot")} />
          <Play size={16} />
          <span>One-shot</span>
        </label>
        <label className={runMode === "campaign" ? "run-mode-option selected" : "run-mode-option"}>
          <input type="radio" name="run-mode" value="campaign" checked={runMode === "campaign"} onChange={() => setRunMode("campaign")} />
          <FolderPlus size={16} />
          <span>Campaign</span>
        </label>
        <label className={runMode === "loop" ? "run-mode-option selected" : "run-mode-option"}>
          <input type="radio" name="run-mode" value="loop" checked={runMode === "loop"} onChange={() => setRunMode("loop")} />
          <RefreshCw size={16} />
          <span>Durable loop</span>
        </label>
      </fieldset>
      {runMode === "one-shot" ? (
        <label>
          Completion
          <select value={completionMode} onChange={(event) => setCompletionMode(event.target.value as "local" | "github")}>
            <option value="github">GitHub: open PR when complete</option>
            <option value="local">Local: review diff here and apply result</option>
          </select>
        </label>
      ) : runMode === "loop" ? (
        <div className="loop-config">
          <label>
            Worker kind
            <input list="loop-worker-kinds" value={loopWorkerKind} onChange={(event) => setLoopWorkerKind(event.target.value)} />
            <datalist id="loop-worker-kinds">
              <option value="codex" />
              <option value="claude" />
              <option value="mock" />
            </datalist>
          </label>
          <label>
            Interval seconds
            <input type="number" min="0" step="30" value={loopIntervalSeconds} onChange={(event) => setLoopIntervalSeconds(event.target.value)} />
          </label>
          <label className="loop-role-field">
            Role
            <input value={loopRole} onChange={(event) => setLoopRole(event.target.value)} />
          </label>
        </div>
      ) : null}
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
        <span className="task-row-copy">
          <strong>{title}</strong>
          <small className="task-row-meta">Start request in progress</small>
        </span>
        <span className="status starting">
          <LoaderCircle className="spin" size={12} />
          Starting
        </span>
        {task.runMode === "campaign" && <span className="pill subtle">Campaign</span>}
        {isDurableLoopMetadata(task.metadata) && <span className="pill subtle">Loop</span>}
      </div>
    </div>
  );
}

function TargetPinSelect({
  value,
  targets,
  onChange,
}: {
  value: string;
  targets: TargetState[];
  onChange: (value: string) => void;
}) {
  return (
    <label>
      Execution target
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        <option value="">Auto-select target</option>
        {targets.map((target) => (
          <option key={target.id} value={target.id}>
            {targetOptionLabel(target)}
          </option>
        ))}
      </select>
    </label>
  );
}

function targetOptionLabel(target: TargetState): string {
  const details = [target.kind, target.host, target.health?.status && `health ${target.health.status}`].filter(Boolean).join(" · ");
  return details ? `${target.id} (${details})` : target.id;
}

function requiredTargetIDFromMetadata(metadata: Record<string, unknown> | undefined): string {
  return String(metadata?.requiredTargetID ?? "").trim();
}

function ProjectPanel({
  projects,
  promptSets,
  onCreate,
  onUpdate,
  onDelete,
  onHealth,
  onError,
}: {
  projects: Project[];
  promptSets: PromptSet[];
  onCreate: (input: ProjectInput) => Promise<Project>;
  onUpdate: (id: string, input: ProjectInput) => Promise<Project>;
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
  const [requiredMemoryMb, setRequiredMemoryMb] = useState("");
  const [requiredStorageMb, setRequiredStorageMb] = useState("");
  const [remoteCheckoutEntries, setRemoteCheckoutEntries] = useState<PluginConfigEntry[]>([]);
  const [pollGitHubIssues, setPollGitHubIssues] = useState(false);
  const [issueLabels, setIssueLabels] = useState("aged");
  const [issueLimit, setIssueLimit] = useState("20");
  const [issueAutoPublish, setIssueAutoPublish] = useState(true);
  const [pollGitHubMentions, setPollGitHubMentions] = useState(false);
  const [mentionReasons, setMentionReasons] = useState("mention, team_mention, review_requested");
  const [mentionLimit, setMentionLimit] = useState("20");
  const [reviewGateEnabled, setReviewGateEnabled] = useState(false);
  const [reviewBeforeCompletionPR, setReviewBeforeCompletionPR] = useState(true);
  const [reviewBeforeIntermediatePR, setReviewBeforeIntermediatePR] = useState(true);
  const [reviewBlockingSeverities, setReviewBlockingSeverities] = useState("P0, P1");
  const [reviewerKinds, setReviewerKinds] = useState("claude, codex");
  const [reviewPromptSetId, setReviewPromptSetId] = useState("");
  const [reviewMaxAttempts, setReviewMaxAttempts] = useState("2");
  const [reviewInstructions, setReviewInstructions] = useState("");
  const [draftPRs, setDraftPRs] = useState(false);
  const [allowMerge, setAllowMerge] = useState(false);
  const [autoMerge, setAutoMerge] = useState(false);
  const [mergeMethod, setMergeMethod] = useState<NonNullable<PullRequestPolicy["mergeMethod"]>>("squash");
  const [monitorPullRequests, setMonitorPullRequests] = useState(true);
  const [projectFormOpen, setProjectFormOpen] = useState(false);
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
    setRequiredMemoryMb(project.requirements?.memoryMb ? String(project.requirements.memoryMb) : "");
    setRequiredStorageMb(project.requirements?.storageMb ? String(project.requirements.storageMb) : "");
    setRemoteCheckoutEntries(configEntriesFromRecord(project.remoteCheckouts));
    setPollGitHubIssues(Boolean(project.githubIssues?.enabled));
    setIssueLabels((project.githubIssues?.labels ?? []).join(", "));
    setIssueLimit(project.githubIssues?.issueLimit ? String(project.githubIssues.issueLimit) : "20");
    setIssueAutoPublish(project.githubIssues?.autoPublish ?? true);
    setPollGitHubMentions(Boolean(project.githubMentions?.enabled));
    setMentionReasons((project.githubMentions?.reasons ?? ["mention", "team_mention", "review_requested"]).join(", "));
    setMentionLimit(project.githubMentions?.limit ? String(project.githubMentions.limit) : "20");
    setReviewGateEnabled(Boolean(project.reviewPolicy?.enabled));
    setReviewBeforeCompletionPR(project.reviewPolicy?.beforeCompletionPr ?? true);
    setReviewBeforeIntermediatePR(project.reviewPolicy?.beforeIntermediatePr ?? true);
    setReviewBlockingSeverities((project.reviewPolicy?.blockingSeverities ?? ["P0", "P1"]).join(", "));
    setReviewerKinds((project.reviewPolicy?.reviewerKinds ?? ["claude", "codex"]).join(", "));
    setReviewPromptSetId(project.reviewPolicy?.promptSetId ?? "");
    setReviewMaxAttempts(project.reviewPolicy?.maxAttempts ? String(project.reviewPolicy.maxAttempts) : "2");
    setReviewInstructions(project.reviewPolicy?.instructions ?? "");
    setDraftPRs(Boolean(project.pullRequestPolicy?.draft));
    setAllowMerge(Boolean(project.pullRequestPolicy?.allowMerge));
    setAutoMerge(Boolean(project.pullRequestPolicy?.autoMerge));
    setMergeMethod(project.pullRequestPolicy?.mergeMethod ?? "squash");
    setMonitorPullRequests(project.pullRequestPolicy?.monitorPullRequests ?? true);
    setProjectFormOpen(true);
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
    setRequiredMemoryMb("");
    setRequiredStorageMb("");
    setRemoteCheckoutEntries([]);
    setPollGitHubIssues(false);
    setIssueLabels("aged");
    setIssueLimit("20");
    setIssueAutoPublish(true);
    setPollGitHubMentions(false);
    setMentionReasons("mention, team_mention, review_requested");
    setMentionLimit("20");
    setReviewGateEnabled(false);
    setReviewBeforeCompletionPR(true);
    setReviewBeforeIntermediatePR(true);
    setReviewBlockingSeverities("P0, P1");
    setReviewerKinds("claude, codex");
    setReviewPromptSetId("");
    setReviewMaxAttempts("2");
    setReviewInstructions("");
    setDraftPRs(false);
    setAllowMerge(false);
    setAutoMerge(false);
    setMergeMethod("squash");
    setMonitorPullRequests(true);
    setProjectFormOpen(false);
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      const parsedRemoteCheckouts = remoteCheckoutRecordFromEntries(remoteCheckoutEntries);
      const parsedIssueLimit = Math.max(0, Number.parseInt(issueLimit, 10) || 0);
      const parsedMentionLimit = Math.max(0, Number.parseInt(mentionLimit, 10) || 0);
      const parsedReviewMaxAttempts = Math.max(0, Number.parseInt(reviewMaxAttempts, 10) || 0);
      const parsedRequiredMemoryMb = Math.max(0, Number.parseInt(requiredMemoryMb, 10) || 0);
      const parsedRequiredStorageMb = Math.max(0, Number.parseInt(requiredStorageMb, 10) || 0);
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
        requirements: parsedRequiredMemoryMb || parsedRequiredStorageMb ? {
          memoryMb: parsedRequiredMemoryMb || undefined,
          storageMb: parsedRequiredStorageMb || undefined,
        } : undefined,
        remoteCheckouts: Object.keys(parsedRemoteCheckouts).length ? parsedRemoteCheckouts : undefined,
        githubIssues: {
          enabled: pollGitHubIssues,
          labels: splitCommaList(issueLabels),
          issueLimit: parsedIssueLimit || undefined,
          autoPublish: issueAutoPublish,
        },
        githubMentions: {
          enabled: pollGitHubMentions,
          reasons: splitCommaList(mentionReasons),
          limit: parsedMentionLimit || undefined,
        },
        reviewPolicy: {
          enabled: reviewGateEnabled,
          beforeCompletionPr: reviewBeforeCompletionPR,
          beforeIntermediatePr: reviewBeforeIntermediatePR,
          blockingSeverities: splitCommaList(reviewBlockingSeverities),
          reviewerKinds: splitCommaList(reviewerKinds),
          promptSetId: reviewPromptSetId || undefined,
          maxAttempts: parsedReviewMaxAttempts || undefined,
          instructions: reviewInstructions || undefined,
        },
        pullRequestPolicy: {
          branchPrefix: branchPrefix || undefined,
          draft: draftPRs,
          allowMerge,
          autoMerge,
          mergeMethod,
          monitorPullRequests,
        },
      };
      if (editingId) {
        await onUpdate(editingId, input);
      } else {
        await onCreate(input);
      }
      resetForm();
    } catch (err) { onError(errorMessage(err)); } finally {
      setBusy(false);
    }
  }

  async function checkHealth(projectId: string) {
    setHealthBusy(projectId);
    try {
      const result = await onHealth(projectId);
      setHealth((current) => ({ ...current, [projectId]: result }));
    } catch (err) { onError(errorMessage(err)); } finally {
      setHealthBusy("");
    }
  }

  async function removeProject(projectId: string) {
    try {
      await onDelete(projectId);
      if (editingId === projectId) resetForm();
    } catch (err) { onError(errorMessage(err)); }
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
      <details className="project-add" open={projectFormOpen} onToggle={(event) => setProjectFormOpen(event.currentTarget.open)}>
        <summary>{editingId ? "Edit project" : "Add project"}</summary>
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
          <fieldset className="target-label-field">
            <legend>Minimum resources</legend>
            <div className="loop-config">
              <label>
                Memory MiB
                <input type="number" min="0" step="1" value={requiredMemoryMb} onChange={(event) => setRequiredMemoryMb(event.target.value)} placeholder="16384" />
              </label>
              <label>
                Storage MiB
                <input type="number" min="0" step="1" value={requiredStorageMb} onChange={(event) => setRequiredStorageMb(event.target.value)} placeholder="102400" />
              </label>
            </div>
          </fieldset>
          <fieldset className="target-label-field">
            <legend>Remote checkouts</legend>
            <KeyValueRows entries={remoteCheckoutEntries} setEntries={setRemoteCheckoutEntries} emptyText="No checkout overrides" keyPlaceholder="perf-1" valuePlaceholder="/srv/aged/checkouts/node" removeTitle="Remove checkout override" addLabel="Add checkout" />
          </fieldset>
          <label className="checkbox-label">
            <input type="checkbox" checked={pollGitHubIssues} onChange={(event) => setPollGitHubIssues(event.target.checked)} />
            Poll GitHub issues
          </label>
          {pollGitHubIssues && (
            <div className="loop-config">
              <label>
                Issue labels
                <input value={issueLabels} onChange={(event) => setIssueLabels(event.target.value)} placeholder="aged, help wanted" />
              </label>
              <label>
                Issue limit
                <input type="number" min="1" step="1" value={issueLimit} onChange={(event) => setIssueLimit(event.target.value)} />
              </label>
              <label className="checkbox-label">
                <input type="checkbox" checked={issueAutoPublish} onChange={(event) => setIssueAutoPublish(event.target.checked)} />
                Publish PRs
              </label>
            </div>
          )}
          <label className="checkbox-label">
            <input type="checkbox" checked={pollGitHubMentions} onChange={(event) => setPollGitHubMentions(event.target.checked)} />
            Poll GitHub mentions
          </label>
          {pollGitHubMentions && (
            <div className="loop-config">
              <label>
                Mention reasons
                <input value={mentionReasons} onChange={(event) => setMentionReasons(event.target.value)} placeholder="mention, team_mention, review_requested" />
              </label>
              <label>
                Mention limit
                <input type="number" min="1" step="1" value={mentionLimit} onChange={(event) => setMentionLimit(event.target.value)} />
              </label>
            </div>
          )}
          <label className="checkbox-label">
            <input type="checkbox" checked={reviewGateEnabled} onChange={(event) => setReviewGateEnabled(event.target.checked)} />
            Require pre-publication code review
          </label>
          {reviewGateEnabled && (
            <div className="loop-config">
              <label className="checkbox-label">
                <input type="checkbox" checked={reviewBeforeCompletionPR} onChange={(event) => setReviewBeforeCompletionPR(event.target.checked)} />
                Review completion PRs
              </label>
              <label className="checkbox-label">
                <input type="checkbox" checked={reviewBeforeIntermediatePR} onChange={(event) => setReviewBeforeIntermediatePR(event.target.checked)} />
                Review intermediate PRs
              </label>
              <label>
                Blocking severities
                <input value={reviewBlockingSeverities} onChange={(event) => setReviewBlockingSeverities(event.target.value)} placeholder="P0, P1" />
              </label>
              <label>
                Reviewer workers
                <input value={reviewerKinds} onChange={(event) => setReviewerKinds(event.target.value)} placeholder="claude, codex" />
              </label>
              <label>
                Review prompt set
                <select value={reviewPromptSetId} onChange={(event) => setReviewPromptSetId(event.target.value)}>
                  <option value="">Default</option>
                  {promptSets.filter((promptSet) => !promptSet.builtIn).map((promptSet) => (
                    <option key={promptSet.id} value={promptSet.id}>
                      {promptSet.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Review attempts
                <input type="number" min="1" step="1" value={reviewMaxAttempts} onChange={(event) => setReviewMaxAttempts(event.target.value)} />
              </label>
              <label>
                Project review instructions
                <textarea value={reviewInstructions} onChange={(event) => setReviewInstructions(event.target.value)} placeholder="Project-specific checks for the reviewer..." />
              </label>
            </div>
          )}
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
          <label>
            Merge method
            <select value={mergeMethod} onChange={(event) => setMergeMethod(event.target.value as NonNullable<PullRequestPolicy["mergeMethod"]>)}>
              <option value="squash">Squash</option>
              <option value="merge">Merge commit</option>
              <option value="rebase">Rebase</option>
            </select>
          </label>
          <label className="checkbox-label">
            <input type="checkbox" checked={monitorPullRequests} onChange={(event) => setMonitorPullRequests(event.target.checked)} />
            Monitor tracked PRs
          </label>
          <button disabled={busy}>
            <FolderPlus size={16} />
            {busy ? "Saving" : editingId ? "Save Project" : "Add Project"}
          </button>
          {editingId && <button type="button" className="secondary" onClick={resetForm}>Cancel Edit</button>}
        </form>
      </details>
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
    } catch (err) { onError(errorMessage(err)); } finally {
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
  targets,
  events,
  onCancel,
  onRetry,
  onReview,
  onApply,
  onApplied,
  onSteer,
  onUpdateLoopConfig,
  onLoopConfigUpdated,
  retrying,
  onError,
}: {
  task: Task;
  workers: Worker[];
  nodes: ExecutionNode[];
  targets: TargetState[];
  events: EventRecord[];
  onCancel: (id: string) => Promise<void>;
  onRetry: (id: string) => Promise<void>;
  onReview: (id: string) => Promise<WorkerChangesReview>;
  onApply: (id: string) => Promise<void>;
  onApplied: () => Promise<void>;
  onSteer: (id: string, message: string) => Promise<void>;
  onUpdateLoopConfig: (id: string, input: { loopIntervalSeconds?: number; loopPrompt?: string; requiredTargetID?: string }) => Promise<Task>;
  onLoopConfigUpdated: () => Promise<void>;
  retrying: boolean;
  onError: (message: string) => void;
}) {
  const [message, setMessage] = useState("");
  const [applying, setApplying] = useState(false);
  const [loopIntervalInput, setLoopIntervalInput] = useState("");
  const [loopPromptInput, setLoopPromptInput] = useState("");
  const [loopTargetInput, setLoopTargetInput] = useState("");
  const [savingLoopConfig, setSavingLoopConfig] = useState(false);
  const [diff, setDiff] = useState<DiffReviewState | undefined>();
  const completionMode = String(task.metadata?.completionMode ?? "local");
  const durableLoop = isDurableLoopMetadata(task.metadata);
  const loopInterval = durableLoopIntervalSeconds(task.metadata);
  const currentLoopPrompt = durableLoopPromptValue(task);
  const requiredTargetID = requiredTargetIDFromMetadata(task.metadata);
  const hasCustomLoopPrompt = durableLoop && currentLoopPrompt !== task.prompt;
  const finalWorkerId = task.finalCandidateWorkerId ?? "";
  const finalWorkerApplied = finalWorkerId !== "" && (task.appliedWorkerId === finalWorkerId || workerChangesApplied(events, finalWorkerId));
  const canApplyResult = !durableLoop && completionMode !== "github" && completionMode !== "campaign" && isTerminalTask(task) && finalWorkerId !== "" && !finalWorkerApplied;
  const finalCompletion = finalWorkerId ? latestWorkerCompletion(events, finalWorkerId) : {};
  const finalChangedFiles = finalCompletion.changedFiles ?? finalCompletion.workspaceChanges?.changedFiles ?? [];
  const workerUpdate = currentWorkerUpdate(workers, nodes, events);
  const approvals = approvalStates(events);
  const taskError = task.error || latestTaskStatusError(events);

  useEffect(() => {
    setDiff(undefined);
  }, [finalWorkerId]);

  useEffect(() => {
    setLoopIntervalInput(String(loopInterval));
    setLoopPromptInput(currentLoopPrompt);
    setLoopTargetInput(requiredTargetID);
  }, [currentLoopPrompt, loopInterval, requiredTargetID, task.id]);

  async function steer(event: React.FormEvent) {
    event.preventDefault();
    try {
      await onSteer(task.id, message);
      setMessage("");
    } catch (err) { onError(errorMessage(err)); }
  }

  async function applyResult() {
    setApplying(true);
    try {
      await onApply(task.id);
      await onApplied();
    } catch (err) { onError(errorMessage(err)); } finally {
      setApplying(false);
    }
  }

  async function updateLoopConfig(event: React.FormEvent) {
    event.preventDefault();
    const nextInterval = Number.parseInt(loopIntervalInput, 10);
    if (!Number.isFinite(nextInterval) || nextInterval < 0) {
      onError("Interval seconds must be 0 or greater.");
      return;
    }
    const nextPrompt = loopPromptInput.trim();
    if (nextPrompt === "") {
      onError("Loop prompt must not be empty.");
      return;
    }
    const input: { loopIntervalSeconds?: number; loopPrompt?: string; requiredTargetID?: string } = {};
    if (nextInterval !== loopInterval) {
      input.loopIntervalSeconds = nextInterval;
    }
    if (nextPrompt !== currentLoopPrompt) {
      input.loopPrompt = nextPrompt;
    }
    if (loopTargetInput !== requiredTargetID) {
      input.requiredTargetID = loopTargetInput;
    }
    if (input.loopIntervalSeconds === undefined && input.loopPrompt === undefined && input.requiredTargetID === undefined) {
      onError("No loop config changes to save.");
      return;
    }
    setSavingLoopConfig(true);
    try {
      await onUpdateLoopConfig(task.id, input);
      await onLoopConfigUpdated();
    } catch (err) { onError(errorMessage(err)); } finally {
      setSavingLoopConfig(false);
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
      const error = errorMessage(err);
      setDiff({ open: true, loading: false, loaded: true, diff: "", error });
      onError(error);
    }
  }

  return (
    <section className="panel detail">
      <div className="detail-heading">
        <div className="detail-title-block">
          <h2>{task.title}</h2>
          <div className="task-detail-meta">
            {task.projectId && <span>Project {task.projectId}</span>}
            <span>{task.id.slice(0, 8)}</span>
            {requiredTargetID && <span>Target {requiredTargetID}</span>}
            <span>{completionMode === "github" ? "GitHub completion" : completionMode === "campaign" ? "Campaign coordinator" : "Local completion"}</span>
            {task.updatedAt && <span>Updated {new Date(task.updatedAt).toLocaleTimeString()}</span>}
          </div>
        </div>
        <div className="detail-actions">
          <Status value={task.status} />
          {task.objectiveStatus && <Status value={task.objectiveStatus} />}
          {task.objectivePhase && <span className="pill">{humanizeKey(task.objectivePhase)}</span>}
          {durableLoop && <span className="pill">Loop mode</span>}
          {!durableLoop && completionMode === "github" && <span className="pill">GitHub mode</span>}
          {!durableLoop && completionMode === "campaign" && <span className="pill">Campaign coordinator</span>}
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
          <button className="icon-button danger" onClick={() => onCancel(task.id).catch((err) => onError(errorMessage(err)))} title="Cancel task">
            <CircleStop size={18} />
          </button>
        </div>
      </div>
      <details className="task-prompt-block" open={task.prompt.length < 520 && !hasCustomLoopPrompt}>
        <summary>
          <span>{durableLoop ? "Task prompts" : "Task request"}</span>
          <small>{task.prompt.length.toLocaleString()} chars</small>
        </summary>
        <div className="task-prompt-content">
          <small>{durableLoop ? "Original prompt" : "Prompt"}</small>
          <p>{task.prompt}</p>
          {hasCustomLoopPrompt && (
            <>
              <small>Current loop prompt</small>
              <p>{currentLoopPrompt}</p>
            </>
          )}
        </div>
      </details>
      {(task.artifacts?.length || task.milestones?.length) && (
        <TaskObjectiveStrip task={task} />
      )}
      {taskError && (
        <div className="task-failure">
          <strong>Failure details</strong>
          <TruncatedBlock label="Error" value={taskError} className="tool-output failed" limit={1600} />
        </div>
      )}
      {approvals.length > 0 && <ApprovalPanel approvals={approvals} onUseMessage={setMessage} />}
      <WorkerProgressSpotlight update={workerUpdate} />
      {durableLoop && (
        <form className="loop-settings" onSubmit={updateLoopConfig}>
          <label>
            Interval seconds
            <input type="number" min="0" step="30" value={loopIntervalInput} onChange={(event) => setLoopIntervalInput(event.target.value)} />
          </label>
          <label className="loop-prompt-field">
            Loop prompt
            <textarea value={loopPromptInput} onChange={(event) => setLoopPromptInput(event.target.value)} rows={4} />
          </label>
          <div className="loop-target-field">
            <TargetPinSelect value={loopTargetInput} targets={targets} onChange={setLoopTargetInput} />
          </div>
          <button className="secondary compact" disabled={savingLoopConfig} title="Update durable loop settings">
            <RefreshCw size={16} />
            {savingLoopConfig ? "Saving" : "Update Loop"}
          </button>
        </form>
      )}
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
  summary?: string;
  target?: string;
  project?: string;
  resumeHint?: string;
  commands?: string[];
  workerId?: string;
  decided: boolean;
  answer?: string;
};

function ApprovalPanel({ approvals, onUseMessage }: { approvals: ApprovalState[]; onUseMessage: (message: string) => void }) {
  return (
    <section className="approval-panel">
      <div className="approval-title">
        <strong>User Action Required</strong>
        <span>{approvals.filter((approval) => !approval.decided).length} pending</span>
      </div>
      {approvals.slice(0, 4).map((approval) => (
        <div className={approval.decided ? "approval-card decided" : "approval-card"} key={approval.id}>
          <div>
            <small>{new Date(approval.at).toLocaleTimeString()} · {humanizeKey(approval.reason || "approval")}</small>
            {approval.summary && <span>{approval.summary}</span>}
            {(approval.target || approval.project) && (
              <span>
                {[approval.project && `Project: ${approval.project}`, approval.target && `Target: ${approval.target}`].filter(Boolean).join(" · ")}
              </span>
            )}
            <p>{approval.question}</p>
            {approval.commands && approval.commands.length > 0 && (
              <pre className="approval-commands">{approval.commands.join("\n")}</pre>
            )}
            {approval.resumeHint && <span>{approval.resumeHint}</span>}
            {approval.answer && <span>Answer: {approval.answer}</span>}
          </div>
          {!approval.decided && (
            <button className="secondary compact" type="button" onClick={() => onUseMessage(`I handled this setup blocker: ${approval.question}\n\n`)}>
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
    } catch (err) { onError(errorMessage(err)); } finally {
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
    } catch (err) { onError(errorMessage(err)); } finally {
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
      const message = errorMessage(err);
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
            const idle = formatWorkerIdle(status, worker?.updatedAt ?? node?.updatedAt ?? "");
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
                <button className="icon-button danger" disabled={!workerId || isTerminalWorkerStatus(status)} onClick={() => onCancel(workerId).catch((err) => onError(errorMessage(err)))} title="Cancel worker">
                  <CircleStop size={16} />
                </button>
                <div className="worker-context">
                  <WorkerContextItem label="Kind" value={kind} />
                  <WorkerContextItem label="Node" value={node?.id.slice(0, 8) ?? graphNode?.id.slice(0, 8) ?? "none"} />
                  <WorkerContextItem label="Target" value={targetLabel(node, graphNode)} />
                  <WorkerContextItem label="Updated" value={worker ? new Date(worker.updatedAt).toLocaleTimeString() : node ? new Date(node.updatedAt).toLocaleTimeString() : ""} />
                  {duration && <WorkerContextItem label="Duration" value={duration} />}
                  {idle && <WorkerContextItem label="Idle" value={idle} />}
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

function formatWorkerIdle(status: WorkerStatus, updatedAt: string): string {
  if (isTerminalWorkerStatus(status)) return "";
  const updatedMs = Date.parse(updatedAt);
  if (!Number.isFinite(updatedMs)) return "";
  const elapsed = Date.now() - updatedMs;
  if (elapsed < 30_000) return "";
  return formatElapsed(elapsed);
}

function formatElapsed(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return "";
  const seconds = Math.max(0, Math.round(milliseconds / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
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
        <WorkerMetaItem label="Idle" value={formatWorkerIdle(worker.status, worker.updatedAt)} />
      </div>
      {created && <FullCommand event={created} />}
      <WorkerPrompt worker={worker} created={created} />
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

function WorkerPrompt({ worker, created }: { worker: Worker; created: EventRecord | undefined }) {
  const payload = asRecord(created?.payload);
  const prompt = payloadValue(worker.prompt) || payloadValue(payload.prompt);
  const promptPath = payloadValue(worker.promptPath) || payloadValue(payload.promptPath);
  const promptError = payloadValue(worker.promptError) || payloadValue(payload.promptError);
  return (
    <section className="worker-section-card">
      <div className="section-title-row">
        <strong>Prompt</strong>
        {promptPath && <span className="tool-status neutral">{promptPath}</span>}
      </div>
      {prompt ? (
        <CodeBlock label="prompt" value={prompt} className="worker-prompt-block" />
      ) : (
        <p className="empty">{promptError ? `Prompt unavailable: ${promptError}` : "Prompt unavailable for this worker."}</p>
      )}
    </section>
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
  return (
    <section className="worker-section-card">
      <WorkspaceStateBlock title="Workspace" payload={payload} headerClassName="section-title-row" showUnknownBadges />
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

function WorkspaceStateBlock({ title, payload, headerClassName, showUnknownBadges = false }: { title: string; payload: Record<string, unknown>; headerClassName: string; showUnknownBadges?: boolean }) {
  const mode = payloadValue(payload.mode), vcsType = payloadValue(payload.vcsType);
  const cwd = payloadValue(payload.cwd || payload.root) || "unknown", root = payloadValue(payload.root);
  const dirty = payload.dirty === true || asRecord(payload.workspaceChanges).dirty === true;
  return (
    <>
      <div className={headerClassName}>
        <strong>{title}</strong>
        {(mode || showUnknownBadges) && <span className="tool-status neutral">{mode || "unknown"}</span>}
        {(vcsType || showUnknownBadges) && <span className="tool-status neutral">{vcsType || "vcs unknown"}</span>}
        <span className={dirty ? "tool-status warning" : "tool-status"}>{dirty ? "dirty" : "clean"}</span>
      </div>
      <div className="path-list">
        <PathRow label="CWD" value={cwd} />
        {root && root !== payloadValue(payload.cwd) && <PathRow label="Root" value={root} />}
        {payloadValue(payload.sourceRoot) && <PathRow label="Source" value={payloadValue(payload.sourceRoot)} />}
        {payloadValue(payload.workspaceName) && <PathRow label="Workspace" value={payloadValue(payload.workspaceName)} />}
      </div>
    </>
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
  if (isClaudeThinkingEvent(event)) {
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
  if (isClaudeRaw(raw)) {
    return isClaudeProgressRaw(raw);
  }
  const item = asRecord(raw.item);
  if (raw.type === "thread.started" || raw.type === "turn.started" || raw.type === "turn.completed") {
    return false;
  }
  return Boolean(payload.text || item.type === "agent_message" || item.type === "command_execution" || item.type === "file_change");
}

function workerEventLabel(event: EventRecord): string {
  if (event.type === "worker.output") {
    const payload = event.payload as { kind?: string; stream?: string; raw?: unknown; rawResult?: unknown };
    const raw = asRecord(payload.raw ?? payload.rawResult);
    const item = asRecord(raw.item);
    const claudeLabel = isClaudeRaw(raw) ? claudeWorkerEventLabel(raw, payload.kind) : "";
    if (claudeLabel) return claudeLabel;
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

function isClaudeThinkingEvent(event: EventRecord): boolean {
  if (event.type !== "worker.output") {
    return false;
  }
  const payload = asRecord(event.payload);
  const raw = asRecord(payload.raw ?? payload.rawResult);
  return isClaudeRaw(raw) && payloadValue(raw.type) === "assistant" && payloadValue(claudeMessageContent(raw)[0]?.type) === "thinking";
}

function TargetPanel({
  targets,
  onRegister,
  onUpdate,
  onDelete,
  onProbe,
  onError,
}: {
  targets: TargetState[];
  onRegister: (target: TargetInput) => Promise<void>;
  onUpdate: (id: string, target: TargetInput) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  onProbe: (id: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState("");
  const [busy, setBusy] = useState(false);
  const [id, setId] = useState("");
  const [kind, setKind] = useState("ssh");
  const [host, setHost] = useState("");
  const [user, setUser] = useState("");
  const [port, setPort] = useState("");
  const [identityFile, setIdentityFile] = useState("");
  const [insecureIgnoreHostKey, setInsecureIgnoreHostKey] = useState(false);
  const [checkoutRoot, setCheckoutRoot] = useState("");
  const [workRoot, setWorkRoot] = useState("");
  const [maxWorkers, setMaxWorkers] = useState("1");
  const [cpuWeight, setCpuWeight] = useState("1");
  const [memoryGB, setMemoryGB] = useState("");
  const [labelEntries, setLabelEntries] = useState<PluginConfigEntry[]>([]);
  const [probingTargetId, setProbingTargetId] = useState("");

  const reset = () => {
    setShowForm(false);
    setEditingId("");
    setId("");
    setKind("ssh");
    setHost("");
    setUser("");
    setPort("");
    setIdentityFile("");
    setInsecureIgnoreHostKey(false);
    setCheckoutRoot("");
    setWorkRoot("");
    setMaxWorkers("1");
    setCpuWeight("1");
    setMemoryGB("");
    setLabelEntries([]);
  };

  const edit = (target: TargetState) => {
    setEditingId(target.id);
    setId(target.id);
    setKind(target.kind || "ssh");
    setHost(target.host ?? "");
    setUser(target.user ?? "");
    setPort(target.port ? String(target.port) : "");
    setIdentityFile(target.identityFile ?? "");
    setInsecureIgnoreHostKey(Boolean(target.insecureIgnoreHostKey));
    setCheckoutRoot(target.checkoutRoot ?? target.workDir ?? "");
    setWorkRoot(target.workRoot ?? "");
    setMaxWorkers(String(target.capacity?.maxWorkers ?? 1));
    setCpuWeight(String(target.capacity?.cpuWeight ?? 1));
    setMemoryGB(target.capacity?.memoryGB ? String(target.capacity.memoryGB) : "");
    setLabelEntries(configEntriesFromRecord(target.labels));
    setShowForm(true);
  };

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      setBusy(true);
      const labels = configRecordFromEntries(labelEntries);
      const target: TargetInput = {
        id: id.trim(),
        kind,
        host: host.trim() || undefined,
        user: user.trim() || undefined,
        port: port.trim() ? Number(port) : undefined,
        identityFile: identityFile.trim() || undefined,
        insecureIgnoreHostKey,
        checkoutRoot: checkoutRoot.trim() || undefined,
        workRoot: workRoot.trim() || undefined,
        labels,
        capacity: {
          maxWorkers: Math.max(1, Number(maxWorkers) || 1),
          cpuWeight: Number(cpuWeight) || 1,
          memoryGB: memoryGB.trim() ? Number(memoryGB) : undefined,
        },
      };
      if (editingId) {
        await onUpdate(editingId, target);
      } else {
        await onRegister(target);
      }
      reset();
    } catch (err) { onError(errorMessage(err)); } finally {
      setBusy(false);
    }
  };

  const retryHealth = async (targetId: string) => {
    try {
      setProbingTargetId(targetId);
      await onProbe(targetId);
    } catch (err) { onError(errorMessage(err)); } finally {
      setProbingTargetId("");
    }
  };

  if (targets.length === 0) return null;
  return (
    <section className="panel plugin-panel">
      <div className="panel-title split-title">
        <span>
          <Activity size={18} />
          <h2>Targets</h2>
        </span>
        <button className="secondary compact" type="button" onClick={() => setShowForm((value) => !value)}>
          {showForm ? "Close" : "Add"}
        </button>
      </div>
      {showForm && (
        <form className="target-register" onSubmit={submit}>
          <label>
            ID
            <input value={id} onChange={(event) => setId(event.target.value)} placeholder="vm-1" required disabled={Boolean(editingId)} />
          </label>
          <label>
            Kind
            <select value={kind} onChange={(event) => setKind(event.target.value)}>
              <option value="ssh">SSH</option>
              <option value="local">Local</option>
            </select>
          </label>
          <label>
            Host
            <input value={host} onChange={(event) => setHost(event.target.value)} placeholder="vm.example.com" disabled={kind === "local"} />
          </label>
          <label>
            User
            <input value={user} onChange={(event) => setUser(event.target.value)} placeholder="aged" disabled={kind === "local"} />
          </label>
          <label>
            Port
            <input value={port} onChange={(event) => setPort(event.target.value)} inputMode="numeric" placeholder="22" disabled={kind === "local"} />
          </label>
          <label>
            Identity file (optional)
            <input value={identityFile} onChange={(event) => setIdentityFile(event.target.value)} placeholder="Blank uses ssh-agent or ~/.ssh/config" disabled={kind === "local"} />
          </label>
          <label className="target-wide-field">
            Checkout root
            <input value={checkoutRoot} onChange={(event) => setCheckoutRoot(event.target.value)} placeholder="/srv/aged/checkouts" />
          </label>
          <label className="target-wide-field">
            Work root
            <input value={workRoot} onChange={(event) => setWorkRoot(event.target.value)} placeholder="/srv/aged/runs" disabled={kind === "local"} />
          </label>
          <label>
            Max workers
            <input value={maxWorkers} onChange={(event) => setMaxWorkers(event.target.value)} inputMode="numeric" />
          </label>
          <label>
            CPU weight
            <input value={cpuWeight} onChange={(event) => setCpuWeight(event.target.value)} inputMode="decimal" />
          </label>
          <label>
            Memory GB
            <input value={memoryGB} onChange={(event) => setMemoryGB(event.target.value)} inputMode="decimal" placeholder="32" />
          </label>
          <fieldset className="target-label-field">
            <legend>Labels</legend>
            <KeyValueRows entries={labelEntries} setEntries={setLabelEntries} emptyText="No labels" keyPlaceholder="location" valuePlaceholder="remote" removeTitle="Remove label" addLabel="Add label" />
          </fieldset>
          <div className="plugin-form-footer">
            <label className="checkbox-label">
              <input type="checkbox" checked={insecureIgnoreHostKey} onChange={(event) => setInsecureIgnoreHostKey(event.target.checked)} disabled={kind === "local"} />
              Ignore host key
            </label>
            <div className="plugin-form-actions">
              <button type="submit" disabled={busy}>{editingId ? "Update" : "Register"}</button>
              <button type="button" className="secondary" onClick={reset}>Cancel</button>
            </div>
          </div>
        </form>
      )}
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
            {target.kind === "ssh" && (
              <p className="target-location">{[target.user, target.host].filter(Boolean).join("@") || target.host}{target.checkoutRoot || target.workDir ? ` · ${target.checkoutRoot ?? target.workDir}` : ""}</p>
            )}
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
            {target.health && (
              <div className="target-debug">
                <span>reachable: {healthFlag(target.health.reachable)}</span>
                <span>tmux: {healthFlag(target.health.tmux)}</span>
                <span>repo: {healthFlag(target.health.repoPresent)}</span>
                {target.health.checkedAt && <span>checked: {new Date(target.health.checkedAt).toLocaleTimeString()}</span>}
              </div>
            )}
            <div className="plugin-card-actions">
              <button className="secondary" disabled={probingTargetId === target.id} onClick={() => retryHealth(target.id)}>
                <RefreshCw size={14} />
                Health
              </button>
              <button className="secondary" onClick={() => edit(target)}>Edit</button>
              <button
                className="secondary danger-text"
                disabled={target.running > 0 || targets.length <= 1}
                onClick={() => onDelete(target.id).catch((err) => onError(errorMessage(err)))}
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

function formatMB(value: number): string {
  if (value >= 1024) return `${(value / 1024).toFixed(value >= 10240 ? 0 : 1)} GB`;
  return `${Math.round(value)} MB`;
}

function healthFlag(value?: boolean): string {
  if (value === true) return "yes";
  if (value === false) return "no";
  return "unknown";
}

const SYSTEM_PLUGIN_IDS = new Set([
  "brain:prompt",
  "brain:codex",
  "brain:claude",
  "brain:api",
  "runner:codex",
  "runner:claude",
  "runner:shell",
  "runner:benchmark_compare",
  "driver:http",
  "driver:github",
  "driver:discord",
]);

const PROMPT_TEMPLATE_KEYS = ["system", "plan", "github_review_request", "replan", "completion_review", "publication_review"];

function PromptSetPanel({
  promptSets,
  onRegister,
  onUpdate,
  onDelete,
  onError,
}: {
  promptSets: PromptSet[];
  onRegister: (promptSet: PromptSet) => Promise<void>;
  onUpdate: (id: string, promptSet: PromptSet) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [editingId, setEditingId] = useState("");
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [makeDefault, setMakeDefault] = useState(false);
  const [templates, setTemplates] = useState<Record<string, string>>({});
  const [showForm, setShowForm] = useState(false);
  const [busy, setBusy] = useState(false);

  const reset = () => {
    setEditingId("");
    setId("");
    setName("");
    setDescription("");
    setMakeDefault(false);
    setTemplates({});
    setShowForm(false);
  };

  const edit = (promptSet: PromptSet) => {
    if (promptSet.builtIn) return;
    setEditingId(promptSet.id);
    setId(promptSet.id);
    setName(promptSet.name);
    setDescription(promptSet.description ?? "");
    setMakeDefault(Boolean(promptSet.default));
    setTemplates(promptSet.templates ?? {});
    setShowForm(true);
  };

  const copy = (promptSet: PromptSet) => {
    setEditingId("");
    setId("");
    setName(`${promptSet.name} Custom`);
    setDescription(promptSet.description ?? "");
    setMakeDefault(false);
    setTemplates(promptSet.templates ?? {});
    setShowForm(true);
  };

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      setBusy(true);
      const cleanTemplates = Object.fromEntries(Object.entries(templates).map(([key, value]) => [key, value.trim()]).filter(([, value]) => value));
      const promptSet: PromptSet = {
        id: id.trim(),
        name: name.trim() || id.trim(),
        description: description.trim() || undefined,
        templates: cleanTemplates,
        default: makeDefault,
      };
      if (editingId) await onUpdate(editingId, promptSet);
      else await onRegister(promptSet);
      reset();
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel plugin-panel">
      <div className="panel-title split-title">
        <span>
          <FileText size={18} />
          <h2>Prompt Sets</h2>
        </span>
        <button className="secondary compact" type="button" onClick={() => setShowForm((value) => !value)}>
          {showForm ? "Close" : "Add"}
        </button>
      </div>
      {showForm && (
        <form className="plugin-register" onSubmit={submit}>
          <label>
            ID
            <input value={id} onChange={(event) => setId(event.target.value)} placeholder="perf" required disabled={Boolean(editingId)} />
          </label>
          <label>
            Name
            <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Performance" />
          </label>
          <label className="plugin-command-field">
            Description
            <input value={description} onChange={(event) => setDescription(event.target.value)} placeholder="Planner and replanner prompt overrides" />
          </label>
          {PROMPT_TEMPLATE_KEYS.map((key) => (
            <label className="plugin-command-field" key={key}>
              {humanizeKey(key)}
              <textarea value={templates[key] ?? ""} onChange={(event) => setTemplates((current) => ({ ...current, [key]: event.target.value }))} placeholder={`Optional ${key} template. Use {{system}}, {{input_json}}, {{task_json}}, {{task_prompt}}.`} rows={5} />
            </label>
          ))}
          <div className="plugin-form-footer">
            <label className="checkbox-label">
              <input type="checkbox" checked={makeDefault} onChange={(event) => setMakeDefault(event.target.checked)} />
              Default
            </label>
            <div className="plugin-form-actions">
              <button type="submit" disabled={busy}>{editingId ? "Update" : "Create"}</button>
              <button type="button" className="secondary" onClick={reset}>Cancel</button>
            </div>
          </div>
        </form>
      )}
      <div className="plugin-grid">
        {promptSets.map((promptSet) => (
          <article key={promptSet.id} className={["plugin-card", promptSet.builtIn ? "system" : ""].filter(Boolean).join(" ")}>
            <div>
              <strong>{promptSet.name}</strong>
              <small>{promptSet.id}</small>
            </div>
            <div className="plugin-status-row">
              {promptSet.default && <span className="tool-status">default</span>}
              {promptSet.builtIn && <span className="tool-status neutral">built in</span>}
              <span className="tool-status neutral">{Object.keys(promptSet.templates ?? {}).length} templates</span>
            </div>
            {promptSet.description && <p>{promptSet.description}</p>}
            <div className="plugin-card-actions">
              {promptSet.builtIn ? (
                <>
                  <span className="plugin-system-note">Built-in fallback</span>
                  <button className="secondary" onClick={() => copy(promptSet)}>Copy</button>
                </>
              ) : (
                <>
                  <button className="secondary" onClick={() => edit(promptSet)}>Edit</button>
                  <button className="secondary danger-text" onClick={() => onDelete(promptSet.id).catch((err) => onError(errorMessage(err)))}>
                    <Trash2 size={14} />
                  </button>
                </>
              )}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

type PluginConfigEntry = {
  id: string;
  key: string;
  value: string;
};
type KeyValueRowsProps = { entries: PluginConfigEntry[]; setEntries: React.Dispatch<React.SetStateAction<PluginConfigEntry[]>>; emptyText: string; keyPlaceholder: string; valuePlaceholder: string; removeTitle: string; addLabel: string };
type RecordFromEntriesMessages = { missingKey: string; duplicateKey: (key: string) => string; missingValue?: (key: string) => string };

function pluginConfigEntry(key = "", value = ""): PluginConfigEntry {
  return { id: `${Date.now()}-${Math.random()}`, key, value };
}

function configEntriesFromRecord(config?: Record<string, string>): PluginConfigEntry[] {
  return Object.entries(config ?? {}).map(([key, value]) => pluginConfigEntry(key, value ?? ""));
}

function updateKeyValueEntry(setEntries: React.Dispatch<React.SetStateAction<PluginConfigEntry[]>>, entryId: string, values: Partial<PluginConfigEntry>) {
  setEntries((entries) => entries.map((entry) => entry.id === entryId ? { ...entry, ...values } : entry));
}

function KeyValueRows({
  entries,
  setEntries,
  emptyText,
  keyPlaceholder,
  valuePlaceholder,
  removeTitle,
  addLabel,
}: KeyValueRowsProps) {
  return (
    <>
      {entries.length === 0 ? <p className="plugin-config-empty">{emptyText}</p> : (
        <div className="plugin-config-list">
          {entries.map((entry) => (
            <div className="plugin-config-row" key={entry.id}>
              <input value={entry.key} onChange={(event) => updateKeyValueEntry(setEntries, entry.id, { key: event.target.value })} placeholder={keyPlaceholder} />
              <input value={entry.value} onChange={(event) => updateKeyValueEntry(setEntries, entry.id, { value: event.target.value })} placeholder={valuePlaceholder} />
              <button type="button" className="icon-button ghost danger-text" onClick={() => setEntries((items) => items.filter((item) => item.id !== entry.id))} title={removeTitle}>
                <Trash2 size={14} />
              </button>
            </div>
          ))}
        </div>
      )}
      <button type="button" className="secondary compact" onClick={() => setEntries((items) => [...items, pluginConfigEntry()])}>{addLabel}</button>
    </>
  );
}

function recordFromEntries(entries: PluginConfigEntry[], messages: RecordFromEntriesMessages): Record<string, string> {
  const out: Record<string, string> = {};
  for (const entry of entries) {
    const key = entry.key.trim();
    const value = entry.value.trim();
    if (!key && !value) continue;
    if (!key) throw new Error(messages.missingKey);
    if (!value && messages.missingValue) throw new Error(messages.missingValue(key));
    if (Object.prototype.hasOwnProperty.call(out, key)) throw new Error(messages.duplicateKey(key));
    out[key] = value;
  }
  return out;
}

function configRecordFromEntries(entries: PluginConfigEntry[]): Record<string, string> {
  return recordFromEntries(entries, {
    missingKey: "config fields need a key",
    duplicateKey: (key) => `duplicate config key ${key}`,
  });
}

function remoteCheckoutRecordFromEntries(entries: PluginConfigEntry[]): Record<string, string> {
  return recordFromEntries(entries, {
    missingKey: "remote checkout entries need a target id",
    missingValue: (targetID) => `remote checkout ${targetID} needs a path`,
    duplicateKey: (targetID) => `duplicate remote checkout target ${targetID}`,
  });
}

function splitCommaList(value: string): string[] {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function isSystemPlugin(plugin: Plugin): boolean {
  return Boolean(plugin.builtIn) || SYSTEM_PLUGIN_IDS.has(plugin.id);
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
  const systemPlugins = plugins.filter(isSystemPlugin);
  const customPlugins = plugins.filter((plugin) => !isSystemPlugin(plugin));
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [kind, setKind] = useState("runner");
  const [protocol, setProtocol] = useState("aged-runner-v1");
  const [command, setCommand] = useState("");
  const [enabledValue, setEnabledValue] = useState(true);
  const [configEntries, setConfigEntries] = useState<PluginConfigEntry[]>([]);
  const [editingId, setEditingId] = useState("");
  const [busy, setBusy] = useState(false);
  const [showForm, setShowForm] = useState(false);

  const reset = () => {
    setId("");
    setName("");
    setKind("runner");
    setProtocol("aged-runner-v1");
    setCommand("");
    setEnabledValue(true);
    setConfigEntries([]);
    setEditingId("");
    setShowForm(false);
  };

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      setBusy(true);
      const parsedConfig = configRecordFromEntries(configEntries);
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
    } catch (err) { onError(errorMessage(err)); } finally {
      setBusy(false);
    }
  };

  const edit = (plugin: Plugin) => {
    if (isSystemPlugin(plugin)) return;
    setEditingId(plugin.id);
    setId(plugin.id);
    setName(plugin.name ?? "");
    setKind(plugin.kind || "runner");
    setProtocol(plugin.protocol || (plugin.kind === "runner" ? "aged-runner-v1" : "aged-plugin-v1"));
    setCommand(plugin.command?.join(" ") ?? "");
    setEnabledValue(plugin.enabled);
    setConfigEntries(configEntriesFromRecord(plugin.config));
    setShowForm(true);
  };

  const renderPluginCards = (items: Plugin[]) => (
    <div className="plugin-grid">
      {items.map((plugin) => {
        const system = isSystemPlugin(plugin);
        return (
          <article key={plugin.id} className={[plugin.enabled ? "plugin-card" : "plugin-card disabled", system ? "system" : ""].filter(Boolean).join(" ")}>
            <div>
              <strong>{plugin.name}</strong>
              <small>{plugin.id}</small>
            </div>
            <div className="plugin-status-row">
              <span className={plugin.enabled ? "tool-status" : "tool-status neutral"}>{plugin.kind}</span>
              {system && <span className="tool-status neutral">system</span>}
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
              {system ? (
                <span className="plugin-system-note">Built in</span>
              ) : (
                <>
                  <button className="secondary" onClick={() => edit(plugin)}>Edit</button>
                  <button
                    className="secondary danger-text"
                    onClick={() => onDelete(plugin.id).catch((err) => onError(errorMessage(err)))}
                  >
                    <Trash2 size={14} />
                  </button>
                </>
              )}
            </div>
          </article>
        );
      })}
    </div>
  );

  return (
    <section className="panel plugin-panel">
      <div className="panel-title split-title">
        <span>
          <Puzzle size={18} />
          <h2>Plugins</h2>
        </span>
        <span className="plugin-title-actions">
          <span className="pill">{enabled}/{plugins.length} enabled</span>
          <button className="secondary compact" type="button" onClick={() => setShowForm((value) => !value)}>
            {showForm ? "Close" : "Add"}
          </button>
        </span>
      </div>
      {showForm && (
        <form className="plugin-register" onSubmit={submit}>
          <label>
            ID
            <input value={id} onChange={(event) => setId(event.target.value)} placeholder="runner:lint" required disabled={Boolean(editingId)} />
          </label>
          <label>
            Name
            <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Lint runner" />
          </label>
          <label>
            Kind
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
          </label>
          <label>
            Protocol
            <input value={protocol} onChange={(event) => setProtocol(event.target.value)} placeholder="aged-runner-v1" />
          </label>
          <label className="plugin-command-field">
            Command
            <input value={command} onChange={(event) => setCommand(event.target.value)} placeholder="command arg..." />
          </label>
          <fieldset className="plugin-config-field">
            <legend>Config</legend>
            <KeyValueRows entries={configEntries} setEntries={setConfigEntries} emptyText="No config fields" keyPlaceholder="restart" valuePlaceholder="on_failure" removeTitle="Remove config field" addLabel="Add field" />
          </fieldset>
          <div className="plugin-form-footer">
            <label className="checkbox-label">
              <input type="checkbox" checked={enabledValue} onChange={(event) => setEnabledValue(event.target.checked)} />
              Enabled
            </label>
            <div className="plugin-form-actions">
              <button type="submit" disabled={busy}>{editingId ? "Update" : "Register"}</button>
              <button type="button" className="secondary" onClick={reset}>Cancel</button>
            </div>
          </div>
        </form>
      )}
      <details className="plugin-inventory">
        <summary>
          <span>Plugin inventory</span>
          <small>{customPlugins.length} custom · {systemPlugins.length} system</small>
        </summary>
        <div className="plugin-section">
          <div className="plugin-section-title">Custom</div>
          {customPlugins.length > 0 ? renderPluginCards(customPlugins) : <p className="empty-state">No custom plugins registered.</p>}
        </div>
        <div className="plugin-section">
          <div className="plugin-section-title">System</div>
          {renderPluginCards(systemPlugins)}
        </div>
      </details>
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
  checksConclusion?: string;
  mergeStatus?: string;
  mergeable?: string;
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

function latestTaskStatusError(events: EventRecord[]): string {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event.type !== "task.status") continue;
    const payload = asRecord(event.payload);
    const error = payloadValue(payload.error);
    if (error) return error;
    if (payloadValue(payload.status) !== "failed") return "";
  }
  return "";
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
        summary: payloadValue(payload.summary),
        target: payloadValue(payload.target || payload.targetId),
        project: payloadValue(payload.project || payload.projectId),
        resumeHint: payloadValue(payload.resumeHint),
        commands: payloadStringArray(payload.commands),
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
  const claudeEvent = isClaudeRaw(raw) ? structuredClaudeWorkerEvent(payload, raw) : null;
  if (claudeEvent) {
    return claudeEvent;
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

function structuredClaudeWorkerEvent(payload: Record<string, unknown>, raw: Record<string, unknown>): React.ReactNode {
  switch (payloadValue(raw.type)) {
    case "assistant":
      return <ClaudeAssistantCard payload={payload} raw={raw} />;
    case "user":
      return <ClaudeToolResultCard payload={payload} raw={raw} />;
    case "system":
      return <ClaudeSystemCard payload={payload} raw={raw} />;
    case "rate_limit_event":
      return <ClaudeRateLimitCard raw={raw} />;
    case "result":
      return <ClaudeResultCard payload={payload} raw={raw} />;
    default:
      return null;
  }
}

function ClaudeAssistantCard({ payload, raw }: { payload: Record<string, unknown>; raw: Record<string, unknown> }) {
  const parts = claudeMessageContent(raw);
  const toolUse = parts.find((part) => payloadValue(part.type) === "tool_use");
  const textPart = parts.find((part) => payloadValue(part.type) === "text");
  const thinkingPart = parts.find((part) => payloadValue(part.type) === "thinking");
  const model = payloadValue(asRecord(raw.message).model);

  if (toolUse) {
    const input = asRecord(toolUse.input);
    const toolName = payloadValue(toolUse.name) || "Tool";
    const description = payloadValue(input.description);
    const command = payloadValue(input.command);
    const todos = Array.isArray(input.todos) ? input.todos.map(asRecord) : [];
    return (
      <div className="tool-card">
        <div className="tool-card-header">
          <strong>{toolName}</strong>
          <span className="tool-status neutral">tool use</span>
          {model && <span className="tool-status neutral">{model}</span>}
        </div>
        {description && <p>{description}</p>}
        {command && <CodeBlock label="command" value={command} className="shell-script" />}
        {todos.length > 0 && <ClaudeTodoList todos={todos} />}
        <RawPayloadDetails value={raw} />
      </div>
    );
  }

  if (textPart) {
    return (
      <div className="agent-message-card">
        <div className="tool-card-header">
          <strong>Claude Message</strong>
          <span className="tool-status">{payloadValue(payload.kind) || "message"}</span>
          {model && <span className="tool-status neutral">{model}</span>}
        </div>
        <ReadableBlock label="Message" value={payloadValue(textPart.text) || payloadValue(payload.text)} className="agent-message-body" limit={1600} />
        <RawPayloadDetails value={raw} />
      </div>
    );
  }

  if (thinkingPart) {
    return (
      <div className="lifecycle-card compact-card">
        <div className="tool-card-header">
          <strong>Claude Thinking</strong>
          <span className="tool-status neutral">collapsed</span>
          {model && <span className="tool-status neutral">{model}</span>}
        </div>
        <p>Thinking block recorded.</p>
        <RawPayloadDetails value={raw} />
      </div>
    );
  }

  return <LifecycleCard raw={raw} />;
}

function ClaudeToolResultCard({ payload, raw }: { payload: Record<string, unknown>; raw: Record<string, unknown> }) {
  const content = claudeMessageContent(raw).find((part) => payloadValue(part.type) === "tool_result") ?? {};
  const result = asRecord(raw.tool_use_result);
  const failed = content.is_error === true || result.is_error === true;
  const stdout = payloadValue(result.stdout);
  const stderr = payloadValue(result.stderr);
  const contentText = payloadValue(content.content);
  const backgroundTaskId = payloadValue(result.backgroundTaskId);
  const interrupted = result.interrupted === true;
  const summaryText = claudeToolResultSummary(stdout || stderr || contentText, failed);
  return (
    <div className={failed ? "tool-card failed" : "tool-card"}>
      <div className="tool-card-header">
        <strong>Tool Result</strong>
        <span className={failed ? "tool-status failed" : "tool-status"}>{failed ? "failed" : "ok"}</span>
        {backgroundTaskId && <span className="tool-status neutral">background {backgroundTaskId}</span>}
        {interrupted && <span className="tool-status warning">interrupted</span>}
      </div>
      {summaryText && <p>{summaryText}</p>}
      {stdout && <ReadableBlock label="Stdout" value={stdout} className="tool-output" />}
      {stderr && <ReadableBlock label="Stderr" value={stderr} className={failed ? "tool-output failed" : "tool-output"} />}
      {!stdout && !stderr && contentText && <ReadableBlock label="Result" value={contentText} className={failed ? "tool-output failed" : "tool-output"} />}
      {!stdout && !stderr && !contentText && <p>{payloadValue(payload.text) || "Tool completed."}</p>}
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function ClaudeSystemCard({ payload, raw }: { payload: Record<string, unknown>; raw: Record<string, unknown> }) {
  const subtype = payloadValue(raw.subtype) || "system";
  const tools = Array.isArray(raw.tools) ? raw.tools.length : 0;
  const fields = [
    { label: "CWD", value: payloadValue(raw.cwd) },
    { label: "Model", value: payloadValue(raw.model) },
    { label: "Version", value: payloadValue(raw.claude_code_version) },
    { label: "Permission", value: payloadValue(raw.permissionMode) },
    { label: "Task", value: payloadValue(raw.task_id) },
    { label: "Description", value: payloadValue(raw.description) || payloadValue(payload.text) },
    { label: "Tools", value: tools ? String(tools) : "" },
  ].filter((field) => field.value);
  return (
    <div className="lifecycle-card">
      <div className="tool-card-header">
        <strong>Claude {humanizeKey(subtype)}</strong>
        <span className="tool-status neutral">system</span>
      </div>
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
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function ClaudeRateLimitCard({ raw }: { raw: Record<string, unknown> }) {
  const info = asRecord(raw.rate_limit_info);
  const resetsAt = Number(info.resetsAt);
  const resetText = Number.isFinite(resetsAt) ? new Date(resetsAt * 1000).toLocaleTimeString() : "";
  return (
    <div className="lifecycle-card compact-card">
      <div className="tool-card-header">
        <strong>Rate Limit</strong>
        {payloadValue(info.status) && <span className="tool-status">{payloadValue(info.status)}</span>}
        {payloadValue(info.rateLimitType) && <span className="tool-status neutral">{payloadValue(info.rateLimitType)}</span>}
        {resetText && <span className="tool-status neutral">resets {resetText}</span>}
      </div>
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function ClaudeResultCard({ payload, raw }: { payload: Record<string, unknown>; raw: Record<string, unknown> }) {
  const failed = raw.is_error === true || payloadValue(raw.subtype) !== "success";
  return (
    <div className={failed ? "completion-card failed" : "completion-card"}>
      <div className="tool-card-header">
        <strong>Claude Result</strong>
        <span className={failed ? "tool-status failed" : "tool-status"}>{payloadValue(raw.subtype) || payloadValue(payload.kind) || "result"}</span>
        {payloadValue(raw.total_cost_usd) && <span className="tool-status neutral">${payloadValue(raw.total_cost_usd)}</span>}
      </div>
      <ReadableBlock label="Result" value={payloadValue(raw.result) || payloadValue(payload.text)} className={failed ? "tool-output failed" : "agent-message-body"} limit={1600} />
      <RawPayloadDetails value={raw} />
    </div>
  );
}

function ClaudeTodoList({ todos }: { todos: Record<string, unknown>[] }) {
  return (
    <ul className="claude-todo-list">
      {todos.slice(0, 8).map((todo, index) => (
        <li key={index}>
          <code>{payloadValue(todo.status) || "todo"}</code>
          <span>{payloadValue(todo.content || todo.activeForm)}</span>
        </li>
      ))}
    </ul>
  );
}

function claudeMessageContent(raw: Record<string, unknown>): Record<string, unknown>[] {
  const message = asRecord(raw.message);
  return Array.isArray(message.content) ? message.content.map(asRecord) : [];
}

function claudeToolResultSummary(value: string, failed: boolean): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  if (parseJSONPayload(trimmed) !== undefined) {
    return failed ? "JSON result, failed" : "JSON result";
  }
  if (failed) {
    return firstUsefulLine(trimmed) || summarizeText(trimmed);
  }
  return summarizeText(trimmed);
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
  return (
    <div className="lifecycle-card">
      <WorkspaceStateBlock title="Workspace Ready" payload={payload} headerClassName="tool-card-header" />
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

function ReadableBlock({ label, value, className, limit = 2400 }: { label: string; value: string; className?: string; limit?: number }) {
  const json = parseJSONPayload(value);
  if (json !== undefined) {
    return (
      <details className="event-raw compact json-blob">
        <summary>{label} JSON blob</summary>
        <pre className={className}>{prettyPayload(json)}</pre>
      </details>
    );
  }
  return <TruncatedBlock label={label} value={value} className={className} limit={limit} />;
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

function parseJSONPayload(value: string): unknown {
  const trimmed = value.trim();
  if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return undefined;
  }
}

function summarizeText(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "empty output";
  if (parseJSONPayload(trimmed) !== undefined) return "JSON output";
  const lines = trimmed.split(/\r?\n/).filter((line) => line.trim() !== "");
  const chars = trimmed.length;
  const lineText = lines.length === 1 ? "1 line" : `${lines.length} lines`;
  const first = firstUsefulLine(trimmed);
  if (!first) return `${lineText}, ${chars} chars`;
  return first.length > 80 ? `${lineText}, ${chars} chars` : `${lineText}: ${first}`;
}

function firstUsefulLine(value: string): string {
  const line = value.split(/\r?\n/).map((item) => item.trim()).find(Boolean) ?? "";
  return line.length > 120 ? `${line.slice(0, 117)}...` : line;
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
    const claudeText = isClaudeRaw(raw) ? claudeEventDisplayText(payload, raw) : "";
    if (claudeText) return claudeText;
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

function isClaudeProgressRaw(raw: Record<string, unknown>): boolean {
  switch (payloadValue(raw.type)) {
    case "assistant": {
      const type = payloadValue(claudeMessageContent(raw)[0]?.type);
      return type === "tool_use" || type === "text";
    }
    case "user":
      return payloadValue(claudeMessageContent(raw)[0]?.type) === "tool_result";
    case "result":
      return true;
    default:
      return false;
  }
}

function isClaudeRaw(raw: Record<string, unknown>): boolean {
  switch (payloadValue(raw.type)) {
    case "assistant":
    case "user":
    case "system":
    case "rate_limit_event":
      return true;
    case "result":
      return raw.subtype !== undefined || raw.total_cost_usd !== undefined || raw.is_error !== undefined;
    default:
      return false;
  }
}

function claudeWorkerEventLabel(raw: Record<string, unknown>, kind: string | undefined): string {
  switch (payloadValue(raw.type)) {
    case "assistant": {
      const part = claudeMessageContent(raw)[0] ?? {};
      const partType = payloadValue(part.type);
      if (partType === "tool_use") return payloadValue(part.name) || kind || "tool";
      if (partType === "text") return "message";
      if (partType === "thinking") return "thinking";
      return "assistant";
    }
    case "user":
      return "result";
    case "system":
      return payloadValue(raw.subtype) || "system";
    case "rate_limit_event":
      return "rate-limit";
    case "result":
      return "result";
    default:
      return "";
  }
}

function claudeEventDisplayText(payload: Record<string, unknown>, raw: Record<string, unknown>): string {
  switch (payloadValue(raw.type)) {
    case "assistant": {
      const part = claudeMessageContent(raw)[0] ?? {};
      const partType = payloadValue(part.type);
      if (partType === "tool_use") {
        const input = asRecord(part.input);
        const name = payloadValue(part.name) || "Tool";
        const description = payloadValue(input.description);
        const command = payloadValue(input.command);
        return description ? `${name}: ${description}` : command ? `${name}: ${command}` : `${name} tool use`;
      }
      if (partType === "text") return payloadValue(part.text) || payloadValue(payload.text);
      if (partType === "thinking") return "Claude thinking block";
      return payloadValue(payload.text);
    }
    case "user": {
      const result = asRecord(raw.tool_use_result);
      const backgroundTaskId = payloadValue(result.backgroundTaskId);
      if (backgroundTaskId) return `Started background task ${backgroundTaskId}`;
      const stdout = payloadValue(result.stdout).trim();
      const stderr = payloadValue(result.stderr).trim();
      if (stderr) return `Tool failed: ${firstUsefulLine(stderr) || summarizeText(stderr)}`;
      if (stdout) return `Tool result: ${summarizeText(stdout)}`;
      const content = payloadValue(claudeMessageContent(raw)[0]?.content) || payloadValue(payload.text);
      return content ? `Tool result: ${summarizeText(content)}` : "Tool completed";
    }
    case "system":
      return payloadValue(raw.description) || payloadValue(payload.text) || `Claude ${payloadValue(raw.subtype) || "system event"}`;
    case "rate_limit_event": {
      const info = asRecord(raw.rate_limit_info);
      return [`Rate limit`, payloadValue(info.status), payloadValue(info.rateLimitType)].filter(Boolean).join(" ");
    }
    case "result":
      return payloadValue(raw.result) || payloadValue(payload.text);
    default:
      return "";
  }
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

function payloadStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map(payloadValue).filter(Boolean);
}

function latestTimestamp(left: string | undefined, right: string): string {
  if (!left) return right;
  const leftMs = Date.parse(left);
  const rightMs = Date.parse(right);
  if (!Number.isFinite(leftMs) || !Number.isFinite(rightMs)) return left;
  return rightMs > leftMs ? right : left;
}

function workerFromCreatedEvent(existing: Worker | undefined, event: EventRecord, payload: Record<string, unknown>): Worker {
  const prompt = payloadValue(payload.prompt);
  const promptPath = payloadValue(payload.promptPath);
  const promptError = payloadValue(payload.promptError);
  const metadata = isRecord(payload.metadata) ? { ...(existing?.metadata ?? {}), ...payload.metadata } : existing?.metadata;
  return {
    id: event.workerId ?? existing?.id ?? "",
    taskId: event.taskId ?? existing?.taskId ?? "",
    kind: payloadValue(payload.kind) || existing?.kind || "unknown",
    status: existing?.status ?? "queued",
    command: Array.isArray(payload.command) ? payload.command.map(String) : existing?.command,
    prompt: prompt || existing?.prompt,
    promptPath: promptPath || existing?.promptPath,
    promptError: promptError || existing?.promptError,
    createdAt: existing?.createdAt || event.at,
    updatedAt: latestTimestamp(existing?.updatedAt, event.at),
    metadata,
  };
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
  return <span className={`status ${value}`}>{humanizeKey(value)}</span>;
}

function normalizeSnapshot(snapshot: Snapshot): AppSnapshot {
  const executionNodes = snapshot.executionNodes ?? [];
  const tasks = snapshot.tasks ?? [];
  const lastEventId = snapshot.lastEventId ?? snapshot.events?.at(-1)?.id ?? 0;
  return {
    campaigns: snapshot.campaigns ?? [],
    tasks,
    workers: snapshot.workers ?? [],
    executionNodes,
    targets: snapshot.targets ?? [],
    plugins: snapshot.plugins ?? [],
    promptSets: snapshot.promptSets ?? [],
    projects: snapshot.projects ?? [],
    pullRequests: snapshot.pullRequests ?? [],
    orchestrationGraphs: snapshot.orchestrationGraphs ?? deriveOrchestrationGraphs(tasks, executionNodes),
    lastEventId,
    snapshotEventId: lastEventId,
    events: snapshot.events ?? [],
  };
}

function upsertTask(snapshot: AppSnapshot, task: Task): AppSnapshot {
  const tasks = snapshot.tasks.some((candidate) => candidate.id === task.id)
    ? snapshot.tasks.map((candidate) => (candidate.id === task.id ? task : candidate))
    : [...snapshot.tasks, task];
  return { ...snapshot, tasks };
}

function upsertCampaign(snapshot: AppSnapshot, campaign: Campaign): AppSnapshot {
  return { ...snapshot, campaigns: upsertById(snapshot.campaigns, campaign) };
}

function upsertProject(snapshot: AppSnapshot, project: Project): AppSnapshot {
  const projects = snapshot.projects.some((candidate) => candidate.id === project.id)
    ? snapshot.projects.map((candidate) => (candidate.id === project.id ? project : candidate))
    : [...snapshot.projects, project];
  return { ...snapshot, projects };
}

function removeProjectFromSnapshot(snapshot: AppSnapshot, projectId: string): AppSnapshot {
  return {
    ...snapshot,
    projects: snapshot.projects.filter((project) => project.id !== projectId),
  };
}

function mergeTaskSnapshot(snapshot: AppSnapshot, taskSnapshot: AppSnapshot): AppSnapshot {
  const taskIds = new Set(taskSnapshot.tasks.map((task) => task.id));
  if (taskIds.size === 0) return snapshot;
  const tasks = [
    ...snapshot.tasks.filter((task) => !taskIds.has(task.id)),
    ...taskSnapshot.tasks,
  ].sort((left, right) => Date.parse(left.createdAt) - Date.parse(right.createdAt));
  return {
    ...snapshot,
    campaigns: mergeById(snapshot.campaigns, taskSnapshot.campaigns),
    tasks,
    workers: [
      ...snapshot.workers.filter((worker) => !taskIds.has(worker.taskId)),
      ...taskSnapshot.workers,
    ],
    executionNodes: [
      ...snapshot.executionNodes.filter((node) => !taskIds.has(node.taskId)),
      ...taskSnapshot.executionNodes,
    ],
    pullRequests: [
      ...snapshot.pullRequests.filter((pr) => !taskIds.has(pr.taskId)),
      ...taskSnapshot.pullRequests,
    ],
    orchestrationGraphs: [
      ...snapshot.orchestrationGraphs.filter((graph) => !taskIds.has(graph.taskId)),
      ...taskSnapshot.orchestrationGraphs,
    ],
    lastEventId: Math.max(snapshot.lastEventId, taskSnapshot.lastEventId),
  };
}

function reduceEvent(snapshot: AppSnapshot, event: EventRecord): AppSnapshot {
  if (snapshot.events.some((existing) => existing.id === event.id)) {
    return snapshot;
  }
  return applyProjectionEvent({
    ...snapshot,
    events: mergeEvents(snapshot.events, [event]),
    lastEventId: Math.max(snapshot.lastEventId, event.id),
  }, event);
}

function applyTaskHistoryEvents(snapshot: AppSnapshot, events: EventRecord[]): AppSnapshot {
  const existingEventIds = new Set(snapshot.events.map((event) => event.id));
  let next = {
    ...snapshot,
    events: mergeEvents(snapshot.events, events),
    lastEventId: Math.max(snapshot.lastEventId, maxEventId(events)),
  };
  for (const event of [...events].sort((left, right) => left.id - right.id)) {
    if (event.id <= snapshot.snapshotEventId || existingEventIds.has(event.id)) {
      continue;
    }
    next = applyProjectionEvent(next, event);
  }
  return next;
}

function mergeEvents(current: EventRecord[], next: EventRecord[]): EventRecord[] {
  const byId = new Map<number, EventRecord>();
  for (const event of current) byId.set(event.id, event);
  for (const event of next) byId.set(event.id, event);
  return [...byId.values()].sort((left, right) => left.id - right.id);
}

function maxEventId(events: EventRecord[]): number {
  return events.reduce((max, event) => Math.max(max, event.id), 0);
}

function applyProjectionEvent(snapshot: AppSnapshot, event: EventRecord): AppSnapshot {
  const payload = asRecord(event.payload);
  if (event.type === "task.created" && event.taskId) {
    const task: Task = {
      id: event.taskId,
      projectId: String(payload.projectId ?? "") || (isRecord(payload.metadata) ? String(payload.metadata.projectId ?? "") : undefined),
      campaignId: isRecord(payload.metadata) ? String(payload.metadata.campaignId ?? "") || undefined : undefined,
      workstreamId: isRecord(payload.metadata) ? String(payload.metadata.workstreamId ?? "") || undefined : undefined,
      title: String(payload.title ?? "Untitled task"),
      prompt: String(payload.prompt ?? ""),
      status: "queued",
      objectiveStatus: "active",
      objectivePhase: "queued",
      createdAt: event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
    };
    return { ...snapshot, tasks: upsertById(snapshot.tasks, task) };
  }
  if (event.type === "task.updated" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    const metadataPatch = asRecord(payload.metadataPatch);
    return {
      ...snapshot,
      tasks: upsertById(snapshot.tasks, {
        ...task,
        title: payloadValue(payload.title) || task.title,
        prompt: payloadValue(payload.prompt) || task.prompt,
        metadata: Object.keys(metadataPatch).length > 0 ? { ...(task.metadata ?? {}), ...metadataPatch } : task.metadata,
        updatedAt: event.at,
      }),
    };
  }
  if (event.type === "task.status" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    const status = String(payload.status) as Task["status"];
    const objective = taskObjectiveForStatus(task.objectiveStatus, task.objectivePhase, status);
    const tasks = upsertById(snapshot.tasks, {
      ...task,
      status,
      error: payloadValue(payload.error) || undefined,
      objectiveStatus: objective.status,
      objectivePhase: objective.phase,
      updatedAt: event.at,
    });
    return {
      ...snapshot,
      tasks,
      orchestrationGraphs: deriveOrchestrationGraphs(tasks, snapshot.executionNodes),
    };
  }
  if (event.type === "task.final_candidate_selected" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, finalCandidateWorkerId: String(payload.workerId ?? "") || undefined, updatedAt: event.at }) } : snapshot;
  }
  if (event.type === "task.objective_updated" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, objectiveStatus: String(payload.status ?? task.objectiveStatus) as Task["objectiveStatus"], objectivePhase: String(payload.phase ?? task.objectivePhase ?? ""), updatedAt: event.at }) } : snapshot;
  }
  if (event.type === "task.milestone_reached" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    return {
      ...snapshot,
      tasks: upsertById(snapshot.tasks, {
        ...task,
        milestones: [...(task.milestones ?? []), {
          name: String(payload.name ?? ""),
          phase: String(payload.phase ?? "") || undefined,
          summary: String(payload.summary ?? "") || undefined,
          at: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
        }],
        updatedAt: event.at,
      }),
    };
  }
  if (event.type === "task.artifact_recorded" && event.taskId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    if (!task) return snapshot;
    return {
      ...snapshot,
      tasks: upsertById(snapshot.tasks, {
        ...task,
        artifacts: upsertTaskArtifactClient(task.artifacts ?? [], {
          id: String(payload.id ?? ""),
          kind: String(payload.kind ?? ""),
          name: String(payload.name ?? "") || undefined,
          url: String(payload.url ?? "") || undefined,
          ref: String(payload.ref ?? "") || undefined,
          createdAt: event.at,
          updatedAt: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
        }),
        updatedAt: event.at,
      }),
    };
  }
  if (event.type === "task.cleared" && event.taskId) {
    return {
      ...snapshot,
      tasks: snapshot.tasks.filter((task) => task.id !== event.taskId),
      workers: snapshot.workers.filter((worker) => worker.taskId !== event.taskId),
      executionNodes: snapshot.executionNodes.filter((node) => node.taskId !== event.taskId),
      pullRequests: snapshot.pullRequests.filter((pr) => pr.taskId !== event.taskId),
      orchestrationGraphs: snapshot.orchestrationGraphs.filter((graph) => graph.taskId !== event.taskId),
    };
  }
  if (event.type === "execution.node_planned" && event.taskId) {
    const nodeId = String(payload.nodeId ?? "");
    if (!nodeId) return snapshot;
    const executionNodes = upsertById(snapshot.executionNodes, {
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
    return { ...snapshot, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "execution.node_status") {
    const nodeId = String(payload.nodeId ?? "");
    const executionNodes = snapshot.executionNodes.map((node) => node.id === nodeId ? { ...node, status: String(payload.status) as WorkerStatus, updatedAt: event.at } : node);
    return { ...snapshot, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "worker.created" && event.workerId && event.taskId) {
    const existing = snapshot.workers.find((worker) => worker.id === event.workerId);
    return {
      ...snapshot,
      workers: upsertById(snapshot.workers, workerFromCreatedEvent(existing, event, payload)),
    };
  }
  if (event.type === "worker.workspace_prepared" && event.workerId) {
    return {
      ...snapshot,
      workers: snapshot.workers.map((worker) => worker.id === event.workerId ? {
        ...worker,
        metadata: { ...(worker.metadata ?? {}), ...payload },
        updatedAt: event.at,
      } : worker),
    };
  }
  if ((event.type === "worker.started" || event.type === "worker.completed") && event.workerId) {
    const status = event.type === "worker.started" ? "running" : String(payload.status) as WorkerStatus;
    const workers = snapshot.workers.map((worker) => worker.id === event.workerId ? { ...worker, status, updatedAt: event.at } : worker);
    const executionNodes = snapshot.executionNodes.map((node) => node.workerId === event.workerId ? { ...node, status, updatedAt: event.at } : node);
    return { ...snapshot, workers, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "worker.output" && event.workerId) {
    const workers = snapshot.workers.map((worker) => worker.id === event.workerId && !isTerminalWorkerStatus(worker.status) ? { ...worker, updatedAt: event.at } : worker);
    const executionNodes = snapshot.executionNodes.map((node) => node.workerId === event.workerId && !isTerminalWorkerStatus(node.status) ? { ...node, updatedAt: event.at } : node);
    return { ...snapshot, workers, executionNodes, orchestrationGraphs: deriveOrchestrationGraphs(snapshot.tasks, executionNodes) };
  }
  if (event.type === "worker.changes_applied" && event.taskId && event.workerId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, appliedWorkerId: event.workerId, updatedAt: event.at }) } : snapshot;
  }
  if ((event.type === "pull_request.published" || event.type === "pull_request.updated") && event.taskId) {
    const id = String(payload.id ?? "") || `${String(payload.repo ?? "")}#${String(payload.number ?? "")}`;
    if (!id) return snapshot;
    const existing = snapshot.pullRequests.find((candidate) => candidate.id === id);
    return {
      ...snapshot,
      pullRequests: upsertById(snapshot.pullRequests, {
        ...existing,
        id,
        taskId: event.taskId,
        repo: String(payload.repo ?? "") || existing?.repo || "",
        number: typeof payload.number === "number" ? payload.number : existing?.number,
        url: String(payload.url ?? "") || existing?.url || "",
        branch: String(payload.branch ?? "") || existing?.branch || "",
        base: String(payload.base ?? "") || existing?.base || "",
        title: String(payload.title ?? "") || existing?.title || "",
        state: String(payload.state ?? "") || existing?.state,
        draft: Boolean(payload.draft),
        checksStatus: String(payload.checksStatus ?? "") || existing?.checksStatus,
        checksConclusion: String(payload.checksConclusion ?? "") || existing?.checksConclusion,
        mergeStatus: String(payload.mergeStatus ?? "") || existing?.mergeStatus,
        mergeable: String(payload.mergeable ?? "") || existing?.mergeable,
        reviewStatus: String(payload.reviewStatus ?? "") || existing?.reviewStatus,
        createdAt: existing?.createdAt || event.at,
        updatedAt: event.at,
        metadata: isRecord(payload.metadata) ? payload.metadata : existing?.metadata,
      }),
    };
  }
  if (event.type === "pull_request.status_checked") {
    const id = String(payload.id ?? "");
    const pr = snapshot.pullRequests.find((candidate) => candidate.id === id);
    return pr ? { ...snapshot, pullRequests: upsertById(snapshot.pullRequests, { ...pr, state: String(payload.state ?? "") || pr.state, draft: Boolean(payload.draft), checksStatus: String(payload.checksStatus ?? "") || pr.checksStatus, checksConclusion: String(payload.checksConclusion ?? "") || pr.checksConclusion, mergeStatus: String(payload.mergeStatus ?? "") || pr.mergeStatus, mergeable: String(payload.mergeable ?? "") || pr.mergeable, reviewStatus: String(payload.reviewStatus ?? "") || pr.reviewStatus, updatedAt: event.at, metadata: isRecord(payload.metadata) ? payload.metadata : pr.metadata }) } : snapshot;
  }
  if (event.type === "pull_request.babysitter_started") {
    const id = String(payload.id ?? "");
    const pr = snapshot.pullRequests.find((candidate) => candidate.id === id);
    return pr ? { ...snapshot, pullRequests: upsertById(snapshot.pullRequests, { ...pr, babysitterTaskId: String(payload.babysitterTaskId ?? "") || pr.babysitterTaskId, updatedAt: event.at }) } : snapshot;
  }
  return snapshot;
}

function upsertById<T extends { id: string }>(items: T[], next: T): T[] {
  return items.some((item) => item.id === next.id)
    ? items.map((item) => (item.id === next.id ? next : item))
    : [...items, next];
}

function mergeById<T extends { id: string }>(left: T[], right: T[]): T[] {
  let merged = left;
  for (const item of right) {
    merged = upsertById(merged, item);
  }
  return merged;
}

function objectiveStatusForTaskStatus(status: Task["status"]): Task["objectiveStatus"] {
  if (status === "succeeded") return "satisfied";
  if (status === "failed" || status === "canceled") return "abandoned";
  if (status === "waiting") return "waiting_user";
  return "active";
}

function taskObjectiveForStatus(currentStatus: Task["objectiveStatus"], currentPhase: string | undefined, status: Task["status"]): { status: Task["objectiveStatus"]; phase: string } {
  if (status === "succeeded" || status === "failed" || status === "canceled") {
    return { status: objectiveStatusForTaskStatus(status), phase: objectivePhaseForTaskStatus(status) };
  }
  if (status === "waiting" && (!currentStatus || currentStatus === "active")) {
    return { status: "waiting_user", phase: objectivePhaseForTaskStatus(status) };
  }
  return {
    status: currentStatus || objectiveStatusForTaskStatus(status),
    phase: currentPhase || objectivePhaseForTaskStatus(status),
  };
}

function objectivePhaseForTaskStatus(status: Task["status"]): string {
  if (status === "succeeded") return "satisfied";
  return status;
}

function upsertTaskArtifactClient(items: NonNullable<Task["artifacts"]>, next: NonNullable<Task["artifacts"]>[number]): NonNullable<Task["artifacts"]> {
  if (!next.id) return [...items, next];
  return items.some((item) => item.id === next.id)
    ? items.map((item) => (item.id === next.id ? { ...next, createdAt: next.createdAt || item.createdAt } : item))
    : [...items, next];
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
        campaignId: isRecord(payload.metadata) ? String(payload.metadata.campaignId ?? "") || undefined : undefined,
        workstreamId: isRecord(payload.metadata) ? String(payload.metadata.workstreamId ?? "") || undefined : undefined,
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
        tasks.set(event.taskId, {
          ...task,
          status: String(payload.status) as Task["status"],
          error: payloadValue(payload.error) || undefined,
          updatedAt: event.at,
        });
      }
    }
    if (event.type === "task.updated" && event.taskId) {
      const task = tasks.get(event.taskId);
      if (task) {
        const metadataPatch = asRecord(payload.metadataPatch);
        tasks.set(event.taskId, {
          ...task,
          title: payloadValue(payload.title) || task.title,
          prompt: payloadValue(payload.prompt) || task.prompt,
          metadata: Object.keys(metadataPatch).length > 0 ? { ...(task.metadata ?? {}), ...metadataPatch } : task.metadata,
          updatedAt: event.at,
        });
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
      workers.set(event.workerId, workerFromCreatedEvent(workers.get(event.workerId), event, payload));
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
    if (event.type === "worker.output" && event.workerId) {
      const worker = workers.get(event.workerId);
      if (worker && !isTerminalWorkerStatus(worker.status)) workers.set(event.workerId, { ...worker, updatedAt: event.at });
      const node = [...executionNodes.values()].find((candidate) => candidate.workerId === event.workerId);
      if (node && !isTerminalWorkerStatus(node.status)) executionNodes.set(node.id, { ...node, updatedAt: event.at });
    }
    if (event.type === "worker.changes_applied" && event.taskId && event.workerId) {
      const task = tasks.get(event.taskId);
      if (task) {
        tasks.set(event.taskId, { ...task, appliedWorkerId: event.workerId, updatedAt: event.at });
      }
    }
    if ((event.type === "pull_request.published" || event.type === "pull_request.updated") && event.taskId) {
      const prId = String(payload.id ?? "");
      if (prId) {
        const existing = pullRequests.get(prId);
        pullRequests.set(prId, {
          ...existing,
          id: prId,
          taskId: event.taskId,
          repo: String(payload.repo ?? "") || existing?.repo || "",
          number: typeof payload.number === "number" ? payload.number : existing?.number,
          url: String(payload.url ?? "") || existing?.url || "",
          branch: String(payload.branch ?? "") || existing?.branch || "",
          base: String(payload.base ?? "") || existing?.base || "",
          title: String(payload.title ?? "") || existing?.title || "",
          state: String(payload.state ?? "") || existing?.state,
          draft: Boolean(payload.draft),
          checksStatus: String(payload.checksStatus ?? "") || existing?.checksStatus,
          checksConclusion: String(payload.checksConclusion ?? "") || existing?.checksConclusion,
          mergeStatus: String(payload.mergeStatus ?? "") || existing?.mergeStatus,
          mergeable: String(payload.mergeable ?? "") || existing?.mergeable,
          reviewStatus: String(payload.reviewStatus ?? "") || existing?.reviewStatus,
          createdAt: existing?.createdAt || event.at,
          updatedAt: event.at,
          metadata: isRecord(payload.metadata) ? payload.metadata : existing?.metadata,
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
          checksConclusion: String(payload.checksConclusion ?? "") || pr.checksConclusion,
          mergeStatus: String(payload.mergeStatus ?? "") || pr.mergeStatus,
          mergeable: String(payload.mergeable ?? "") || pr.mergeable,
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
    campaigns: snapshot.campaigns,
    tasks: [...tasks.values()].filter((task) => !clearedTasks.has(task.id)),
    workers: [...workers.values()].filter((worker) => !clearedTasks.has(worker.taskId)),
    executionNodes: [...executionNodes.values()].filter((node) => !clearedTasks.has(node.taskId)),
    orchestrationGraphs: deriveOrchestrationGraphs(
      [...tasks.values()].filter((task) => !clearedTasks.has(task.id)),
      [...executionNodes.values()].filter((node) => !clearedTasks.has(node.taskId)),
    ),
    projects: snapshot.projects,
    plugins: snapshot.plugins,
    promptSets: snapshot.promptSets,
    pullRequests: [...pullRequests.values()].filter((pr) => !clearedTasks.has(pr.taskId)),
    targets: snapshot.targets,
    lastEventId: snapshot.lastEventId,
    snapshotEventId: snapshot.snapshotEventId,
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
