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
import { answerTaskQuestion, applyWorkerChanges, askAssistant, babysitPullRequest, cancelTask, cancelWorkItem, cancelWorker, clearFinishedTasks, clearTask, createProject, createTarget, createTask, deletePlugin, deleteProject, deletePromptSet, deleteTarget, getProjectHealth, getSnapshot, getTaskEvents, getTaskSnapshot, getWorkerChanges, publishTaskPullRequest, refreshPullRequest, refreshTargetHealth, registerPlugin, registerPromptSet, retryTask, steerTask, steerWorker, updatePlugin, updateProject, updatePromptSet, updateTarget, updateTaskLoopConfig, watchTaskPullRequests } from "./api";
import type { TargetInput } from "./api";
import type { Artifact, EventRecord, ExecutionNode, MemoryEntry, Plugin, Project, ProjectHealth, ProjectInput, PromptSet, PullRequestFeedback, PullRequestPolicy, PullRequestState, Question, Session, Snapshot, SteeringItem, TargetState, Task, WatchPullRequestsInput, WorkItem, Worker, WorkerChangesReview, WorkerStatus } from "./types";
import "./styles.css";

type AppSnapshot = {
  tasks: Task[];
  workers: Worker[];
  executionNodes: ExecutionNode[];
  workItems: WorkItem[];
  artifacts: Artifact[];
  memoryEntries: MemoryEntry[];
  questions: Question[];
  sessions: Session[];
  targets: TargetState[];
  plugins: Plugin[];
  promptSets: PromptSet[];
  projects: Project[];
  pullRequests: PullRequestState[];
  pullRequestFeedback: PullRequestFeedback[];
  steering: SteeringItem[];
  lastEventId: number;
  snapshotEventId: number;
  events: EventRecord[];
};

type TaskStartInput = {
  taskMode?: TaskMode;
  projectId?: string;
  title: string;
  prompt: string;
  metadata?: Record<string, unknown>;
};

type TaskMode = "one-shot" | "objective" | "loop";

type InitialSnapshotStatus = "loading" | "ready" | "error";

type AttentionTone = "good" | "info" | "warning" | "danger";

type TaskAttentionItem = {
  tone: AttentionTone;
  icon: React.ReactNode;
  label: string;
  title: string;
  detail: string;
};

type AssignmentKind = "session" | "work" | "pull_request" | "feedback" | "question" | "artifact" | "debug";

type AssignmentAction =
  | { kind: "inspect-session"; sessionId: string }
  | { kind: "open-pr"; url: string }
  | { kind: "cancel-session"; workerId: string }
  | { kind: "cancel-work-item"; workItemId: string };

const ASSIGNMENT_ROW_LIMIT = 18;

export type AssignmentRow = {
  id: string;
  kind: AssignmentKind;
  title: string;
  subtitle: string;
  status: string;
  tone: AttentionTone;
  updatedAt: string;
  currentAction?: string;
  owner?: string;
  model?: string;
  projectContext?: string;
  prContext?: string;
  action?: AssignmentAction | AssignmentAction[];
};

const emptySnapshot: AppSnapshot = {
  tasks: [],
  workers: [],
  executionNodes: [],
  workItems: [],
  artifacts: [],
  memoryEntries: [],
  questions: [],
  sessions: [],
  targets: [],
  plugins: [],
  promptSets: [],
  projects: [],
  pullRequests: [],
  pullRequestFeedback: [],
  steering: [],
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

const LEGACY_DASHBOARD_LAYOUT_STORAGE_KEYS = ["aged.dashboard.layout.v3"];
const DASHBOARD_LAYOUT_STORAGE_KEY = "aged.dashboard.layout.v4";
const DASHBOARD_MIN_SPAN = 4;
const DASHBOARD_MAX_SPAN = 12;
const DASHBOARD_MIN_HEIGHT = 0;
const DASHBOARD_MAX_HEIGHT = 900;
const DASHBOARD_HEIGHT_STEP = 48;
const SELECTED_TASK_OUTPUT_EVENT_LIMIT = 250;
const TASK_EVENT_HISTORY_LIMIT = 300;
const EMPTY_WORKERS: Worker[] = [];
const EMPTY_EXECUTION_NODES: ExecutionNode[] = [];
const EMPTY_EVENTS: EventRecord[] = [];
const EMPTY_PULL_REQUESTS: PullRequestState[] = [];
const EMPTY_PULL_REQUEST_FEEDBACK: PullRequestFeedback[] = [];
const EMPTY_STEERING: SteeringItem[] = [];
const EMPTY_WORK_ITEMS: WorkItem[] = [];
const EMPTY_ARTIFACTS: Artifact[] = [];
const EMPTY_MEMORY_ENTRIES: MemoryEntry[] = [];
const EMPTY_QUESTIONS: Question[] = [];
const EMPTY_SESSIONS: Session[] = [];
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

function groupByTask<T>(items: T[], taskId: (item: T) => string | undefined): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const id = taskId(item);
    if (!id) continue;
    const group = groups.get(id);
    if (group) {
      group.push(item);
    } else {
      groups.set(id, [item]);
    }
  }
  return groups;
}

function memoryEntriesForTask(task: Task, byTask: Map<string, MemoryEntry[]>, byProject: Map<string, MemoryEntry[]>): MemoryEntry[] {
  const entries = new Map<string, MemoryEntry>();
  for (const entry of byTask.get(task.id) ?? EMPTY_MEMORY_ENTRIES) {
    entries.set(entry.id, entry);
  }
  if (task.projectId) {
    for (const entry of byProject.get(task.projectId) ?? EMPTY_MEMORY_ENTRIES) {
      entries.set(entry.id, entry);
    }
  }
  return entries.size > 0 ? [...entries.values()] : EMPTY_MEMORY_ENTRIES;
}

function mapByTask<T>(items: T[], taskId: (item: T) => string | undefined): Map<string, T> {
  const byTask = new Map<string, T>();
  for (const item of items) {
    const id = taskId(item);
    if (id) byTask.set(id, item);
  }
  return byTask;
}

function groupByWorker(events: EventRecord[]): Map<string, EventRecord[]> {
  const groups = new Map<string, EventRecord[]>();
  for (const event of events) {
    if (!event.workerId) continue;
    const group = groups.get(event.workerId);
    if (group) {
      group.push(event);
    } else {
      groups.set(event.workerId, [event]);
    }
  }
  return groups;
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
    setHydratedTaskIds(new Set(next.tasks.filter(taskHasDetailPayload).map((task) => task.id)));
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

  const taskById = useMemo(() => new Map(snapshot.tasks.map((task) => [task.id, task])), [snapshot.tasks]);
  const workersByTask = useMemo(() => groupByTask(snapshot.workers, (worker) => worker.taskId), [snapshot.workers]);
  const nodesByTask = useMemo(() => groupByTask(snapshot.executionNodes, (node) => node.taskId), [snapshot.executionNodes]);
  const workItemsByTask = useMemo(() => groupByTask(snapshot.workItems, (item) => item.taskId), [snapshot.workItems]);
  const artifactsByTask = useMemo(() => groupByTask(snapshot.artifacts, (artifact) => artifact.taskId), [snapshot.artifacts]);
  const memoryEntriesByTask = useMemo(() => groupByTask(snapshot.memoryEntries, (entry) => entry.taskId), [snapshot.memoryEntries]);
  const memoryEntriesByProject = useMemo(() => groupByTask(snapshot.memoryEntries, (entry) => entry.projectId), [snapshot.memoryEntries]);
  const questionsByTask = useMemo(() => groupByTask(snapshot.questions, (question) => question.taskId), [snapshot.questions]);
  const sessionsByTask = useMemo(() => groupByTask(snapshot.sessions, (session) => session.taskId), [snapshot.sessions]);
  const eventsByTask = useMemo(() => groupByTask(snapshot.events, (event) => event.taskId), [snapshot.events]);
  const pullRequestsByTask = useMemo(() => groupByTask(snapshot.pullRequests, (pr) => pr.taskId), [snapshot.pullRequests]);
  const pullRequestFeedbackByTask = useMemo(() => groupByTask(snapshot.pullRequestFeedback, (feedback) => feedback.taskId), [snapshot.pullRequestFeedback]);
  const steeringByTask = useMemo(() => groupByTask(snapshot.steering, (item) => item.taskId), [snapshot.steering]);
  const selectedTask = taskById.get(selectedTaskId) ?? preferredTask(snapshot.tasks);

  useEffect(() => {
    if (!selectedTask?.id || initialSnapshotStatus !== "ready") {
      return;
    }
    if (!hydratedTaskIds.has(selectedTask.id)) {
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
  const selectedWorkers = selectedTask ? workersByTask.get(selectedTask.id) ?? EMPTY_WORKERS : EMPTY_WORKERS;
  const selectedNodes = selectedTask ? nodesByTask.get(selectedTask.id) ?? EMPTY_EXECUTION_NODES : EMPTY_EXECUTION_NODES;
  const selectedWorkItems = selectedTask ? workItemsByTask.get(selectedTask.id) ?? EMPTY_WORK_ITEMS : EMPTY_WORK_ITEMS;
  const selectedArtifacts = selectedTask ? artifactsByTask.get(selectedTask.id) ?? selectedTask.artifacts?.map((artifact) => ({ ...artifact, taskId: selectedTask.id })) ?? EMPTY_ARTIFACTS : EMPTY_ARTIFACTS;
  const selectedMemoryEntries = selectedTask ? memoryEntriesForTask(selectedTask, memoryEntriesByTask, memoryEntriesByProject) : EMPTY_MEMORY_ENTRIES;
  const selectedQuestions = selectedTask ? questionsByTask.get(selectedTask.id) ?? EMPTY_QUESTIONS : EMPTY_QUESTIONS;
  const selectedSessions = selectedTask ? sessionsByTask.get(selectedTask.id) ?? EMPTY_SESSIONS : EMPTY_SESSIONS;
  const selectedEvents = selectedTask ? eventsByTask.get(selectedTask.id) ?? EMPTY_EVENTS : EMPTY_EVENTS;
  const selectedEventsByWorker = useMemo(() => groupByWorker(selectedEvents), [selectedEvents]);
  const selectedPullRequests = selectedTask ? pullRequestsByTask.get(selectedTask.id) ?? EMPTY_PULL_REQUESTS : EMPTY_PULL_REQUESTS;
  const selectedPullRequestFeedback = selectedTask ? pullRequestFeedbackByTask.get(selectedTask.id) ?? EMPTY_PULL_REQUEST_FEEDBACK : EMPTY_PULL_REQUEST_FEEDBACK;
  const selectedSteering = selectedTask ? steeringByTask.get(selectedTask.id) ?? EMPTY_STEERING : EMPTY_STEERING;
  const selectedWorker = selectedWorkers.find((worker) => worker.id === selectedWorkerId);
  const selectedWorkerNode = selectedNodes.find((node) => node.workerId === selectedWorker?.id);
  const selectedWorkerEvents = selectedWorker ? selectedEventsByWorker.get(selectedWorker.id) ?? EMPTY_EVENTS : EMPTY_EVENTS;
  const progress = workProgress(selectedTask, selectedWorkers, selectedNodes);
  const hasTerminalTasks = useMemo(() => snapshot.tasks.some(isTerminalTask), [snapshot.tasks]);
  const activeTasks = useMemo(() => snapshot.tasks.filter((task) => !isTerminalTask(task)), [snapshot.tasks]);
  const completedTasks = useMemo(() => tasksByNewestCompletion(snapshot.tasks.filter(isTerminalTask)), [snapshot.tasks]);
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
          element: <TaskDetail task={selectedTask} workers={selectedWorkers} nodes={selectedNodes} workItems={selectedWorkItems} artifacts={selectedArtifacts} memoryEntries={selectedMemoryEntries} questions={selectedQuestions} sessions={selectedSessions} pullRequests={selectedPullRequests} pullRequestFeedback={selectedPullRequestFeedback} steering={selectedSteering} targets={snapshot.targets} events={selectedEvents} onCancel={cancelTask} onCancelWorker={cancelWorker} onCancelWorkItem={cancelWorkItem} onRetry={handleRetryTask} onSteer={steerTask} onSteerWorker={steerWorker} onAnswerQuestion={answerTaskQuestion} onUpdateLoopConfig={updateTaskLoopConfig} onLoopConfigUpdated={refresh} retrying={retryingTaskId === selectedTask.id} onError={setError} />,
        },
        {
          id: "pull-requests",
          title: "Pull Requests",
          element: (
            <PullRequestPanel
              task={selectedTask}
              pullRequests={selectedPullRequests}
              pullRequestFeedback={selectedPullRequestFeedback}
              onPublish={publishTaskPullRequest}
              onWatch={watchTaskPullRequests}
              onRefresh={refreshPullRequest}
              onBabysit={babysitPullRequest}
              onSteer={steerTask}
              onDone={refresh}
              onError={setError}
            />
          ),
        },
        {
          id: "current-state",
          title: "Current State",
          element: <WorkSummary progress={progress} nodes={selectedNodes} workers={selectedWorkers} workItems={selectedWorkItems} sessions={selectedSessions} />,
        },
        {
          id: "workers",
          title: "Orchestration",
          element: (
            <WorkerList
              workers={selectedWorkers}
              nodes={selectedNodes}
              progress={progress}
              task={selectedTask}
              eventsByWorker={selectedEventsByWorker}
              selectedWorkerId={selectedWorkerId}
              onSelect={setSelectedWorkerId}
              onReview={getWorkerChanges}
              onApply={applyWorkerChanges}
              onApplied={refresh}
              onCancel={cancelWorker}
              onSteer={steerWorker}
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
          <p>Agent orchestration dashboard</p>
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
              const { taskMode, ...request } = input;
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
      <OverviewMetric label="Active Tasks" value={String(activeTasks.length)} />
      <OverviewMetric label="Running Workers" value={String(runningWorkers || progress.running)} />
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
          {isBroadObjectiveMetadata(task.metadata) && <span className="pill subtle">Objective</span>}
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
    for (const key of LEGACY_DASHBOARD_LAYOUT_STORAGE_KEYS) {
      window.localStorage.removeItem(key);
    }
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

function tasksByNewestCompletion(tasks: Task[]): Task[] {
  return [...tasks].sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
}

function preferredTask(tasks: Task[]): Task | undefined {
  return [...tasks].reverse().find((task) => !isTerminalTask(task)) ?? tasks.at(-1);
}

function isRetryableTask(task: Task): boolean {
  return task.status === "failed" || task.status === "canceled";
}

function taskHasDetailPayload(task: Task): boolean {
  return task.prompt.length > 0 || Boolean(task.workPlan) || Boolean(task.artifacts?.length) || Boolean(task.milestones?.length);
}

function isDurableLoopMetadata(metadata: Record<string, unknown> | undefined): boolean {
  const mode = String(metadata?.executionMode ?? "").trim().toLowerCase();
  return mode === "loop" || mode === "durable_loop" || mode === "agent_loop";
}

function isBroadObjectiveMetadata(metadata: Record<string, unknown> | undefined): boolean {
  return String(metadata?.objectiveMode ?? "").trim().toLowerCase() === "broad";
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
  return isTerminalTask(task);
}

function WorkSummary({ progress, nodes, workers, workItems, sessions }: { progress: WorkProgress; nodes: ExecutionNode[]; workers: Worker[]; workItems: WorkItem[]; sessions: Session[] }) {
  const activeNodes = nodes.filter((node) => node.status === "running" || node.status === "queued" || node.status === "waiting");
  const activeWorkers = workers.filter((worker) => worker.status === "running" || worker.status === "queued" || worker.status === "waiting");
  const activeWorkItems = workItems.filter((item) => item.status === "queued" || item.status === "running");
  const activeSessions = sessions.filter((session) => session.status === "running" || session.status === "queued" || session.status === "waiting");
  const activeCount = activeSessions.length || activeNodes.length || activeWorkers.length || activeWorkItems.length;
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
        <Metric label="Failed/Canceled" value={String(progress.failed)} />
      </div>
      <div className="progress-track" aria-label={`Progress ${progress.percent}%`}>
        <div style={{ width: `${progress.percent}%` }} />
      </div>
      <div className="active-work">
        <strong>{activeCount} active</strong>
        {activeSessions.length > 0
          ? activeSessions.slice(0, 4).map((item) => {
              const idle = formatWorkerIdle(item.status, item.updatedAt);
              return (
                <span key={item.id}>
                  {item.role || item.workerKind || "worker"} <Status value={item.status} />
                  {item.remoteSession ? ` ${item.remoteSession}` : ""}
                  {idle ? ` idle ${idle}` : ""}
                </span>
              );
            })
          : activeNodes.length > 0
          ? activeNodes.slice(0, 4).map((item) => {
              const idle = formatWorkerIdle(item.status, item.updatedAt);
              return (
                <span key={item.id}>
                  {item.role || item.workerKind} <Status value={item.status} />
                  {idle ? ` idle ${idle}` : ""}
                </span>
              );
            })
          : activeWorkers.length > 0
            ? activeWorkers.slice(0, 4).map((item) => {
                const idle = formatWorkerIdle(item.status, item.updatedAt);
                return (
                  <span key={item.id}>
                    {item.kind} <Status value={item.status} />
                    {idle ? ` idle ${idle}` : ""}
                  </span>
                );
              })
            : activeWorkItems.slice(0, 4).map((item) => (
                <span key={item.id}>
                  {item.kind} <Status value={item.status} />
                  {item.leaseOwner ? ` ${item.leaseOwner}` : ""}
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
  promptSets,
  targets,
}: {
  onCreate: (input: TaskStartInput) => Promise<Task>;
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
  const [taskMode, setTaskMode] = useState<TaskMode>("one-shot");
  const [loopWorkerKind, setLoopWorkerKind] = useState("codex");
  const [loopRole, setLoopRole] = useState("maintenance_pr_loop");
  const [loopIntervalSeconds, setLoopIntervalSeconds] = useState("300");
  const [busy, setBusy] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const hasAdvancedSelection = Boolean(promptSetId || requiredTargetID || taskMode !== "one-shot");

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const interval = Math.max(0, Number.parseInt(loopIntervalSeconds, 10) || 0);
    const metadata: Record<string, unknown> = taskMode === "loop"
      ? {
          executionMode: "loop",
          loopWorkerKind: loopWorkerKind.trim() || "codex",
          loopRole: loopRole.trim() || "maintenance_pr_loop",
          loopIntervalSeconds: interval,
        }
      : taskMode === "objective"
        ? { objectiveMode: "broad" }
        : {};
    if (promptSetId) {
      metadata.promptSetId = promptSetId;
    }
    if (requiredTargetID) {
      metadata.requiredTargetID = requiredTargetID;
    }
    const input = { taskMode, projectId: projectId || undefined, title, prompt, metadata };
    setBusy(true);
    onStartPending(input);
    try {
      await onCreate(input);
      setTitle("");
      setPrompt("");
      setPromptSetId("");
      setRequiredTargetID("");
      setTaskMode("one-shot");
      setLoopWorkerKind("codex");
      setLoopRole("maintenance_pr_loop");
      setLoopIntervalSeconds("300");
      setAdvancedOpen(false);
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
        Prompt
        <textarea value={prompt} onChange={(event) => setPrompt(event.target.value)} placeholder="Describe the development task..." required />
      </label>
      <details className="composer-advanced" open={advancedOpen || hasAdvancedSelection} onToggle={(event) => setAdvancedOpen(event.currentTarget.open)}>
        <summary>
          <span>Advanced</span>
          <small>{hasAdvancedSelection ? "Custom settings active" : "Defaults"}</small>
        </summary>
        <div className="composer-advanced-content">
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
          <fieldset className="task-mode-control">
            <legend>Run mode</legend>
            <label className={taskMode === "one-shot" ? "task-mode-option selected" : "task-mode-option"}>
              <input type="radio" name="task-mode" value="one-shot" checked={taskMode === "one-shot"} onChange={() => setTaskMode("one-shot")} />
              <Play size={16} />
              <span>One-shot</span>
            </label>
            <label className={taskMode === "objective" ? "task-mode-option selected" : "task-mode-option"}>
              <input type="radio" name="task-mode" value="objective" checked={taskMode === "objective"} onChange={() => setTaskMode("objective")} />
              <FolderPlus size={16} />
              <span>Objective</span>
            </label>
            <label className={taskMode === "loop" ? "task-mode-option selected" : "task-mode-option"}>
              <input type="radio" name="task-mode" value="loop" checked={taskMode === "loop"} onChange={() => setTaskMode("loop")} />
              <RefreshCw size={16} />
              <span>Durable loop</span>
            </label>
          </fieldset>
          {taskMode === "loop" ? (
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
        </div>
      </details>
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
        {task.taskMode === "objective" && <span className="pill subtle">Objective</span>}
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
	                Review final PR artifacts
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
  workItems,
  artifacts,
  memoryEntries,
  questions,
  sessions,
  pullRequests,
  pullRequestFeedback,
  steering,
  targets,
  events,
  onCancel,
  onCancelWorker,
  onRetry,
  onSteer,
  onSteerWorker,
  onAnswerQuestion,
  onCancelWorkItem,
  onUpdateLoopConfig,
  onLoopConfigUpdated,
  retrying,
  onError,
}: {
  task: Task;
  workers: Worker[];
  nodes: ExecutionNode[];
  workItems: WorkItem[];
  artifacts: Artifact[];
  memoryEntries: MemoryEntry[];
  questions: Question[];
  sessions: Session[];
  pullRequests: PullRequestState[];
  pullRequestFeedback: PullRequestFeedback[];
  steering: SteeringItem[];
  targets: TargetState[];
  events: EventRecord[];
  onCancel: (id: string) => Promise<void>;
  onCancelWorker: (id: string) => Promise<void>;
  onRetry: (id: string) => Promise<void>;
  onSteer: (id: string, message: string, target?: { targetKind?: string; targetId?: string }) => Promise<void>;
  onSteerWorker: (id: string, message: string) => Promise<void>;
  onAnswerQuestion: (taskId: string, questionId: string, answer: string) => Promise<void>;
  onCancelWorkItem: (taskId: string, itemId: string) => Promise<void>;
  onUpdateLoopConfig: (id: string, input: { loopIntervalSeconds?: number; loopPrompt?: string; requiredTargetID?: string }) => Promise<Task>;
  onLoopConfigUpdated: () => Promise<void>;
  retrying: boolean;
  onError: (message: string) => void;
}) {
  const [message, setMessage] = useState("");
  const [loopIntervalInput, setLoopIntervalInput] = useState("");
  const [loopPromptInput, setLoopPromptInput] = useState("");
  const [loopTargetInput, setLoopTargetInput] = useState("");
  const [savingLoopConfig, setSavingLoopConfig] = useState(false);
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const durableLoop = isDurableLoopMetadata(task.metadata);
  const broadObjective = isBroadObjectiveMetadata(task.metadata);
  const loopInterval = durableLoopIntervalSeconds(task.metadata);
  const currentLoopPrompt = durableLoopPromptValue(task);
  const requiredTargetID = requiredTargetIDFromMetadata(task.metadata);
  const hasCustomLoopPrompt = durableLoop && currentLoopPrompt !== task.prompt;
  const eventsByWorker = useMemo(() => groupByWorker(events), [events]);
  const workerUpdate = currentWorkerUpdate(workers, nodes, eventsByWorker);
  const approvals = questions.length > 0 ? questionApprovalStates(questions) : approvalStates(events);
  const pendingApprovals = approvals.filter((approval) => !approval.decided).slice(0, 4);
  const taskError = task.error || latestTaskStatusError(events);
  const pendingFeedback = pullRequestFeedback.filter((item) => item.status === "pending");
  const activeWorkers = workers.filter((worker) => !isTerminalWorkerStatus(worker.status)).length;
  const activeNodes = nodes.filter((node) => !isTerminalWorkerStatus(node.status)).length;
  const activeWorkItems = workItems.filter((item) => item.status === "queued" || item.status === "running").length;
  const assignments = useMemo(
    () => deriveAssignmentRows({ task, workers, nodes, workItems, artifacts, questions, sessions, pullRequests, pullRequestFeedback, steering, eventsByWorker }),
    [artifacts, eventsByWorker, nodes, pullRequestFeedback, pullRequests, questions, sessions, steering, task, workItems, workers],
  );
  const selectedSession = useMemo(() => selectedLiveSession(sessions, selectedSessionId), [selectedSessionId, sessions]);
  const pullRequestArtifacts = artifacts.filter((artifact) => artifact.kind.toLowerCase().includes("pull") || artifact.kind.toLowerCase().includes("pr"));
  const attentionItems = taskAttentionItems({
    task,
    taskError,
    pendingApprovalCount: pendingApprovals.length,
    pendingFeedbackCount: pendingFeedback.length,
    workerUpdate,
    activeCount: activeNodes || activeWorkers || activeWorkItems,
  });

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

  return (
    <section className="panel detail">
      <div className="detail-heading">
        <div className="detail-title-block">
          <h2>{task.title}</h2>
          <div className="task-detail-meta">
            {task.projectId && <span>Project {task.projectId}</span>}
            <span>{task.id.slice(0, 8)}</span>
            {requiredTargetID && <span>Target {requiredTargetID}</span>}
            <span>{broadObjective ? "Objective orchestration" : durableLoop ? "Durable loop" : "Task orchestration"}</span>
            {task.updatedAt && <span>Updated {new Date(task.updatedAt).toLocaleTimeString()}</span>}
          </div>
        </div>
        <div className="detail-actions">
          <Status value={task.status} />
          {task.objectiveStatus && <Status value={task.objectiveStatus} />}
          {task.objectivePhase && <span className="pill">{humanizeKey(task.objectivePhase)}</span>}
          {broadObjective && <span className="pill">Objective mode</span>}
          {durableLoop && <span className="pill">Loop mode</span>}
          {task.appliedWorkerId && <span className="pill">Applied worker {task.appliedWorkerId.slice(0, 8)}</span>}
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
      <AssignmentBoard
        taskId={task.id}
        rows={assignments}
        approvals={pendingApprovals}
        onInspectSession={setSelectedSessionId}
        onCancelSession={onCancelWorker}
        onCancelWorkItem={onCancelWorkItem}
        onAnswerQuestion={onAnswerQuestion}
        onDone={onLoopConfigUpdated}
        onError={onError}
      />
      <LiveSessionPanel
        session={selectedSession}
        worker={selectedSession ? workers.find((worker) => worker.id === selectedSession.workerId) : undefined}
        node={selectedSession ? nodes.find((node) => node.id === selectedSession.nodeId || node.workerId === selectedSession.workerId) : undefined}
        events={selectedSession ? eventsByWorker.get(selectedSession.workerId) ?? EMPTY_EVENTS : EMPTY_EVENTS}
        onSteer={onSteerWorker}
        onCancel={onCancelWorker}
        onDone={onLoopConfigUpdated}
        onError={onError}
      />
      <ManagerPullRequestSummary pullRequests={pullRequests} feedback={pullRequestFeedback} artifacts={pullRequestArtifacts} />
      <ObjectiveBrief
        task={task}
        artifacts={artifacts.length ? artifacts : task.artifacts?.map((artifact) => ({ ...artifact, taskId: task.id })) ?? []}
        memoryEntries={memoryEntries}
        sessions={sessions}
        attentionItems={attentionItems}
        taskError={taskError}
        currentLoopPrompt={currentLoopPrompt}
        hasCustomLoopPrompt={hasCustomLoopPrompt}
        durableLoop={durableLoop}
      />
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
      <details className="debug-pane">
        <summary>
          <span>Debug</span>
          <small>{workers.length || nodes.length} workers · {workItems.length} work items · {events.length} events</small>
        </summary>
        <div className="debug-pane-content">
          <WorkerProgressSpotlight update={workerUpdate} />
          <WideWorkProgress items={workItems} />
          <WorkItemQueue taskId={task.id} items={workItems} onCancel={onCancelWorkItem} onSteer={onSteer} onError={onError} />
          <SessionQueue sessions={sessions} onCancel={onCancelWorker} onSteer={onSteerWorker} onError={onError} />
          <SteeringQueue items={steering} />
          <PullRequestFeedbackQueue feedback={pullRequestFeedback} />
        </div>
      </details>
    </section>
  );
}

function deriveAssignmentRows({
  task,
  workers,
  nodes,
  workItems,
  artifacts,
  questions,
  sessions,
  pullRequests,
  pullRequestFeedback,
  steering,
  eventsByWorker,
}: {
  task: Task;
  workers: Worker[];
  nodes: ExecutionNode[];
  workItems: WorkItem[];
  artifacts: Artifact[];
  questions: Question[];
  sessions: Session[];
  pullRequests: PullRequestState[];
  pullRequestFeedback: PullRequestFeedback[];
  steering: SteeringItem[];
  eventsByWorker: Map<string, EventRecord[]>;
}): AssignmentRow[] {
  const workersById = new Map(workers.map((worker) => [worker.id, worker]));
  const nodesByWorkerId = new Map(nodes.filter((node) => node.workerId).map((node) => [node.workerId!, node]));
  const rows: AssignmentRow[] = [];

  for (const question of questions.filter((item) => !item.decided)) {
    rows.push({
      id: `question:${question.id}`,
      kind: "question",
      title: question.question || "Question needs an answer",
      subtitle: question.reason ? humanizeKey(question.reason) : "User input required",
      status: "waiting_user",
      tone: "warning",
      updatedAt: question.updatedAt || question.createdAt,
      currentAction: question.answer ? `Answered: ${question.answer}` : "Waiting for a response",
      owner: question.workerId ? `Worker ${shortID(question.workerId)}` : "Objective",
      projectContext: task.projectId,
    });
  }

  for (const feedback of pullRequestFeedback.filter((item) => item.status === "pending")) {
    rows.push({
      id: `feedback:${feedback.id}`,
      kind: "feedback",
      title: pullRequestFeedbackTitle(feedback),
      subtitle: feedback.reason ? humanizeKey(feedback.reason) : "Pull request feedback",
      status: feedback.status || "pending",
      tone: "warning",
      updatedAt: feedback.updatedAt || feedback.createdAt,
      currentAction: feedback.feedbackBody || feedback.prompt || "Follow-up work is queued.",
      owner: feedback.attempt ? `Attempt ${feedback.attempt}` : undefined,
      prContext: prContextFromParts(feedback.repo, feedback.number, feedback.branch),
      action: feedback.url ? { kind: "open-pr", url: feedback.url } : undefined,
    });
  }

  for (const session of sessions) {
    const worker = workersById.get(session.workerId);
    const node = nodesByWorkerId.get(session.workerId) ?? nodes.find((item) => item.id === session.nodeId);
    const workerEvents = eventsByWorker.get(session.workerId) ?? EMPTY_EVENTS;
    const latestEvent = latestWorkerProgressEvent(workerEvents) ?? latestInspectableWorkerEvent(workerEvents);
    const isActive = !isTerminalWorkerStatus(session.status);
    rows.push({
      id: `session:${session.id}`,
      kind: "session",
      title: session.role ? humanizeKey(session.role) : session.workerKind || worker?.kind || "Live session",
      subtitle: [session.remoteSession, session.targetId, session.workspaceName].filter(Boolean).join(" · ") || session.workerId.slice(0, 8),
      status: session.status,
      tone: toneForStatus(session.status),
      updatedAt: session.updatedAt || session.startedAt || session.createdAt,
      currentAction: session.currentAction || (latestEvent ? eventDisplayText(latestEvent) : undefined),
      owner: `Worker ${shortID(session.workerId)}`,
      model: metadataString(worker?.metadata, "model") || metadataString(worker?.metadata, "brain") || metadataString(session.metadata, "model"),
      projectContext: [node?.targetKind && humanizeKey(node.targetKind), node?.targetId].filter(Boolean).join(" "),
      action: isActive
        ? [
            { kind: "inspect-session", sessionId: session.id },
            { kind: "cancel-session", workerId: session.workerId },
          ]
        : { kind: "inspect-session", sessionId: session.id },
    });
  }

  const sessionWorkerIds = new Set(sessions.map((session) => session.workerId));
  for (const worker of workers.filter((item) => !sessionWorkerIds.has(item.id))) {
    const node = nodesByWorkerId.get(worker.id);
    rows.push({
      id: `debug-worker:${worker.id}`,
      kind: "debug",
      title: node?.role || worker.kind || "Worker",
      subtitle: node?.reason || worker.prompt || "Worker without session details",
      status: worker.status,
      tone: toneForStatus(worker.status),
      updatedAt: worker.updatedAt || worker.createdAt,
      currentAction: latestWorkerProgressEvent(eventsByWorker.get(worker.id) ?? EMPTY_EVENTS)?.type,
      owner: `Worker ${shortID(worker.id)}`,
      model: metadataString(worker.metadata, "model") || metadataString(worker.metadata, "brain"),
      projectContext: targetLabel(node),
      action: !isTerminalWorkerStatus(worker.status) ? { kind: "cancel-session", workerId: worker.id } : undefined,
    });
  }

  for (const item of workItems.filter((workItem) => workItem.status === "queued" || workItem.status === "running" || workItem.status === "failed")) {
    rows.push({
      id: `work:${item.id}`,
      kind: "work",
      title: humanizeKey(item.kind),
      subtitle: item.reason || [item.targetKind && humanizeKey(item.targetKind), item.targetId].filter(Boolean).join(" · ") || "Work item",
      status: item.status,
      tone: toneForStatus(item.status),
      updatedAt: item.updatedAt || item.createdAt,
      currentAction: item.error || item.prompt,
      owner: item.workerId ? `Worker ${shortID(item.workerId)}` : item.leaseOwner ? `Lease ${shortID(item.leaseOwner)}` : undefined,
      projectContext: task.projectId,
      action: item.status === "queued" || item.status === "running" ? { kind: "cancel-work-item", workItemId: item.id } : undefined,
    });
  }

  for (const pr of pullRequests) {
    const feedbackCount = pullRequestFeedback.filter((item) => item.pullRequestId === pr.id && item.status === "pending").length;
    rows.push({
      id: `pr:${pr.id}`,
      kind: "pull_request",
      title: pr.title || prContextFromParts(pr.repo, pr.number, pr.branch),
      subtitle: prContextFromParts(pr.repo, pr.number, pr.branch),
      status: pr.reviewStatus || pr.checksStatus || pr.state || "open",
      tone: feedbackCount > 0 ? "warning" : toneForStatus(pr.state || pr.checksStatus || "open"),
      updatedAt: pr.updatedAt || pr.createdAt,
      currentAction: feedbackCount > 0 ? `${feedbackCount} pending feedback item${feedbackCount === 1 ? "" : "s"}` : pr.mergeStatus || pr.checksConclusion,
      owner: pr.branchOwner ? `Owner ${shortID(pr.branchOwner)}` : undefined,
      prContext: [pr.base && `base ${pr.base}`, pr.branch && `head ${pr.branch}`].filter(Boolean).join(" · "),
      action: pr.url ? { kind: "open-pr", url: pr.url } : undefined,
    });
  }

  for (const artifact of artifacts) {
    rows.push({
      id: `artifact:${artifact.id || artifact.ref || artifact.url}`,
      kind: "artifact",
      title: artifact.name || artifact.ref || humanizeKey(artifact.kind),
      subtitle: humanizeKey(artifact.kind),
      status: "available",
      tone: "good",
      updatedAt: artifact.updatedAt || artifact.createdAt,
      currentAction: artifact.ref || artifact.url,
      owner: artifact.metadata ? metadataString(artifact.metadata, "workerId") : undefined,
      projectContext: task.projectId,
      action: artifact.url ? { kind: "open-pr", url: artifact.url } : undefined,
    });
  }

  for (const item of steering.filter((entry) => entry.status === "pending" || entry.status === "queued" || entry.status === "running")) {
    rows.push({
      id: `steering:${item.id}`,
      kind: "debug",
      title: steeringTitle(item),
      subtitle: item.reason ? humanizeKey(item.reason) : "Steering queued",
      status: item.status || "pending",
      tone: "info",
      updatedAt: item.updatedAt || item.createdAt,
      currentAction: item.message,
      owner: item.workerId ? `Worker ${shortID(item.workerId)}` : item.targetKind ? humanizeKey(item.targetKind) : "Objective",
      projectContext: item.targetId,
    });
  }

  return rows.sort((left, right) => assignmentRank(left) - assignmentRank(right) || Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
}

export function AssignmentBoard({
  taskId,
  rows,
  approvals,
  onInspectSession,
  onCancelSession,
  onCancelWorkItem,
  onAnswerQuestion,
  onDone,
  onError,
}: {
  taskId: string;
  rows: AssignmentRow[];
  approvals: ApprovalState[];
  onInspectSession: (sessionId: string) => void;
  onCancelSession: (workerId: string) => Promise<void>;
  onCancelWorkItem: (taskId: string, itemId: string) => Promise<void>;
  onAnswerQuestion: (taskId: string, questionId: string, answer: string) => Promise<void>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
  const [busyAction, setBusyAction] = useState("");
  const [showAllRows, setShowAllRows] = useState(false);
  const pendingCount = rows.filter((row) => row.tone === "warning" || row.tone === "danger").length;
  const hiddenCount = Math.max(0, rows.length - ASSIGNMENT_ROW_LIMIT);
  const visibleRows = showAllRows ? rows : rows.slice(0, ASSIGNMENT_ROW_LIMIT);

  async function run(actionId: string, action: () => Promise<void>) {
    setBusyAction(actionId);
    try {
      await action();
      await onDone();
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setBusyAction("");
    }
  }

  function renderAction(row: AssignmentRow, action: AssignmentAction, index: number) {
    const actionId = `${row.id}:${action.kind}:${index}`;
    switch (action.kind) {
      case "inspect-session":
        return (
          <button key={actionId} className="icon-button ghost small" onClick={() => onInspectSession(action.sessionId)} title="Inspect live session" aria-label={`Inspect ${row.title}`}>
            <Terminal size={14} />
          </button>
        );
      case "open-pr":
        return (
          <a key={actionId} className="icon-button ghost small" href={action.url} target="_blank" rel="noreferrer" title="Open pull request or artifact" aria-label={`Open ${row.title}`}>
            <GitPullRequest size={14} />
          </a>
        );
      case "cancel-session":
        return (
          <button key={actionId} className="icon-button danger small" disabled={busyAction === actionId} onClick={() => run(actionId, () => onCancelSession(action.workerId))} title="Cancel worker" aria-label={`Cancel ${row.title}`}>
            <CircleStop size={14} />
          </button>
        );
      case "cancel-work-item":
        return (
          <button key={actionId} className="icon-button danger small" disabled={busyAction === actionId} onClick={() => run(actionId, () => onCancelWorkItem(taskId, action.workItemId))} title="Cancel work item" aria-label={`Cancel ${row.title}`}>
            <CircleStop size={14} />
          </button>
        );
    }
  }

  function renderActions(row: AssignmentRow) {
    const actions = Array.isArray(row.action) ? row.action : row.action ? [row.action] : [];
    return actions.map((action, index) => renderAction(row, action, index));
  }

  return (
    <section className="manager-section assignments-panel" aria-label="Assignments">
      <div className="manager-section-title">
        <div>
          <span>Assignments</span>
          <strong>{rows.length} active signals</strong>
        </div>
        {pendingCount > 0 && <span className="pill">{pendingCount} need attention</span>}
      </div>
      {approvals.length > 0 && <ApprovalPanel taskId={taskId} approvals={approvals} onAnswer={onAnswerQuestion} onDone={onDone} onError={onError} />}
      {rows.length === 0 ? (
        <p className="empty">No assignments, sessions, pull requests, questions, or artifacts are attached yet.</p>
      ) : (
        <div className="assignment-list">
          {visibleRows.map((row) => (
            <article key={row.id} className={`assignment-row ${row.tone}`}>
              <div className="assignment-kind">{assignmentKindLabel(row.kind)}</div>
              <div className="assignment-main">
                <div className="assignment-title-line">
                  <strong>{row.title}</strong>
                  <Status value={row.status} />
                </div>
                <small>{row.subtitle}</small>
                {row.currentAction && <p>{row.currentAction}</p>}
              </div>
              <div className="assignment-context">
                {row.owner && <span>{row.owner}</span>}
                {row.model && <span>{row.model}</span>}
                {row.projectContext && <span>{row.projectContext}</span>}
                {row.prContext && <span>{row.prContext}</span>}
                {row.updatedAt && <time>{new Date(row.updatedAt).toLocaleTimeString()}</time>}
              </div>
              <div className="assignment-actions">{renderActions(row)}</div>
            </article>
          ))}
          {hiddenCount > 0 && (
            <button type="button" className="secondary compact assignment-list-toggle" onClick={() => setShowAllRows((value) => !value)}>
              {showAllRows ? "Show fewer" : `Show ${hiddenCount} more`}
            </button>
          )}
        </div>
      )}
    </section>
  );
}

function LiveSessionPanel({
  session,
  worker,
  node,
  events,
  onSteer,
  onCancel,
  onDone,
  onError,
}: {
  session: Session | undefined;
  worker: Worker | undefined;
  node: ExecutionNode | undefined;
  events: EventRecord[];
  onSteer: (id: string, message: string) => Promise<void>;
  onCancel: (id: string) => Promise<void>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [canceling, setCanceling] = useState(false);
  if (!session) {
    return (
      <section className="manager-section live-session-panel empty-session" aria-label="Agent session details">
        <div className="manager-section-title">
          <div>
            <span>Agent Session</span>
            <strong>No active session selected</strong>
          </div>
        </div>
        <p className="empty">Inspect an Agent Session assignment to see its live terminal context.</p>
      </section>
    );
  }
  const activeSession = session;
  const latestEvent = latestWorkerProgressEvent(events) ?? latestInspectableWorkerEvent(events);
  const completion = latestWorkerCompletion(events, activeSession.workerId);
  const changedFiles = completion.changedFiles ?? completion.workspaceChanges?.changedFiles ?? [];
  const command = worker?.command?.join(" ") || metadataString(activeSession.metadata, "command") || metadataString(worker?.metadata, "command");
  const branch = metadataString(activeSession.metadata, "branch") || metadataString(worker?.metadata, "branch");
  const latestOutput = activeSession.currentAction || (latestEvent ? eventDisplayText(latestEvent) : "");
  const location = activeSession.workspaceCwd || activeSession.remoteWorkDir || activeSession.workspaceRoot || node?.remoteWorkDir || "";
  const scratch = activeSession.sharedWorkerDir || activeSession.sharedArtifactsDir || activeSession.sharedRoot || "";

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const trimmed = message.trim();
    if (!trimmed) return;
    setBusy(true);
    try {
      await onSteer(activeSession.workerId, trimmed);
      setMessage("");
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  async function cancel() {
    setCanceling(true);
    try {
      await onCancel(activeSession.workerId);
      await onDone();
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setCanceling(false);
    }
  }

  const canCancel = !isTerminalWorkerStatus(activeSession.status);

  return (
    <section className="manager-section live-session-panel" aria-label="Agent session details">
      <div className="manager-section-title">
        <div>
          <span>Agent Session</span>
          <strong>{activeSession.role ? humanizeKey(activeSession.role) : activeSession.workerKind || worker?.kind || "Worker"}</strong>
        </div>
        <div className="manager-section-actions">
          <Status value={activeSession.status} />
          {canCancel && (
            <button type="button" className="icon-button danger small" disabled={canceling} onClick={cancel} title="Cancel session" aria-label="Cancel selected session">
              <CircleStop size={14} />
            </button>
          )}
        </div>
      </div>
      <div className="terminal-shell">
        <div className="terminal-topline">
          <span>{activeSession.remoteSession || activeSession.workerId.slice(0, 8)}</span>
          {activeSession.targetId && <span>{activeSession.targetKind ? `${humanizeKey(activeSession.targetKind)} ` : ""}{activeSession.targetId}</span>}
          {branch && <span>{branch}</span>}
        </div>
        <dl className="terminal-facts">
          {command && <TerminalFact label="Command" value={command} />}
          {location && <TerminalFact label="Worktree" value={location} />}
          {scratch && <TerminalFact label="Scratch" value={scratch} />}
          {activeSession.remoteRunDir && <TerminalFact label="Run dir" value={activeSession.remoteRunDir} />}
          {activeSession.currentActionLabel && <TerminalFact label="Action" value={activeSession.currentActionLabel} />}
        </dl>
        {latestOutput ? (
          <pre className="terminal-output">{latestOutput}</pre>
        ) : (
          <p className="terminal-empty">No live output has been reported yet.</p>
        )}
        {changedFiles.length > 0 && (
          <div className="terminal-files">
            <span>{changedFiles.length} changed files</span>
            {changedFiles.slice(0, 6).map((file) => (
              <code key={`${file.status ?? "changed"}-${file.path}`}>{file.status ?? "changed"} {file.path}</code>
            ))}
          </div>
        )}
      </div>
      {canCancel && (
        <form className="session-steer manager-session-steer" onSubmit={submit}>
          <input value={message} onChange={(event) => setMessage(event.target.value)} placeholder="Steer this exact session..." required />
          <button className="icon-button" disabled={busy || !message.trim()} title="Send session steering">
            <Send size={16} />
          </button>
        </form>
      )}
    </section>
  );
}

function TerminalFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function ManagerPullRequestSummary({ pullRequests, feedback, artifacts }: { pullRequests: PullRequestState[]; feedback: PullRequestFeedback[]; artifacts: Artifact[] }) {
  const pendingFeedback = feedback.filter((item) => item.status === "pending");
  return (
    <section className="manager-section manager-pr-summary">
      <div className="manager-section-title">
        <div>
          <span>Pull Requests</span>
          <strong>{pullRequests.length} tracked · {pendingFeedback.length} feedback</strong>
        </div>
      </div>
      {pullRequests.length === 0 && artifacts.length === 0 && pendingFeedback.length === 0 ? (
        <p className="empty">No pull request output is available yet.</p>
      ) : (
        <div className="manager-pr-grid">
          {pullRequests.slice(0, 4).map((pr) => (
            <a key={pr.id} className="manager-pr-card" href={pr.url} target="_blank" rel="noreferrer">
              <strong>{pr.repo}{pr.number ? `#${pr.number}` : ""}</strong>
              <span>{pr.title}</span>
              <small>{[pr.state, pr.checksStatus, pr.reviewStatus, pr.mergeStatus].filter(Boolean).join(" · ")}</small>
            </a>
          ))}
          {artifacts.slice(0, 4).map((artifact) => (
            artifact.url ? (
              <a key={artifact.id || artifact.url} className="manager-pr-card" href={artifact.url} target="_blank" rel="noreferrer">
                <strong>{artifact.name || artifact.ref || humanizeKey(artifact.kind)}</strong>
                <span>{artifact.url}</span>
                <small>{humanizeKey(artifact.kind)}</small>
              </a>
            ) : (
              <div key={artifact.id || artifact.ref} className="manager-pr-card">
                <strong>{artifact.name || artifact.ref || humanizeKey(artifact.kind)}</strong>
                <span>{artifact.ref}</span>
                <small>{humanizeKey(artifact.kind)}</small>
              </div>
            )
          ))}
        </div>
      )}
    </section>
  );
}

function ObjectiveBrief({
  task,
  artifacts,
  memoryEntries,
  sessions,
  attentionItems,
  taskError,
  currentLoopPrompt,
  hasCustomLoopPrompt,
  durableLoop,
}: {
  task: Task;
  artifacts: Artifact[];
  memoryEntries: MemoryEntry[];
  sessions: Session[];
  attentionItems: TaskAttentionItem[];
  taskError: string;
  currentLoopPrompt: string;
  hasCustomLoopPrompt: boolean;
  durableLoop: boolean;
}) {
  return (
    <section className="manager-section objective-brief">
      <div className="manager-section-title">
        <div>
          <span>Objective Brief</span>
          <strong>{task.objectivePhase ? humanizeKey(task.objectivePhase) : humanizeKey(task.status)}</strong>
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
      <TaskAttentionPanel items={attentionItems} />
      {(artifacts.length || task.milestones?.length) && <TaskObjectiveStrip task={task} artifacts={artifacts} />}
      <MemoryEntryPanel taskId={task.id} entries={memoryEntries} />
      <SharedScratchPanel sessions={sessions} />
      {taskError && (
        <div className="task-failure">
          <strong>Failure details</strong>
          <TruncatedBlock label="Error" value={taskError} className="tool-output failed" limit={1600} />
        </div>
      )}
    </section>
  );
}

function selectedLiveSession(sessions: Session[], selectedSessionId: string): Session | undefined {
  const sorted = [...sessions].sort((left, right) => Date.parse(right.updatedAt || right.startedAt || right.createdAt) - Date.parse(left.updatedAt || left.startedAt || left.createdAt));
  return sorted.find((session) => session.id === selectedSessionId)
    ?? sorted.find((session) => !isTerminalWorkerStatus(session.status))
    ?? sorted[0];
}

function assignmentRank(row: AssignmentRow): number {
  const toneRank = row.tone === "danger" ? 0 : row.tone === "warning" ? 1 : row.tone === "info" ? 2 : 3;
  const kindRank: Record<AssignmentKind, number> = {
    question: 0,
    feedback: 1,
    session: 2,
    work: 3,
    pull_request: 4,
    artifact: 5,
    debug: 6,
  };
  return toneRank * 10 + kindRank[row.kind];
}

function assignmentKindLabel(kind: AssignmentKind): string {
  switch (kind) {
    case "pull_request":
      return "Pull Request";
    case "session":
      return "Agent Session";
    case "work":
      return "Assignment";
    case "debug":
      return "Debug";
    default:
      return humanizeKey(kind);
  }
}

function toneForStatus(status: string): AttentionTone {
  const normalized = status.toLowerCase();
  if (normalized === "failed" || normalized === "canceled" || normalized === "abandoned" || normalized.includes("failure")) return "danger";
  if (normalized === "waiting" || normalized === "waiting_user" || normalized === "pending" || normalized === "queued") return "warning";
  if (normalized === "succeeded" || normalized === "satisfied" || normalized === "available" || normalized === "closed") return "good";
  return "info";
}

function metadataString(metadata: Record<string, unknown> | undefined, key: string): string {
  if (!metadata) return "";
  const value = metadata[key];
  if (Array.isArray(value)) return value.map(String).join(" ");
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
}

function prContextFromParts(repo?: string, number?: number, branch?: string): string {
  if (repo && number) return `${repo}#${number}`;
  return [repo, branch].filter(Boolean).join(" · ");
}

function taskAttentionItems({
  task,
  taskError,
  pendingApprovalCount,
  pendingFeedbackCount,
  workerUpdate,
  activeCount,
}: {
  task: Task;
  taskError: string;
  pendingApprovalCount: number;
  pendingFeedbackCount: number;
  workerUpdate: WorkerProgressUpdate | undefined;
  activeCount: number;
}): TaskAttentionItem[] {
  if (taskError) {
    return [{
      tone: "danger",
      icon: <CircleStop size={16} />,
      label: "Blocked",
      title: "Failure needs review",
      detail: taskError,
    }];
  }
  if (pendingApprovalCount > 0) {
    return [{
      tone: "warning",
      icon: <MessageSquarePlus size={16} />,
      label: "Waiting",
      title: `${pendingApprovalCount} approval${pendingApprovalCount === 1 ? "" : "s"} pending`,
      detail: "Answer the approval request to let orchestration continue.",
    }];
  }
  if (pendingFeedbackCount > 0) {
    return [{
      tone: "warning",
      icon: <GitPullRequest size={16} />,
      label: "Review",
      title: `${pendingFeedbackCount} PR feedback item${pendingFeedbackCount === 1 ? "" : "s"} pending`,
      detail: "Pull request feedback is queued for follow-up work.",
    }];
  }
  if (workerUpdate) {
    return [{
      tone: "info",
      icon: <Bot size={16} />,
      label: "Now",
      title: workerUpdate.title,
      detail: workerUpdate.text || workerUpdate.label || "Worker is active.",
    }];
  }
  if (activeCount > 0) {
    return [{
      tone: "info",
      icon: <Activity size={16} />,
      label: "Active",
      title: `${activeCount} active work item${activeCount === 1 ? "" : "s"}`,
      detail: "Work is running or queued for this task.",
    }];
  }
  if (task.status === "succeeded") {
    return [{
      tone: "good",
      icon: <Check size={16} />,
      label: "Done",
      title: "Task completed",
      detail: task.updatedAt ? `Last updated ${new Date(task.updatedAt).toLocaleTimeString()}` : "No action needed.",
    }];
  }
  return [{
    tone: "info",
    icon: <Activity size={16} />,
    label: "Status",
    title: humanizeKey(task.status),
    detail: task.updatedAt ? `Last updated ${new Date(task.updatedAt).toLocaleTimeString()}` : "Awaiting the next event.",
  }];
}

function TaskAttentionPanel({ items }: { items: TaskAttentionItem[] }) {
  return (
    <section className="task-attention" aria-label="Task attention">
      {items.map((item) => (
        <article key={`${item.label}:${item.title}`} className={`task-attention-card ${item.tone}`}>
          <span className="task-attention-icon">{item.icon}</span>
          <div>
            <span>{item.label}</span>
            <strong>{item.title}</strong>
            <p>{item.detail}</p>
          </div>
        </article>
      ))}
    </section>
  );
}

function TaskObjectiveStrip({ task, artifacts }: { task: Task; artifacts: Artifact[] }) {
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

function WorkItemQueue({ taskId, items, onCancel, onSteer, onError }: { taskId: string; items: WorkItem[]; onCancel: (taskId: string, itemId: string) => Promise<void>; onSteer: (taskId: string, message: string, target?: { targetKind?: string; targetId?: string }) => Promise<void>; onError: (message: string) => void }) {
  const [canceling, setCanceling] = useState<Record<string, boolean>>({});
  const [steering, setSteering] = useState<Record<string, string>>({});
  const [steeringBusy, setSteeringBusy] = useState<Record<string, boolean>>({});
  const sorted = [...items].sort((left, right) => Date.parse(right.updatedAt || right.createdAt) - Date.parse(left.updatedAt || left.createdAt));
  const activeCount = sorted.filter((item) => item.status === "queued" || item.status === "running").length;
  if (sorted.length === 0) return null;
  async function cancelItem(item: WorkItem) {
    setCanceling((current) => ({ ...current, [item.id]: true }));
    try {
      await onCancel(taskId, item.id);
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setCanceling((current) => ({ ...current, [item.id]: false }));
    }
  }
  async function steerItem(event: React.FormEvent, item: WorkItem) {
    event.preventDefault();
    const message = (steering[item.id] ?? "").trim();
    if (!message) return;
    setSteeringBusy((current) => ({ ...current, [item.id]: true }));
    try {
      await onSteer(taskId, message, { targetKind: "work_item", targetId: item.id });
      setSteering((current) => ({ ...current, [item.id]: "" }));
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setSteeringBusy((current) => ({ ...current, [item.id]: false }));
    }
  }
  return (
    <section className="work-item-queue">
      <div className="work-item-title">
        <strong>Work Queue</strong>
        <span>{activeCount} active</span>
      </div>
      <div className="work-item-list">
        {sorted.slice(0, 8).map((item) => {
          const isActive = item.status === "queued" || item.status === "running";
          const canSteer = isActive || item.status === "failed";
          return (
            <article key={item.id} className="work-item-card">
              <div className="work-item-card-header">
                <div className="work-item-card-heading">
                  <strong>{humanizeKey(item.kind)}</strong>
                  <small>Updated {new Date(item.updatedAt || item.createdAt).toLocaleTimeString()}</small>
                </div>
              </div>
              <div className="work-item-meta" role="group" aria-label={`${humanizeKey(item.kind)} status and metadata`}>
                <Status value={item.status} />
                {item.targetKind && <span className="pill">{humanizeKey(item.targetKind)}</span>}
                {item.targetId && <span className="pill">{item.targetId}</span>}
                {item.workerId && <span className="pill">Worker {item.workerId.slice(0, 8)}</span>}
                {item.leaseOwner && <span className="pill">Lease {item.leaseOwner}</span>}
                {item.attempt ? <span className="pill">Attempt {item.attempt}</span> : null}
                {item.leaseUntil && <span className="pill">Until {new Date(item.leaseUntil).toLocaleTimeString()}</span>}
              </div>
              {item.reason && <p>{item.reason}</p>}
              {item.error && <TruncatedBlock label="Work item error" value={item.error} className="tool-output failed" limit={900} />}
              {canSteer && (
                <div className="work-item-card-actions" role="group" aria-label={`${humanizeKey(item.kind)} actions`}>
                  {isActive && (
                    <button type="button" className="icon-button danger small" disabled={Boolean(canceling[item.id])} onClick={() => cancelItem(item)} title="Cancel work item" aria-label={`Cancel ${humanizeKey(item.kind)} work item`}>
                      <CircleStop size={14} />
                    </button>
                  )}
                  <form className="inline-steer-form" onSubmit={(event) => steerItem(event, item)}>
                    <input aria-label={`Steer ${humanizeKey(item.kind)} work item`} value={steering[item.id] ?? ""} onChange={(event) => setSteering((current) => ({ ...current, [item.id]: event.target.value }))} placeholder="Steer this work item..." required />
                    <button className="secondary compact" disabled={Boolean(steeringBusy[item.id]) || !(steering[item.id] ?? "").trim()}>
                      {steeringBusy[item.id] ? "Queued" : "Steer"}
                    </button>
                  </form>
                </div>
              )}
            </article>
          );
        })}
      </div>
    </section>
  );
}

function WideWorkProgress({ items }: { items: WorkItem[] }) {
  const wideItems = items
    .filter((item) => item.kind === "objective.slice" || item.kind === "objective.compose" || item.kind === "objective.validate")
    .sort((left, right) => {
      const leftRank = wideWorkRank(left.kind);
      const rightRank = wideWorkRank(right.kind);
      if (leftRank !== rightRank) return leftRank - rightRank;
      return Date.parse(left.createdAt) - Date.parse(right.createdAt);
    });
  if (wideItems.length === 0) return null;
  const counts = wideItems.reduce<Record<string, number>>((out, item) => {
    out[item.status] = (out[item.status] ?? 0) + 1;
    return out;
  }, {});
  const active = (counts.queued ?? 0) + (counts.running ?? 0);
  const done = counts.succeeded ?? 0;
  return (
    <section className="wide-work">
      <div className="work-item-title">
        <strong>Wide Work</strong>
        <span>{done}/{wideItems.length} done · {active} active</span>
      </div>
      <div className="wide-work-lanes">
        {wideItems.map((item) => (
          <article key={item.id} className={`wide-work-card ${item.kind.replace(".", "-")}`}>
            <div className="wide-work-card-heading">
              <strong>{wideWorkTitle(item)}</strong>
              <Status value={item.status} />
            </div>
            <div className="wide-work-meta">
              <span>{humanizeKey(item.kind.replace("objective.", ""))}</span>
              {item.workerId && <span>Worker {item.workerId.slice(0, 8)}</span>}
              {item.targetId && <span>{item.targetId}</span>}
            </div>
            {wideWorkScope(item).length > 0 && (
              <div className="wide-work-scope">
                {wideWorkScope(item).slice(0, 4).map((scope) => <code key={scope}>{scope}</code>)}
              </div>
            )}
            {item.reason && <p>{item.reason}</p>}
            {item.error && <TruncatedBlock label="Slice error" value={item.error} className="tool-output failed" limit={700} />}
          </article>
        ))}
      </div>
    </section>
  );
}

function wideWorkRank(kind: string): number {
  switch (kind) {
    case "objective.slice":
      return 0;
    case "objective.compose":
      return 1;
    case "objective.validate":
      return 2;
    default:
      return 3;
  }
}

function wideWorkTitle(item: WorkItem): string {
  const metadata = item.metadata ?? {};
  const explicit = payloadValue(metadata.title) || payloadValue(metadata.name) || payloadValue(metadata.slice) || payloadValue(metadata.subsystem);
  if (explicit) return explicit;
  return item.id || humanizeKey(item.kind);
}

function wideWorkScope(item: WorkItem): string[] {
  const metadata = item.metadata ?? {};
  return [
    ...payloadStringArray(metadata.files),
    ...payloadStringArray(metadata.paths),
    ...payloadStringArray(metadata.fileSet),
    ...payloadStringArray(metadata.subsystems),
  ].filter(Boolean);
}

function MemoryEntryPanel({ taskId, entries }: { taskId: string; entries: MemoryEntry[] }) {
  const sorted = [...entries].sort((left, right) => Date.parse(right.updatedAt || right.createdAt) - Date.parse(left.updatedAt || left.createdAt));
  if (sorted.length === 0) return null;
  const projectEntries = sorted.filter((entry) => entry.taskId && entry.taskId !== taskId).length;
  return (
    <section className="work-item-queue">
      <div className="work-item-title">
        <strong>Memory</strong>
        <span>{sorted.length} entries{projectEntries ? ` · ${projectEntries} project` : ""}</span>
      </div>
      <div className="work-item-list">
        {sorted.slice(0, 8).map((entry) => (
          <article key={entry.id} className="work-item-card">
            <div>
              <strong>{humanizeKey(entry.kind)}</strong>
              <small>{new Date(entry.updatedAt || entry.createdAt).toLocaleTimeString()}</small>
            </div>
            <div className="work-item-meta">
              <span className="pill">{entry.taskId && entry.taskId !== taskId ? "Project memory" : "Task memory"}</span>
              {entry.workerId && <span className="pill">Worker {entry.workerId.slice(0, 8)}</span>}
              {entry.sourceEvent && <span className="pill">{entry.sourceEvent}</span>}
            </div>
            <p>{entry.summary}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

type SharedScratchSummary = {
  root: string;
  artifactsDir?: string;
  workerDirs: string[];
};

function SharedScratchPanel({ sessions }: { sessions: Session[] }) {
  const items = sharedScratchSummaries(sessions);
  if (items.length === 0) return null;
  return (
    <section className="scratch-panel">
      <div className="work-item-title">
        <strong>Scratch</strong>
        <span>{items.length} shared {items.length === 1 ? "workspace" : "workspaces"}</span>
      </div>
      <div className="work-item-list">
        {items.map((item) => (
          <article key={item.root} className="work-item-card">
            <div>
              <strong>{scratchName(item.root)}</strong>
              <small>{item.workerDirs.length} workers</small>
            </div>
            <code className="session-path">{item.root}</code>
            {item.artifactsDir && <code className="session-path muted">{item.artifactsDir}</code>}
            {item.workerDirs.length > 0 && (
              <div className="work-item-meta">
                {item.workerDirs.slice(0, 4).map((dir) => (
                  <span key={dir} className="pill">{scratchName(dir)}</span>
                ))}
                {item.workerDirs.length > 4 && <span className="pill">+{item.workerDirs.length - 4}</span>}
              </div>
            )}
          </article>
        ))}
      </div>
    </section>
  );
}

function sharedScratchSummaries(sessions: Session[]): SharedScratchSummary[] {
  const byRoot = new Map<string, SharedScratchSummary>();
  for (const session of sessions) {
    const root = session.sharedRoot?.trim();
    if (!root) continue;
    const summary = byRoot.get(root) ?? { root, workerDirs: [] };
    if (!summary.artifactsDir && session.sharedArtifactsDir) {
      summary.artifactsDir = session.sharedArtifactsDir;
    }
    if (session.sharedWorkerDir && !summary.workerDirs.includes(session.sharedWorkerDir)) {
      summary.workerDirs.push(session.sharedWorkerDir);
    }
    byRoot.set(root, summary);
  }
  return [...byRoot.values()].sort((left, right) => left.root.localeCompare(right.root));
}

function scratchName(path: string): string {
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts.at(-1) ?? path;
}

function PullRequestFeedbackQueue({ feedback }: { feedback: PullRequestFeedback[] }) {
  const pending = feedback
    .filter((item) => item.status === "pending")
    .sort((left, right) => Date.parse(right.updatedAt || right.createdAt) - Date.parse(left.updatedAt || left.createdAt));
  if (pending.length === 0) return null;
  return (
    <section className="work-item-queue">
      <div className="work-item-title">
        <strong>PR Feedback Queue</strong>
        <span>{pending.length} pending</span>
      </div>
      <div className="work-item-list">
        {pending.slice(0, 6).map((item) => (
          <article key={item.id} className="work-item-card">
            <div>
              <strong>{pullRequestFeedbackTitle(item)}</strong>
              <small>{new Date(item.updatedAt || item.createdAt).toLocaleTimeString()}</small>
            </div>
            <div className="work-item-meta">
              <Status value={item.status ?? "pending"} />
              {item.reviewStatus && <span className="pill">{item.reviewStatus.toLowerCase()}</span>}
              {item.checksStatus && <span className="pill">{item.checksStatus.toLowerCase()}</span>}
              {item.mergeStatus && <span className="pill">{item.mergeStatus.toLowerCase()}</span>}
            </div>
            {item.reason && <p>{item.reason}</p>}
            {item.feedbackBody && <TruncatedBlock label="Feedback" value={item.feedbackBody} className="tool-output" limit={900} />}
            {item.prompt && <TruncatedBlock label="Follow-up prompt" value={item.prompt} className="tool-output" limit={700} />}
          </article>
        ))}
      </div>
    </section>
  );
}

function pullRequestFeedbackTitle(item: PullRequestFeedback): string {
  if (item.repo && item.number) return `${item.repo}#${item.number}`;
  if (item.url) return item.url;
  if (item.branch) return item.branch;
  return item.pullRequestId || "Pull request";
}

function SteeringQueue({ items }: { items: SteeringItem[] }) {
  const visible = [...items]
    .filter((item) => item.status === "pending" || item.status === "queued" || item.status === "running" || item.status === "applied")
    .sort((left, right) => Date.parse(right.updatedAt || right.createdAt) - Date.parse(left.updatedAt || left.createdAt));
  const pendingCount = visible.filter((item) => item.status === "pending" || item.status === "queued" || item.status === "running").length;
  if (visible.length === 0) return null;
  return (
    <section className="work-item-queue">
      <div className="work-item-title">
        <strong>Steering Queue</strong>
        <span>{pendingCount} pending</span>
      </div>
      <div className="work-item-list">
        {visible.slice(0, 6).map((item) => (
          <article key={item.id} className="work-item-card">
            <div>
              <strong>{steeringTitle(item)}</strong>
              <small>{new Date(item.updatedAt || item.createdAt).toLocaleTimeString()}</small>
            </div>
            <div className="work-item-meta">
              <Status value={item.status ?? "pending"} />
              {item.targetKind && <span className="pill">{humanizeKey(item.targetKind)}</span>}
              {item.targetId && <span className="pill">{item.targetKind === "worker" ? item.targetId.slice(0, 8) : item.targetId}</span>}
              {item.workerKind && <span className="pill">{item.workerKind}</span>}
              {item.role && <span className="pill">{humanizeKey(item.role)}</span>}
            </div>
            <TruncatedBlock label="Steering" value={item.message} className="tool-output" limit={900} />
          </article>
        ))}
      </div>
    </section>
  );
}

function steeringTitle(item: SteeringItem): string {
  if (item.targetKind === "worker" && item.workerId) return `Worker ${item.workerId.slice(0, 8)}`;
  if (item.targetKind === "task") return "Task steering";
  return item.reason ? humanizeKey(item.reason) : "Steering";
}

function SessionQueue({
  sessions,
  onCancel,
  onSteer,
  onError,
}: {
  sessions: Session[];
  onCancel: (id: string) => Promise<void>;
  onSteer: (id: string, message: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [steering, setSteering] = useState<Record<string, string>>({});
  const [canceling, setCanceling] = useState<Record<string, boolean>>({});
  const sorted = [...sessions].sort((left, right) => Date.parse(right.updatedAt || right.createdAt) - Date.parse(left.updatedAt || left.createdAt));
  const activeCount = sorted.filter((session) => session.status === "queued" || session.status === "running" || session.status === "waiting").length;
  if (sorted.length === 0) return null;
  async function cancelSession(workerID: string) {
    setCanceling((items) => ({ ...items, [workerID]: true }));
    try {
      await onCancel(workerID);
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setCanceling((items) => ({ ...items, [workerID]: false }));
    }
  }
  async function steer(event: React.FormEvent, workerID: string) {
    event.preventDefault();
    const message = steering[workerID] ?? "";
    try {
      await onSteer(workerID, message);
      setSteering((items) => ({ ...items, [workerID]: "" }));
    } catch (err) {
      onError(errorMessage(err));
    }
  }
  return (
    <section className="session-queue">
      <div className="work-item-title">
        <strong>Sessions</strong>
        <span>{activeCount} active</span>
      </div>
      <div className="session-list">
        {sorted.slice(0, 10).map((session) => {
          const location = session.workspaceCwd || session.remoteWorkDir || session.workspaceRoot || "";
          const scratchLocation = session.sharedWorkerDir || session.sharedArtifactsDir || session.sharedRoot || "";
          const updatedAt = session.updatedAt || session.startedAt || session.createdAt;
          const currentActionAt = session.currentActionAt || updatedAt;
          const title = session.role ? humanizeKey(session.role) : session.workerKind || "Worker";
          const isActive = !isTerminalWorkerStatus(session.status);
          return (
            <article key={session.id} className="session-card">
              <div className="session-card-header">
                <div className="session-main">
                  <strong>{title}</strong>
                  <small>{session.workerId.slice(0, 8)}</small>
                </div>
              </div>
              <div className="session-status-group" role="group" aria-label={`${title} session status and metadata`}>
                <Status value={session.status} />
                {session.targetKind && <span>{humanizeKey(session.targetKind)}</span>}
                {session.targetId && <span>{session.targetId}</span>}
                {session.remoteSession && <span>{session.remoteSession}</span>}
                {session.workspaceName && <span>{session.workspaceName}</span>}
                {session.workspaceMode && <span>{humanizeKey(session.workspaceMode)}</span>}
                {session.vcsType && <span>{session.vcsType}</span>}
                {scratchLocation && <span>scratch</span>}
                {updatedAt && <span>{new Date(updatedAt).toLocaleTimeString()}</span>}
              </div>
              {location && <code className="session-path">{location}</code>}
              {scratchLocation && <code className="session-path muted">{scratchLocation}</code>}
              {session.currentAction && (
                <div className="session-current-action">
                  <span>{session.currentActionLabel || "output"}</span>
                  <p>{session.currentAction}</p>
                  {currentActionAt && <small>{new Date(currentActionAt).toLocaleTimeString()}</small>}
                </div>
              )}
              {isActive && (
                <div className="session-actions" role="group" aria-label={`${title} session actions`}>
                  <button type="button" className="icon-button danger small" disabled={Boolean(canceling[session.workerId])} onClick={() => cancelSession(session.workerId)} title="Cancel session" aria-label={`Cancel ${title} session`}>
                    <CircleStop size={14} />
                  </button>
                  <form className="session-steer" onSubmit={(event) => steer(event, session.workerId)}>
                    <input aria-label={`Steer ${title} session`} value={steering[session.workerId] ?? ""} onChange={(event) => setSteering((items) => ({ ...items, [session.workerId]: event.target.value }))} placeholder="Steer this session..." required />
                    <button className="icon-button" title="Send session steering" aria-label={`Send steering to ${title} session`}>
                      <Send size={16} />
                    </button>
                  </form>
                </div>
              )}
            </article>
          );
        })}
      </div>
    </section>
  );
}

export type ApprovalState = {
  id: string;
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

function questionApprovalStates(questions: Question[]): ApprovalState[] {
  return [...questions]
    .sort((left, right) => Date.parse(right.updatedAt || right.createdAt) - Date.parse(left.updatedAt || left.createdAt))
    .map((question) => ({
      id: question.id,
      at: question.updatedAt || question.createdAt,
      question: question.question || "Approval needed.",
      reason: question.reason || "approval",
      workerId: question.workerId,
      decided: question.decided,
      answer: question.answer,
    }));
}

function ApprovalPanel({
  taskId,
  approvals,
  onAnswer,
  onDone,
  onError,
}: {
  taskId: string;
  approvals: ApprovalState[];
  onAnswer: (taskId: string, questionId: string, answer: string) => Promise<void>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
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
            <ApprovalResponseForm
              taskId={taskId}
              approval={approval}
              onAnswer={onAnswer}
              onDone={onDone}
              onError={onError}
            />
          )}
        </div>
      ))}
    </section>
  );
}

function ApprovalResponseForm({
  taskId,
  approval,
  onAnswer,
  onDone,
  onError,
}: {
  taskId: string;
  approval: ApprovalState;
  onAnswer: (taskId: string, questionId: string, answer: string) => Promise<void>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
  const [answer, setAnswer] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const trimmed = answer.trim();
    if (!trimmed) {
      onError("Answer is required.");
      return;
    }
    setSubmitting(true);
    try {
      await onAnswer(taskId, approval.id, trimmed);
      setAnswer("");
      await onDone();
    } catch (err) {
      onError(errorMessage(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form className="approval-response" onSubmit={submit}>
      <input value={answer} onChange={(event) => setAnswer(event.target.value)} placeholder="Answer this question..." required />
      <button className="icon-button" disabled={submitting} title="Send answer">
        <Send size={16} />
      </button>
    </form>
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
  pullRequestFeedback,
  onPublish,
  onWatch,
  onRefresh,
  onBabysit,
  onSteer,
  onDone,
  onError,
}: {
  task: Task;
  pullRequests: PullRequestState[];
  pullRequestFeedback: PullRequestFeedback[];
  onPublish: (taskId: string) => Promise<PullRequestState>;
  onWatch: (taskId: string, input: WatchPullRequestsInput) => Promise<PullRequestState[]>;
  onRefresh: (id: string) => Promise<PullRequestState>;
  onBabysit: (id: string) => Promise<unknown>;
  onSteer: (taskId: string, message: string, target?: { targetKind?: string; targetId?: string }) => Promise<void>;
  onDone: () => Promise<void>;
  onError: (message: string) => void;
}) {
  const [busy, setBusy] = useState("");
  const [steering, setSteering] = useState<Record<string, string>>({});
  const [watchRepo, setWatchRepo] = useState("");
  const [watchNumber, setWatchNumber] = useState("");
  const [watchUrl, setWatchUrl] = useState("");
  const broadObjective = isBroadObjectiveMetadata(task.metadata);
  const canPublish = canPublishPullRequest(task) && (broadObjective || pullRequests.length === 0);
  const feedbackByPullRequest = useMemo(() => {
    const byPR = new Map<string, PullRequestFeedback[]>();
    for (const item of pullRequestFeedback) {
      byPR.set(item.pullRequestId, [...(byPR.get(item.pullRequestId) ?? []), item]);
    }
    return byPR;
  }, [pullRequestFeedback]);

  async function run(action: string, fn: () => Promise<unknown>) {
    setBusy(action);
    try {
      await fn();
      await onDone();
    } catch (err) { onError(errorMessage(err)); } finally {
      setBusy("");
    }
  }
  async function steerPullRequest(event: React.FormEvent, pr: PullRequestState) {
    event.preventDefault();
    const message = (steering[pr.id] ?? "").trim();
    if (!message) return;
    await run(`steer:${pr.id}`, async () => {
      await onSteer(task.id, message, { targetKind: "pull_request", targetId: pr.id });
      setSteering((current) => ({ ...current, [pr.id]: "" }));
    });
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
          {busy === "publish" ? "Opening" : broadObjective && pullRequests.length > 0 ? "Open PR Output" : "Open PR"}
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
          {pullRequests.map((pr) => {
            const pendingFeedback = (feedbackByPullRequest.get(pr.id) ?? []).filter((item) => item.status === "pending");
            return (
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
                  {pendingFeedback.length > 0 && <span className="pill">{pendingFeedback.length} feedback</span>}
                </div>
                {(pr.branchOwner || pr.updateLeaseOwner || pr.branchHead) && (
                  <div className="pr-lease-row">
                    {pr.branchOwner && <span className="pill">Owner {shortID(pr.branchOwner)}</span>}
                    {pr.branchHead && <span className="pill">Head {shortID(pr.branchHead)}</span>}
                    {pr.updateLeaseOwner && <span className="pill">Lease {shortID(pr.updateLeaseOwner)}</span>}
                    {pr.updateBaseHead && <span className="pill">Base {shortID(pr.updateBaseHead)}</span>}
                  </div>
                )}
                {pendingFeedback.length > 0 && (
                  <div className="pr-feedback-list">
                    {pendingFeedback.slice(0, 3).map((item) => (
                      <div key={item.id} className="pr-feedback-item">
                        <small>{item.reason ? humanizeKey(item.reason) : "Pending feedback"}</small>
                        <span>{item.feedbackBody || item.prompt || item.feedbackSignature || "Feedback is queued for follow-up."}</span>
                      </div>
                    ))}
                  </div>
                )}
                <form className="inline-steer-form" onSubmit={(event) => steerPullRequest(event, pr)}>
                  <input value={steering[pr.id] ?? ""} onChange={(event) => setSteering((current) => ({ ...current, [pr.id]: event.target.value }))} placeholder="Steer this PR..." required />
                  <button className="secondary compact" disabled={busy === `steer:${pr.id}` || !(steering[pr.id] ?? "").trim()}>
                    {busy === `steer:${pr.id}` ? "Queued" : "Steer"}
                  </button>
                </form>
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
            );
          })}
        </div>
      )}
    </section>
  );
}

function shortID(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length <= 12) return trimmed;
  return trimmed.slice(0, 8);
}

function WorkerList({
  task,
  workers,
  nodes,
  progress,
  eventsByWorker,
  selectedWorkerId,
  onSelect,
  onReview,
  onApply,
  onApplied,
  onCancel,
  onSteer,
  onError,
}: {
  task: Task;
  workers: Worker[];
  nodes: ExecutionNode[];
  progress: WorkProgress;
  eventsByWorker: Map<string, EventRecord[]>;
  selectedWorkerId: string;
  onSelect: (id: string) => void;
  onReview: (id: string) => Promise<WorkerChangesReview>;
  onApply: (id: string) => Promise<void>;
  onApplied: () => Promise<void>;
  onCancel: (id: string) => Promise<void>;
  onSteer: (id: string, message: string) => Promise<void>;
  onError: (message: string) => void;
}) {
  const [applying, setApplying] = useState<string>("");
  const [diffs, setDiffs] = useState<Record<string, DiffReviewState>>({});
  const [steering, setSteering] = useState<Record<string, string>>({});

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

  async function steer(event: React.FormEvent, workerId: string) {
    event.preventDefault();
    const message = steering[workerId] ?? "";
    try {
      await onSteer(workerId, message);
      setSteering((items) => ({ ...items, [workerId]: "" }));
    } catch (err) {
      onError(errorMessage(err));
    }
  }

  const rows = orchestrationRows(workers, nodes);

  function selectWorker(workerId: string) {
    if (!workerId) return;
    onSelect(workerId);
    document.getElementById(workerCardDomId(workerId))?.scrollIntoView({ behavior: "smooth", block: "center" });
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
      <OrchestrationOverview progress={progress} nodes={nodes} workers={workers} />
      {workers.length === 0 && nodes.length === 0 ? (
        <p className="empty">No workers have been spawned.</p>
      ) : (
        <>
          <WorkerNavigator rows={rows} progress={progress} selectedWorkerId={selectedWorkerId} onSelect={selectWorker} />
          <div className="worker-grid">
            {rows.map(({ worker, node }) => {
              const rowId = worker?.id ?? node?.id ?? "";
              const status = worker?.status ?? node?.status ?? "queued";
              const workerId = worker?.id ?? node?.workerId ?? "";
              const workerEvents = workerId ? eventsByWorker.get(workerId) ?? EMPTY_EVENTS : EMPTY_EVENTS;
              const kind = worker?.kind ?? node?.workerKind ?? "worker";
              const completion = workerId ? latestWorkerCompletion(workerEvents, workerId) : {};
              const changes = completion.changedFiles ?? completion.workspaceChanges?.changedFiles ?? [];
              const applied = workerId ? workerChangesApplied(workerEvents, workerId) : false;
              const latestEvent = latestWorkerProgressEvent(workerEvents) ?? latestInspectableWorkerEvent(workerEvents);
            const diff = workerId ? diffs[workerId] : undefined;
            const dependencies = node?.dependsOn ?? [];
            const blockers = dependencies.filter((dependencyId) => {
              const dependency = nodes.find((candidate) => candidate.id === dependencyId || candidate.spawnId === dependencyId);
              return dependency && dependency.status !== "succeeded";
            });
            const duration = worker ? formatDuration(worker.createdAt, worker.updatedAt) : node ? formatDuration(node.createdAt, node.updatedAt) : "";
            const idle = formatWorkerIdle(status, worker?.updatedAt ?? node?.updatedAt ?? "");
            return (
              <article id={workerId ? workerCardDomId(workerId) : undefined} key={rowId} className={workerId === selectedWorkerId ? "worker-card selected" : "worker-card"}>
                <div>
                  <strong>{node?.role || kind}</strong>
                  <small>{workerId ? workerId.slice(0, 8) : rowId.slice(0, 8)}</small>
                </div>
                <Status value={status} />
                <button className="icon-button ghost" disabled={!workerId} onClick={() => selectWorker(workerId)} title="Inspect worker">
                  <Eye size={16} />
                </button>
                <button className="icon-button danger" disabled={!workerId || isTerminalWorkerStatus(status)} onClick={() => onCancel(workerId).catch((err) => onError(errorMessage(err)))} title="Cancel worker">
                  <CircleStop size={16} />
                </button>
                <div className="worker-context">
                  <WorkerContextItem label="Kind" value={kind} />
                  <WorkerContextItem label="Node" value={node?.id.slice(0, 8) ?? "none"} />
                  <WorkerContextItem label="Target" value={targetLabel(node)} />
                  <WorkerContextItem label="Updated" value={worker ? new Date(worker.updatedAt).toLocaleTimeString() : node ? new Date(node.updatedAt).toLocaleTimeString() : ""} />
                  {duration && <WorkerContextItem label="Duration" value={duration} />}
                  {idle && <WorkerContextItem label="Idle" value={idle} />}
                  {node?.spawnId ? <WorkerContextItem label="Spawn" value={node.spawnId} /> : null}
                </div>
                {(dependencies.length > 0 || blockers.length > 0 || node?.reason) && (
                  <div className="worker-graph-context">
                    {dependencies.length > 0 && <span>Depends on {dependencies.map((id) => id.slice(0, 8)).join(", ")}</span>}
                    {blockers.length > 0 && <span className="warning">Blocked by {blockers.map((id) => id.slice(0, 8)).join(", ")}</span>}
                    {node?.reason && <p>{node.reason}</p>}
                  </div>
                )}
                <div className="worker-current">
                  <span>Latest</span>
                  <p>{latestEvent ? eventDisplayText(latestEvent) : "No worker events yet."}</p>
                </div>
                <WorkerActivity events={workerEvents} defaultOpen={status === "failed"} />
                {workerId && (
                  <form className="worker-steer" onSubmit={(event) => steer(event, workerId)}>
                    <input value={steering[workerId] ?? ""} onChange={(event) => setSteering((items) => ({ ...items, [workerId]: event.target.value }))} placeholder="Steer this worker..." required />
                    <button className="icon-button" title="Send worker steering">
                      <Send size={16} />
                    </button>
                  </form>
                )}
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
                      <button className="secondary compact" disabled={!workerId || applied || applying === workerId} onClick={() => apply(workerId)} title={applied ? "Worker changes already applied" : "Manual worker apply"}>
                        <Check size={16} />
                        {applied ? "Applied" : applying === workerId ? "Applying" : "Manual Apply"}
                      </button>
                    </div>
                    {diff?.open && <DiffViewer state={diff} />}
                  </div>
                )}
              </article>
            );
          })}
          </div>
        </>
      )}
    </section>
  );
}

function WorkerNavigator({
  rows,
  progress,
  selectedWorkerId,
  onSelect,
}: {
  rows: OrchestrationRow[];
  progress: WorkProgress;
  selectedWorkerId: string;
  onSelect: (id: string) => void;
}) {
  const items = rows.map((row, index) => {
    const workerId = row.worker?.id ?? row.node?.workerId ?? "";
    const rowId = workerId || row.node?.id || String(index + 1);
    const status = row.worker?.status ?? row.node?.status ?? "queued";
    const label = row.node?.role || row.worker?.kind || row.node?.workerKind || "worker";
    return { rowId, workerId, status, label };
  });
  const activeCount = items.filter((item) => item.status === "running" || item.status === "waiting" || item.status === "queued").length;
  const failedCount = items.filter((item) => item.status === "failed").length;
  const canceledCount = items.filter((item) => item.status === "canceled").length;
  return (
    <div className="worker-navigator" aria-label="Worker navigator">
      <div className="worker-navigator-summary">
        <strong>{progress.done}/{progress.total} complete</strong>
        <span>{activeCount} active</span>
        {failedCount > 0 && <span className="warning">{failedCount} failed</span>}
        {canceledCount > 0 && <span className="warning">{canceledCount} canceled</span>}
      </div>
      <div className="worker-navigator-list">
        {items.map((item, index) => (
          <button
            key={item.rowId}
            type="button"
            className={item.workerId === selectedWorkerId ? "worker-nav-chip selected" : "worker-nav-chip"}
            disabled={!item.workerId}
            onClick={() => onSelect(item.workerId)}
            title={item.workerId ? `Jump to ${item.label} ${item.workerId.slice(0, 8)}` : item.label}
          >
            <span className="worker-nav-index">{index + 1}</span>
            <strong>{item.label}</strong>
            <Status value={item.status} />
          </button>
        ))}
      </div>
    </div>
  );
}

function OrchestrationOverview({
  progress,
  nodes,
  workers,
}: {
  progress: WorkProgress;
  nodes: ExecutionNode[];
  workers: Worker[];
}) {
  const edgeCount = nodes.reduce((total, node) => total + (node.parentNodeId ? 1 : 0) + (node.dependsOn?.length ?? 0), 0);
  return (
    <div className="orchestration-overview">
      <div className="summary-grid compact">
        <Metric label="Progress" value={`${progress.percent}%`} />
        <Metric label="Done" value={`${progress.done}/${progress.total}`} />
        <Metric label="Running" value={String(progress.running)} />
        <Metric label="Waiting" value={String(progress.waiting)} />
        <Metric label="Failed/Canceled" value={String(progress.failed)} />
      </div>
      <div className="progress-track" aria-label={`Progress ${progress.percent}%`}>
        <div style={{ width: `${progress.percent}%` }} />
      </div>
      <div className="orchestration-meta">
        <span>{nodes.length || workers.length} execution nodes</span>
        <span>{edgeCount} dependencies</span>
      </div>
    </div>
  );
}

type OrchestrationRow = {
  worker?: Worker;
  node?: ExecutionNode;
};

function orchestrationRows(workers: Worker[], nodes: ExecutionNode[]): OrchestrationRow[] {
  const rows = new Map<string, OrchestrationRow>();
  for (const node of nodes) {
    rows.set(node.workerId ?? node.id, { node });
  }
  for (const worker of workers) {
    rows.set(worker.id, { ...rows.get(worker.id), worker });
  }
  return [...rows.values()];
}

function workerCardDomId(workerId: string): string {
  return `worker-card-${workerId}`;
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

function targetLabel(node: ExecutionNode | undefined): string {
  const targetId = node?.targetId;
  if (!targetId) return "local";
  return `${node?.targetKind ?? "target"}:${targetId}`;
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
        id: String(event.id),
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

function currentWorkerUpdate(workers: Worker[], nodes: ExecutionNode[], eventsByWorker: Map<string, EventRecord[]>): WorkerProgressUpdate | undefined {
  if (workers.length === 0 && nodes.length === 0) {
    return undefined;
  }

  const nodesByWorkerId = new Map(nodes.filter((node) => node.workerId).map((node) => [node.workerId!, node]));
  const activeWorkers = workers.filter((worker) => !isTerminalWorkerStatus(worker.status));
  const candidates = (activeWorkers.length > 0 ? activeWorkers : [...workers])
    .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
  const progressCandidates = candidates
    .map((worker) => {
      const progressEvent = latestWorkerProgressEvent(eventsByWorker.get(worker.id) ?? EMPTY_EVENTS);
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
    const workerEvents = eventsByWorker.get(worker.id) ?? EMPTY_EVENTS;
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
  if (event.type === "work_item.queued") {
    const kind = payloadValue(payload.kind) || "work";
    const target = payloadValue(payload.targetId);
    return target ? `Queued ${kind} for ${target}` : `Queued ${kind}`;
  }
  if (event.type === "work_item.started") {
    return "Work item started";
  }
  if (event.type === "work_item.completed") {
    return payloadValue(payload.status) || "Work item completed";
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
  const tasks = snapshot.tasks ?? [];
  const artifacts = snapshot.artifacts ?? tasks.flatMap((task) => (task.artifacts ?? []).map((artifact) => ({ ...artifact, taskId: task.id })));
  const lastEventId = snapshot.lastEventId ?? snapshot.events?.at(-1)?.id ?? 0;
  return {
    tasks,
    workers: snapshot.workers ?? [],
    executionNodes: snapshot.executionNodes ?? [],
    workItems: snapshot.workItems ?? [],
    artifacts,
    memoryEntries: snapshot.memoryEntries ?? [],
    questions: snapshot.questions ?? [],
    sessions: snapshot.sessions ?? [],
    targets: snapshot.targets ?? [],
    plugins: snapshot.plugins ?? [],
    promptSets: snapshot.promptSets ?? [],
    projects: snapshot.projects ?? [],
    pullRequests: snapshot.pullRequests ?? [],
    pullRequestFeedback: snapshot.pullRequestFeedback ?? [],
    steering: snapshot.steering ?? [],
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
    tasks,
    workers: [
      ...snapshot.workers.filter((worker) => !taskIds.has(worker.taskId)),
      ...taskSnapshot.workers,
    ],
    executionNodes: [
      ...snapshot.executionNodes.filter((node) => !taskIds.has(node.taskId)),
      ...taskSnapshot.executionNodes,
    ],
    workItems: [
      ...snapshot.workItems.filter((item) => !taskIds.has(item.taskId)),
      ...taskSnapshot.workItems,
    ],
    artifacts: [
      ...snapshot.artifacts.filter((artifact) => !taskIds.has(artifact.taskId)),
      ...taskSnapshot.artifacts,
    ],
    memoryEntries: [
      ...snapshot.memoryEntries.filter((entry) => !entry.taskId || !taskIds.has(entry.taskId)),
      ...taskSnapshot.memoryEntries,
    ],
    questions: [
      ...snapshot.questions.filter((question) => !taskIds.has(question.taskId)),
      ...taskSnapshot.questions,
    ],
    sessions: [
      ...snapshot.sessions.filter((session) => !taskIds.has(session.taskId)),
      ...taskSnapshot.sessions,
    ],
    pullRequests: [
      ...snapshot.pullRequests.filter((pr) => !taskIds.has(pr.taskId)),
      ...taskSnapshot.pullRequests,
    ],
    pullRequestFeedback: [
      ...snapshot.pullRequestFeedback.filter((feedback) => !taskIds.has(feedback.taskId)),
      ...taskSnapshot.pullRequestFeedback,
    ],
    steering: [
      ...snapshot.steering.filter((item) => !taskIds.has(item.taskId)),
      ...taskSnapshot.steering,
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
  return trimTaskEventHistory([...byId.values()].sort((left, right) => left.id - right.id));
}

function trimTaskEventHistory(events: EventRecord[]): EventRecord[] {
  const taskCounts = new Map<string, number>();
  const kept: EventRecord[] = [];
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (!event.taskId) {
      kept.push(event);
      continue;
    }
    const count = taskCounts.get(event.taskId) ?? 0;
    if (count >= TASK_EVENT_HISTORY_LIMIT) {
      continue;
    }
    taskCounts.set(event.taskId, count + 1);
    kept.push(event);
  }
  return kept.reverse();
}

function maxEventId(events: EventRecord[]): number {
  return events.reduce((max, event) => Math.max(max, event.id), 0);
}

function applyProjectionEvent(snapshot: AppSnapshot, event: EventRecord): AppSnapshot {
  const payload = asRecord(event.payload);
  const nextSessions = applySessionProjection(snapshot.sessions, event, payload);
  if (nextSessions !== snapshot.sessions) {
    snapshot = { ...snapshot, sessions: nextSessions };
  }
  if (event.type === "work_item.queued" || event.type === "work_item.started" || event.type === "work_item.completed") {
    const workItems = applyWorkItemProjection(snapshot.workItems, event, payload);
    return {
      ...snapshot,
      workItems,
      steering: event.type === "work_item.completed" ? applySteeringWorkItemCompletedProjection(snapshot.steering, workItems, event, payload) : snapshot.steering,
    };
  }
  if (event.type === "approval.needed" || event.type === "approval.decided") {
    return { ...snapshot, questions: applyQuestionProjection(snapshot.questions, event, payload) };
  }
  if (event.type === "pull_request.followup_started") {
    return { ...snapshot, pullRequestFeedback: applyPullRequestFeedbackProjection(snapshot.pullRequestFeedback, snapshot.pullRequests, event, payload) };
  }
  if (event.type === "task.action_executed") {
    const memoryEntry = memoryEntryFromTaskAction(snapshot.tasks, event, payload);
    return {
      ...snapshot,
      pullRequestFeedback: applyPullRequestFeedbackActionProjection(snapshot.pullRequestFeedback, event, payload),
      memoryEntries: memoryEntry ? upsertById(snapshot.memoryEntries, memoryEntry) : snapshot.memoryEntries,
    };
  }
  if (event.type === "task.steered" || event.type === "worker.steering_queued") {
    return { ...snapshot, steering: applySteeringProjection(snapshot.steering, snapshot.workers, snapshot.executionNodes, event, payload) };
  }
  if (event.type === "task.planned" || event.type === "task.replanned") {
    return { ...snapshot, steering: applyTaskSteeringAppliedProjection(snapshot.steering, event.taskId ?? "", event.id, event.at) };
  }
  if (event.type === "task.created" && event.taskId) {
    const task: Task = {
      id: event.taskId,
      projectId: String(payload.projectId ?? "") || (isRecord(payload.metadata) ? String(payload.metadata.projectId ?? "") : undefined),
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
    return { ...snapshot, tasks };
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
    const artifact: Artifact = {
      id: String(payload.id ?? "") || `event-${event.id}`,
      taskId: event.taskId,
      kind: String(payload.kind ?? ""),
      name: String(payload.name ?? "") || undefined,
      url: String(payload.url ?? "") || undefined,
      ref: String(payload.ref ?? "") || undefined,
      createdAt: event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
    };
    return {
      ...snapshot,
      artifacts: upsertArtifactClient(snapshot.artifacts, artifact),
      tasks: upsertById(snapshot.tasks, {
        ...task,
        artifacts: upsertTaskArtifactClient(task.artifacts ?? [], {
          id: artifact.id,
          kind: artifact.kind,
          name: artifact.name,
          url: artifact.url,
          ref: artifact.ref,
          createdAt: artifact.createdAt,
          updatedAt: artifact.updatedAt,
          metadata: artifact.metadata,
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
      workItems: snapshot.workItems.filter((item) => item.taskId !== event.taskId),
      artifacts: snapshot.artifacts.filter((artifact) => artifact.taskId !== event.taskId),
      memoryEntries: snapshot.memoryEntries.filter((entry) => entry.taskId !== event.taskId),
      questions: snapshot.questions.filter((question) => question.taskId !== event.taskId),
      sessions: snapshot.sessions.filter((session) => session.taskId !== event.taskId),
      pullRequests: snapshot.pullRequests.filter((pr) => pr.taskId !== event.taskId),
      pullRequestFeedback: snapshot.pullRequestFeedback.filter((feedback) => feedback.taskId !== event.taskId),
      steering: snapshot.steering.filter((item) => item.taskId !== event.taskId),
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
    return { ...snapshot, executionNodes };
  }
  if (event.type === "execution.node_status") {
    const nodeId = String(payload.nodeId ?? "");
    const executionNodes = snapshot.executionNodes.map((node) => node.id === nodeId ? { ...node, status: String(payload.status) as WorkerStatus, updatedAt: event.at } : node);
    return { ...snapshot, executionNodes };
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
    return { ...snapshot, workers, executionNodes };
  }
  if (event.type === "worker.output" && event.workerId) {
    const workers = snapshot.workers.map((worker) => worker.id === event.workerId && !isTerminalWorkerStatus(worker.status) ? { ...worker, updatedAt: event.at } : worker);
    const executionNodes = snapshot.executionNodes.map((node) => node.workerId === event.workerId && !isTerminalWorkerStatus(node.status) ? { ...node, updatedAt: event.at } : node);
    return { ...snapshot, workers, executionNodes };
  }
  if (event.type === "worker.changes_applied" && event.taskId && event.workerId) {
    const task = snapshot.tasks.find((candidate) => candidate.id === event.taskId);
    return task ? { ...snapshot, tasks: upsertById(snapshot.tasks, { ...task, appliedWorkerId: event.workerId, updatedAt: event.at }) } : snapshot;
  }
  if ((event.type === "pull_request.published" || event.type === "pull_request.updated") && event.taskId) {
    const id = String(payload.id ?? "") || `${String(payload.repo ?? "")}#${String(payload.number ?? "")}`;
    if (!id) return snapshot;
    const existing = snapshot.pullRequests.find((candidate) => candidate.id === id);
    const nextPullRequest = {
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
      branchOwner: String(payload.branchOwner ?? "") || existing?.branchOwner,
      branchOwnerDir: String(payload.branchOwnerDir ?? "") || existing?.branchOwnerDir,
      branchHead: String(payload.branchHead ?? "") || existing?.branchHead,
      updateLeaseOwner: String(payload.updateLeaseOwner ?? "") || existing?.updateLeaseOwner,
      updateLeaseDir: String(payload.updateLeaseDir ?? "") || existing?.updateLeaseDir,
      updateBaseHead: String(payload.updateBaseHead ?? "") || existing?.updateBaseHead,
      createdAt: existing?.createdAt || event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : existing?.metadata,
    };
    return {
      ...snapshot,
      pullRequests: upsertById(snapshot.pullRequests, nextPullRequest),
      pullRequestFeedback: refreshPullRequestFeedbackProjection(snapshot.pullRequestFeedback, nextPullRequest, event.at),
    };
  }
  if (event.type === "pull_request.status_checked") {
    const id = String(payload.id ?? "");
    const pr = snapshot.pullRequests.find((candidate) => candidate.id === id);
    if (!pr) return snapshot;
    const nextPullRequest = { ...pr, state: String(payload.state ?? "") || pr.state, draft: Boolean(payload.draft), checksStatus: String(payload.checksStatus ?? "") || pr.checksStatus, checksConclusion: String(payload.checksConclusion ?? "") || pr.checksConclusion, mergeStatus: String(payload.mergeStatus ?? "") || pr.mergeStatus, mergeable: String(payload.mergeable ?? "") || pr.mergeable, reviewStatus: String(payload.reviewStatus ?? "") || pr.reviewStatus, updatedAt: event.at, metadata: isRecord(payload.metadata) ? payload.metadata : pr.metadata };
    return {
      ...snapshot,
      pullRequests: upsertById(snapshot.pullRequests, nextPullRequest),
      pullRequestFeedback: refreshPullRequestFeedbackProjection(snapshot.pullRequestFeedback, nextPullRequest, event.at),
    };
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

function applySteeringProjection(items: SteeringItem[], workers: Worker[], nodes: ExecutionNode[], event: EventRecord, payload: Record<string, unknown>): SteeringItem[] {
  if (!event.taskId) return items;
  if (event.type === "task.steered") {
    const message = payloadValue(payload.message).trim();
    if (!message) return items;
    return upsertById(items, {
      id: `task_steering_${event.id}`,
      taskId: event.taskId,
      targetKind: payloadValue(payload.targetKind) || "task",
      targetId: payloadValue(payload.targetId) || event.taskId,
      status: "pending",
      reason: payloadValue(payload.reason) || "user_task_steering",
      message,
      createdAt: event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
    });
  }
  const workerId = payloadValue(payload.workerId) || event.workerId || "";
  const message = payloadValue(payload.message).trim();
  if (!workerId || !message) return items;
  const worker = workers.find((candidate) => candidate.id === workerId);
  const node = nodes.find((candidate) => candidate.workerId === workerId);
  return upsertById(items, {
    id: `worker_steering_${event.id}`,
    taskId: event.taskId,
    workerId,
    nodeId: payloadValue(payload.nodeId) || node?.id,
    workerKind: payloadValue(payload.workerKind) || worker?.kind || node?.workerKind,
    role: payloadValue(payload.role) || node?.role,
    spawnId: payloadValue(payload.spawnId) || node?.spawnId,
    candidateWorkerId: undefined,
    reviewPhase: undefined,
    targetKind: "worker",
    targetId: workerId,
    status: payloadValue(payload.status) || "pending",
    reason: payloadValue(payload.reason) || "user_worker_steering",
    message,
    createdAt: event.at,
    updatedAt: event.at,
    metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
  });
}

function applyTaskSteeringAppliedProjection(items: SteeringItem[], taskId: string, eventId: number, at: string): SteeringItem[] {
  if (!taskId) return items;
  return items.map((item) => {
    if (item.taskId !== taskId || item.targetKind !== "task" || item.status !== "pending") return item;
    const itemEventId = Number.parseInt(item.id.split("_").at(-1) ?? "0", 10);
    if (!Number.isFinite(itemEventId) || itemEventId >= eventId) return item;
    return { ...item, status: "applied", appliedAt: at, updatedAt: at };
  });
}

function applySteeringWorkItemCompletedProjection(items: SteeringItem[], workItems: WorkItem[], event: EventRecord, payload: Record<string, unknown>): SteeringItem[] {
  const id = payloadValue(payload.id);
  if (!id.startsWith("worker_steering_")) return items;
  const workItem = workItems.find((item) => item.id === id);
  if (workItem?.kind !== "user.worker_steering") return items;
  return items.map((item) => item.id === id ? {
    ...item,
    status: workItem.status,
    workerId: workItem.workerId || item.workerId,
    appliedAt: event.at,
    updatedAt: event.at,
  } : item);
}

function applyPullRequestFeedbackProjection(items: PullRequestFeedback[], pullRequests: PullRequestState[], event: EventRecord, payload: Record<string, unknown>): PullRequestFeedback[] {
  if (!event.taskId) return items;
  const rawID = payloadValue(payload.id);
  if (!rawID) return items;
  const pr = pullRequests.find((candidate) => candidate.taskId === event.taskId && pullRequestFeedbackTargetMatches(candidate, rawID, payload));
  const pullRequestId = pr?.id || rawID;
  const feedbackSignature = pr ? unhandledPullRequestFeedbackSignature(pr, payloadValue(payload.feedbackSignature)) : payloadValue(payload.feedbackSignature);
  const id = `${event.taskId}:${pullRequestId}:${feedbackSignature || event.id}`;
  const existing = items.find((item) => item.id === id);
  const next: PullRequestFeedback = refreshPullRequestFeedbackProjectionItem({
    id,
    taskId: event.taskId,
    pullRequestId,
    eventId: event.id,
    attempt: numberPayload(payload.attempt),
    status: "pending",
    reason: payloadValue(payload.reason) || undefined,
    repo: payloadValue(payload.repo) || pr?.repo,
    number: numberPayload(payload.number) || pr?.number,
    url: payloadValue(payload.url) || pr?.url,
    branch: payloadValue(payload.branch) || pr?.branch,
    base: payloadValue(payload.base) || pr?.base,
    state: payloadValue(payload.state) || pr?.state,
    checksStatus: payloadValue(payload.checksStatus) || pr?.checksStatus,
    mergeStatus: payloadValue(payload.mergeStatus) || pr?.mergeStatus,
    reviewStatus: payloadValue(payload.reviewStatus) || pr?.reviewStatus,
    feedbackSignature,
    feedbackBody: pr && feedbackSignature ? latestPullRequestFeedbackBody(pr.metadata) : undefined,
    prompt: payloadValue(payload.prompt) || undefined,
    createdAt: existing?.createdAt || event.at,
    updatedAt: event.at,
    metadata: existing?.metadata,
  }, pr, event.at);
  return upsertById(items, next);
}

function applyPullRequestFeedbackActionProjection(items: PullRequestFeedback[], event: EventRecord, payload: Record<string, unknown>): PullRequestFeedback[] {
  const kind = payloadValue(payload.kind);
  const status = payloadValue(payload.status);
  if (status === "started" || status === "waiting" || status === "continued") return items;
  const inputs = isRecord(payload.inputs) ? payload.inputs : {};
  const pullRequestId = payloadValue(payload.pullRequestId);
  return items.map((item) => {
    if (item.taskId !== event.taskId || item.status !== "pending") return item;
    let handled = false;
    if (kind === "watch_pull_requests") {
      handled = !item.feedbackSignature && pullRequestFeedbackActionMatches(item, pullRequestId, inputs);
    }
    if (kind === "update_pull_request") {
      handled = !status && pullRequestFeedbackActionMatches(item, pullRequestId, inputs);
      if (handled && pullRequestFeedbackRequiresMetadataUpdate(item) && !updatePullRequestActionHasMetadata(inputs)) {
        handled = false;
      }
    }
    return handled ? { ...item, status: "handled", handledAt: event.at, updatedAt: event.at } : item;
  });
}

function refreshPullRequestFeedbackProjection(items: PullRequestFeedback[], pr: PullRequestState, at: string): PullRequestFeedback[] {
  return items.map((item) => {
    if (item.taskId !== pr.taskId || item.pullRequestId !== pr.id || item.status !== "pending") return item;
    return refreshPullRequestFeedbackProjectionItem(item, pr, at);
  });
}

function refreshPullRequestFeedbackProjectionItem(item: PullRequestFeedback, pr: PullRequestState | undefined, at: string): PullRequestFeedback {
  if (!pr) return item;
  const next: PullRequestFeedback = {
    ...item,
    repo: item.repo || pr.repo,
    number: item.number || pr.number,
    url: item.url || pr.url,
    branch: item.branch || pr.branch,
    base: item.base || pr.base,
    state: pr.state || item.state,
    checksStatus: pr.checksStatus || item.checksStatus,
    mergeStatus: pr.mergeStatus || item.mergeStatus,
    reviewStatus: pr.reviewStatus || item.reviewStatus,
    feedbackBody: item.feedbackSignature ? latestPullRequestFeedbackBody(pr.metadata) : item.feedbackBody,
    updatedAt: at,
  };
  if (isTerminalPullRequestState(pr.state) || (item.feedbackSignature && !unhandledPullRequestFeedbackSignature(pr, item.feedbackSignature))) {
    return { ...next, status: "handled", handledAt: at };
  }
  return next;
}

function pullRequestFeedbackTargetMatches(pr: PullRequestState, id: string, payload: Record<string, unknown>): boolean {
  if (id && pr.id === id) return true;
  const repo = payloadValue(payload.repo);
  const number = numberPayload(payload.number);
  if (repo && number && pr.repo.toLowerCase() === repo.toLowerCase() && pr.number === number) return true;
  const url = payloadValue(payload.url);
  if (url && pr.url.toLowerCase() === url.toLowerCase()) return true;
  const branch = payloadValue(payload.branch);
  return Boolean(branch && pr.branch === branch && (!repo || pr.repo.toLowerCase() === repo.toLowerCase()));
}

function pullRequestFeedbackActionMatches(item: PullRequestFeedback, pullRequestId: string, inputs: Record<string, unknown>): boolean {
  if (pullRequestId && pullRequestId === item.pullRequestId) return true;
  const id = payloadValue(inputs.id);
  if (id && id === item.pullRequestId) return true;
  const url = payloadValue(inputs.url);
  if (url && item.url && url.toLowerCase() === item.url.toLowerCase()) return true;
  const repo = payloadValue(inputs.repo);
  const number = numberPayload(inputs.number);
  if (repo && number && item.repo && repo.toLowerCase() === item.repo.toLowerCase() && number === item.number) return true;
  const branch = payloadValue(inputs.branch) || payloadValue(inputs.headBranch);
  return Boolean(branch && branch === item.branch && (!repo || !item.repo || repo.toLowerCase() === item.repo.toLowerCase()));
}

function unhandledPullRequestFeedbackSignature(pr: PullRequestState, signature: string): string {
  signature = signature.trim();
  if (!signature) return "";
  const current = latestPullRequestFeedbackSignature(pr.metadata);
  const triggered = latestPullRequestTriggeredFeedbackSignature(pr.metadata);
  return current === signature && triggered !== signature ? signature : "";
}

function latestPullRequestFeedbackSignature(metadata: Record<string, unknown> | undefined): string {
  return payloadValue(metadata?.latestPullRequestFeedbackSignature) || payloadValue(metadata?.latestConversationCommentSignature);
}

function latestPullRequestTriggeredFeedbackSignature(metadata: Record<string, unknown> | undefined): string {
  return payloadValue(metadata?.latestPullRequestFeedbackTriggeredSignature) || payloadValue(metadata?.latestConversationCommentTriggeredSignature);
}

function latestPullRequestFeedbackBody(metadata: Record<string, unknown> | undefined): string {
  return payloadValue(metadata?.latestPullRequestFeedbackBody) || payloadValue(metadata?.latestConversationCommentBody);
}

function pullRequestFeedbackRequiresMetadataUpdate(item: PullRequestFeedback): boolean {
  const body = (item.feedbackBody ?? "").toLowerCase();
  return body.includes("title") || body.includes("description") || body.includes("pr body") || body.includes("pull request body");
}

function updatePullRequestActionHasMetadata(inputs: Record<string, unknown>): boolean {
  return Boolean(payloadValue(inputs.title) || payloadValue(inputs.body));
}

function isTerminalPullRequestState(state: string | undefined): boolean {
  return state === "MERGED" || state === "CLOSED";
}

function numberPayload(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function applyWorkItemProjection(items: WorkItem[], event: EventRecord, payload: Record<string, unknown>): WorkItem[] {
  const id = payloadValue(payload.id);
  if (!id) return items;
  const existing = items.find((item) => item.id === id);
  if (event.type === "work_item.queued") {
    const next: WorkItem = {
      id,
      taskId: event.taskId ?? existing?.taskId ?? "",
      kind: payloadValue(payload.kind) || existing?.kind || "work",
      status: "queued",
      targetKind: payloadValue(payload.targetKind) || undefined,
      targetId: payloadValue(payload.targetId) || undefined,
      reason: payloadValue(payload.reason) || undefined,
      prompt: payloadValue(payload.prompt) || undefined,
      workerId: existing?.workerId,
      leaseOwner: undefined,
      leaseUntil: undefined,
      attempt: existing?.attempt,
      createdAt: existing?.createdAt || event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : existing?.metadata,
    };
    return upsertById(items, next);
  }
  if (!existing) return items;
  if (event.type === "work_item.started") {
    return upsertById(items, {
      ...existing,
      status: "running",
      workerId: payloadValue(payload.workerId) || undefined,
      leaseOwner: payloadValue(payload.leaseOwner) || undefined,
      leaseUntil: payloadValue(payload.leaseUntil) || undefined,
      attempt: Number(payload.attempt ?? existing.attempt ?? 0) || (existing.attempt ?? 0) + 1,
      updatedAt: event.at,
    });
  }
  if (event.type === "work_item.completed") {
    return upsertById(items, {
      ...existing,
      status: payloadValue(payload.status) || existing.status,
      workerId: payloadValue(payload.workerId) || existing.workerId,
      leaseOwner: undefined,
      leaseUntil: undefined,
      error: payloadValue(payload.error) || undefined,
      updatedAt: event.at,
    });
  }
  return items;
}

function applyQuestionProjection(questions: Question[], event: EventRecord, payload: Record<string, unknown>): Question[] {
  if (event.type === "approval.needed") {
    const id = `approval_${event.id}`;
    const existing = questions.find((question) => question.id === id);
    return upsertById(questions, {
      ...existing,
      id,
      taskId: event.taskId ?? existing?.taskId ?? "",
      workerId: event.workerId || payloadValue(payload.workerId) || existing?.workerId,
      reason: payloadValue(payload.reason) || existing?.reason,
      question: payloadValue(payload.question || payload.error || payload.summary) || existing?.question || "Approval needed.",
      decided: existing?.decided ?? false,
      answer: existing?.answer,
      approved: existing?.approved,
      createdAt: existing?.createdAt || event.at,
      updatedAt: event.at,
      metadata: isRecord(payload.metadata) ? payload.metadata : existing?.metadata,
    });
  }
  if (event.type !== "approval.decided") {
    return questions;
  }
  const questionId = payloadValue(payload.questionId);
  if (questionId) {
    const selected = questions.find((question) => question.id === questionId && question.taskId === event.taskId && !question.decided);
    if (selected) {
      return upsertById(questions, {
        ...selected,
        decided: true,
        answer: payloadValue(payload.answer || payload.message) || selected.answer,
        approved: typeof payload.approved === "boolean" ? payload.approved : selected.approved,
        updatedAt: event.at,
      });
    }
  }
  const workerId = event.workerId || payloadValue(payload.workerId);
  let selected: Question | undefined;
  for (const question of questions) {
    if (question.taskId !== event.taskId || question.decided) continue;
    if (workerId && question.workerId && question.workerId !== workerId) continue;
    if (!selected || Date.parse(question.createdAt) > Date.parse(selected.createdAt)) {
      selected = question;
    }
  }
  if (!selected && workerId) {
    for (const question of questions) {
      if (question.taskId !== event.taskId || question.decided) continue;
      if (!selected || Date.parse(question.createdAt) > Date.parse(selected.createdAt)) {
        selected = question;
      }
    }
  }
  if (!selected) return questions;
  return upsertById(questions, {
    ...selected,
    decided: true,
    answer: payloadValue(payload.answer || payload.message) || selected.answer,
    approved: typeof payload.approved === "boolean" ? payload.approved : selected.approved,
    updatedAt: event.at,
  });
}

function applySessionProjection(sessions: Session[], event: EventRecord, payload: Record<string, unknown>): Session[] {
  if (event.type === "execution.node_planned" && event.taskId) {
    const workerId = payloadValue(payload.workerId) || event.workerId || "";
    if (!workerId) return sessions;
    const existing = sessions.find((session) => session.id === workerId);
    return upsertById(sessions, {
      ...existing,
      id: workerId,
      taskId: event.taskId,
      workerId,
      nodeId: payloadValue(payload.nodeId) || existing?.nodeId,
      workerKind: payloadValue(payload.workerKind) || existing?.workerKind,
      role: payloadValue(payload.role) || existing?.role,
      spawnId: payloadValue(payload.spawnId) || existing?.spawnId,
      status: existing?.status || "queued",
      targetId: payloadValue(payload.targetId) || existing?.targetId,
      targetKind: payloadValue(payload.targetKind) || existing?.targetKind,
      remoteSession: payloadValue(payload.remoteSession) || existing?.remoteSession,
      remoteRunDir: payloadValue(payload.remoteRunDir) || existing?.remoteRunDir,
      remoteWorkDir: payloadValue(payload.remoteWorkDir) || existing?.remoteWorkDir,
      createdAt: existing?.createdAt || event.at,
      startedAt: existing?.startedAt,
      updatedAt: event.at,
      completedAt: existing?.completedAt,
      metadata: isRecord(payload.metadata) ? payload.metadata : existing?.metadata,
    });
  }
  if (event.type === "execution.node_status") {
    const nodeId = payloadValue(payload.nodeId);
    const status = payloadValue(payload.status) as WorkerStatus;
    const existing = sessions.find((session) => session.nodeId === nodeId);
    if (!existing) return sessions;
    return upsertById(sessions, sessionWithStatus(existing, status, event.at));
  }
  if (event.type === "worker.created" && event.workerId && event.taskId) {
    const existing = sessions.find((session) => session.id === event.workerId);
    const metadata = isRecord(payload.metadata) ? payload.metadata : existing?.metadata;
    return upsertById(sessions, {
      ...existing,
      id: event.workerId,
      taskId: event.taskId,
      workerId: event.workerId,
      workerKind: payloadValue(payload.kind) || existing?.workerKind,
      status: existing?.status || "queued",
      providerSessionId: metadata ? payloadValue(metadata.providerSessionId) || existing?.providerSessionId : existing?.providerSessionId,
      createdAt: existing?.createdAt || event.at,
      startedAt: existing?.startedAt,
      updatedAt: event.at,
      completedAt: existing?.completedAt,
      metadata,
    });
  }
  if (event.type === "worker.workspace_prepared" && event.workerId) {
    const existing = sessions.find((session) => session.id === event.workerId);
    if (!existing && !event.taskId) return sessions;
    return upsertById(sessions, {
      ...existing,
      id: event.workerId,
      taskId: existing?.taskId || event.taskId || payloadValue(payload.taskId),
      workerId: event.workerId,
      status: existing?.status || "queued",
      targetId: payloadValue(payload.targetId) || existing?.targetId,
      targetKind: payloadValue(payload.targetKind) || existing?.targetKind,
      workspaceRoot: payloadValue(payload.root) || existing?.workspaceRoot,
      workspaceCwd: payloadValue(payload.cwd) || existing?.workspaceCwd,
      sourceRoot: payloadValue(payload.sourceRoot) || existing?.sourceRoot,
      workspaceName: payloadValue(payload.workspaceName) || existing?.workspaceName,
      workspaceMode: payloadValue(payload.mode) || existing?.workspaceMode,
      vcsType: payloadValue(payload.vcsType) || existing?.vcsType,
      sharedRoot: payloadValue(payload.sharedRoot) || existing?.sharedRoot,
      sharedArtifactsDir: payloadValue(payload.sharedArtifactsDir) || existing?.sharedArtifactsDir,
      sharedWorkerDir: payloadValue(payload.sharedWorkerDir) || existing?.sharedWorkerDir,
      createdAt: existing?.createdAt || event.at,
      startedAt: existing?.startedAt,
      updatedAt: event.at,
      completedAt: existing?.completedAt,
      metadata: { ...(existing?.metadata ?? {}), workspace: payload },
    });
  }
  if ((event.type === "worker.started" || event.type === "worker.completed") && event.workerId) {
    const existing = sessions.find((session) => session.id === event.workerId);
    if (!existing && !event.taskId) return sessions;
    const status = event.type === "worker.started" ? "running" : payloadValue(payload.status) as WorkerStatus;
    return upsertById(sessions, sessionWithStatus({
      ...existing,
      id: event.workerId,
      taskId: existing?.taskId || event.taskId || "",
      workerId: event.workerId,
      status: existing?.status || "queued",
      createdAt: existing?.createdAt || event.at,
      updatedAt: existing?.updatedAt || event.at,
    }, status, event.at));
  }
  if (event.type === "worker.output" && event.workerId) {
    const existing = sessions.find((session) => session.id === event.workerId);
    if (!existing || isTerminalWorkerStatus(existing.status)) return sessions;
    return upsertById(sessions, {
      ...existing,
      currentAction: truncateSessionAction(eventDisplayText(event), 600),
      currentActionLabel: truncateSessionAction(workerEventLabel(event), 80),
      currentActionAt: event.at,
      currentActionEvent: event.id,
      updatedAt: event.at,
    });
  }
  return sessions;
}

function truncateSessionAction(value: string, limit: number): string {
  const compact = value.trim().replace(/\s+/g, " ");
  if (limit <= 0 || compact.length <= limit) return compact;
  return `${compact.slice(0, limit)}...`;
}

function sessionWithStatus(session: Session, status: WorkerStatus, at: string): Session {
  const terminal = isTerminalWorkerStatus(status);
  return {
    ...session,
    status: status || session.status,
    startedAt: status === "running" ? session.startedAt || at : session.startedAt,
    completedAt: terminal ? session.completedAt || at : session.completedAt,
    updatedAt: at,
  };
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

function upsertArtifactClient(items: Artifact[], next: Artifact): Artifact[] {
  if (!next.id) return [...items, next];
  return items.some((item) => item.id === next.id)
    ? items.map((item) => (item.id === next.id ? { ...next, createdAt: next.createdAt || item.createdAt } : item))
    : [...items, next];
}

function memoryEntryFromTaskAction(tasks: Task[], event: EventRecord, payload: Record<string, unknown>): MemoryEntry | undefined {
  if (!event.taskId) return undefined;
  const kind = payloadValue(payload.kind);
  const status = payloadValue(payload.status);
  const summary = payloadValue(payload.summary) || payloadValue(payload.reason);
  if (!summary) return undefined;
  const important = kind === "worker_result_digest" || status === "failed" || status === "waiting" || status === "rejected" || highValueMemoryText(summary);
  if (!important) return undefined;
  const task = tasks.find((candidate) => candidate.id === event.taskId);
  return {
    id: `memory-${event.id}`,
    projectId: task?.projectId,
    taskId: event.taskId,
    kind: kind || "task_action",
    sourceEventId: event.id,
    sourceEvent: event.type,
    workerId: payloadValue(payload.workerId) || event.workerId || undefined,
    summary,
    createdAt: event.at,
    updatedAt: event.at,
    metadata: isRecord(payload.metadata) ? payload.metadata : undefined,
  };
}

function highValueMemoryText(text: string): boolean {
  const lower = text.toLowerCase();
  return ["decision:", "decided", "blocked", "blocker", "root cause", "baseline", "benchmark", "regression", "invariant"].some((marker) => lower.includes(marker));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const rootElement = typeof document !== "undefined" ? document.getElementById("root") : null;
if (rootElement) {
  createRoot(rootElement).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}
