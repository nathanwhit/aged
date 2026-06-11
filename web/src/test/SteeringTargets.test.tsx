import { describe, expect, it, vi, afterEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import {
  ApprovalResponseForm,
  LiveSessionPanel,
  ManagerPullRequestSummary,
  WorkItemQueue,
  type ApprovalState,
} from "../main";
import * as api from "../api";
import type {
  ExecutionNode,
  PullRequestState,
  Session,
  Task,
  Worker,
  WorkItem,
} from "../types";

vi.mock("../api", async () => ({
  ...(await vi.importActual<typeof import("../api")>("../api")),
  getSessionTail: vi.fn(),
}));

afterEach(() => {
  vi.clearAllMocks();
});

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: "task-12345678abcdef",
    title: "Tune the ranking pipeline",
    prompt: "Tune the ranking pipeline",
    status: "running",
    createdAt: "2026-06-10T00:00:00Z",
    updatedAt: "2026-06-10T00:00:00Z",
    ...overrides,
  };
}

function makePullRequest(overrides: Partial<PullRequestState> = {}): PullRequestState {
  return {
    id: "pr-12345678abcdef",
    taskId: "task-1",
    repo: "owner/repo",
    number: 42,
    url: "https://github.com/owner/repo/pull/42",
    branch: "feature/ranking",
    base: "main",
    title: "Tune ranking",
    state: "OPEN",
    createdAt: "2026-06-10T00:00:00Z",
    updatedAt: "2026-06-10T00:00:00Z",
    ...overrides,
  };
}

function makeWorkItem(overrides: Partial<WorkItem> = {}): WorkItem {
  return {
    id: "work-item-12345678abcdef",
    taskId: "task-1",
    kind: "objective.slice",
    status: "queued",
    targetKind: "slice",
    targetId: "slice-12345678",
    createdAt: "2026-06-10T00:00:00Z",
    updatedAt: "2026-06-10T00:00:00Z",
    ...overrides,
  };
}

function makeSession(overrides: Partial<Session> = {}): Session {
  return {
    id: "session-12345678abcdef",
    taskId: "task-1",
    workerId: "worker-12345678abcdef",
    nodeId: "node-1",
    workerKind: "codex",
    role: "implementation",
    status: "running",
    targetId: "vultr-vm",
    targetKind: "ssh",
    remoteSession: "aged-worker-1",
    remoteWorkDir: "/work/repo",
    createdAt: "2026-06-10T00:00:00Z",
    updatedAt: "2026-06-10T00:00:00Z",
    ...overrides,
  };
}

function makeWorker(overrides: Partial<Worker> = {}): Worker {
  return {
    id: "worker-12345678abcdef",
    taskId: "task-1",
    kind: "codex",
    status: "running",
    command: ["codex", "exec"],
    createdAt: "2026-06-10T00:00:00Z",
    updatedAt: "2026-06-10T00:00:00Z",
    metadata: { model: "gpt-5" },
    ...overrides,
  };
}

function makeNode(overrides: Partial<ExecutionNode> = {}): ExecutionNode {
  return {
    id: "node-1",
    taskId: "task-1",
    workerId: "worker-12345678abcdef",
    workerKind: "codex",
    status: "running",
    targetId: "vultr-vm",
    targetKind: "ssh",
    remoteWorkDir: "/work/repo",
    createdAt: "2026-06-10T00:00:00Z",
    updatedAt: "2026-06-10T00:00:00Z",
    ...overrides,
  };
}

describe("LiveSessionPanel steering target", () => {
  it("shows live session steering labels with session, worker, role, and target", async () => {
    vi.mocked(api.getSessionTail).mockRejectedValue(new Error("no tail"));
    const onSteer = vi.fn().mockResolvedValue(undefined);
    render(
      <LiveSessionPanel
        session={makeSession()}
        worker={makeWorker()}
        node={makeNode()}
        events={[]}
        onSteer={onSteer}
        onCancel={vi.fn().mockResolvedValue(undefined)}
        onDone={vi.fn().mockResolvedValue(undefined)}
        onError={vi.fn()}
      />,
    );

    const form = await screen.findByRole("form", { name: /steer live session/i });
    expect(within(form).getByText("Live session steering")).toBeInTheDocument();
    expect(within(form).getByText(/session session-/i)).toBeInTheDocument();
    expect(within(form).getByText(/worker worker-/i)).toBeInTheDocument();
    expect(within(form).getByText("Implementation")).toBeInTheDocument();
    expect(within(form).getByText(/ssh vultr-vm/i)).toBeInTheDocument();

    await userEvent.type(within(form).getByPlaceholderText(/steer this exact session/i), "shift left");
    await userEvent.click(within(form).getByRole("button", { name: /send session steering/i }));

    expect(onSteer).toHaveBeenCalledWith("session-12345678abcdef", "shift left");
  });
});

describe("WorkItemQueue steering target", () => {
  it("shows work-item steering labels and routes onSteer with targetKind/targetId", async () => {
    const onSteer = vi.fn().mockResolvedValue(undefined);
    render(
      <WorkItemQueue
        taskId="task-1"
        items={[makeWorkItem()]}
        onCancel={vi.fn().mockResolvedValue(undefined)}
        onSteer={onSteer}
        onError={vi.fn()}
      />,
    );

    const form = screen.getByRole("form", { name: /steer objective slice work item/i });
    expect(within(form).getByText("Work item steering")).toBeInTheDocument();
    // humanized kind appears twice (work-item card heading + steering label).
    expect(within(form).getAllByText("Objective Slice").length).toBeGreaterThan(0);
    expect(within(form).getByText(/work-ite/i)).toBeInTheDocument();
    expect(within(form).getByText("queued")).toBeInTheDocument();
    expect(within(form).getByText(/slice slice-12/i)).toBeInTheDocument();

    await userEvent.type(within(form).getByPlaceholderText(/steer this work item/i), "narrow to parser");
    await userEvent.click(within(form).getByRole("button", { name: /^steer$/i }));

    expect(onSteer).toHaveBeenCalledWith("task-1", "narrow to parser", {
      targetKind: "work_item",
      targetId: "work-item-12345678abcdef",
    });
  });
});

describe("ManagerPullRequestSummary steering target", () => {
  it("shows PR follow-up steering labels and routes onSteer with pull_request target", async () => {
    const onSteer = vi.fn().mockResolvedValue(undefined);
    render(
      <ManagerPullRequestSummary
        task={makeTask()}
        pullRequests={[makePullRequest()]}
        selectedPullRequest={makePullRequest()}
        feedback={[]}
        artifacts={[]}
        onPublish={vi.fn().mockResolvedValue(makePullRequest())}
        onWatch={vi.fn().mockResolvedValue([])}
        onRefresh={vi.fn().mockResolvedValue(makePullRequest())}
        onBabysit={vi.fn().mockResolvedValue(undefined)}
        onSteer={onSteer}
        onDone={vi.fn().mockResolvedValue(undefined)}
        onError={vi.fn()}
      />,
    );

    const form = screen.getByRole("form", { name: /steer pull request follow-up/i });
    expect(within(form).getByText("PR follow-up steering")).toBeInTheDocument();
    expect(within(form).getByText("owner/repo#42")).toBeInTheDocument();
    expect(within(form).getByText("OPEN")).toBeInTheDocument();
    expect(within(form).getByText(/head feature\/ranking/i)).toBeInTheDocument();

    await userEvent.type(within(form).getByPlaceholderText(/steer this pr/i), "answer reviewer");
    await userEvent.click(within(form).getByRole("button", { name: /^steer$/i }));

    expect(onSteer).toHaveBeenCalledWith("task-12345678abcdef", "answer reviewer", {
      targetKind: "pull_request",
      targetId: "pr-12345678abcdef",
    });
  });
});

describe("ApprovalResponseForm steering target", () => {
  it("shows question answer labels with question id, reason, and worker", async () => {
    const onAnswer = vi.fn().mockResolvedValue(undefined);
    const approval: ApprovalState = {
      id: "q-12345678abcdef",
      at: "2026-06-10T00:00:00Z",
      question: "Approve destructive cleanup?",
      reason: "destructive_action",
      decided: false,
      workerId: "worker-87654321abcdef",
    };
    render(
      <ApprovalResponseForm
        taskId="task-1"
        approval={approval}
        onAnswer={onAnswer}
        onDone={vi.fn().mockResolvedValue(undefined)}
        onError={vi.fn()}
      />,
    );

    const form = screen.getByRole("form", { name: /answer question/i });
    expect(within(form).getByText("Question answer")).toBeInTheDocument();
    expect(within(form).getByText("q-123456")).toBeInTheDocument();
    expect(within(form).getByText("Destructive Action")).toBeInTheDocument();
    expect(within(form).getByText(/worker worker-/i)).toBeInTheDocument();

    await userEvent.type(within(form).getByPlaceholderText(/answer this question/i), "yes proceed");
    await userEvent.click(within(form).getByRole("button", { name: /send answer/i }));

    expect(onAnswer).toHaveBeenCalledWith("task-1", "q-12345678abcdef", "yes proceed");
  });
});
