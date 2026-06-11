import { act, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import * as api from "../api";
import { LiveSessionPanel } from "../main";
import type { EventRecord, ExecutionNode, Session, Worker } from "../types";

vi.mock("../api", async () => ({
  ...(await vi.importActual<typeof import("../api")>("../api")),
  getSessionTail: vi.fn(),
}));

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
});

const baseSession: Session = {
  id: "session-1",
  taskId: "task-1",
  workerId: "worker-1",
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
};

const baseWorker: Worker = {
  id: "worker-1",
  taskId: "task-1",
  kind: "codex",
  status: "running",
  command: ["codex", "exec"],
  createdAt: "2026-06-10T00:00:00Z",
  updatedAt: "2026-06-10T00:00:00Z",
  metadata: { model: "gpt-5" },
};

const baseNode: ExecutionNode = {
  id: "node-1",
  taskId: "task-1",
  workerId: "worker-1",
  workerKind: "codex",
  status: "running",
  targetId: "vultr-vm",
  targetKind: "ssh",
  remoteWorkDir: "/work/repo",
  createdAt: "2026-06-10T00:00:00Z",
  updatedAt: "2026-06-10T00:00:00Z",
};

function renderPanel(events: EventRecord[] = []) {
  return render(
    <LiveSessionPanel
      session={baseSession}
      worker={baseWorker}
      node={baseNode}
      events={events}
      onSteer={vi.fn()}
      onCancel={vi.fn()}
      onDone={vi.fn()}
      onError={vi.fn()}
    />,
  );
}

describe("LiveSessionPanel", () => {
  it("renders polled session tail context", async () => {
    vi.mocked(api.getSessionTail).mockResolvedValue({
      sessionId: "session-1",
      workerId: "worker-1",
      taskId: "task-1",
      status: "running",
      lastEventId: 22,
      currentAction: { label: "tool", text: "running npm test", eventId: 22, at: "2026-06-10T00:00:02Z" },
      events: [
        {
          id: 22,
          at: "2026-06-10T00:00:02Z",
          type: "worker.output",
          taskId: "task-1",
          workerId: "worker-1",
          payload: { text: "running npm test" },
        },
      ],
      session: { ...baseSession, currentAction: "running npm test", currentActionLabel: "tool" },
      worker: baseWorker,
      node: baseNode,
      pullRequests: [
        {
          id: "pr-1",
          taskId: "task-1",
          repo: "owner/repo",
          number: 7,
          url: "https://github.com/owner/repo/pull/7",
          branch: "session-tail",
          base: "main",
          title: "Session tail",
          state: "OPEN",
          createdAt: "2026-06-10T00:00:00Z",
          updatedAt: "2026-06-10T00:00:00Z",
        },
      ],
      completion: {
        status: "succeeded",
        summary: "done",
        eventId: 23,
        at: "2026-06-10T00:00:03Z",
        changedFiles: [{ path: "web/src/main.tsx", status: "modified" }],
      },
      changedFiles: [{ path: "web/src/main.tsx", status: "modified" }],
    });

    renderPanel();

    await waitFor(() => expect(api.getSessionTail).toHaveBeenCalledWith("session-1", { after: 0, limit: 50 }));
    expect(await screen.findAllByText("running npm test")).not.toHaveLength(0);
    expect(screen.getByText("owner/repo #7 OPEN")).toBeInTheDocument();
    expect(screen.getByText("modified web/src/main.tsx")).toBeInTheDocument();
  });

  it("polls incrementally after receiving the initial latest tail", async () => {
    vi.useFakeTimers();
    try {
      vi.mocked(api.getSessionTail)
        .mockResolvedValueOnce({
          sessionId: "session-1",
          workerId: "worker-1",
          taskId: "task-1",
          status: "running",
          lastEventId: 42,
          events: [
            {
              id: 41,
              at: "2026-06-10T00:00:01Z",
              type: "worker.output",
              taskId: "task-1",
              workerId: "worker-1",
              payload: { text: "tail line a" },
            },
            {
              id: 42,
              at: "2026-06-10T00:00:02Z",
              type: "worker.output",
              taskId: "task-1",
              workerId: "worker-1",
              payload: { text: "tail line b" },
            },
          ],
          session: baseSession,
          worker: baseWorker,
          node: baseNode,
          pullRequests: [],
          changedFiles: [],
        })
        .mockResolvedValueOnce({
          sessionId: "session-1",
          workerId: "worker-1",
          taskId: "task-1",
          status: "running",
          lastEventId: 43,
          events: [
            {
              id: 43,
              at: "2026-06-10T00:00:03Z",
              type: "worker.output",
              taskId: "task-1",
              workerId: "worker-1",
              payload: { text: "incremental line" },
            },
          ],
          session: baseSession,
          worker: baseWorker,
          node: baseNode,
          pullRequests: [],
          changedFiles: [],
        });

      renderPanel();

      await act(async () => {
        await Promise.resolve();
      });
      await vi.waitFor(() =>
        expect(api.getSessionTail).toHaveBeenNthCalledWith(1, "session-1", { after: 0, limit: 50 }),
      );
      await act(async () => {
        await vi.advanceTimersByTimeAsync(3000);
      });

      expect(api.getSessionTail).toHaveBeenNthCalledWith(2, "session-1", { after: 42, limit: 50 });
    } finally {
      vi.useRealTimers();
    }
  });

  it("falls back to snapshot events when tail polling fails", async () => {
    vi.mocked(api.getSessionTail).mockRejectedValue(new Error("network down"));

    renderPanel([
      {
        id: 5,
        at: "2026-06-10T00:00:01Z",
        type: "worker.output",
        taskId: "task-1",
        workerId: "worker-1",
        payload: { text: "local fallback output" },
      },
    ]);

    expect(await screen.findAllByText("local fallback output")).not.toHaveLength(0);
    expect(api.getSessionTail).toHaveBeenCalledWith("session-1", { after: 5, limit: 50 });
  });
});
