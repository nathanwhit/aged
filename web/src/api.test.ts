import { afterEach, expect, it, vi } from "vitest";
import { cancelSession, getSessionTail, steerSession } from "./api";

afterEach(() => {
  vi.restoreAllMocks();
});

function mockFetch(body: unknown = {}): ReturnType<typeof vi.fn> {
  const fetchMock = vi.fn().mockResolvedValue({
    ok: true,
    json: async () => body,
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

it("gets session tail with cursor options", async () => {
  const fetchMock = mockFetch({ sessionId: "session 1", events: [] });

  await getSessionTail("session 1", { after: 12, limit: 25, kinds: ["worker.output", "worker.completed"] });

  expect(fetchMock).toHaveBeenCalledWith(
    "/api/sessions/session%201/tail?after=12&limit=25&kinds=worker.output%2Cworker.completed",
    undefined,
  );
});

it("sends session steering to the session endpoint", async () => {
  const fetchMock = mockFetch();

  await steerSession("session 1", "focus here");

  expect(fetchMock).toHaveBeenCalledWith("/api/sessions/session%201/steer", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message: "focus here" }),
  });
});

it("cancels sessions through the session endpoint", async () => {
  const fetchMock = mockFetch();

  await cancelSession("session 1");

  expect(fetchMock).toHaveBeenCalledWith("/api/sessions/session%201/cancel", { method: "POST" });
});
