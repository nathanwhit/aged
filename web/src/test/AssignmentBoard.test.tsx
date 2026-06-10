import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { AssignmentBoard, type AssignmentRow, type ApprovalState } from "../main";

const baseRow: Omit<AssignmentRow, "id" | "kind" | "title" | "subtitle" | "status" | "tone"> = {
  updatedAt: "2026-06-10T12:00:00Z",
};

function rosterFixture(): AssignmentRow[] {
  return [
    {
      id: "question:q-1",
      kind: "question",
      title: "Approve destructive cleanup?",
      subtitle: "User input required",
      status: "waiting_user",
      tone: "warning",
      owner: "Worker abc12345",
      projectContext: "proj-a",
      selection: { kind: "question", questionId: "q-1" },
      ...baseRow,
    },
    {
      id: "session:sess-1",
      kind: "session",
      title: "Implementer",
      subtitle: "tmux-1 · vm-1 · workspace",
      status: "running",
      tone: "info",
      currentAction: "Running tests...",
      owner: "Worker abc12345",
      model: "opus-4-7",
      projectContext: "vultr vm-1",
      selection: { kind: "session", sessionId: "sess-1" },
      action: [
        { kind: "inspect-session", sessionId: "sess-1" },
        { kind: "cancel-session", sessionId: "sess-1" },
      ],
      ...baseRow,
    },
    {
      id: "pr:pr-1",
      kind: "pull_request",
      title: "Refactor manager view",
      subtitle: "acme/repo#42",
      status: "open",
      tone: "info",
      owner: "Owner alice",
      prContext: "base main · head feature",
      selection: { kind: "pull_request", pullRequestId: "pr-1" },
      action: [
        { kind: "open-pr", url: "https://github.com/acme/repo/pull/42" },
        { kind: "refresh-pr", pullRequestId: "pr-1" },
        { kind: "babysit-pr", pullRequestId: "pr-1" },
      ],
      ...baseRow,
    },
    {
      id: "feedback:fb-1",
      kind: "feedback",
      title: "Review feedback needs follow-up",
      subtitle: "Changes requested",
      status: "pending",
      tone: "warning",
      currentAction: "Address failing smoke test before merge.",
      prContext: "acme/repo#42 · feature",
      selection: { kind: "pull_request", pullRequestId: "pr-1" },
      action: { kind: "open-pr", url: "https://github.com/acme/repo/pull/42#discussion_r1" },
      ...baseRow,
    },
    {
      id: "work:w-1",
      kind: "work",
      title: "Plan",
      subtitle: "Planning work",
      status: "queued",
      tone: "warning",
      action: { kind: "cancel-work-item", workItemId: "w-1" },
      ...baseRow,
    },
  ];
}

function makeProps(overrides: Partial<React.ComponentProps<typeof AssignmentBoard>> = {}) {
  return {
    taskId: "task-1",
    rows: rosterFixture(),
    approvals: [] as ApprovalState[],
    onInspectSession: vi.fn(),
    onCancelSession: vi.fn().mockResolvedValue(undefined),
    onRefreshPullRequest: vi.fn().mockResolvedValue(undefined),
    onBabysitPullRequest: vi.fn().mockResolvedValue(undefined),
    onCancelWorkItem: vi.fn().mockResolvedValue(undefined),
    onAnswerQuestion: vi.fn().mockResolvedValue(undefined),
    onDone: vi.fn().mockResolvedValue(undefined),
    onError: vi.fn(),
    ...overrides,
  };
}

describe("AssignmentBoard", () => {
  it("renders the assignment roster with each kind of row", () => {
    render(<AssignmentBoard {...makeProps()} />);

    expect(screen.getByText("Assignments")).toBeInTheDocument();
    expect(screen.getByText("5 active signals")).toBeInTheDocument();
    // The 3 warning rows should bubble up as "needs attention".
    expect(screen.getByText(/3 need attention/i)).toBeInTheDocument();

    expect(screen.getByText("Approve destructive cleanup?")).toBeInTheDocument();
    expect(screen.getByText("Implementer")).toBeInTheDocument();
    expect(screen.getByText("Refactor manager view")).toBeInTheDocument();
    expect(screen.getByText("Review feedback needs follow-up")).toBeInTheDocument();
    expect(screen.getByText("Plan")).toBeInTheDocument();
  });

  it("invokes onInspectSession when the live-session action is clicked", async () => {
    const props = makeProps();
    render(<AssignmentBoard {...props} />);

    const inspect = screen.getByRole("button", { name: /inspect implementer/i });
    await userEvent.click(inspect);

    expect(props.onInspectSession).toHaveBeenCalledTimes(1);
    expect(props.onInspectSession).toHaveBeenCalledWith("sess-1");
  });

  it("selects assignment rows with stable target metadata", async () => {
    const onSelectAssignment = vi.fn();
    render(<AssignmentBoard {...makeProps({ onSelectAssignment })} />);

    await userEvent.click(screen.getByText("Implementer"));

    expect(onSelectAssignment).toHaveBeenCalledWith(expect.objectContaining({
      id: "session:sess-1",
      selection: { kind: "session", sessionId: "sess-1" },
    }));
  });

  it("routes question and pull request row selection to the selected detail target", async () => {
    const onSelectAssignment = vi.fn();
    render(<AssignmentBoard {...makeProps({ onSelectAssignment })} />);

    await userEvent.click(screen.getByText("Approve destructive cleanup?"));
    await userEvent.click(screen.getByText("Refactor manager view"));
    await userEvent.click(screen.getByText("Review feedback needs follow-up"));

    expect(onSelectAssignment).toHaveBeenNthCalledWith(1, expect.objectContaining({
      id: "question:q-1",
      selection: { kind: "question", questionId: "q-1" },
    }));
    expect(onSelectAssignment).toHaveBeenNthCalledWith(2, expect.objectContaining({
      id: "pr:pr-1",
      selection: { kind: "pull_request", pullRequestId: "pr-1" },
    }));
    expect(onSelectAssignment).toHaveBeenNthCalledWith(3, expect.objectContaining({
      id: "feedback:fb-1",
      selection: { kind: "pull_request", pullRequestId: "pr-1" },
    }));
  });

  it("dispatches cancel for an active session row", async () => {
    const props = makeProps();
    render(<AssignmentBoard {...props} />);

    const sessionRow = screen.getByText("Implementer").closest(".assignment-row") as HTMLElement;
    expect(sessionRow).toBeTruthy();
    const cancel = within(sessionRow).getByRole("button", { name: /cancel implementer/i });
    await userEvent.click(cancel);

    expect(props.onCancelSession).toHaveBeenCalledWith("sess-1");
    expect(props.onDone).toHaveBeenCalled();
  });

  it("renders a pull request row with the linked PR affordance", () => {
    render(<AssignmentBoard {...makeProps()} />);

    const link = screen.getByRole("link", { name: /open refactor manager view/i });
    expect(link).toHaveAttribute("href", "https://github.com/acme/repo/pull/42");
    expect(link).toHaveAttribute("target", "_blank");

    // PR row context (repo#number) should be visible.
    expect(screen.getByText("acme/repo#42")).toBeInTheDocument();
    expect(screen.getByText(/base main · head feature/)).toBeInTheDocument();
  });

  it("dispatches pull request refresh and babysit actions from a PR row", async () => {
    const props = makeProps();
    render(<AssignmentBoard {...props} />);

    const prRow = screen.getByText("Refactor manager view").closest(".assignment-row") as HTMLElement;
    expect(prRow).toBeTruthy();

    await userEvent.click(within(prRow).getByRole("button", { name: /refresh refactor manager view/i }));
    await userEvent.click(within(prRow).getByRole("button", { name: /babysit refactor manager view/i }));

    expect(props.onRefreshPullRequest).toHaveBeenCalledWith("pr-1");
    expect(props.onBabysitPullRequest).toHaveBeenCalledWith("pr-1");
    expect(props.onDone).toHaveBeenCalledTimes(2);
  });

  it("renders a pending PR feedback status row with its action link", () => {
    render(<AssignmentBoard {...makeProps()} />);

    expect(screen.getByText("Review feedback needs follow-up")).toBeInTheDocument();
    expect(screen.getByText("Address failing smoke test before merge.")).toBeInTheDocument();
    expect(screen.getByText("acme/repo#42 · feature")).toBeInTheDocument();

    const feedbackLink = screen.getByRole("link", { name: /open review feedback needs follow-up/i });
    expect(feedbackLink).toHaveAttribute("href", "https://github.com/acme/repo/pull/42#discussion_r1");
  });

  it("renders a pending question approval with a direct answer affordance", async () => {
    const approvals: ApprovalState[] = [
      {
        id: "q-1",
        at: "2026-06-10T12:00:00Z",
        question: "Should we drop the legacy schema?",
        reason: "destructive_action",
        decided: false,
      },
    ];
    const props = makeProps({ approvals });
    render(<AssignmentBoard {...props} />);

    expect(screen.getByText("User Action Required")).toBeInTheDocument();
    expect(screen.getByText("Should we drop the legacy schema?")).toBeInTheDocument();

    const input = screen.getByPlaceholderText(/answer this question/i);
    await userEvent.type(input, "yes proceed");

    const send = screen.getByRole("button", { name: /send answer/i });
    await userEvent.click(send);

    expect(props.onAnswerQuestion).toHaveBeenCalledTimes(1);
    expect(props.onAnswerQuestion).toHaveBeenCalledWith("task-1", "q-1", "yes proceed");
  });

  it("does not show an answer form once an approval is decided", () => {
    const approvals: ApprovalState[] = [
      {
        id: "q-1",
        at: "2026-06-10T12:00:00Z",
        question: "Approve deploy?",
        reason: "approval",
        decided: true,
        answer: "yes",
      },
    ];
    render(<AssignmentBoard {...makeProps({ approvals })} />);

    expect(screen.getByText(/Answer: yes/)).toBeInTheDocument();
    expect(screen.queryByPlaceholderText(/answer this question/i)).not.toBeInTheDocument();
  });

  it("renders the empty state when there are no rows or approvals", () => {
    render(<AssignmentBoard {...makeProps({ rows: [] })} />);
    expect(
      screen.getByText(/No assignments, sessions, pull requests, questions, or artifacts/i),
    ).toBeInTheDocument();
  });

  it("dispatches cancel for a queued work item", async () => {
    const props = makeProps();
    render(<AssignmentBoard {...props} />);

    const planRow = screen.getByText("Plan").closest(".assignment-row") as HTMLElement;
    expect(planRow).toBeTruthy();
    const cancel = within(planRow).getByRole("button", { name: /cancel plan/i });
    await userEvent.click(cancel);

    expect(props.onCancelWorkItem).toHaveBeenCalledWith("task-1", "w-1");
    expect(props.onDone).toHaveBeenCalled();
  });

  it("shows overflow rows through an explicit expansion control", async () => {
    const rows = Array.from({ length: 20 }, (_, index): AssignmentRow => ({
      id: `session:${index}`,
      kind: "session",
      title: `Session ${index + 1}`,
      subtitle: "tmux",
      status: "running",
      tone: "info",
      action: { kind: "inspect-session", sessionId: `sess-${index}` },
      ...baseRow,
    }));
    render(<AssignmentBoard {...makeProps({ rows })} />);

    expect(screen.getByText("20 active signals")).toBeInTheDocument();
    expect(screen.getByText("Session 18")).toBeInTheDocument();
    expect(screen.queryByText("Session 19")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /show 2 more/i }));

    expect(screen.getByText("Session 19")).toBeInTheDocument();
    expect(screen.getByText("Session 20")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /show fewer/i })).toBeInTheDocument();
  });
});
