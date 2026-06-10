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
      action: { kind: "inspect-session", sessionId: "sess-1" },
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
      action: { kind: "open-pr", url: "https://github.com/acme/repo/pull/42" },
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
    expect(screen.getByText("4 active signals")).toBeInTheDocument();
    // The 2 warning rows should bubble up as "needs attention".
    expect(screen.getByText(/2 need attention/i)).toBeInTheDocument();

    expect(screen.getByText("Approve destructive cleanup?")).toBeInTheDocument();
    expect(screen.getByText("Implementer")).toBeInTheDocument();
    expect(screen.getByText("Refactor manager view")).toBeInTheDocument();
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

  it("renders a pull request row with the linked PR affordance", () => {
    render(<AssignmentBoard {...makeProps()} />);

    const link = screen.getByRole("link", { name: /open refactor manager view/i });
    expect(link).toHaveAttribute("href", "https://github.com/acme/repo/pull/42");
    expect(link).toHaveAttribute("target", "_blank");

    // PR row context (repo#number) should be visible.
    expect(screen.getByText("acme/repo#42")).toBeInTheDocument();
    expect(screen.getByText(/base main · head feature/)).toBeInTheDocument();
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
});
