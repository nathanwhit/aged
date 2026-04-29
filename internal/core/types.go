package core

import (
	"encoding/json"
	"time"
)

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskPlanning  TaskStatus = "planning"
	TaskRunning   TaskStatus = "running"
	TaskWaiting   TaskStatus = "waiting"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskCanceled  TaskStatus = "canceled"
)

type WorkerStatus string

const (
	WorkerQueued    WorkerStatus = "queued"
	WorkerRunning   WorkerStatus = "running"
	WorkerWaiting   WorkerStatus = "waiting"
	WorkerSucceeded WorkerStatus = "succeeded"
	WorkerFailed    WorkerStatus = "failed"
	WorkerCanceled  WorkerStatus = "canceled"
)

type EventType string

const (
	EventTaskCreated      EventType = "task.created"
	EventTaskStatus       EventType = "task.status"
	EventTaskPlanned      EventType = "task.planned"
	EventTaskReplanned    EventType = "task.replanned"
	EventTaskSteered      EventType = "task.steered"
	EventTaskCleared      EventType = "task.cleared"
	EventExecutionPlanned EventType = "execution.node_planned"
	EventExecutionStatus  EventType = "execution.node_status"
	EventApplyPolicy      EventType = "apply.policy_recommended"
	EventWorkerWorkspace  EventType = "worker.workspace_prepared"
	EventWorkerCleanup    EventType = "worker.workspace_cleaned"
	EventWorkerCreated    EventType = "worker.created"
	EventWorkerStarted    EventType = "worker.started"
	EventWorkerOutput     EventType = "worker.output"
	EventWorkerCompleted  EventType = "worker.completed"
	EventWorkerApplied    EventType = "worker.changes_applied"
	EventApprovalNeeded   EventType = "approval.needed"
	EventApprovalDecided  EventType = "approval.decided"
)

type Event struct {
	ID       int64           `json:"id"`
	At       time.Time       `json:"at"`
	Type     EventType       `json:"type"`
	TaskID   string          `json:"taskId,omitempty"`
	WorkerID string          `json:"workerId,omitempty"`
	Payload  json.RawMessage `json:"payload"`
}

type Task struct {
	ID        string          `json:"id"`
	Title     string          `json:"title"`
	Prompt    string          `json:"prompt"`
	Status    TaskStatus      `json:"status"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type Worker struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"taskId"`
	Kind      string          `json:"kind"`
	Status    WorkerStatus    `json:"status"`
	Command   []string        `json:"command,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type ExecutionNode struct {
	ID            string          `json:"id"`
	TaskID        string          `json:"taskId"`
	WorkerID      string          `json:"workerId,omitempty"`
	WorkerKind    string          `json:"workerKind"`
	Status        WorkerStatus    `json:"status"`
	PlanID        string          `json:"planId,omitempty"`
	ParentNodeID  string          `json:"parentNodeId,omitempty"`
	SpawnID       string          `json:"spawnId,omitempty"`
	Role          string          `json:"role,omitempty"`
	Reason        string          `json:"reason,omitempty"`
	TargetID      string          `json:"targetId,omitempty"`
	TargetKind    string          `json:"targetKind,omitempty"`
	RemoteSession string          `json:"remoteSession,omitempty"`
	RemoteRunDir  string          `json:"remoteRunDir,omitempty"`
	RemoteWorkDir string          `json:"remoteWorkDir,omitempty"`
	DependsOn     []string        `json:"dependsOn,omitempty"`
	CreatedAt     time.Time       `json:"createdAt"`
	UpdatedAt     time.Time       `json:"updatedAt"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

type TargetCapacity struct {
	MaxWorkers int     `json:"maxWorkers"`
	CPUWeight  float64 `json:"cpuWeight,omitempty"`
	MemoryGB   float64 `json:"memoryGB,omitempty"`
}

type TargetState struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"`
	Labels    map[string]string `json:"labels,omitempty"`
	Capacity  TargetCapacity    `json:"capacity"`
	Running   int               `json:"running"`
	Available bool              `json:"available"`
}

type CreateTaskRequest struct {
	Title      string          `json:"title"`
	Prompt     string          `json:"prompt"`
	Source     string          `json:"source,omitempty"`
	ExternalID string          `json:"externalId,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

type SteeringRequest struct {
	Message string `json:"message"`
}

type ApprovalDecision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type Snapshot struct {
	Tasks          []Task          `json:"tasks"`
	Workers        []Worker        `json:"workers"`
	ExecutionNodes []ExecutionNode `json:"executionNodes"`
	Targets        []TargetState   `json:"targets,omitempty"`
	Events         []Event         `json:"events"`
}

func MustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
