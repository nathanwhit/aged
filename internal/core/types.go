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
	WorkerSucceeded WorkerStatus = "succeeded"
	WorkerFailed    WorkerStatus = "failed"
	WorkerCanceled  WorkerStatus = "canceled"
)

type EventType string

const (
	EventTaskCreated     EventType = "task.created"
	EventTaskStatus      EventType = "task.status"
	EventTaskSteered     EventType = "task.steered"
	EventWorkerCreated   EventType = "worker.created"
	EventWorkerStarted   EventType = "worker.started"
	EventWorkerOutput    EventType = "worker.output"
	EventWorkerCompleted EventType = "worker.completed"
	EventApprovalNeeded  EventType = "approval.needed"
	EventApprovalDecided EventType = "approval.decided"
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

type CreateTaskRequest struct {
	Title  string `json:"title"`
	Prompt string `json:"prompt"`
	Kind   string `json:"kind,omitempty"`
}

type SteeringRequest struct {
	Message string `json:"message"`
}

type ApprovalDecision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type Snapshot struct {
	Tasks   []Task   `json:"tasks"`
	Workers []Worker `json:"workers"`
	Events  []Event  `json:"events"`
}

func MustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
