package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"aged/internal/core"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(ctx context.Context, path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	at TEXT NOT NULL,
	type TEXT NOT NULL,
	task_id TEXT NOT NULL DEFAULT '',
	worker_id TEXT NOT NULL DEFAULT '',
	payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS events_task_idx ON events(task_id, id);
CREATE INDEX IF NOT EXISTS events_worker_idx ON events(worker_id, id);
`)
	return err
}

func (s *SQLiteStore) Append(ctx context.Context, event core.Event) (core.Event, error) {
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	if event.Payload == nil {
		event.Payload = json.RawMessage(`{}`)
	}

	res, err := s.db.ExecContext(ctx, `
INSERT INTO events (at, type, task_id, worker_id, payload)
VALUES (?, ?, ?, ?, ?)`,
		event.At.Format(time.RFC3339Nano),
		string(event.Type),
		event.TaskID,
		event.WorkerID,
		string(event.Payload),
	)
	if err != nil {
		return core.Event{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return core.Event{}, err
	}
	event.ID = id
	return event, nil
}

func (s *SQLiteStore) ListEvents(ctx context.Context, afterID int64, limit int) ([]core.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT id, at, type, task_id, worker_id, payload
FROM events
WHERE id > ?
ORDER BY id ASC
LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []core.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *SQLiteStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	events, err := s.allEvents(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}

	tasks := map[string]core.Task{}
	workers := map[string]core.Worker{}
	nodes := map[string]core.ExecutionNode{}
	clearedTasks := map[string]bool{}
	workerNodes := map[string]string{}
	workspaceMetadata := map[string]json.RawMessage{}

	for _, event := range events {
		switch event.Type {
		case core.EventTaskCreated:
			var payload struct {
				Title    string          `json:"title"`
				Prompt   string          `json:"prompt"`
				Metadata json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.created: %w", err)
			}
			tasks[event.TaskID] = core.Task{
				ID:        event.TaskID,
				Title:     payload.Title,
				Prompt:    payload.Prompt,
				Status:    core.TaskQueued,
				CreatedAt: event.At,
				UpdatedAt: event.At,
				Metadata:  payload.Metadata,
			}
		case core.EventTaskStatus:
			var payload struct {
				Status core.TaskStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.status: %w", err)
			}
			task := tasks[event.TaskID]
			task.Status = payload.Status
			task.UpdatedAt = event.At
			tasks[event.TaskID] = task
		case core.EventTaskCleared:
			clearedTasks[event.TaskID] = true
		case core.EventExecutionPlanned:
			var payload struct {
				NodeID        string          `json:"nodeId"`
				WorkerID      string          `json:"workerId,omitempty"`
				WorkerKind    string          `json:"workerKind"`
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
				Metadata      json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode execution.node_planned: %w", err)
			}
			node := core.ExecutionNode{
				ID:            payload.NodeID,
				TaskID:        event.TaskID,
				WorkerID:      payload.WorkerID,
				WorkerKind:    payload.WorkerKind,
				Status:        core.WorkerQueued,
				PlanID:        payload.PlanID,
				ParentNodeID:  payload.ParentNodeID,
				SpawnID:       payload.SpawnID,
				Role:          payload.Role,
				Reason:        payload.Reason,
				TargetID:      payload.TargetID,
				TargetKind:    payload.TargetKind,
				RemoteSession: payload.RemoteSession,
				RemoteRunDir:  payload.RemoteRunDir,
				RemoteWorkDir: payload.RemoteWorkDir,
				DependsOn:     payload.DependsOn,
				CreatedAt:     event.At,
				UpdatedAt:     event.At,
				Metadata:      payload.Metadata,
			}
			nodes[payload.NodeID] = node
			if payload.WorkerID != "" {
				workerNodes[payload.WorkerID] = payload.NodeID
			}
		case core.EventExecutionStatus:
			var payload struct {
				NodeID string            `json:"nodeId"`
				Status core.WorkerStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode execution.node_status: %w", err)
			}
			node := nodes[payload.NodeID]
			if node.ID != "" {
				node.Status = payload.Status
				node.UpdatedAt = event.At
				nodes[payload.NodeID] = node
			}
		case core.EventWorkerCreated:
			var payload struct {
				Kind     string          `json:"kind"`
				Command  []string        `json:"command,omitempty"`
				Metadata json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode worker.created: %w", err)
			}
			metadata := mergeMetadata(payload.Metadata, workspaceMetadata[event.WorkerID])
			workers[event.WorkerID] = core.Worker{
				ID:        event.WorkerID,
				TaskID:    event.TaskID,
				Kind:      payload.Kind,
				Status:    core.WorkerQueued,
				Command:   payload.Command,
				CreatedAt: event.At,
				UpdatedAt: event.At,
				Metadata:  metadata,
			}
			if nodeID := workerNodes[event.WorkerID]; nodeID != "" {
				node := nodes[nodeID]
				node.WorkerKind = payload.Kind
				node.UpdatedAt = event.At
				nodes[nodeID] = node
			}
		case core.EventWorkerWorkspace:
			workspaceMetadata[event.WorkerID] = event.Payload
			worker := workers[event.WorkerID]
			if worker.ID != "" {
				worker.Metadata = mergeMetadata(worker.Metadata, event.Payload)
				worker.UpdatedAt = event.At
				workers[event.WorkerID] = worker
			}
		case core.EventWorkerStarted:
			worker := workers[event.WorkerID]
			worker.Status = core.WorkerRunning
			worker.UpdatedAt = event.At
			workers[event.WorkerID] = worker
			if nodeID := workerNodes[event.WorkerID]; nodeID != "" {
				node := nodes[nodeID]
				node.Status = core.WorkerRunning
				node.UpdatedAt = event.At
				nodes[nodeID] = node
			}
		case core.EventWorkerCompleted:
			var payload struct {
				Status core.WorkerStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode worker.completed: %w", err)
			}
			worker := workers[event.WorkerID]
			worker.Status = payload.Status
			worker.UpdatedAt = event.At
			workers[event.WorkerID] = worker
			if nodeID := workerNodes[event.WorkerID]; nodeID != "" {
				node := nodes[nodeID]
				node.Status = payload.Status
				node.UpdatedAt = event.At
				nodes[nodeID] = node
			}
		}
	}

	return core.Snapshot{
		Tasks:          orderedTasks(filterClearedTasks(tasks, clearedTasks)),
		Workers:        orderedWorkers(filterClearedWorkers(workers, clearedTasks)),
		ExecutionNodes: orderedExecutionNodes(filterClearedExecutionNodes(nodes, clearedTasks)),
		Events:         events,
	}, nil
}

func filterClearedTasks(values map[string]core.Task, cleared map[string]bool) map[string]core.Task {
	out := map[string]core.Task{}
	for id, task := range values {
		if !cleared[id] {
			out[id] = task
		}
	}
	return out
}

func filterClearedWorkers(values map[string]core.Worker, cleared map[string]bool) map[string]core.Worker {
	out := map[string]core.Worker{}
	for id, worker := range values {
		if !cleared[worker.TaskID] {
			out[id] = worker
		}
	}
	return out
}

func filterClearedExecutionNodes(values map[string]core.ExecutionNode, cleared map[string]bool) map[string]core.ExecutionNode {
	out := map[string]core.ExecutionNode{}
	for id, node := range values {
		if !cleared[node.TaskID] {
			out[id] = node
		}
	}
	return out
}

func (s *SQLiteStore) allEvents(ctx context.Context) ([]core.Event, error) {
	var events []core.Event
	var afterID int64
	for {
		batch, err := s.ListEvents(ctx, afterID, 1000)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return events, nil
		}
		events = append(events, batch...)
		afterID = batch[len(batch)-1].ID
		if len(batch) < 1000 {
			return events, nil
		}
	}
}

func mergeMetadata(base json.RawMessage, workspace json.RawMessage) json.RawMessage {
	if len(workspace) == 0 {
		return base
	}
	out := map[string]any{}
	if len(base) > 0 {
		_ = json.Unmarshal(base, &out)
	}
	var workspacePayload any
	if err := json.Unmarshal(workspace, &workspacePayload); err == nil {
		out["workspace"] = workspacePayload
	}
	return core.MustJSON(out)
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanEvent(scanner eventScanner) (core.Event, error) {
	var event core.Event
	var at string
	var eventType string
	var payload string
	if err := scanner.Scan(&event.ID, &at, &eventType, &event.TaskID, &event.WorkerID, &payload); err != nil {
		return core.Event{}, err
	}
	parsedAt, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return core.Event{}, err
	}
	event.At = parsedAt
	event.Type = core.EventType(eventType)
	event.Payload = json.RawMessage(payload)
	return event, nil
}

func orderedTasks(values map[string]core.Task) []core.Task {
	out := make([]core.Task, 0, len(values))
	for _, task := range values {
		if task.ID != "" {
			out = append(out, task)
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].CreatedAt.Before(out[i].CreatedAt) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func orderedWorkers(values map[string]core.Worker) []core.Worker {
	out := make([]core.Worker, 0, len(values))
	for _, worker := range values {
		if worker.ID != "" {
			out = append(out, worker)
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].CreatedAt.Before(out[i].CreatedAt) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func orderedExecutionNodes(values map[string]core.ExecutionNode) []core.ExecutionNode {
	out := make([]core.ExecutionNode, 0, len(values))
	for _, node := range values {
		if node.ID != "" {
			out = append(out, node)
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].CreatedAt.Before(out[i].CreatedAt) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

var ErrNotFound = errors.New("not found")
