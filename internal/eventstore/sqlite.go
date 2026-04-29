package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

CREATE TABLE IF NOT EXISTS projects (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	local_path TEXT NOT NULL,
	repo TEXT NOT NULL DEFAULT '',
	vcs TEXT NOT NULL DEFAULT '',
	default_base TEXT NOT NULL DEFAULT '',
	workspace_root TEXT NOT NULL DEFAULT '',
	target_labels TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
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

func (s *SQLiteStore) ListProjects(ctx context.Context) ([]core.Project, string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, local_path, repo, vcs, default_base, workspace_root, target_labels
FROM projects
ORDER BY id ASC`)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var projects []core.Project
	for rows.Next() {
		project, err := scanProject(rows)
		if err != nil {
			return nil, "", err
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	defaultID, err := s.setting(ctx, "default_project_id")
	if err != nil {
		return nil, "", err
	}
	if defaultID == "" && len(projects) > 0 {
		defaultID = projects[0].ID
	}
	return projects, defaultID, nil
}

func (s *SQLiteStore) CreateProject(ctx context.Context, project core.Project) (core.Project, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	labels, err := json.Marshal(project.TargetLabels)
	if err != nil {
		return core.Project{}, err
	}
	if string(labels) == "null" {
		labels = []byte("{}")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.Project{}, err
	}
	defer tx.Rollback()

	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects`).Scan(&count); err != nil {
		return core.Project{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO projects (id, name, local_path, repo, vcs, default_base, workspace_root, target_labels, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		project.ID,
		project.Name,
		project.LocalPath,
		project.Repo,
		project.VCS,
		project.DefaultBase,
		project.WorkspaceRoot,
		string(labels),
		now,
		now,
	); err != nil {
		return core.Project{}, err
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO settings (key, value) VALUES ('default_project_id', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, project.ID); err != nil {
			return core.Project{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return core.Project{}, err
	}
	return project, nil
}

func (s *SQLiteStore) SaveProject(ctx context.Context, project core.Project, makeDefault bool) (core.Project, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	labels, err := json.Marshal(project.TargetLabels)
	if err != nil {
		return core.Project{}, err
	}
	if string(labels) == "null" {
		labels = []byte("{}")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.Project{}, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO projects (id, name, local_path, repo, vcs, default_base, workspace_root, target_labels, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	name = excluded.name,
	local_path = excluded.local_path,
	repo = excluded.repo,
	vcs = excluded.vcs,
	default_base = excluded.default_base,
	workspace_root = excluded.workspace_root,
	target_labels = excluded.target_labels,
	updated_at = excluded.updated_at`,
		project.ID,
		project.Name,
		project.LocalPath,
		project.Repo,
		project.VCS,
		project.DefaultBase,
		project.WorkspaceRoot,
		string(labels),
		now,
		now,
	); err != nil {
		return core.Project{}, err
	}
	if makeDefault {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO settings (key, value) VALUES ('default_project_id', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, project.ID); err != nil {
			return core.Project{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return core.Project{}, err
	}
	return project, nil
}

func (s *SQLiteStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	events, err := s.allEvents(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}

	tasks := map[string]core.Task{}
	workers := map[string]core.Worker{}
	nodes := map[string]core.ExecutionNode{}
	pullRequests := map[string]core.PullRequest{}
	clearedTasks := map[string]bool{}
	workerNodes := map[string]string{}
	workspaceMetadata := map[string]json.RawMessage{}

	for _, event := range events {
		switch event.Type {
		case core.EventTaskCreated:
			var payload struct {
				ProjectID string          `json:"projectId,omitempty"`
				Title     string          `json:"title"`
				Prompt    string          `json:"prompt"`
				Metadata  json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.created: %w", err)
			}
			projectID := payload.ProjectID
			if projectID == "" {
				projectID = projectIDFromMetadata(payload.Metadata)
			}
			tasks[event.TaskID] = core.Task{
				ID:        event.TaskID,
				ProjectID: projectID,
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
		case core.EventTaskCandidate:
			var payload struct {
				WorkerID string `json:"workerId"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.final_candidate_selected: %w", err)
			}
			task := tasks[event.TaskID]
			task.FinalCandidateWorkerID = payload.WorkerID
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
		case core.EventWorkerApplied:
			task := tasks[event.TaskID]
			if task.ID != "" {
				task.AppliedWorkerID = event.WorkerID
				task.UpdatedAt = event.At
				tasks[event.TaskID] = task
			}
		case core.EventPRPublished:
			var payload struct {
				ID           string          `json:"id"`
				Repo         string          `json:"repo"`
				Number       int             `json:"number,omitempty"`
				URL          string          `json:"url"`
				Branch       string          `json:"branch"`
				Base         string          `json:"base"`
				Title        string          `json:"title"`
				State        string          `json:"state,omitempty"`
				Draft        bool            `json:"draft,omitempty"`
				ChecksStatus string          `json:"checksStatus,omitempty"`
				MergeStatus  string          `json:"mergeStatus,omitempty"`
				ReviewStatus string          `json:"reviewStatus,omitempty"`
				Metadata     json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode pull_request.published: %w", err)
			}
			id := payload.ID
			if id == "" {
				id = fmt.Sprintf("%s#%d", payload.Repo, payload.Number)
			}
			pullRequests[id] = core.PullRequest{
				ID:           id,
				TaskID:       event.TaskID,
				Repo:         payload.Repo,
				Number:       payload.Number,
				URL:          payload.URL,
				Branch:       payload.Branch,
				Base:         payload.Base,
				Title:        payload.Title,
				State:        payload.State,
				Draft:        payload.Draft,
				ChecksStatus: payload.ChecksStatus,
				MergeStatus:  payload.MergeStatus,
				ReviewStatus: payload.ReviewStatus,
				CreatedAt:    event.At,
				UpdatedAt:    event.At,
				Metadata:     payload.Metadata,
			}
		case core.EventPRStatusChecked:
			var payload struct {
				ID           string          `json:"id"`
				State        string          `json:"state,omitempty"`
				Draft        bool            `json:"draft,omitempty"`
				ChecksStatus string          `json:"checksStatus,omitempty"`
				MergeStatus  string          `json:"mergeStatus,omitempty"`
				ReviewStatus string          `json:"reviewStatus,omitempty"`
				Metadata     json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode pull_request.status_checked: %w", err)
			}
			pr := pullRequests[payload.ID]
			if pr.ID != "" {
				pr.State = payload.State
				pr.Draft = payload.Draft
				pr.ChecksStatus = payload.ChecksStatus
				pr.MergeStatus = payload.MergeStatus
				pr.ReviewStatus = payload.ReviewStatus
				pr.UpdatedAt = event.At
				if len(payload.Metadata) > 0 {
					pr.Metadata = payload.Metadata
				}
				pullRequests[payload.ID] = pr
			}
		case core.EventPRBabysitter:
			var payload struct {
				ID               string `json:"id"`
				BabysitterTaskID string `json:"babysitterTaskId"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode pull_request.babysitter_started: %w", err)
			}
			pr := pullRequests[payload.ID]
			if pr.ID != "" {
				pr.BabysitterTaskID = payload.BabysitterTaskID
				pr.UpdatedAt = event.At
				pullRequests[payload.ID] = pr
			}
		}
	}

	filteredTasks := filterClearedTasks(tasks, clearedTasks)
	filteredNodes := filterClearedExecutionNodes(nodes, clearedTasks)
	return core.Snapshot{
		Tasks:               orderedTasks(filteredTasks),
		Workers:             orderedWorkers(filterClearedWorkers(workers, clearedTasks)),
		ExecutionNodes:      orderedExecutionNodes(filteredNodes),
		PullRequests:        orderedPullRequests(filterClearedPullRequests(pullRequests, clearedTasks)),
		OrchestrationGraphs: orchestrationGraphs(filteredTasks, filteredNodes),
		Events:              events,
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

func filterClearedPullRequests(values map[string]core.PullRequest, cleared map[string]bool) map[string]core.PullRequest {
	out := map[string]core.PullRequest{}
	for id, pr := range values {
		if !cleared[pr.TaskID] {
			out[id] = pr
		}
	}
	return out
}

func orchestrationGraphs(tasks map[string]core.Task, nodes map[string]core.ExecutionNode) []core.OrchestrationGraph {
	byTask := map[string][]core.ExecutionNode{}
	for _, node := range nodes {
		byTask[node.TaskID] = append(byTask[node.TaskID], node)
	}
	graphs := make([]core.OrchestrationGraph, 0, len(byTask))
	for taskID, taskNodes := range byTask {
		sort.Slice(taskNodes, func(i, j int) bool {
			return taskNodes[i].CreatedAt.Before(taskNodes[j].CreatedAt)
		})
		spawnToNode := map[string]string{}
		for _, node := range taskNodes {
			if node.SpawnID != "" {
				spawnToNode[node.SpawnID] = node.ID
			}
		}
		graphNodes := make([]core.OrchestrationGraphNode, 0, len(taskNodes))
		edges := []core.OrchestrationGraphEdge{}
		summary := core.OrchestrationGraphSummary{Total: len(taskNodes)}
		var updatedAt time.Time
		for _, node := range taskNodes {
			graphNodes = append(graphNodes, core.OrchestrationGraphNode{
				ID:         node.ID,
				WorkerID:   node.WorkerID,
				WorkerKind: node.WorkerKind,
				Status:     node.Status,
				Role:       node.Role,
				Reason:     node.Reason,
				SpawnID:    node.SpawnID,
				TargetID:   node.TargetID,
				TargetKind: node.TargetKind,
			})
			if node.ParentNodeID != "" {
				edges = append(edges, core.OrchestrationGraphEdge{From: node.ParentNodeID, To: node.ID, Reason: "parent"})
			}
			for _, dep := range node.DependsOn {
				if from := spawnToNode[dep]; from != "" {
					edges = append(edges, core.OrchestrationGraphEdge{From: from, To: node.ID, Reason: "depends_on:" + dep})
				}
			}
			switch node.Status {
			case core.WorkerRunning:
				summary.Running++
			case core.WorkerWaiting, core.WorkerQueued:
				summary.Waiting++
			case core.WorkerSucceeded:
				summary.Done++
			case core.WorkerFailed:
				summary.Failed++
			case core.WorkerCanceled:
				summary.Canceled++
			}
			if node.UpdatedAt.After(updatedAt) {
				updatedAt = node.UpdatedAt
			}
		}
		task := tasks[taskID]
		graphs = append(graphs, core.OrchestrationGraph{
			TaskID:    taskID,
			Status:    task.Status,
			Nodes:     graphNodes,
			Edges:     edges,
			Summary:   summary,
			UpdatedAt: updatedAt,
		})
	}
	sort.Slice(graphs, func(i, j int) bool {
		return graphs[i].UpdatedAt.Before(graphs[j].UpdatedAt)
	})
	return graphs
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

func (s *SQLiteStore) setting(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
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

func projectIDFromMetadata(metadata json.RawMessage) string {
	if len(metadata) == 0 {
		return ""
	}
	var values map[string]any
	if err := json.Unmarshal(metadata, &values); err != nil {
		return ""
	}
	if value, ok := values["projectId"].(string); ok {
		return value
	}
	return ""
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanProject(scanner eventScanner) (core.Project, error) {
	var project core.Project
	var labels string
	if err := scanner.Scan(
		&project.ID,
		&project.Name,
		&project.LocalPath,
		&project.Repo,
		&project.VCS,
		&project.DefaultBase,
		&project.WorkspaceRoot,
		&labels,
	); err != nil {
		return core.Project{}, err
	}
	if labels != "" {
		if err := json.Unmarshal([]byte(labels), &project.TargetLabels); err != nil {
			return core.Project{}, err
		}
	}
	return project, nil
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

func orderedPullRequests(values map[string]core.PullRequest) []core.PullRequest {
	out := make([]core.PullRequest, 0, len(values))
	for _, pr := range values {
		if pr.ID != "" {
			out = append(out, pr)
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
