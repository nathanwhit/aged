package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"aged/internal/core"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func isTerminalWorkerStatus(status core.WorkerStatus) bool {
	return status == core.WorkerSucceeded || status == core.WorkerFailed || status == core.WorkerCanceled
}

func jsonString(value any, nullDefault string) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	if string(data) == "null" && nullDefault != "" {
		return nullDefault, nil
	}
	return string(data), nil
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
	upstream_repo TEXT NOT NULL DEFAULT '',
	head_repo_owner TEXT NOT NULL DEFAULT '',
	push_remote TEXT NOT NULL DEFAULT '',
	vcs TEXT NOT NULL DEFAULT '',
	default_base TEXT NOT NULL DEFAULT '',
	workspace_root TEXT NOT NULL DEFAULT '',
	target_labels TEXT NOT NULL DEFAULT '{}',
	github_issues TEXT NOT NULL DEFAULT '{}',
	github_mentions TEXT NOT NULL DEFAULT '{}',
	pull_request_policy TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plugins (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	kind TEXT NOT NULL,
	protocol TEXT NOT NULL DEFAULT '',
	enabled INTEGER NOT NULL DEFAULT 0,
	status TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	command TEXT NOT NULL DEFAULT '[]',
	endpoint TEXT NOT NULL DEFAULT '',
	capabilities TEXT NOT NULL DEFAULT '[]',
	config TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS targets (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	host TEXT NOT NULL DEFAULT '',
	user TEXT NOT NULL DEFAULT '',
	port INTEGER NOT NULL DEFAULT 0,
	identity_file TEXT NOT NULL DEFAULT '',
	insecure_ignore_host_key INTEGER NOT NULL DEFAULT 0,
	work_dir TEXT NOT NULL DEFAULT '',
	work_root TEXT NOT NULL DEFAULT '',
	labels TEXT NOT NULL DEFAULT '{}',
	capacity TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
`)
	if err != nil {
		return err
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{"upstream_repo", "TEXT NOT NULL DEFAULT ''"},
		{"head_repo_owner", "TEXT NOT NULL DEFAULT ''"},
		{"push_remote", "TEXT NOT NULL DEFAULT ''"},
		{"remote_checkouts", "TEXT NOT NULL DEFAULT '{}'"},
		{"github_issues", "TEXT NOT NULL DEFAULT '{}'"},
		{"github_mentions", "TEXT NOT NULL DEFAULT '{}'"},
		{"pull_request_policy", "TEXT NOT NULL DEFAULT '{}'"},
	} {
		if err := s.ensureColumn(ctx, "projects", column.name, column.definition); err != nil {
			return err
		}
	}
	if err := s.ensureColumn(ctx, "targets", "checkout_root", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) ensureColumn(ctx context.Context, table string, name string, definition string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var columnName, columnType string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if columnName == name {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, name, definition))
	return err
}

func (s *SQLiteStore) ListPlugins(ctx context.Context) ([]core.Plugin, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, kind, protocol, enabled, status, error, command, endpoint, capabilities, config
FROM plugins
ORDER BY kind ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var plugins []core.Plugin
	for rows.Next() {
		plugin, err := scanPlugin(rows)
		if err != nil {
			return nil, err
		}
		plugins = append(plugins, plugin)
	}
	return plugins, rows.Err()
}

func (s *SQLiteStore) SavePlugin(ctx context.Context, plugin core.Plugin) (core.Plugin, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	command, err := jsonString(plugin.Command, "[]")
	if err != nil {
		return core.Plugin{}, err
	}
	capabilities, err := jsonString(plugin.Capabilities, "[]")
	if err != nil {
		return core.Plugin{}, err
	}
	config, err := jsonString(plugin.Config, "{}")
	if err != nil {
		return core.Plugin{}, err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO plugins (id, name, kind, protocol, enabled, status, error, command, endpoint, capabilities, config, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	name = excluded.name,
	kind = excluded.kind,
	protocol = excluded.protocol,
	enabled = excluded.enabled,
	status = excluded.status,
	error = excluded.error,
	command = excluded.command,
	endpoint = excluded.endpoint,
	capabilities = excluded.capabilities,
	config = excluded.config,
	updated_at = excluded.updated_at`,
		plugin.ID,
		plugin.Name,
		plugin.Kind,
		plugin.Protocol,
		boolInt(plugin.Enabled),
		plugin.Status,
		plugin.Error,
		command,
		plugin.Endpoint,
		capabilities,
		config,
		now,
		now,
	)
	if err != nil {
		return core.Plugin{}, err
	}
	return plugin, nil
}

func (s *SQLiteStore) DeletePlugin(ctx context.Context, id string) error {
	return s.deleteByID(ctx, id, "plugin id is required", `DELETE FROM plugins WHERE id = ?`)
}

func (s *SQLiteStore) ListTargets(ctx context.Context) ([]core.TargetConfig, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, kind, host, user, port, identity_file, insecure_ignore_host_key, checkout_root, work_dir, work_root, labels, capacity
FROM targets
ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []core.TargetConfig
	for rows.Next() {
		target, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (s *SQLiteStore) SaveTarget(ctx context.Context, target core.TargetConfig) (core.TargetConfig, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	labels, err := jsonString(target.Labels, "{}")
	if err != nil {
		return core.TargetConfig{}, err
	}
	capacity, err := jsonString(target.Capacity, "{}")
	if err != nil {
		return core.TargetConfig{}, err
	}
	checkoutRoot := strings.TrimSpace(target.CheckoutRoot)
	if checkoutRoot == "" {
		checkoutRoot = strings.TrimSpace(target.WorkDir)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO targets (id, kind, host, user, port, identity_file, insecure_ignore_host_key, checkout_root, work_dir, work_root, labels, capacity, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	kind = excluded.kind,
	host = excluded.host,
	user = excluded.user,
	port = excluded.port,
	identity_file = excluded.identity_file,
	insecure_ignore_host_key = excluded.insecure_ignore_host_key,
	checkout_root = excluded.checkout_root,
	work_dir = excluded.work_dir,
	work_root = excluded.work_root,
	labels = excluded.labels,
	capacity = excluded.capacity,
	updated_at = excluded.updated_at`,
		target.ID,
		target.Kind,
		target.Host,
		target.User,
		target.Port,
		target.IdentityFile,
		boolInt(target.InsecureIgnoreHostKey),
		checkoutRoot,
		target.WorkDir,
		target.WorkRoot,
		labels,
		capacity,
		now,
		now,
	)
	if err != nil {
		return core.TargetConfig{}, err
	}
	return target, nil
}

func (s *SQLiteStore) DeleteTarget(ctx context.Context, id string) error {
	return s.deleteByID(ctx, id, "target id is required", `DELETE FROM targets WHERE id = ?`)
}

func (s *SQLiteStore) deleteByID(ctx context.Context, id, requiredMsg, deleteSQL string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New(requiredMsg)
	}
	res, err := s.db.ExecContext(ctx, deleteSQL, id)
	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
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

func (s *SQLiteStore) ListTaskEvents(ctx context.Context, taskID string, limit int) ([]core.Event, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, errors.New("task id is required")
	}
	if limit <= 0 {
		rows, err := s.db.QueryContext(ctx, `
SELECT id, at, type, task_id, worker_id, payload
FROM events
WHERE task_id = ?
ORDER BY id ASC`, taskID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanEvents(rows)
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
WITH recent_output AS (
	SELECT id
	FROM events
	WHERE task_id = ? AND type = 'worker.output'
	ORDER BY id DESC
	LIMIT ?
)
SELECT id, at, type, task_id, worker_id, payload
FROM events
WHERE task_id = ?
	AND (type != 'worker.output' OR id IN (SELECT id FROM recent_output))
ORDER BY id ASC`, taskID, limit, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func scanEvents(rows *sql.Rows) ([]core.Event, error) {
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
SELECT id, name, local_path, repo, upstream_repo, head_repo_owner, push_remote, vcs, default_base, workspace_root, target_labels, remote_checkouts, github_issues, github_mentions, pull_request_policy
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

const projectInsertSQL = `
INSERT INTO projects (id, name, local_path, repo, upstream_repo, head_repo_owner, push_remote, vcs, default_base, workspace_root, target_labels, remote_checkouts, github_issues, github_mentions, pull_request_policy, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func projectInsertArgs(project core.Project, now string) ([]any, error) {
	labels, err := jsonString(project.TargetLabels, "{}")
	if err != nil {
		return nil, err
	}
	remoteCheckouts, err := jsonString(project.RemoteCheckouts, "{}")
	if err != nil {
		return nil, err
	}
	githubIssues, err := jsonString(project.GitHubIssues, "{}")
	if err != nil {
		return nil, err
	}
	githubMentions, err := jsonString(project.GitHubMentions, "{}")
	if err != nil {
		return nil, err
	}
	policy, err := jsonString(project.PullRequestPolicy, "")
	if err != nil {
		return nil, err
	}
	return []any{
		project.ID,
		project.Name,
		project.LocalPath,
		project.Repo,
		project.UpstreamRepo,
		project.HeadRepoOwner,
		project.PushRemote,
		project.VCS,
		project.DefaultBase,
		project.WorkspaceRoot,
		labels,
		remoteCheckouts,
		githubIssues,
		githubMentions,
		policy,
		now,
		now,
	}, nil
}

func (s *SQLiteStore) CreateProject(ctx context.Context, project core.Project) (core.Project, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	args, err := projectInsertArgs(project, now)
	if err != nil {
		return core.Project{}, err
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
	if _, err := tx.ExecContext(ctx, projectInsertSQL, args...); err != nil {
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
	args, err := projectInsertArgs(project, now)
	if err != nil {
		return core.Project{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.Project{}, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, projectInsertSQL+`
ON CONFLICT(id) DO UPDATE SET
	name = excluded.name,
	local_path = excluded.local_path,
	repo = excluded.repo,
	upstream_repo = excluded.upstream_repo,
	head_repo_owner = excluded.head_repo_owner,
	push_remote = excluded.push_remote,
	vcs = excluded.vcs,
	default_base = excluded.default_base,
	workspace_root = excluded.workspace_root,
	target_labels = excluded.target_labels,
	remote_checkouts = excluded.remote_checkouts,
	github_issues = excluded.github_issues,
	github_mentions = excluded.github_mentions,
	pull_request_policy = excluded.pull_request_policy,
	updated_at = excluded.updated_at`,
		args...,
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

func (s *SQLiteStore) DeleteProject(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("project id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects WHERE id = ?`, id).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	var total int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects`).Scan(&total); err != nil {
		return err
	}
	if total <= 1 {
		return errors.New("cannot delete the last project")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM projects WHERE id = ?`, id); err != nil {
		return err
	}
	var defaultID string
	_ = tx.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = 'default_project_id'`).Scan(&defaultID)
	if defaultID == id {
		var nextID string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM projects ORDER BY id LIMIT 1`).Scan(&nextID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO settings (key, value) VALUES ('default_project_id', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, nextID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	events, err := s.allEvents(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	return s.snapshotFromEvents(ctx, events, true)
}

func (s *SQLiteStore) SnapshotSummary(ctx context.Context) (core.Snapshot, error) {
	events, err := s.projectionEvents(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	return s.snapshotFromEvents(ctx, events, false)
}

func (s *SQLiteStore) snapshotFromEvents(ctx context.Context, events []core.Event, includeEvents bool) (core.Snapshot, error) {
	lastEventID, err := s.latestEventID(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if lastEventID == 0 {
		lastEventID = maxEventID(events)
	}

	tasks := map[string]core.Task{}
	workers := map[string]core.Worker{}
	nodes := map[string]core.ExecutionNode{}
	pullRequests := map[string]core.PullRequest{}
	pullRequestAliases := map[string]string{}
	pullRequestIdentities := map[string]string{}
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
				ID:              event.TaskID,
				ProjectID:       projectID,
				Title:           payload.Title,
				Prompt:          payload.Prompt,
				Status:          core.TaskQueued,
				ObjectiveStatus: core.ObjectiveActive,
				ObjectivePhase:  "queued",
				CreatedAt:       event.At,
				UpdatedAt:       event.At,
				Metadata:        payload.Metadata,
			}
		case core.EventTaskUpdated:
			var payload struct {
				Title         string          `json:"title,omitempty"`
				Prompt        string          `json:"prompt,omitempty"`
				MetadataPatch json.RawMessage `json:"metadataPatch,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.updated: %w", err)
			}
			task, ok := tasks[event.TaskID]
			if !ok {
				continue
			}
			if payload.Title != "" {
				task.Title = payload.Title
			}
			if payload.Prompt != "" {
				task.Prompt = payload.Prompt
			}
			task.Metadata = mergeMetadataPatch(task.Metadata, payload.MetadataPatch)
			task.UpdatedAt = event.At
			tasks[event.TaskID] = task
		case core.EventTaskStatus:
			var payload struct {
				Status core.TaskStatus `json:"status"`
				Error  string          `json:"error,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.status: %w", err)
			}
			task := tasks[event.TaskID]
			task.Status = payload.Status
			task.Error = payload.Error
			switch payload.Status {
			case core.TaskSucceeded, core.TaskFailed, core.TaskCanceled:
				nextObjective := objectiveStatusForTaskStatus(payload.Status)
				if task.ObjectiveStatus == "" || task.ObjectiveStatus == core.ObjectiveActive || task.ObjectiveStatus != nextObjective {
					task.ObjectiveStatus = nextObjective
					task.ObjectivePhase = objectivePhaseForTaskStatus(payload.Status)
				}
			case core.TaskWaiting:
				if task.ObjectiveStatus == "" || task.ObjectiveStatus == core.ObjectiveActive {
					task.ObjectiveStatus = core.ObjectiveWaitingUser
					task.ObjectivePhase = objectivePhaseForTaskStatus(payload.Status)
				}
			default:
				if task.ObjectiveStatus == "" {
					task.ObjectiveStatus = objectiveStatusForTaskStatus(payload.Status)
					task.ObjectivePhase = objectivePhaseForTaskStatus(payload.Status)
				}
			}
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
		case core.EventTaskObjective:
			var payload struct {
				Status core.ObjectiveStatus `json:"status"`
				Phase  string               `json:"phase,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.objective_updated: %w", err)
			}
			task := tasks[event.TaskID]
			if payload.Status != "" {
				task.ObjectiveStatus = payload.Status
			}
			if payload.Phase != "" {
				task.ObjectivePhase = payload.Phase
			}
			task.UpdatedAt = event.At
			tasks[event.TaskID] = task
		case core.EventTaskMilestone:
			var payload struct {
				Name     string          `json:"name"`
				Phase    string          `json:"phase,omitempty"`
				Summary  string          `json:"summary,omitempty"`
				Metadata json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.milestone_reached: %w", err)
			}
			task := tasks[event.TaskID]
			task.Milestones = append(task.Milestones, core.TaskMilestone{
				Name:     payload.Name,
				Phase:    payload.Phase,
				Summary:  payload.Summary,
				At:       event.At,
				Metadata: payload.Metadata,
			})
			task.UpdatedAt = event.At
			tasks[event.TaskID] = task
		case core.EventTaskArtifact:
			var payload struct {
				ID       string          `json:"id"`
				Kind     string          `json:"kind"`
				Name     string          `json:"name,omitempty"`
				URL      string          `json:"url,omitempty"`
				Ref      string          `json:"ref,omitempty"`
				Metadata json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode task.artifact_recorded: %w", err)
			}
			task := tasks[event.TaskID]
			task.Artifacts = upsertTaskArtifact(task.Artifacts, core.TaskArtifact{
				ID:        payload.ID,
				Kind:      payload.Kind,
				Name:      payload.Name,
				URL:       payload.URL,
				Ref:       payload.Ref,
				CreatedAt: event.At,
				UpdatedAt: event.At,
				Metadata:  payload.Metadata,
			})
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
				Kind        string          `json:"kind"`
				Command     []string        `json:"command,omitempty"`
				Prompt      string          `json:"prompt,omitempty"`
				PromptPath  string          `json:"promptPath,omitempty"`
				PromptError string          `json:"promptError,omitempty"`
				Metadata    json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode worker.created: %w", err)
			}
			metadata := mergeMetadata(payload.Metadata, workspaceMetadata[event.WorkerID])
			workers[event.WorkerID] = core.Worker{
				ID:          event.WorkerID,
				TaskID:      event.TaskID,
				Kind:        payload.Kind,
				Status:      core.WorkerQueued,
				Command:     payload.Command,
				Prompt:      payload.Prompt,
				PromptPath:  payload.PromptPath,
				PromptError: payload.PromptError,
				CreatedAt:   event.At,
				UpdatedAt:   event.At,
				Metadata:    metadata,
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
		case core.EventWorkerOutput:
			worker := workers[event.WorkerID]
			if worker.ID != "" && !isTerminalWorkerStatus(worker.Status) {
				worker.UpdatedAt = event.At
				workers[event.WorkerID] = worker
			}
			if nodeID := workerNodes[event.WorkerID]; nodeID != "" {
				node := nodes[nodeID]
				if node.ID != "" && !isTerminalWorkerStatus(node.Status) {
					node.UpdatedAt = event.At
					nodes[nodeID] = node
				}
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
		case core.EventPRPublished, core.EventPRUpdated:
			var payload struct {
				ID               string          `json:"id"`
				Repo             string          `json:"repo"`
				Number           int             `json:"number,omitempty"`
				URL              string          `json:"url"`
				Branch           string          `json:"branch"`
				Base             string          `json:"base"`
				Title            string          `json:"title"`
				State            string          `json:"state,omitempty"`
				Draft            bool            `json:"draft,omitempty"`
				ChecksStatus     string          `json:"checksStatus,omitempty"`
				ChecksConclusion string          `json:"checksConclusion,omitempty"`
				MergeStatus      string          `json:"mergeStatus,omitempty"`
				Mergeable        string          `json:"mergeable,omitempty"`
				ReviewStatus     string          `json:"reviewStatus,omitempty"`
				Metadata         json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode %s: %w", event.Type, err)
			}
			id := payload.ID
			if id == "" {
				id = fmt.Sprintf("%s#%d", payload.Repo, payload.Number)
			}
			next := core.PullRequest{
				ID:               id,
				TaskID:           event.TaskID,
				Repo:             payload.Repo,
				Number:           payload.Number,
				URL:              payload.URL,
				Branch:           payload.Branch,
				Base:             payload.Base,
				Title:            payload.Title,
				State:            payload.State,
				Draft:            payload.Draft,
				ChecksStatus:     payload.ChecksStatus,
				ChecksConclusion: payload.ChecksConclusion,
				MergeStatus:      payload.MergeStatus,
				Mergeable:        payload.Mergeable,
				ReviewStatus:     payload.ReviewStatus,
				CreatedAt:        event.At,
				UpdatedAt:        event.At,
				Metadata:         payload.Metadata,
			}
			id = resolvePullRequestSnapshotID(id, next, pullRequests, pullRequestAliases, pullRequestIdentities)
			next.ID = id
			if previous := pullRequests[id]; previous.ID != "" {
				next = mergePublishedPullRequest(previous, next)
			}
			pullRequests[id] = next
		case core.EventPRStatusChecked:
			var payload struct {
				ID               string          `json:"id"`
				State            string          `json:"state,omitempty"`
				Draft            bool            `json:"draft,omitempty"`
				ChecksStatus     string          `json:"checksStatus,omitempty"`
				ChecksConclusion string          `json:"checksConclusion,omitempty"`
				MergeStatus      string          `json:"mergeStatus,omitempty"`
				Mergeable        string          `json:"mergeable,omitempty"`
				ReviewStatus     string          `json:"reviewStatus,omitempty"`
				Metadata         json.RawMessage `json:"metadata,omitempty"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return core.Snapshot{}, fmt.Errorf("decode pull_request.status_checked: %w", err)
			}
			id := payload.ID
			if alias := pullRequestAliases[id]; alias != "" {
				id = alias
			}
			pr := pullRequests[id]
			if pr.ID != "" {
				if payload.State != "" {
					pr.State = payload.State
				}
				pr.Draft = payload.Draft
				if payload.ChecksStatus != "" {
					pr.ChecksStatus = payload.ChecksStatus
				}
				if payload.ChecksConclusion != "" {
					pr.ChecksConclusion = payload.ChecksConclusion
				}
				if payload.MergeStatus != "" {
					pr.MergeStatus = payload.MergeStatus
				}
				if payload.Mergeable != "" {
					pr.Mergeable = payload.Mergeable
				}
				if payload.ReviewStatus != "" {
					pr.ReviewStatus = payload.ReviewStatus
				}
				pr.UpdatedAt = event.At
				if len(payload.Metadata) > 0 {
					pr.Metadata = mergePullRequestMetadata(pr.Metadata, payload.Metadata)
				}
				pullRequests[id] = pr
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
		LastEventID:         lastEventID,
		Events:              snapshotResponseEvents(events, includeEvents),
	}, nil
}

func filterClearedTasks(values map[string]core.Task, cleared map[string]bool) map[string]core.Task {
	out := map[string]core.Task{}
	for id, task := range values {
		if !cleared[id] {
			if task.ObjectiveStatus == "" {
				task.ObjectiveStatus = objectiveStatusForTaskStatus(task.Status)
			}
			if task.ObjectivePhase == "" {
				task.ObjectivePhase = objectivePhaseForTaskStatus(task.Status)
			}
			out[id] = task
		}
	}
	return out
}

func objectiveStatusForTaskStatus(status core.TaskStatus) core.ObjectiveStatus {
	switch status {
	case core.TaskSucceeded:
		return core.ObjectiveSatisfied
	case core.TaskFailed, core.TaskCanceled:
		return core.ObjectiveAbandoned
	case core.TaskWaiting:
		return core.ObjectiveWaitingUser
	default:
		return core.ObjectiveActive
	}
}

func objectivePhaseForTaskStatus(status core.TaskStatus) string {
	switch status {
	case core.TaskQueued:
		return "queued"
	case core.TaskPlanning:
		return "planning"
	case core.TaskRunning:
		return "running"
	case core.TaskWaiting:
		return "waiting"
	case core.TaskSucceeded:
		return "satisfied"
	case core.TaskFailed:
		return "failed"
	case core.TaskCanceled:
		return "canceled"
	default:
		return ""
	}
}

func upsertTaskArtifact(items []core.TaskArtifact, next core.TaskArtifact) []core.TaskArtifact {
	if next.ID == "" {
		return append(items, next)
	}
	for i, item := range items {
		if item.ID != next.ID {
			continue
		}
		if next.CreatedAt.IsZero() {
			next.CreatedAt = item.CreatedAt
		}
		items[i] = next
		return items
	}
	return append(items, next)
}

func resolvePullRequestSnapshotID(id string, next core.PullRequest, pullRequests map[string]core.PullRequest, aliases map[string]string, identities map[string]string) string {
	if alias := aliases[id]; alias != "" {
		return alias
	}
	identity := pullRequestSnapshotIdentity(next)
	if identity == "" {
		return id
	}
	if existingID := identities[identity]; existingID != "" {
		if existingID != id {
			aliases[id] = existingID
		}
		return existingID
	}
	for existingID, existing := range pullRequests {
		if pullRequestSnapshotIdentity(existing) == identity {
			if existingID != id {
				aliases[id] = existingID
			}
			identities[identity] = existingID
			return existingID
		}
	}
	identities[identity] = id
	return id
}

func pullRequestSnapshotIdentity(pr core.PullRequest) string {
	repo := strings.ToLower(strings.TrimSpace(pr.Repo))
	number := pr.Number
	if (repo == "" || number == 0) && strings.TrimSpace(pr.URL) != "" {
		urlRepo, urlNumber := pullRequestURLIdentity(pr.URL)
		if repo == "" {
			repo = urlRepo
		}
		if number == 0 {
			number = urlNumber
		}
	}
	if pr.TaskID == "" || repo == "" || number == 0 {
		return ""
	}
	return pr.TaskID + "\x00" + repo + "#" + fmt.Sprint(number)
}

func pullRequestURLIdentity(value string) (string, int) {
	value = strings.TrimSpace(value)
	const marker = "github.com/"
	index := strings.Index(value, marker)
	if index < 0 {
		return "", 0
	}
	path := strings.Trim(value[index+len(marker):], "/")
	parts := strings.Split(path, "/")
	if len(parts) < 4 || parts[2] != "pull" {
		return "", 0
	}
	var number int
	if _, err := fmt.Sscanf(parts[3], "%d", &number); err != nil {
		return "", 0
	}
	return strings.ToLower(parts[0] + "/" + parts[1]), number
}

func mergePublishedPullRequest(previous core.PullRequest, next core.PullRequest) core.PullRequest {
	next.CreatedAt = previous.CreatedAt
	if next.TaskID == "" {
		next.TaskID = previous.TaskID
	}
	if next.Repo == "" {
		next.Repo = previous.Repo
	}
	if next.Number == 0 {
		next.Number = previous.Number
	}
	if next.URL == "" {
		next.URL = previous.URL
	}
	if next.Branch == "" {
		next.Branch = previous.Branch
	}
	if next.Base == "" {
		next.Base = previous.Base
	}
	if next.Title == "" {
		next.Title = previous.Title
	}
	if next.State == "" {
		next.State = previous.State
	}
	if !next.Draft && previous.Draft {
		next.Draft = previous.Draft
	}
	if next.ChecksStatus == "" {
		next.ChecksStatus = previous.ChecksStatus
	}
	if next.ChecksConclusion == "" {
		next.ChecksConclusion = previous.ChecksConclusion
	}
	if next.MergeStatus == "" {
		next.MergeStatus = previous.MergeStatus
	}
	if next.Mergeable == "" {
		next.Mergeable = previous.Mergeable
	}
	if next.ReviewStatus == "" {
		next.ReviewStatus = previous.ReviewStatus
	}
	next.Metadata = mergePullRequestMetadata(previous.Metadata, next.Metadata)
	return next
}

func mergePullRequestMetadata(previous json.RawMessage, next json.RawMessage) json.RawMessage {
	if len(next) == 0 {
		return previous
	}
	if len(previous) == 0 {
		return next
	}
	var merged map[string]any
	if err := json.Unmarshal(previous, &merged); err != nil || merged == nil {
		return next
	}
	var incoming map[string]any
	if err := json.Unmarshal(next, &incoming); err != nil || incoming == nil {
		return next
	}
	for key, value := range incoming {
		merged[key] = value
	}
	clearMissingTriggeredFeedback(merged, incoming, "latestPullRequestFeedback")
	clearMissingTriggeredFeedback(merged, incoming, "latestConversationComment")
	return core.MustJSON(merged)
}

func clearMissingTriggeredFeedback(merged map[string]any, incoming map[string]any, prefix string) {
	signatureKey := prefix + "Signature"
	triggeredKey := prefix + "TriggeredSignature"
	if _, ok := incoming[signatureKey]; !ok {
		return
	}
	if _, ok := incoming[triggeredKey]; ok {
		return
	}
	delete(merged, triggeredKey)
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

func (s *SQLiteStore) projectionEvents(ctx context.Context) ([]core.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT
	id,
	at,
	type,
	task_id,
	worker_id,
	CASE type WHEN 'worker.output' THEN '{}' ELSE payload END AS payload
FROM events
WHERE type IN (
	'task.created',
	'task.updated',
	'task.status',
	'task.final_candidate_selected',
	'task.objective_updated',
	'task.milestone_reached',
	'task.artifact_recorded',
	'task.cleared',
	'execution.node_planned',
	'execution.node_status',
	'worker.workspace_prepared',
	'worker.created',
	'worker.started',
	'worker.output',
	'worker.completed',
	'worker.changes_applied',
	'pull_request.published',
	'pull_request.updated',
	'pull_request.status_checked',
	'pull_request.babysitter_started'
)
ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *SQLiteStore) latestEventID(ctx context.Context) (int64, error) {
	var id sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(id) FROM events`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func maxEventID(events []core.Event) int64 {
	var max int64
	for _, event := range events {
		if event.ID > max {
			max = event.ID
		}
	}
	return max
}

func snapshotResponseEvents(events []core.Event, includeEvents bool) []core.Event {
	if includeEvents {
		return events
	}
	return nil
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

func mergeMetadataPatch(base json.RawMessage, patch json.RawMessage) json.RawMessage {
	if len(patch) == 0 {
		return base
	}
	out := map[string]any{}
	if len(base) > 0 {
		_ = json.Unmarshal(base, &out)
	}
	var patchValues map[string]any
	if err := json.Unmarshal(patch, &patchValues); err != nil {
		return base
	}
	for key, value := range patchValues {
		out[key] = value
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
	var remoteCheckouts string
	var githubIssues string
	var githubMentions string
	var policy string
	if err := scanner.Scan(
		&project.ID,
		&project.Name,
		&project.LocalPath,
		&project.Repo,
		&project.UpstreamRepo,
		&project.HeadRepoOwner,
		&project.PushRemote,
		&project.VCS,
		&project.DefaultBase,
		&project.WorkspaceRoot,
		&labels,
		&remoteCheckouts,
		&githubIssues,
		&githubMentions,
		&policy,
	); err != nil {
		return core.Project{}, err
	}
	if labels != "" {
		if err := json.Unmarshal([]byte(labels), &project.TargetLabels); err != nil {
			return core.Project{}, err
		}
	}
	if remoteCheckouts != "" {
		if err := json.Unmarshal([]byte(remoteCheckouts), &project.RemoteCheckouts); err != nil {
			return core.Project{}, err
		}
	}
	if githubIssues != "" {
		if err := json.Unmarshal([]byte(githubIssues), &project.GitHubIssues); err != nil {
			return core.Project{}, err
		}
	}
	if githubMentions != "" {
		if err := json.Unmarshal([]byte(githubMentions), &project.GitHubMentions); err != nil {
			return core.Project{}, err
		}
	}
	if policy != "" {
		if err := json.Unmarshal([]byte(policy), &project.PullRequestPolicy); err != nil {
			return core.Project{}, err
		}
	}
	return project, nil
}

func scanPlugin(scanner eventScanner) (core.Plugin, error) {
	var plugin core.Plugin
	var enabled int
	var command string
	var capabilities string
	var config string
	if err := scanner.Scan(
		&plugin.ID,
		&plugin.Name,
		&plugin.Kind,
		&plugin.Protocol,
		&enabled,
		&plugin.Status,
		&plugin.Error,
		&command,
		&plugin.Endpoint,
		&capabilities,
		&config,
	); err != nil {
		return core.Plugin{}, err
	}
	plugin.Enabled = enabled != 0
	if command != "" {
		if err := json.Unmarshal([]byte(command), &plugin.Command); err != nil {
			return core.Plugin{}, err
		}
	}
	if capabilities != "" {
		if err := json.Unmarshal([]byte(capabilities), &plugin.Capabilities); err != nil {
			return core.Plugin{}, err
		}
	}
	if config != "" {
		if err := json.Unmarshal([]byte(config), &plugin.Config); err != nil {
			return core.Plugin{}, err
		}
	}
	return plugin, nil
}

func scanTarget(scanner eventScanner) (core.TargetConfig, error) {
	var target core.TargetConfig
	var insecure int
	var labels string
	var capacity string
	if err := scanner.Scan(
		&target.ID,
		&target.Kind,
		&target.Host,
		&target.User,
		&target.Port,
		&target.IdentityFile,
		&insecure,
		&target.CheckoutRoot,
		&target.WorkDir,
		&target.WorkRoot,
		&labels,
		&capacity,
	); err != nil {
		return core.TargetConfig{}, err
	}
	if target.CheckoutRoot == "" {
		target.CheckoutRoot = target.WorkDir
	}
	if target.WorkDir == "" {
		target.WorkDir = target.CheckoutRoot
	}
	target.InsecureIgnoreHostKey = insecure != 0
	if labels != "" {
		if err := json.Unmarshal([]byte(labels), &target.Labels); err != nil {
			return core.TargetConfig{}, err
		}
	}
	if capacity != "" {
		if err := json.Unmarshal([]byte(capacity), &target.Capacity); err != nil {
			return core.TargetConfig{}, err
		}
	}
	return target, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
