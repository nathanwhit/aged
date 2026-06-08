package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aged/internal/core"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db      *sql.DB
	writeMu sync.Mutex
	appends atomic.Uint64
}

const sqliteBusyTimeoutMillis = 30000

func isTerminalWorkerStatus(status core.WorkerStatus) bool {
	return status == core.WorkerSucceeded || status == core.WorkerFailed || status == core.WorkerCanceled
}

func isTerminalTaskStatus(status core.TaskStatus) bool {
	return status == core.TaskSucceeded || status == core.TaskFailed || status == core.TaskCanceled
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

func withSQLiteBusyRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isSQLiteBusy(err) {
			return err
		}
		wait := 50 * time.Millisecond * time.Duration(1<<attempt)
		if wait > time.Second {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
	return lastErr
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") ||
		strings.Contains(text, "database is locked") ||
		strings.Contains(text, "database table is locked")
}

func (s *SQLiteStore) withWriteTx(ctx context.Context, fn func(*sql.Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return withSQLiteBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
		if err := fn(tx); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	})
}

func OpenSQLite(ctx context.Context, path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	store := &SQLiteStore{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sqliteDSN(path string) string {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator +
		"_txlock=immediate" +
		fmt.Sprintf("&_pragma=busy_timeout%%3d%d", sqliteBusyTimeoutMillis) +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=wal_autocheckpoint(256)" +
		"&_pragma=journal_size_limit(67108864)" +
		"&_pragma=foreign_keys(ON)"
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 30000;
PRAGMA wal_autocheckpoint = 256;
PRAGMA journal_size_limit = 67108864;
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
CREATE INDEX IF NOT EXISTS events_task_type_idx ON events(task_id, type, id);
CREATE INDEX IF NOT EXISTS events_worker_idx ON events(worker_id, id);

CREATE TABLE IF NOT EXISTS projection_meta (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	last_event_id INTEGER NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_read_models (
	id TEXT PRIMARY KEY,
	project_id TEXT NOT NULL DEFAULT '',
	workstream_id TEXT NOT NULL DEFAULT '',
	title TEXT NOT NULL DEFAULT '',
	prompt TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	objective_status TEXT NOT NULL DEFAULT '',
	objective_phase TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	metadata TEXT NOT NULL DEFAULT '',
	applied_worker_id TEXT NOT NULL DEFAULT '',
	milestones TEXT NOT NULL DEFAULT '[]',
	work_plan TEXT NOT NULL DEFAULT '',
	artifacts TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS task_read_models_status_idx ON task_read_models(status, updated_at);

CREATE TABLE IF NOT EXISTS worker_read_models (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	kind TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT '',
	command TEXT NOT NULL DEFAULT '[]',
	prompt TEXT NOT NULL DEFAULT '',
	prompt_path TEXT NOT NULL DEFAULT '',
	prompt_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	metadata TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS worker_read_models_task_idx ON worker_read_models(task_id);

CREATE TABLE IF NOT EXISTS execution_node_read_models (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	worker_id TEXT NOT NULL DEFAULT '',
	worker_kind TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT '',
	plan_id TEXT NOT NULL DEFAULT '',
	parent_node_id TEXT NOT NULL DEFAULT '',
	spawn_id TEXT NOT NULL DEFAULT '',
	role TEXT NOT NULL DEFAULT '',
	reason TEXT NOT NULL DEFAULT '',
	target_id TEXT NOT NULL DEFAULT '',
	target_kind TEXT NOT NULL DEFAULT '',
	remote_session TEXT NOT NULL DEFAULT '',
	remote_run_dir TEXT NOT NULL DEFAULT '',
	remote_work_dir TEXT NOT NULL DEFAULT '',
	depends_on TEXT NOT NULL DEFAULT '[]',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	metadata TEXT NOT NULL DEFAULT ''
);

	CREATE INDEX IF NOT EXISTS execution_node_read_models_task_idx ON execution_node_read_models(task_id);
	CREATE INDEX IF NOT EXISTS execution_node_read_models_worker_idx ON execution_node_read_models(worker_id);

	CREATE TABLE IF NOT EXISTS work_item_read_models (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		kind TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT '',
		target_kind TEXT NOT NULL DEFAULT '',
		target_id TEXT NOT NULL DEFAULT '',
		reason TEXT NOT NULL DEFAULT '',
		prompt TEXT NOT NULL DEFAULT '',
		worker_id TEXT NOT NULL DEFAULT '',
		lease_owner TEXT NOT NULL DEFAULT '',
		lease_until TEXT NOT NULL DEFAULT '',
		attempt INTEGER NOT NULL DEFAULT 0,
		error TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		metadata TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS work_item_read_models_task_idx ON work_item_read_models(task_id);
	CREATE INDEX IF NOT EXISTS work_item_read_models_target_idx ON work_item_read_models(target_kind, target_id, status);
	CREATE INDEX IF NOT EXISTS work_item_read_models_lease_idx ON work_item_read_models(status, lease_owner, lease_until);

	CREATE TABLE IF NOT EXISTS artifact_read_models (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		kind TEXT NOT NULL DEFAULT '',
		name TEXT NOT NULL DEFAULT '',
		url TEXT NOT NULL DEFAULT '',
		ref TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		metadata TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS artifact_read_models_task_idx ON artifact_read_models(task_id, updated_at);
	CREATE INDEX IF NOT EXISTS artifact_read_models_kind_idx ON artifact_read_models(kind, updated_at);

	CREATE TABLE IF NOT EXISTS memory_entry_read_models (
		id TEXT PRIMARY KEY,
		project_id TEXT NOT NULL DEFAULT '',
		task_id TEXT NOT NULL DEFAULT '',
		kind TEXT NOT NULL DEFAULT '',
		source_event_id INTEGER NOT NULL DEFAULT 0,
		source_event TEXT NOT NULL DEFAULT '',
		worker_id TEXT NOT NULL DEFAULT '',
		summary TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		metadata TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS memory_entry_read_models_task_idx ON memory_entry_read_models(task_id, updated_at);
	CREATE INDEX IF NOT EXISTS memory_entry_read_models_project_idx ON memory_entry_read_models(project_id, updated_at);
	CREATE INDEX IF NOT EXISTS memory_entry_read_models_source_idx ON memory_entry_read_models(source_event_id);

	CREATE TABLE IF NOT EXISTS question_read_models (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		worker_id TEXT NOT NULL DEFAULT '',
		reason TEXT NOT NULL DEFAULT '',
		question TEXT NOT NULL DEFAULT '',
		answer TEXT NOT NULL DEFAULT '',
		decided INTEGER NOT NULL DEFAULT 0,
		approved TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		metadata TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS question_read_models_task_idx ON question_read_models(task_id);
	CREATE INDEX IF NOT EXISTS question_read_models_decided_idx ON question_read_models(decided, updated_at);

	CREATE TABLE IF NOT EXISTS session_read_models (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		worker_id TEXT NOT NULL DEFAULT '',
		node_id TEXT NOT NULL DEFAULT '',
		worker_kind TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL DEFAULT '',
		spawn_id TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT '',
		target_id TEXT NOT NULL DEFAULT '',
		target_kind TEXT NOT NULL DEFAULT '',
		remote_session TEXT NOT NULL DEFAULT '',
		remote_run_dir TEXT NOT NULL DEFAULT '',
		remote_work_dir TEXT NOT NULL DEFAULT '',
		workspace_root TEXT NOT NULL DEFAULT '',
		workspace_cwd TEXT NOT NULL DEFAULT '',
		source_root TEXT NOT NULL DEFAULT '',
		workspace_name TEXT NOT NULL DEFAULT '',
		workspace_mode TEXT NOT NULL DEFAULT '',
		vcs_type TEXT NOT NULL DEFAULT '',
		shared_root TEXT NOT NULL DEFAULT '',
		shared_artifacts_dir TEXT NOT NULL DEFAULT '',
		shared_worker_dir TEXT NOT NULL DEFAULT '',
		provider_session_id TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		started_at TEXT NOT NULL DEFAULT '',
		updated_at TEXT NOT NULL,
		completed_at TEXT NOT NULL DEFAULT '',
		metadata TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS session_read_models_task_idx ON session_read_models(task_id);
	CREATE INDEX IF NOT EXISTS session_read_models_worker_idx ON session_read_models(worker_id);
	CREATE INDEX IF NOT EXISTS session_read_models_status_idx ON session_read_models(status, updated_at);

	CREATE TABLE IF NOT EXISTS pull_request_read_models (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	repo TEXT NOT NULL DEFAULT '',
	number INTEGER NOT NULL DEFAULT 0,
	url TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	base TEXT NOT NULL DEFAULT '',
	title TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL DEFAULT '',
	draft INTEGER NOT NULL DEFAULT 0,
	checks_status TEXT NOT NULL DEFAULT '',
	checks_conclusion TEXT NOT NULL DEFAULT '',
	merge_status TEXT NOT NULL DEFAULT '',
	mergeable TEXT NOT NULL DEFAULT '',
	review_status TEXT NOT NULL DEFAULT '',
	babysitter_task_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	metadata TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS pull_request_read_models_task_idx ON pull_request_read_models(task_id);

CREATE TABLE IF NOT EXISTS pull_request_feedback_read_models (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	pull_request_id TEXT NOT NULL,
	event_id INTEGER NOT NULL DEFAULT 0,
	attempt INTEGER NOT NULL DEFAULT 0,
	status TEXT NOT NULL DEFAULT '',
	reason TEXT NOT NULL DEFAULT '',
	repo TEXT NOT NULL DEFAULT '',
	number INTEGER NOT NULL DEFAULT 0,
	url TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	base TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL DEFAULT '',
	checks_status TEXT NOT NULL DEFAULT '',
	merge_status TEXT NOT NULL DEFAULT '',
	review_status TEXT NOT NULL DEFAULT '',
	feedback_signature TEXT NOT NULL DEFAULT '',
	feedback_body TEXT NOT NULL DEFAULT '',
	prompt TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	handled_at TEXT NOT NULL DEFAULT '',
	metadata TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS pull_request_feedback_read_models_task_idx ON pull_request_feedback_read_models(task_id, status, updated_at);
CREATE INDEX IF NOT EXISTS pull_request_feedback_read_models_pr_idx ON pull_request_feedback_read_models(pull_request_id, status);

CREATE TABLE IF NOT EXISTS steering_read_models (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	worker_id TEXT NOT NULL DEFAULT '',
	node_id TEXT NOT NULL DEFAULT '',
	worker_kind TEXT NOT NULL DEFAULT '',
	role TEXT NOT NULL DEFAULT '',
	spawn_id TEXT NOT NULL DEFAULT '',
	candidate_worker_id TEXT NOT NULL DEFAULT '',
	review_phase TEXT NOT NULL DEFAULT '',
	target_kind TEXT NOT NULL DEFAULT '',
	target_id TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT '',
	reason TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	applied_at TEXT NOT NULL DEFAULT '',
	metadata TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS steering_read_models_task_idx ON steering_read_models(task_id, status, updated_at);
CREATE INDEX IF NOT EXISTS steering_read_models_target_idx ON steering_read_models(target_kind, target_id, status);

CREATE TABLE IF NOT EXISTS pull_request_aliases (
	alias TEXT PRIMARY KEY,
	id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pull_request_identities (
	identity TEXT PRIMARY KEY,
	id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cleared_tasks (
	task_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS worker_node_links (
	worker_id TEXT PRIMARY KEY,
	node_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_workspace_metadata (
	worker_id TEXT PRIMARY KEY,
	data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_output_watermarks (
	worker_id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL DEFAULT '',
	event_id INTEGER NOT NULL,
	at TEXT NOT NULL,
	label TEXT NOT NULL DEFAULT '',
	current_action TEXT NOT NULL DEFAULT ''
);

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
	requirements TEXT NOT NULL DEFAULT '{}',
	remote_checkouts TEXT NOT NULL DEFAULT '{}',
	github_issues TEXT NOT NULL DEFAULT '{}',
	github_mentions TEXT NOT NULL DEFAULT '{}',
	review_policy TEXT NOT NULL DEFAULT '{}',
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

CREATE TABLE IF NOT EXISTS prompt_sets (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	templates TEXT NOT NULL DEFAULT '{}',
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
		{"requirements", "TEXT NOT NULL DEFAULT '{}'"},
		{"github_issues", "TEXT NOT NULL DEFAULT '{}'"},
		{"github_mentions", "TEXT NOT NULL DEFAULT '{}'"},
		{"review_policy", "TEXT NOT NULL DEFAULT '{}'"},
		{"pull_request_policy", "TEXT NOT NULL DEFAULT '{}'"},
	} {
		if err := s.ensureColumn(ctx, "projects", column.name, column.definition); err != nil {
			return err
		}
	}
	if err := s.ensureColumn(ctx, "targets", "checkout_root", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{"lease_owner", "TEXT NOT NULL DEFAULT ''"},
		{"lease_until", "TEXT NOT NULL DEFAULT ''"},
		{"attempt", "INTEGER NOT NULL DEFAULT 0"},
	} {
		if err := s.ensureColumn(ctx, "work_item_read_models", column.name, column.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{"shared_root", "TEXT NOT NULL DEFAULT ''"},
		{"shared_artifacts_dir", "TEXT NOT NULL DEFAULT ''"},
		{"shared_worker_dir", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.ensureColumn(ctx, "session_read_models", column.name, column.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{"label", "TEXT NOT NULL DEFAULT ''"},
		{"current_action", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.ensureColumn(ctx, "worker_output_watermarks", column.name, column.definition); err != nil {
			return err
		}
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
	err = withSQLiteBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
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
		return err
	})
	if err != nil {
		return core.Plugin{}, err
	}
	return plugin, nil
}

func (s *SQLiteStore) DeletePlugin(ctx context.Context, id string) error {
	return s.deleteByID(ctx, id, "plugin id is required", `DELETE FROM plugins WHERE id = ?`)
}

func (s *SQLiteStore) ListPromptSets(ctx context.Context) ([]core.PromptSet, string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, description, templates FROM prompt_sets ORDER BY id ASC`)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var promptSets []core.PromptSet
	for rows.Next() {
		promptSet, err := scanPromptSet(rows)
		if err != nil {
			return nil, "", err
		}
		promptSets = append(promptSets, promptSet)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	defaultID, err := s.setting(ctx, "default_prompt_set_id")
	return promptSets, defaultID, err
}

func (s *SQLiteStore) SavePromptSet(ctx context.Context, promptSet core.PromptSet, makeDefault bool) (core.PromptSet, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	templates, err := jsonString(promptSet.Templates, "{}")
	if err != nil {
		return core.PromptSet{}, err
	}
	err = s.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO prompt_sets (id, name, description, templates, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	name = excluded.name,
	description = excluded.description,
	templates = excluded.templates,
	updated_at = excluded.updated_at`,
			promptSet.ID, promptSet.Name, promptSet.Description, templates, now, now,
		); err != nil {
			return err
		}
		if makeDefault {
			if _, err := tx.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES ('default_prompt_set_id', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, promptSet.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return core.PromptSet{}, err
	}
	return promptSet, nil
}

func (s *SQLiteStore) DeletePromptSet(ctx context.Context, id string) error {
	return s.deleteByID(ctx, id, "prompt set id is required", `DELETE FROM prompt_sets WHERE id = ?`)
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
	err = withSQLiteBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
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
		return err
	})
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
	var count int64
	err := withSQLiteBusyRetry(ctx, func() error {
		res, err := s.db.ExecContext(ctx, deleteSQL, id)
		if err != nil {
			return err
		}
		count, err = res.RowsAffected()
		return err
	})
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

	var appended core.Event
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		next := event
		res, err := tx.ExecContext(ctx, `
INSERT INTO events (at, type, task_id, worker_id, payload)
VALUES (?, ?, ?, ?, ?)`,
			next.At.Format(time.RFC3339Nano),
			string(next.Type),
			next.TaskID,
			next.WorkerID,
			string(next.Payload),
		)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		next.ID = id
		if err := updateProjectionReadModelTx(ctx, tx, next); err != nil {
			return err
		}
		appended = next
		return nil
	})
	if err != nil {
		return core.Event{}, err
	}
	s.maybeCheckpointWAL()
	return appended, nil
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
	SELECT id, at, type, task_id, worker_id, payload
	FROM (
		SELECT id, at, type, task_id, worker_id, payload
		FROM events
		WHERE task_id = ?
		ORDER BY id DESC
		LIMIT ?
	)
	ORDER BY id ASC`, taskID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *SQLiteStore) ListTaskLedgerEvents(ctx context.Context, taskID string) ([]core.Event, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, errors.New("task id is required")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, at, type, task_id, worker_id, payload
FROM events
WHERE task_id = ?
	AND type IN (
		'execution.node_planned',
		'worker.created',
		'worker.completed',
		'approval.needed',
		'approval.decided',
		'task.action_executed',
		'task.milestone_reached',
		'task.replanned'
	)
ORDER BY id ASC`, taskID)
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
	SELECT id, name, local_path, repo, upstream_repo, head_repo_owner, push_remote, vcs, default_base, workspace_root, target_labels, requirements, remote_checkouts, github_issues, github_mentions, review_policy, pull_request_policy
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
	INSERT INTO projects (id, name, local_path, repo, upstream_repo, head_repo_owner, push_remote, vcs, default_base, workspace_root, target_labels, requirements, remote_checkouts, github_issues, github_mentions, review_policy, pull_request_policy, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func projectInsertArgs(project core.Project, now string) ([]any, error) {
	labels, err := jsonString(project.TargetLabels, "{}")
	if err != nil {
		return nil, err
	}
	requirements, err := jsonString(project.Requirements, "{}")
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
	reviewPolicy, err := jsonString(project.ReviewPolicy, "{}")
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
		requirements,
		remoteCheckouts,
		githubIssues,
		githubMentions,
		reviewPolicy,
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
	err = s.withWriteTx(ctx, func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects`).Scan(&count); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, projectInsertSQL, args...); err != nil {
			return err
		}
		if count == 0 {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO settings (key, value) VALUES ('default_project_id', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, project.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
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
	err = s.withWriteTx(ctx, func(tx *sql.Tx) error {
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
	requirements = excluded.requirements,
	remote_checkouts = excluded.remote_checkouts,
	github_issues = excluded.github_issues,
	github_mentions = excluded.github_mentions,
	review_policy = excluded.review_policy,
	pull_request_policy = excluded.pull_request_policy,
	updated_at = excluded.updated_at`,
			args...,
		); err != nil {
			return err
		}
		if makeDefault {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO settings (key, value) VALUES ('default_project_id', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, project.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return core.Project{}, err
	}
	return project, nil
}

func (s *SQLiteStore) DeleteProject(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("project id is required")
	}
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
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
		return nil
	})
}

func (s *SQLiteStore) Snapshot(ctx context.Context) (core.Snapshot, error) {
	return s.snapshotFromReadModel(ctx, true)
}

func (s *SQLiteStore) SnapshotSummary(ctx context.Context) (core.Snapshot, error) {
	return s.snapshotFromReadModel(ctx, false)
}

func (s *SQLiteStore) SnapshotTaskCards(ctx context.Context) (core.Snapshot, error) {
	return s.taskCardsFromReadModel(ctx)
}

func (s *SQLiteStore) PullRequestMonitorSnapshot(ctx context.Context) (core.Snapshot, error) {
	lastEventID, err := s.latestEventID(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	state := newReadModelState()
	if err := loadProjectionTasks(ctx, s.db, state.Tasks); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadActiveProjectionWorkers(ctx, s.db, state.Workers); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadActiveProjectionExecutionNodes(ctx, s.db, state.Nodes); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadActiveProjectionWorkItems(ctx, s.db, state.WorkItems); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadActiveProjectionSessions(ctx, s.db, state.Sessions); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionPullRequests(ctx, s.db, state.PullRequests); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadProjectionPullRequestFeedback(ctx, s.db, state.PullRequestFeedback); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadPendingProjectionSteering(ctx, s.db, state.Steering); err != nil {
		return core.Snapshot{}, err
	}
	if err := loadClearedTasks(ctx, s.db, `cleared_tasks`, state.ClearedTasks); err != nil {
		return core.Snapshot{}, err
	}
	monitorTaskIDs := map[string]bool{}
	for _, pr := range state.PullRequests {
		if strings.TrimSpace(pr.TaskID) != "" && !state.ClearedTasks[pr.TaskID] {
			monitorTaskIDs[pr.TaskID] = true
		}
	}
	events, err := s.pullRequestMonitorEvents(ctx, monitorTaskIDs)
	if err != nil {
		return core.Snapshot{}, err
	}
	tasks := filterClearedTasks(state.Tasks, state.ClearedTasks)
	taskIDs := map[string]bool{}
	for id := range tasks {
		taskIDs[id] = true
	}
	return core.Snapshot{
		Tasks:               orderedTasks(tasks),
		Workers:             orderedWorkers(filterTasks(state.Workers, state.ClearedTasks, taskIDs, func(worker core.Worker) string { return worker.TaskID })),
		ExecutionNodes:      orderedExecutionNodes(filterTasks(state.Nodes, state.ClearedTasks, taskIDs, func(node core.ExecutionNode) string { return node.TaskID })),
		WorkItems:           orderedWorkItems(filterTasks(state.WorkItems, state.ClearedTasks, taskIDs, func(item core.WorkItem) string { return item.TaskID })),
		Sessions:            orderedSessions(filterTasks(state.Sessions, state.ClearedTasks, taskIDs, func(session core.Session) string { return session.TaskID })),
		PullRequests:        orderedPullRequests(filterClearedPullRequests(state.PullRequests, state.ClearedTasks)),
		PullRequestFeedback: orderedPullRequestFeedback(filterTasks(state.PullRequestFeedback, state.ClearedTasks, taskIDs, func(feedback core.PullRequestFeedback) string { return feedback.TaskID })),
		Steering:            orderedSteering(filterTasks(state.Steering, state.ClearedTasks, taskIDs, func(item core.SteeringItem) string { return item.TaskID })),
		LastEventID:         lastEventID,
		Events:              events,
	}, nil
}

func (s *SQLiteStore) pullRequestMonitorEvents(ctx context.Context, taskIDs map[string]bool) ([]core.Event, error) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(taskIDs))
	for id := range taskIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	eventTypes := []core.EventType{
		core.EventTaskPlanned,
		core.EventTaskReplanned,
		core.EventWorkerCreated,
		core.EventWorkerCompleted,
		core.EventTaskAction,
		core.EventPRStatusChecked,
		core.EventPRFollowUp,
		core.EventWorkItemQueued,
		core.EventWorkItemStarted,
		core.EventWorkItemCompleted,
	}
	args := make([]any, 0, len(ids)+len(eventTypes))
	taskPlaceholders := make([]string, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
		taskPlaceholders = append(taskPlaceholders, "?")
	}
	typePlaceholders := make([]string, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		args = append(args, eventType)
		typePlaceholders = append(typePlaceholders, "?")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, at, type, task_id, worker_id, payload
FROM events
WHERE task_id IN (`+strings.Join(taskPlaceholders, ",")+`)
	AND type IN (`+strings.Join(typePlaceholders, ",")+`)
ORDER BY id ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *SQLiteStore) TaskStatus(ctx context.Context, taskID string) (core.TaskStatus, bool, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", false, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT type, payload
FROM events
WHERE task_id = ? AND type IN ('task.created', 'task.status', 'task.cleared')
ORDER BY id ASC`, taskID)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	status := core.TaskQueued
	found := false
	cleared := false
	for rows.Next() {
		var eventType core.EventType
		var payload string
		if err := rows.Scan(&eventType, &payload); err != nil {
			return "", false, err
		}
		switch eventType {
		case core.EventTaskCreated:
			found = true
			cleared = false
			status = core.TaskQueued
		case core.EventTaskStatus:
			var decoded struct {
				Status core.TaskStatus `json:"status"`
			}
			if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
				return "", false, fmt.Errorf("decode task.status: %w", err)
			}
			if decoded.Status != "" {
				status = decoded.Status
			}
		case core.EventTaskCleared:
			cleared = true
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if !found || cleared {
		return "", false, nil
	}
	return status, true, nil
}

func (s *SQLiteStore) ActiveTaskWorkerIDs(ctx context.Context, taskID string) ([]string, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id
FROM worker_read_models
WHERE task_id = ? AND status NOT IN (?, ?, ?)
UNION
SELECT worker_id
FROM execution_node_read_models
WHERE task_id = ? AND worker_id != '' AND status NOT IN (?, ?, ?)
ORDER BY 1`, taskID, core.WorkerSucceeded, core.WorkerFailed, core.WorkerCanceled, taskID, core.WorkerSucceeded, core.WorkerFailed, core.WorkerCanceled)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	workerIDs := []string{}
	for rows.Next() {
		var workerID string
		if err := rows.Scan(&workerID); err != nil {
			return nil, err
		}
		workerIDs = append(workerIDs, workerID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return workerIDs, nil
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

func maxTime(a time.Time, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
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

func taskArtifactSnapshotID(id string, eventID int64) string {
	if id != "" {
		return id
	}
	if eventID > 0 {
		return fmt.Sprintf("event-%d", eventID)
	}
	return ""
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

func applyPullRequestFeedbackQueued(feedbackRows map[string]core.PullRequestFeedback, pullRequests map[string]core.PullRequest, aliases map[string]string, event core.Event) error {
	var payload struct {
		ID                string `json:"id"`
		Attempt           int    `json:"attempt"`
		Reason            string `json:"reason"`
		Repo              string `json:"repo"`
		Number            int    `json:"number"`
		URL               string `json:"url"`
		Branch            string `json:"branch"`
		Base              string `json:"base"`
		State             string `json:"state"`
		ChecksStatus      string `json:"checksStatus"`
		MergeStatus       string `json:"mergeStatus"`
		ReviewStatus      string `json:"reviewStatus"`
		FeedbackSignature string `json:"feedbackSignature"`
		Prompt            string `json:"prompt"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode pull_request.followup_started: %w", err)
	}
	if strings.TrimSpace(payload.ID) == "" {
		return nil
	}
	pr, ok := projectionPullRequestForFeedback(pullRequests, aliases, event.TaskID, payload.ID, payload.Repo, payload.Number, payload.URL, payload.Branch)
	if ok {
		payload.ID = pr.ID
		payload.Repo = nonEmptyString(payload.Repo, pr.Repo)
		payload.Number = firstNonZeroInt(payload.Number, pr.Number)
		payload.URL = nonEmptyString(payload.URL, pr.URL)
		payload.Branch = nonEmptyString(payload.Branch, pr.Branch)
		payload.Base = nonEmptyString(payload.Base, pr.Base)
		payload.State = nonEmptyString(payload.State, pr.State)
		payload.ChecksStatus = nonEmptyString(payload.ChecksStatus, pr.ChecksStatus)
		payload.MergeStatus = nonEmptyString(payload.MergeStatus, pr.MergeStatus)
		payload.ReviewStatus = nonEmptyString(payload.ReviewStatus, pr.ReviewStatus)
		payload.FeedbackSignature = projectionUnhandledFeedbackSignature(pr, payload.FeedbackSignature)
	}
	feedback := core.PullRequestFeedback{
		ID:                projectionPullRequestFeedbackID(event.TaskID, payload.ID, payload.FeedbackSignature, event.ID),
		TaskID:            event.TaskID,
		PullRequestID:     payload.ID,
		EventID:           event.ID,
		Attempt:           payload.Attempt,
		Status:            "pending",
		Reason:            payload.Reason,
		Repo:              payload.Repo,
		Number:            payload.Number,
		URL:               payload.URL,
		Branch:            payload.Branch,
		Base:              payload.Base,
		State:             payload.State,
		ChecksStatus:      payload.ChecksStatus,
		MergeStatus:       payload.MergeStatus,
		ReviewStatus:      payload.ReviewStatus,
		FeedbackSignature: payload.FeedbackSignature,
		Prompt:            payload.Prompt,
		CreatedAt:         event.At,
		UpdatedAt:         event.At,
	}
	if ok && feedback.FeedbackSignature != "" {
		feedback.FeedbackBody = projectionPullRequestLatestFeedbackBody(pr.Metadata)
	}
	if ok {
		feedback = refreshPullRequestFeedbackFromPullRequest(feedback, pr, event.At)
	}
	if previous := feedbackRows[feedback.ID]; previous.ID != "" {
		feedback.CreatedAt = previous.CreatedAt
		if previous.Status != "" && previous.Status != "pending" {
			feedback.Status = previous.Status
			feedback.HandledAt = previous.HandledAt
		}
	}
	feedbackRows[feedback.ID] = feedback
	return nil
}

func applyPullRequestFeedbackAction(feedbackRows map[string]core.PullRequestFeedback, event core.Event) error {
	var payload struct {
		Kind          string         `json:"kind"`
		Status        string         `json:"status"`
		PullRequestID string         `json:"pullRequestId"`
		Inputs        map[string]any `json:"inputs"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode task.action_executed: %w", err)
	}
	status := strings.TrimSpace(payload.Status)
	if strings.EqualFold(status, "started") || strings.EqualFold(status, "waiting") || strings.EqualFold(status, "continued") {
		return nil
	}
	for id, feedback := range feedbackRows {
		if feedback.TaskID != event.TaskID || feedback.Status != "pending" {
			continue
		}
		handled := false
		switch strings.TrimSpace(payload.Kind) {
		case "watch_pull_requests":
			handled = strings.TrimSpace(feedback.FeedbackSignature) == "" && projectionPullRequestFeedbackActionMatches(feedback, payload.PullRequestID, payload.Inputs)
		case "update_pull_request":
			handled = status == "" && projectionPullRequestFeedbackActionMatches(feedback, payload.PullRequestID, payload.Inputs)
			if handled && projectionPullRequestFeedbackRequiresMetadataUpdate(feedback) && !projectionUpdatePullRequestActionHasMetadata(payload.Inputs) {
				handled = false
			}
		}
		if handled {
			feedback.Status = "handled"
			feedback.UpdatedAt = event.At
			feedback.HandledAt = &event.At
			feedbackRows[id] = feedback
		}
	}
	return nil
}

func refreshPullRequestFeedbackForPullRequest(feedbackRows map[string]core.PullRequestFeedback, pr core.PullRequest, at time.Time) {
	for id, feedback := range feedbackRows {
		if feedback.TaskID != pr.TaskID || feedback.PullRequestID != pr.ID || feedback.Status != "pending" {
			continue
		}
		feedbackRows[id] = refreshPullRequestFeedbackFromPullRequest(feedback, pr, at)
	}
}

func refreshPullRequestFeedbackFromPullRequest(feedback core.PullRequestFeedback, pr core.PullRequest, at time.Time) core.PullRequestFeedback {
	feedback.Repo = nonEmptyString(feedback.Repo, pr.Repo)
	feedback.Number = firstNonZeroInt(feedback.Number, pr.Number)
	feedback.URL = nonEmptyString(feedback.URL, pr.URL)
	feedback.Branch = nonEmptyString(feedback.Branch, pr.Branch)
	feedback.Base = nonEmptyString(feedback.Base, pr.Base)
	feedback.State = nonEmptyString(pr.State, feedback.State)
	feedback.ChecksStatus = nonEmptyString(pr.ChecksStatus, feedback.ChecksStatus)
	feedback.MergeStatus = nonEmptyString(pr.MergeStatus, feedback.MergeStatus)
	feedback.ReviewStatus = nonEmptyString(pr.ReviewStatus, feedback.ReviewStatus)
	if feedback.FeedbackSignature != "" {
		feedback.FeedbackBody = projectionPullRequestLatestFeedbackBody(pr.Metadata)
	}
	if projectionTerminalPullRequestState(pr.State) || (feedback.FeedbackSignature != "" && projectionUnhandledFeedbackSignature(pr, feedback.FeedbackSignature) == "") {
		feedback.Status = "handled"
		feedback.HandledAt = &at
	}
	feedback.UpdatedAt = at
	return feedback
}

func projectionPullRequestForFeedback(pullRequests map[string]core.PullRequest, aliases map[string]string, taskID string, id string, repo string, number int, url string, branch string) (core.PullRequest, bool) {
	if alias := aliases[id]; alias != "" {
		id = alias
	}
	if pr := pullRequests[id]; pr.ID != "" && pr.TaskID == taskID {
		return pr, true
	}
	repo = strings.ToLower(strings.TrimSpace(repo))
	url = strings.TrimSpace(url)
	branch = strings.TrimSpace(branch)
	for _, pr := range pullRequests {
		if pr.TaskID != taskID {
			continue
		}
		if id != "" && pr.ID == id {
			return pr, true
		}
		if repo != "" && number > 0 && strings.EqualFold(pr.Repo, repo) && pr.Number == number {
			return pr, true
		}
		if url != "" && strings.EqualFold(pr.URL, url) {
			return pr, true
		}
		if branch != "" && pr.Branch == branch && (repo == "" || strings.EqualFold(pr.Repo, repo)) {
			return pr, true
		}
	}
	return core.PullRequest{}, false
}

func projectionPullRequestFeedbackID(taskID string, prID string, signature string, eventID int64) string {
	if strings.TrimSpace(signature) != "" {
		return taskID + "\x00" + prID + "\x00" + strings.TrimSpace(signature)
	}
	return taskID + "\x00" + prID + "\x00" + fmt.Sprint(eventID)
}

func projectionUnhandledFeedbackSignature(pr core.PullRequest, signature string) string {
	signature = strings.TrimSpace(signature)
	if signature == "" || !projectionPullRequestHasUntriggeredFeedback(pr) || signature != projectionPullRequestFeedbackSignature(pr.Metadata) {
		return ""
	}
	return signature
}

func projectionPullRequestHasUntriggeredFeedback(pr core.PullRequest) bool {
	signature := projectionPullRequestFeedbackSignature(pr.Metadata)
	if signature == "" {
		return false
	}
	return projectionPullRequestTriggeredFeedbackSignature(pr.Metadata) != signature
}

func projectionPullRequestFeedbackSignature(raw json.RawMessage) string {
	metadata := projectionMetadataMap(raw)
	signature := strings.TrimSpace(projectionStringMetadataValue(metadata["latestPullRequestFeedbackSignature"]))
	if signature == "" {
		signature = strings.TrimSpace(projectionStringMetadataValue(metadata["latestConversationCommentSignature"]))
	}
	return signature
}

func projectionPullRequestTriggeredFeedbackSignature(raw json.RawMessage) string {
	metadata := projectionMetadataMap(raw)
	signature := strings.TrimSpace(projectionStringMetadataValue(metadata["latestPullRequestFeedbackTriggeredSignature"]))
	if signature == "" {
		signature = strings.TrimSpace(projectionStringMetadataValue(metadata["latestConversationCommentTriggeredSignature"]))
	}
	return signature
}

func projectionPullRequestLatestFeedbackBody(raw json.RawMessage) string {
	metadata := projectionMetadataMap(raw)
	body := strings.TrimSpace(projectionStringMetadataValue(metadata["latestPullRequestFeedbackBody"]))
	if body == "" {
		body = strings.TrimSpace(projectionStringMetadataValue(metadata["latestConversationCommentBody"]))
	}
	return body
}

func projectionMetadataMap(raw json.RawMessage) map[string]any {
	metadata := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &metadata)
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	return metadata
}

func projectionPullRequestFeedbackActionMatches(feedback core.PullRequestFeedback, pullRequestID string, inputs map[string]any) bool {
	if strings.TrimSpace(pullRequestID) != "" && pullRequestID == feedback.PullRequestID {
		return true
	}
	id := projectionStringMetadata(inputs, "id")
	if id != "" && id == feedback.PullRequestID {
		return true
	}
	url := projectionStringMetadata(inputs, "url")
	if url != "" && strings.EqualFold(url, feedback.URL) {
		return true
	}
	repo := projectionStringMetadata(inputs, "repo")
	number := projectionIntMetadata(inputs, "number")
	if repo != "" && number > 0 && strings.EqualFold(repo, feedback.Repo) && number == feedback.Number {
		return true
	}
	branch := projectionStringMetadata(inputs, "branch")
	if branch == "" {
		branch = projectionStringMetadata(inputs, "headBranch")
	}
	return branch != "" && branch == feedback.Branch && (repo == "" || strings.EqualFold(repo, feedback.Repo))
}

func projectionPullRequestFeedbackRequiresMetadataUpdate(feedback core.PullRequestFeedback) bool {
	body := strings.ToLower(strings.TrimSpace(feedback.FeedbackBody))
	return strings.Contains(body, "title") ||
		strings.Contains(body, "description") ||
		strings.Contains(body, "pr body") ||
		strings.Contains(body, "pull request body")
}

func projectionUpdatePullRequestActionHasMetadata(inputs map[string]any) bool {
	return strings.TrimSpace(projectionStringMetadata(inputs, "title")) != "" ||
		strings.TrimSpace(projectionStringMetadata(inputs, "body")) != ""
}

func projectionTerminalPullRequestState(state string) bool {
	return strings.EqualFold(state, "MERGED") || strings.EqualFold(state, "CLOSED")
}

func projectionStringMetadata(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	return strings.TrimSpace(projectionStringMetadataValue(metadata[key]))
}

func projectionStringMetadataValue(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case fmt.Stringer:
		return value.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func projectionIntMetadata(metadata map[string]any, key string) int {
	if metadata == nil {
		return 0
	}
	switch value := metadata[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, _ := value.Int64()
		return int(parsed)
	case string:
		var parsed int
		_, _ = fmt.Sscanf(value, "%d", &parsed)
		return parsed
	default:
		return 0
	}
}

func nonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func applySteeringTaskSteered(steering map[string]core.SteeringItem, event core.Event) error {
	var payload struct {
		Message  string          `json:"message"`
		Reason   string          `json:"reason,omitempty"`
		Metadata json.RawMessage `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode task.steered: %w", err)
	}
	message := strings.TrimSpace(payload.Message)
	if message == "" {
		return nil
	}
	id := "task_steering_" + fmt.Sprint(event.ID)
	steering[id] = core.SteeringItem{
		ID:         id,
		TaskID:     event.TaskID,
		TargetKind: "task",
		TargetID:   event.TaskID,
		Status:     "pending",
		Reason:     nonEmptyString(payload.Reason, "user_task_steering"),
		Message:    message,
		CreatedAt:  event.At,
		UpdatedAt:  event.At,
		Metadata:   payload.Metadata,
	}
	return nil
}

func applySteeringWorkerSteered(steering map[string]core.SteeringItem, workers map[string]core.Worker, nodes map[string]core.ExecutionNode, event core.Event) error {
	var payload struct {
		WorkerID   string          `json:"workerId"`
		NodeID     string          `json:"nodeId"`
		WorkerKind string          `json:"workerKind"`
		Role       string          `json:"role"`
		SpawnID    string          `json:"spawnId"`
		Status     string          `json:"status"`
		Reason     string          `json:"reason"`
		Message    string          `json:"message"`
		Metadata   json.RawMessage `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode worker.steering_queued: %w", err)
	}
	payload.WorkerID = nonEmptyString(payload.WorkerID, event.WorkerID)
	payload.Message = strings.TrimSpace(payload.Message)
	if payload.WorkerID == "" || payload.Message == "" {
		return nil
	}
	if worker, ok := workers[payload.WorkerID]; ok {
		payload.WorkerKind = nonEmptyString(payload.WorkerKind, worker.Kind)
	}
	candidateWorkerID := ""
	reviewPhase := ""
	for _, node := range nodes {
		if node.WorkerID != payload.WorkerID {
			continue
		}
		payload.NodeID = nonEmptyString(payload.NodeID, node.ID)
		payload.WorkerKind = nonEmptyString(payload.WorkerKind, node.WorkerKind)
		payload.Role = nonEmptyString(payload.Role, node.Role)
		payload.SpawnID = nonEmptyString(payload.SpawnID, node.SpawnID)
		metadata := projectionMetadataMap(node.Metadata)
		candidateWorkerID = projectionStringMetadata(metadata, "candidateWorkerID")
		reviewPhase = projectionStringMetadata(metadata, "reviewPhase")
		payload.WorkerKind = nonEmptyString(payload.WorkerKind, projectionStringMetadata(metadata, "workerKind"))
		break
	}
	id := "worker_steering_" + fmt.Sprint(event.ID)
	status := strings.TrimSpace(payload.Status)
	if status == "" {
		status = "pending"
	}
	steering[id] = core.SteeringItem{
		ID:                id,
		TaskID:            event.TaskID,
		WorkerID:          payload.WorkerID,
		NodeID:            payload.NodeID,
		WorkerKind:        payload.WorkerKind,
		Role:              payload.Role,
		SpawnID:           payload.SpawnID,
		CandidateWorkerID: candidateWorkerID,
		ReviewPhase:       reviewPhase,
		TargetKind:        "worker",
		TargetID:          payload.WorkerID,
		Status:            status,
		Reason:            nonEmptyString(payload.Reason, "user_worker_steering"),
		Message:           payload.Message,
		CreatedAt:         event.At,
		UpdatedAt:         event.At,
		Metadata:          payload.Metadata,
	}
	return nil
}

func applyTaskSteeringApplied(steering map[string]core.SteeringItem, taskID string, eventID int64, at time.Time) {
	for id, item := range steering {
		if item.TaskID != taskID || item.TargetKind != "task" || item.Status != "pending" || !strings.HasPrefix(item.ID, "task_steering_") {
			continue
		}
		if steeringEventID(item.ID) >= eventID {
			continue
		}
		item.Status = "applied"
		item.AppliedAt = &at
		item.UpdatedAt = at
		steering[id] = item
	}
}

func applySteeringWorkItemCompleted(steering map[string]core.SteeringItem, workItem core.WorkItem, at time.Time) {
	if workItem.Kind != "user.worker_steering" || !strings.HasPrefix(workItem.ID, "worker_steering_") {
		return
	}
	item := steering[workItem.ID]
	if item.ID == "" {
		return
	}
	item.Status = string(workItem.Status)
	item.WorkerID = nonEmptyString(workItem.WorkerID, item.WorkerID)
	item.AppliedAt = &at
	item.UpdatedAt = at
	steering[workItem.ID] = item
}

func steeringEventID(id string) int64 {
	parts := strings.Split(id, "_")
	if len(parts) == 0 {
		return 0
	}
	var eventID int64
	_, _ = fmt.Sscanf(parts[len(parts)-1], "%d", &eventID)
	return eventID
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

func filterClearedWorkItems(values map[string]core.WorkItem, cleared map[string]bool) map[string]core.WorkItem {
	out := map[string]core.WorkItem{}
	for id, item := range values {
		if !cleared[item.TaskID] {
			out[id] = item
		}
	}
	return out
}

func filterClearedArtifacts(values map[string]core.Artifact, cleared map[string]bool) map[string]core.Artifact {
	out := map[string]core.Artifact{}
	for id, artifact := range values {
		if !cleared[artifact.TaskID] {
			out[id] = artifact
		}
	}
	return out
}

func filterClearedMemoryEntries(values map[string]core.MemoryEntry, cleared map[string]bool) map[string]core.MemoryEntry {
	out := map[string]core.MemoryEntry{}
	for id, entry := range values {
		if !cleared[entry.TaskID] {
			out[id] = entry
		}
	}
	return out
}

func filterClearedQuestions(values map[string]core.Question, cleared map[string]bool) map[string]core.Question {
	out := map[string]core.Question{}
	for id, question := range values {
		if !cleared[question.TaskID] {
			out[id] = question
		}
	}
	return out
}

func filterClearedSessions(values map[string]core.Session, cleared map[string]bool) map[string]core.Session {
	out := map[string]core.Session{}
	for id, session := range values {
		if !cleared[session.TaskID] {
			out[id] = session
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

func filterClearedPullRequestFeedback(values map[string]core.PullRequestFeedback, cleared map[string]bool) map[string]core.PullRequestFeedback {
	out := map[string]core.PullRequestFeedback{}
	for id, feedback := range values {
		if !cleared[feedback.TaskID] {
			out[id] = feedback
		}
	}
	return out
}

func filterClearedSteering(values map[string]core.SteeringItem, cleared map[string]bool) map[string]core.SteeringItem {
	out := map[string]core.SteeringItem{}
	for id, item := range values {
		if !cleared[item.TaskID] {
			out[id] = item
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
WITH latest_worker_output AS (
	SELECT MAX(id) AS id
	FROM events
	WHERE type = 'worker.output'
	GROUP BY worker_id
)
SELECT
	id,
	at,
	type,
	task_id,
	worker_id,
	payload
FROM events
WHERE type IN (
	'task.created',
	'task.updated',
	'task.status',
	'task.objective_updated',
	'task.milestone_reached',
	'task.work_plan_updated',
	'task.artifact_recorded',
	'task.cleared',
		'execution.node_planned',
		'execution.node_status',
		'work_item.queued',
		'work_item.started',
		'work_item.completed',
		'worker.workspace_prepared',
	'worker.created',
	'worker.started',
	'worker.completed',
	'worker.changes_applied',
	'pull_request.published',
	'pull_request.updated',
	'pull_request.status_checked',
	'pull_request.babysitter_started'
)
UNION ALL
SELECT
	id,
	at,
	type,
	task_id,
	worker_id,
	payload
FROM events
WHERE id IN (SELECT id FROM latest_worker_output)
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

func snapshotResponseEvents(events []core.Event, includeEvents bool) []core.Event {
	if includeEvents {
		return events
	}
	return nil
}

func (s *SQLiteStore) Setting(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *SQLiteStore) SaveSetting(ctx context.Context, key string, value string) error {
	return withSQLiteBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO settings (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, key, value)
		return err
	})
}

func (s *SQLiteStore) setting(ctx context.Context, key string) (string, error) {
	return s.Setting(ctx, key)
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
	return stringFromMetadata(metadata, "projectId")
}

func workstreamIDFromMetadata(metadata json.RawMessage) string {
	return stringFromMetadata(metadata, "workstreamId")
}

func stringFromMetadata(metadata json.RawMessage, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	var values map[string]any
	if err := json.Unmarshal(metadata, &values); err != nil {
		return ""
	}
	if value, ok := values[key].(string); ok {
		return value
	}
	return ""
}

func (s *SQLiteStore) Close() error {
	_ = s.checkpointWAL(context.Background(), "TRUNCATE", true)
	return s.db.Close()
}

func (s *SQLiteStore) maybeCheckpointWAL() {
	if s.appends.Add(1)%500 != 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.checkpointWAL(ctx, "PASSIVE", false)
}

func (s *SQLiteStore) checkpointWAL(ctx context.Context, mode string, requireComplete bool) error {
	row := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(`+mode+`)`)
	var busy, logFrames, checkpointedFrames int
	if err := row.Scan(&busy, &logFrames, &checkpointedFrames); err != nil {
		return err
	}
	if requireComplete && busy != 0 {
		return fmt.Errorf("wal checkpoint busy: log=%d checkpointed=%d", logFrames, checkpointedFrames)
	}
	return nil
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanProject(scanner eventScanner) (core.Project, error) {
	var project core.Project
	var labels string
	var requirements string
	var remoteCheckouts string
	var githubIssues string
	var githubMentions string
	var reviewPolicy string
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
		&requirements,
		&remoteCheckouts,
		&githubIssues,
		&githubMentions,
		&reviewPolicy,
		&policy,
	); err != nil {
		return core.Project{}, err
	}
	if labels != "" {
		if err := json.Unmarshal([]byte(labels), &project.TargetLabels); err != nil {
			return core.Project{}, err
		}
	}
	if requirements != "" {
		if err := json.Unmarshal([]byte(requirements), &project.Requirements); err != nil {
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
	if reviewPolicy != "" {
		if err := json.Unmarshal([]byte(reviewPolicy), &project.ReviewPolicy); err != nil {
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

func scanPromptSet(scanner eventScanner) (core.PromptSet, error) {
	var promptSet core.PromptSet
	var templates string
	if err := scanner.Scan(&promptSet.ID, &promptSet.Name, &promptSet.Description, &templates); err != nil {
		return core.PromptSet{}, err
	}
	if templates != "" {
		if err := json.Unmarshal([]byte(templates), &promptSet.Templates); err != nil {
			return core.PromptSet{}, err
		}
	}
	return promptSet, nil
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
	return orderedSnapshotValues(values, func(task core.Task) string { return task.ID }, func(task core.Task) time.Time { return task.CreatedAt })
}

func orderedWorkers(values map[string]core.Worker) []core.Worker {
	return orderedSnapshotValues(values, func(worker core.Worker) string { return worker.ID }, func(worker core.Worker) time.Time { return worker.CreatedAt })
}

func orderedExecutionNodes(values map[string]core.ExecutionNode) []core.ExecutionNode {
	return orderedSnapshotValues(values, func(node core.ExecutionNode) string { return node.ID }, func(node core.ExecutionNode) time.Time { return node.CreatedAt })
}

func orderedWorkItems(values map[string]core.WorkItem) []core.WorkItem {
	return orderedSnapshotValues(values, func(item core.WorkItem) string { return item.ID }, func(item core.WorkItem) time.Time { return item.CreatedAt })
}

func orderedArtifacts(values map[string]core.Artifact) []core.Artifact {
	return orderedSnapshotValues(values, func(artifact core.Artifact) string { return artifact.ID }, func(artifact core.Artifact) time.Time { return artifact.CreatedAt })
}

func orderedMemoryEntries(values map[string]core.MemoryEntry) []core.MemoryEntry {
	return orderedSnapshotValues(values, func(entry core.MemoryEntry) string { return entry.ID }, func(entry core.MemoryEntry) time.Time { return entry.CreatedAt })
}

func orderedQuestions(values map[string]core.Question) []core.Question {
	return orderedSnapshotValues(values, func(question core.Question) string { return question.ID }, func(question core.Question) time.Time { return question.CreatedAt })
}

func orderedSessions(values map[string]core.Session) []core.Session {
	return orderedSnapshotValues(values, func(session core.Session) string { return session.ID }, func(session core.Session) time.Time { return session.CreatedAt })
}

func orderedPullRequests(values map[string]core.PullRequest) []core.PullRequest {
	return orderedSnapshotValues(values, func(pr core.PullRequest) string { return pr.ID }, func(pr core.PullRequest) time.Time { return pr.CreatedAt })
}

func orderedPullRequestFeedback(values map[string]core.PullRequestFeedback) []core.PullRequestFeedback {
	return orderedSnapshotValues(values, func(feedback core.PullRequestFeedback) string { return feedback.ID }, func(feedback core.PullRequestFeedback) time.Time { return feedback.CreatedAt })
}

func orderedSteering(values map[string]core.SteeringItem) []core.SteeringItem {
	return orderedSnapshotValues(values, func(item core.SteeringItem) string { return item.ID }, func(item core.SteeringItem) time.Time { return item.CreatedAt })
}

func orderedSnapshotValues[T any](values map[string]T, id func(T) string, createdAt func(T) time.Time) []T {
	out := make([]T, 0, len(values))
	for _, value := range values {
		if id(value) != "" {
			out = append(out, value)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return createdAt(out[i]).Before(createdAt(out[j]))
	})
	return out
}

var ErrNotFound = errors.New("not found")
