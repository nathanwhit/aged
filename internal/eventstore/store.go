package eventstore

import (
	"context"

	"aged/internal/core"
)

type Store interface {
	Append(ctx context.Context, event core.Event) (core.Event, error)
	ListEvents(ctx context.Context, afterID int64, limit int) ([]core.Event, error)
	Snapshot(ctx context.Context) (core.Snapshot, error)
	ListPlugins(ctx context.Context) ([]core.Plugin, error)
	SavePlugin(ctx context.Context, plugin core.Plugin) (core.Plugin, error)
	DeletePlugin(ctx context.Context, id string) error
	ListTargets(ctx context.Context) ([]core.TargetConfig, error)
	SaveTarget(ctx context.Context, target core.TargetConfig) (core.TargetConfig, error)
	DeleteTarget(ctx context.Context, id string) error
	ListProjects(ctx context.Context) ([]core.Project, string, error)
	CreateProject(ctx context.Context, project core.Project) (core.Project, error)
	SaveProject(ctx context.Context, project core.Project, makeDefault bool) (core.Project, error)
	DeleteProject(ctx context.Context, id string) error
	Close() error
}
