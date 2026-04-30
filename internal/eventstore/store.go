package eventstore

import (
	"context"

	"aged/internal/core"
)

type Store interface {
	Append(ctx context.Context, event core.Event) (core.Event, error)
	ListEvents(ctx context.Context, afterID int64, limit int) ([]core.Event, error)
	Snapshot(ctx context.Context) (core.Snapshot, error)
	ListProjects(ctx context.Context) ([]core.Project, string, error)
	CreateProject(ctx context.Context, project core.Project) (core.Project, error)
	SaveProject(ctx context.Context, project core.Project, makeDefault bool) (core.Project, error)
	DeleteProject(ctx context.Context, id string) error
	Close() error
}
