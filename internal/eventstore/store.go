package eventstore

import (
	"context"

	"aged/internal/core"
)

type Store interface {
	Append(ctx context.Context, event core.Event) (core.Event, error)
	ListEvents(ctx context.Context, afterID int64, limit int) ([]core.Event, error)
	Snapshot(ctx context.Context) (core.Snapshot, error)
	Close() error
}
