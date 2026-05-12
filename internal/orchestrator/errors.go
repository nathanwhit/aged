package orchestrator

import "aged/internal/eventstore"

type notFoundError string

func (e notFoundError) Error() string {
	return string(e)
}

func (e notFoundError) Unwrap() error {
	return eventstore.ErrNotFound
}
