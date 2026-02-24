package queueconfig

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/entity/queueconfig"
)

// ErrNotFound is returned when the requested queue configuration does not exist.
var ErrNotFound = errors.New("queue config not found")

// Store loads and provides queue configurations.
// Implementations may read from YAML files, databases, remote services, etc.
type Store interface {
	// Get returns the configuration for a named queue.
	// Returns ErrNotFound if no configuration exists for the given name.
	Get(ctx context.Context, name string) (queueconfig.QueueConfig, error)

	// List returns all configured queues.
	List(ctx context.Context) ([]queueconfig.QueueConfig, error)
}
