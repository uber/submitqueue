package queueconfig

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/entity"
)

// ErrNotFound is returned when the requested queue configuration does not exist.
var ErrNotFound = errors.New("queue config not found")

// ErrAlreadyExists is returned when a queue configuration with the same name already exists.
var ErrAlreadyExists = errors.New("queue config already exists")

// Store loads and provides queue configurations.
// Implementations may read from YAML files, databases, remote services, etc.
type Store interface {
	// Create adds a new queue configuration.
	// Returns ErrAlreadyExists if a configuration with the same name already exists.
	Create(ctx context.Context, config entity.QueueConfig) error

	// Get returns the configuration for a named queue.
	// Returns ErrNotFound if no configuration exists for the given name.
	Get(ctx context.Context, name string) (entity.QueueConfig, error)

	// List returns all configured queues.
	List(ctx context.Context) ([]entity.QueueConfig, error)
}
