package storage

//go:generate mockgen -source=batch_store.go -destination=mock/batch_store.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// BatchStore is an interface that defines methods for managing batches in the database.
type BatchStore interface {
	// Get retrieves a batch by ID. Returns ErrNotFound if the batch is not found.
	Get(ctx context.Context, id string) (entity.Batch, error)

	// Create creates a new batch. The batch must have a unique ID already assigned.
	// Returns ErrAlreadyExists if a batch with the same ID already exists.
	Create(ctx context.Context, batch entity.Batch) error

	// UpdateState updates the state of a batch if the current version matches the expected version. If versions do not match, returns ErrVersionMismatch.
	// The implementation should increment the version by 1 atomically with the state update.
	UpdateState(ctx context.Context, id string, version int32, newState entity.BatchState) error

	// GetByQueueAndStates retrieves all batches that belong to the given queue and are in the given states.
	GetByQueueAndStates(ctx context.Context, queue string, states []entity.BatchState) ([]entity.Batch, error)
}
