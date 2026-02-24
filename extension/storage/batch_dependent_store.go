package storage

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// BatchDependentStore is an interface that defines methods for managing batch dependent information in the database.
type BatchDependentStore interface {
	// Get retrieves the batch dependent by batch ID.
	// Returns ErrNotFound if the batch dependent is not found.
	Get(ctx context.Context, batchID string) (entity.BatchDependent, error)

	// Create creates a new batch dependent.
	// Returns ErrAlreadyExists if the entry already exists.
	Create(ctx context.Context, batchDependent entity.BatchDependent) error

	// There is no update function since once created, data is only ever read from this
	// store.
}
