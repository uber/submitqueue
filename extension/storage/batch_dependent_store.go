package storage

//go:generate mockgen -source=batch_dependent_store.go -destination=mock/batch_dependent_store.go -package=mock

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

	// UpdateDependents updates the dependents of a batch dependent if the current version matches the expected version.
	// If versions do not match, returns ErrVersionMismatch.
	UpdateDependents(ctx context.Context, batchID string, version int32, dependents []string) error
}
