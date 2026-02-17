package storage

import (
	"context"

	"github.com/uber/submitqueue/entities"
)

// RequestStore is an interface that defines methods for managing land requests in the database.
type RequestStore interface {
	// Get retrieves a land request by ID (queue/seq). Returns ErrNotFound if the request is not found.
	Get(ctx context.Context, id string) (entities.Request, error)

	// Create creates a new land request. Returns the created request object with generated sequence number.
	// The implementation must ensure that the sequence number is unique within the queue.
	Create(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error)

	// UpdateState updates the state of a land request if the current version matches the expected version. If versions do not match, returns ErrVersionMismatch.
	// The implementation should increment the version by 1 atomically with the state update.
	UpdateState(ctx context.Context, id string, version int32, newState entities.RequestState) error
}
