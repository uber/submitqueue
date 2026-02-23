package storage

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// ChangeProviderStore is an interface that defines methods for managing change provider information in the database.
type ChangeProviderStore interface {
	// Get retrieves information about a change by ID. Returns ErrNotFound if the change provider is
	// not found.
	Get(ctx context.Context, id string) (entity.ChangeProvider, error)

	// Create creates a new change provider.
	Create(ctx context.Context, request entity.Request) error

	// There is no update function since once created, data is only ever read from this
	// store.
}
