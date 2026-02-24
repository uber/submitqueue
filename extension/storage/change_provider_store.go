package storage

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// ChangeProviderStore is an interface that defines methods for managing change provider information in the database.
type ChangeProviderStore interface {
	// Get retrieves information about a change by ID.
	// Returns ErrNotFound if the change provider is not found.
	//
	// Note: The order of ChangeProvider entities here is not guaranteed to
	// be the same as the request to which it belongs. The caller is repsonsible
	// for inspecting and mapping the result of this function to the
	// order of changes within the original request.
	//
	Get(ctx context.Context, requestID string) ([]entity.ChangeProvider, error)

	// Create creates a new change provider.
	Create(ctx context.Context, changeProvider entity.ChangeProvider) error

	// There is no update function since once created, data is only ever read from this
	// store.
}
