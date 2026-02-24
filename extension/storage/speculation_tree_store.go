package storage

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// SpeculationTreeStore is an interface that defines methods for managing speculation trees in the database.
type SpeculationTreeStore interface {
	// Get retrieves the speculation tree by batch ID.
	// Returns ErrNotFound if the speculation tree is not found.
	Get(ctx context.Context, batchID string) (entity.SpeculationTree, error)

	// Create creates a new speculation tree.
	// Returns ErrAlreadyExists if the entry already exists.
	Create(ctx context.Context, speculationTree entity.SpeculationTree) error

	// UpdateSpeculations updates the speculations of a speculation tree.
	// Returns ErrNotFound if the speculation tree is not found.
	UpdateSpeculations(ctx context.Context, batchID string, speculations []map[string]string) error
}
