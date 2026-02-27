package speculation

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

//go:generate mockgen -source=strategy.go -destination=mock/strategy.go -package=mock

// Strategy generates a speculation tree for a batch given its dependencies.
// Implementations decide how to score and select paths (e.g., top-K, exhaustive).
type Strategy interface {
	// Generate produces a speculation tree for the given batch and its dependencies.
	// The batchID is the current batch being speculated on, and dependencyIDs are
	// the predecessor batch IDs in arrival order.
	Generate(ctx context.Context, batchID string, dependencyIDs []string) (entity.SpeculationTree, error)
}
