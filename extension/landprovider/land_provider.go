package landprovider

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// LandEntry pairs a land strategy with the change to land.
// Each entry represents one request's contribution to a batch land operation.
type LandEntry struct {
	// Strategy is the source control integration method for this change.
	Strategy entity.RequestLandStrategy
	// Change is the code change to land.
	Change entity.Change
}

// LandProvider lands (merges) code changes into the target branch of a source
// control repository. Each implementation is configured for a specific provider
// (GitHub, GitLab, Phabricator).
type LandProvider interface {
	// Land merges the provided changes into the target branch of the given queue.
	// The queue identifies the repository and target branch.
	// Each entry contains a change and the strategy to use for landing it.
	Land(ctx context.Context, queue string, entries []LandEntry) (Result, error)
}

// Result holds the outcome of a land operation.
type Result struct {
	// SHA is the commit SHA of the landed change on the target branch.
	SHA string
}
