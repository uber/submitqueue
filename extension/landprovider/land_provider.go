package landprovider

//go:generate mockgen -source=land_provider.go -destination=mock/land_provider_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// LandProvider lands (merges) code changes into the target branch of a source
// control repository. Each implementation is configured for a specific provider
// (GitHub, GitLab, Phabricator).
type LandProvider interface {
	// Land merges the provided changes into the target branch of the given queue.
	// The queue identifies the repository and target branch.
	// Each entry contains a change and the strategy to use for landing it.
	// Returns ErrLandRejected if the land was rejected due to the changes themselves.
	// Returns ErrAlreadyLanded if the changes have already been landed.
	Land(ctx context.Context, queue string, entries []entity.LandEntry) error
}
