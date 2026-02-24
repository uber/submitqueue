package github

import (
	"context"
	"fmt"
	"net/http"

	changeprovider "github.com/uber/submitqueue/extension/change_provider"
)

// Params holds dependencies for the GitHub change provider.
type Params struct {
	// HTTPClient is a pre-configured HTTP client with authentication.
	// The caller is responsible for setting up GitHub App or token auth.
	HTTPClient *http.Client

	// Owner is the repository owner (user or organization).
	Owner string

	// Repo is the repository name.
	Repo string

	// BaseURL is the GitHub API base URL. Defaults to https://api.github.com.
	BaseURL string
}

type githubChangeProvider struct {
	client *githubClient
}

// NewChangeProvider creates a new GitHub-backed ChangeProvider.
func NewChangeProvider(params Params) changeprovider.ChangeProvider {
	return &githubChangeProvider{
		client: newClient(params),
	}
}

// HasMergeConflicts checks whether the head SHA has merge conflicts with the base SHA.
// Returns true if there are conflicts, false otherwise.
// The MergeableState from GitHub (e.g., "dirty", "blocked", "behind") is available
// in the error message when relevant.
func (p *githubChangeProvider) HasMergeConflicts(ctx context.Context, baseSHA string, headSHA string, PR string) (bool, error) {
	hasConflicts, mergeableState, err := p.client.hasMergeConflicts(ctx, baseSHA, headSHA, PR)
	if err != nil {
		// Include mergeableState in error context when available
		if mergeableState != "" {
			return false, fmt.Errorf("%w (state: %s)", err, mergeableState)
		}
		return false, err
	}
	return hasConflicts, nil
}

// Merge merges the head SHA into the base SHA.
func (p *githubChangeProvider) Merge(ctx context.Context, baseSHA string, headSHA string) error {
	return p.client.merge(ctx, baseSHA, headSHA)
}
