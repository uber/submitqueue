package phab

import (
	"context"

	changeprovider "github.com/uber/submitqueue/extension/change_provider"
)

type phabChangeProvider struct{}

// NewChangeProvider creates a new Phabricator-backed ChangeProvider.
func NewChangeProvider() changeprovider.ChangeProvider {
	return &phabChangeProvider{}
}

// HasMergeConflicts checks whether the head SHA has merge conflicts with the base SHA.
func (p *phabChangeProvider) HasMergeConflicts(ctx context.Context, baseSHA string, headSHA string, PR string) (bool, error) {
	return false, nil
}

// Merge merges the head SHA into the base SHA.
func (p *phabChangeProvider) Merge(ctx context.Context, baseSHA string, headSHA string) error {
	return nil
}
