package changeprovider

import "context"

// ChangeProvider integrates with external code review and version control systems
// to check for merge conflicts and perform merges.
type ChangeProvider interface {
	// HasMergeConflicts checks whether the head SHA has merge conflicts with the base SHA.
	// Returns true if conflicts exist.
	HasMergeConflicts(ctx context.Context, baseSHA string, headSHA string, PR string) (bool, error)

	// Merge merges the head SHA into the base SHA.
	Merge(ctx context.Context, baseSHA string, headSHA string) error
}
