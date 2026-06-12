package phabricator

import (
	"fmt"

	entityphab "github.com/uber/submitqueue/submitqueue/entity/phabricator"
)

// validateChangeConsistency validates that all changeIDs in the stack are consistent.
// Stacked changes must have the same scheme, and each must have a unique revision and diff ID.
func validateChangeConsistency(changeIDs []entityphab.ChangeID) error {
	expectedScheme := changeIDs[0].Scheme

	seenRevisions := make(map[int]bool, len(changeIDs))
	seenDiffs := make(map[int]bool, len(changeIDs))

	for _, cid := range changeIDs {
		if cid.Scheme != expectedScheme {
			return fmt.Errorf("stacked changes must use same change provider: expected %s, got %s for %s",
				expectedScheme, cid.Scheme, cid.Revision())
		}

		if seenRevisions[cid.RevisionID] {
			return fmt.Errorf("duplicate revision %s in stacked changes", cid.Revision())
		}
		seenRevisions[cid.RevisionID] = true

		if seenDiffs[cid.DiffID] {
			return fmt.Errorf("duplicate diff %d in stacked changes", cid.DiffID)
		}
		seenDiffs[cid.DiffID] = true
	}

	return nil
}

// validateDiffResponse checks that the querydiffs result is well-formed: that
// it contains at least one file change and a parseable head SHA.
func validateDiffResponse(diffID int, diff *diffResult) error {
	if len(diff.Changes) == 0 {
		return fmt.Errorf("diff %d has no file changes", diffID)
	}
	if _, err := extractHeadSHA(diff); err != nil {
		return fmt.Errorf("diff %d: %w", diffID, err)
	}
	return nil
}
