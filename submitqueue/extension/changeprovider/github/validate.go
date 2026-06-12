package github

import (
	"fmt"

	entitygithub "github.com/uber/submitqueue/entity/change/github"
)

// validateChangeConsistency validates that all changeIDs in the stack are consistent.
// Stacked changes must have the same change provider (scheme), org, and repo.
func validateChangeConsistency(
	changeIDs []entitygithub.ChangeID,
) error {
	expectedScheme := changeIDs[0].Scheme
	expectedOrg := changeIDs[0].Org
	expectedRepo := changeIDs[0].Repo

	for _, cid := range changeIDs {
		// Validate same change provider (scheme)
		if cid.Scheme != expectedScheme {
			return fmt.Errorf("stacked changes must use same change provider: expected %s, got %s for PR #%d",
				expectedScheme, cid.Scheme, cid.PRNumber)
		}

		// Validate same org and repo
		if cid.Org != expectedOrg || cid.Repo != expectedRepo {
			return fmt.Errorf("stacked changes must be from same org/repository: expected %s/%s, got %s/%s for PR #%d",
				expectedOrg, expectedRepo, cid.Org, cid.Repo, cid.PRNumber)
		}
	}

	return nil
}

// validatePRStaleness validates that the PR hasn't changed since submission.
// Compares the fetched head SHA with the expected SHA from the change URI.
func validatePRStaleness(
	cid entitygithub.ChangeID,
	prData *pullRequestData,
) error {
	if prData.HeadRefOid != cid.HeadCommitSHA {
		return fmt.Errorf("PR #%d head SHA changed: expected %s, got %s",
			cid.PRNumber, cid.HeadCommitSHA, prData.HeadRefOid)
	}
	return nil
}
