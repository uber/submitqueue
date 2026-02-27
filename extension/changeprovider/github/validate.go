package github

import (
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	entitygithub "github.com/uber/submitqueue/entity/github"
)

// validateChangeConsistency validates that all changeIDs in the stack are consistent.
// Stacked changes must have the same change provider (scheme), org, and repo.
// Returns the org and repo if valid, or an error if any change is inconsistent.
func validateChangeConsistency(
	changeIDs []entitygithub.ChangeID,
	logger *zap.SugaredLogger,
	metrics tally.Scope,
) (string, string, error) {
	if len(changeIDs) == 0 {
		return "", "", nil
	}

	expectedScheme := changeIDs[0].Scheme
	expectedOrg := changeIDs[0].Org
	expectedRepo := changeIDs[0].Repo

	for _, cid := range changeIDs {
		// Validate same change provider (scheme)
		if cid.Scheme != expectedScheme {
			logger.Errorw("stacked changes must use same change provider",
				"expected_provider", expectedScheme,
				"got_provider", cid.Scheme,
				"pr", cid.PRNumber,
			)
			metrics.Tagged(map[string]string{"error_type": "mixed_provider_stack"}).Counter("get_errors").Inc(1)
			return "", "", fmt.Errorf("stacked changes must use same change provider: expected %s, got %s for PR #%d",
				expectedScheme, cid.Scheme, cid.PRNumber)
		}

		// Validate same org and repo
		if cid.Org != expectedOrg || cid.Repo != expectedRepo {
			logger.Errorw("stacked changes must be from same repository",
				"expected_org", expectedOrg,
				"expected_repo", expectedRepo,
				"got_org", cid.Org,
				"got_repo", cid.Repo,
				"pr", cid.PRNumber,
			)
			metrics.Tagged(map[string]string{"error_type": "cross_repo_stack"}).Counter("get_errors").Inc(1)
			return "", "", fmt.Errorf("stacked changes must be from same repository: expected %s/%s, got %s/%s for PR #%d",
				expectedOrg, expectedRepo, cid.Org, cid.Repo, cid.PRNumber)
		}
	}

	return expectedOrg, expectedRepo, nil
}

// validatePRStaleness validates that the PR hasn't changed since submission.
// Compares the fetched head SHA with the expected SHA from the change URI.
func validatePRStaleness(
	cid entitygithub.ChangeID,
	prData *pullRequestData,
	logger *zap.SugaredLogger,
	metrics tally.Scope,
) error {
	if prData.HeadRefOid != cid.HeadCommitSHA {
		logger.Errorw("PR head SHA changed since submission",
			"org", cid.Org,
			"repo", cid.Repo,
			"pr", cid.PRNumber,
			"expected_sha", cid.HeadCommitSHA,
			"current_sha", prData.HeadRefOid,
		)
		metrics.Tagged(map[string]string{
			"org":        cid.Org,
			"repo":       cid.Repo,
			"error_type": "stale_pr",
		}).Counter("get_errors").Inc(1)
		return fmt.Errorf("PR #%d head SHA changed: expected %s, got %s",
			cid.PRNumber, cid.HeadCommitSHA, prData.HeadRefOid)
	}
	return nil
}
