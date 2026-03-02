package github

import (
	"fmt"

	"github.com/uber/submitqueue/entity"
)

// validateSinglePR ensures entries contain exactly one PR.
// This implementation does not support batch landing because the GitHub merge API
// operates on individual PRs, making multi-PR landing non-idempotent on retry.
func validateSinglePR(entries []entity.LandEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries to land")
	}

	var totalURIs int
	for _, entry := range entries {
		totalURIs += len(entry.Change.URIs)
	}

	if totalURIs == 0 {
		return fmt.Errorf("no change URIs to land")
	}
	if totalURIs > 1 {
		return fmt.Errorf("this implementation supports landing exactly one PR per call, got %d", totalURIs)
	}

	if len(entries[0].Change.URIs) == 0 {
		return fmt.Errorf("no change URIs in first entry")
	}

	return nil
}
