// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package github

import (
	"fmt"

	entitygithub "github.com/uber/submitqueue/entity/github"
)

// PRMergeableState represents the mergeability state of a pull request.
type PRMergeableState string

const (
	// PRMergeableStateMergeable indicates the PR can be merged cleanly.
	PRMergeableStateMergeable PRMergeableState = "MERGEABLE"
	// PRMergeableStateConflicting indicates the PR has merge conflicts.
	PRMergeableStateConflicting PRMergeableState = "CONFLICTING"
	// PRMergeableStateUnknown indicates GitHub hasn't computed mergeability yet.
	// GitHub computes mergeability asynchronously after pushes. The GraphQL API
	// returns UNKNOWN when the computation hasn't finished, even though the API
	// call itself is synchronous. Callers should retry after a short delay.
	PRMergeableStateUnknown PRMergeableState = "UNKNOWN"
)

// PRState represents the state of a pull request.
type PRState string

const (
	// PRStateOpen indicates the PR is open.
	PRStateOpen PRState = "OPEN"
	// PRStateClosed indicates the PR is closed.
	PRStateClosed PRState = "CLOSED"
	// PRStateMerged indicates the PR has been merged.
	PRStateMerged PRState = "MERGED"
)

// PRInfo holds the relevant pull request information fetched from GitHub.
type PRInfo struct {
	// Number is the pull request number.
	Number int
	// Mergeable is the mergeability state of the PR.
	Mergeable PRMergeableState
	// BaseRefName is the base branch the PR targets.
	BaseRefName string
	// HeadRefName is the head branch of the PR.
	HeadRefName string
	// HeadRefOid is the current head commit SHA of the PR.
	HeadRefOid string
	// State is the current state of the PR (OPEN, CLOSED, MERGED).
	State PRState
}

// validatePRs validates that all PRs are open, individually mergeable, and not stale.
// Returns (true, "", nil) if all PRs pass validation.
// Returns (false, reason, nil) if definitively not mergeable (conflicts, closed, stale SHA).
// Returns (false, "", error) if mergeability is UNKNOWN (retryable — GitHub hasn't computed yet).
func validatePRs(changeIDs []entitygithub.ChangeID, prInfoMap map[int]PRInfo) (bool, string, error) {
	for _, cid := range changeIDs {
		pr, ok := prInfoMap[cid.PRNumber]
		if !ok {
			return false, "", fmt.Errorf("PR #%d not found in API response", cid.PRNumber)
		}

		// Check PR is open
		if pr.State != PRStateOpen {
			return false, fmt.Sprintf("PR #%d is %s", cid.PRNumber, pr.State), nil
		}

		// Check mergeability
		switch pr.Mergeable {
		case PRMergeableStateConflicting:
			return false, fmt.Sprintf("PR #%d has merge conflicts", cid.PRNumber), nil
		case PRMergeableStateUnknown:
			return false, "", fmt.Errorf("mergeability unknown for PR #%d, retry later", cid.PRNumber)
		case PRMergeableStateMergeable:
			// OK, continue
		default:
			return false, "", fmt.Errorf("unexpected mergeable state %q for PR #%d", pr.Mergeable, cid.PRNumber)
		}

		// Check head commit SHA matches (staleness check)
		if pr.HeadRefOid != cid.HeadCommitSHA {
			return false, fmt.Sprintf("PR #%d head SHA changed: expected %s, got %s", cid.PRNumber, cid.HeadCommitSHA, pr.HeadRefOid), nil
		}
	}

	return true, "", nil
}
