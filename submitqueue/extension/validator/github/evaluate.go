// Copyright (c) 2026 Uber Technologies, Inc.
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
	"sort"
	"strings"

	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

// rejectionReason names why a pull request was rejected. The values are metric
// tag values, so they stay stable.
type rejectionReason string

const (
	reasonHost         rejectionReason = "host"
	reasonStale        rejectionReason = "stale"
	reasonNotApproved  rejectionReason = "not_approved"
	reasonCheckFailed  rejectionReason = "check_failed"
	reasonCheckPending rejectionReason = "check_pending"
	reasonBlocked      rejectionReason = "blocked"
)

// GitHub's reviewDecision and mergeStateStatus values the rules act on.
const (
	reviewDecisionChangesRequested = "CHANGES_REQUESTED"
	reviewDecisionReviewRequired   = "REVIEW_REQUIRED"
	mergeStateStatusBlocked        = "BLOCKED"
)

// rejection is why one pull request in a request cannot land.
type rejection struct {
	uri    string
	reason rejectionReason
	detail string
}

func (r rejection) Error() string {
	return fmt.Sprintf("%s: %s", r.uri, r.detail)
}

// Unwrap marks every rejection as validator.ErrRejected for the validate stage.
func (r rejection) Unwrap() error { return validator.ErrRejected }

// checkOutcome is ordered worst to best; requiredChecksNotPassed relies on it.
type checkOutcome int

const (
	checkFailed checkOutcome = iota
	checkPending
	checkPassed
)

// check is one status check reported on a pull request's head commit.
type check struct {
	name     string
	required bool
	outcome  checkOutcome
}

// pullRequestState is what GitHub reports about a pull request, reduced to
// what evaluatePullRequest reads.
type pullRequestState struct {
	headSHA          string
	reviewDecision   string
	mergeStateStatus string
	checks           []check
}

// evaluatePullRequest reports the first rule pr breaks. Whatever GitHub has not
// decided passes: an UNKNOWN merge state or an absent review rule rejects nothing.
func evaluatePullRequest(cid entitygithub.ChangeID, pr pullRequestState) (rejection, bool) {
	reject := func(reason rejectionReason, format string, args ...any) (rejection, bool) {
		return rejection{uri: cid.String(), reason: reason, detail: fmt.Sprintf(format, args...)}, true
	}

	if pr.headSHA != cid.HeadCommitSHA {
		return reject(reasonStale, "head moved from %s to %s", cid.HeadCommitSHA, pr.headSHA)
	}

	switch pr.reviewDecision {
	case reviewDecisionChangesRequested:
		return reject(reasonNotApproved, "changes requested")
	case reviewDecisionReviewRequired:
		return reject(reasonNotApproved, "approving review required")
	}

	failed, pending := requiredChecksNotPassed(pr.checks)
	if len(failed) > 0 {
		return reject(reasonCheckFailed, "required checks failed: %s", strings.Join(failed, ", "))
	}
	if len(pending) > 0 {
		return reject(reasonCheckPending, "required checks not finished: %s", strings.Join(pending, ", "))
	}

	// Last, because it explains nothing: it catches what the rules above
	// cannot see, such as a required check that has not reported yet.
	if pr.mergeStateStatus == mergeStateStatusBlocked {
		return reject(reasonBlocked, "GitHub reports merging as blocked")
	}

	return rejection{}, false
}

// requiredChecksNotPassed returns sorted names. A name reported more than once,
// such as by two workflows, takes its worst outcome: every report must pass.
func requiredChecksNotPassed(checks []check) (failed, pending []string) {
	worst := make(map[string]checkOutcome)
	for _, c := range checks {
		if !c.required {
			continue
		}
		if outcome, seen := worst[c.name]; !seen || c.outcome < outcome {
			worst[c.name] = c.outcome
		}
	}
	for name, outcome := range worst {
		switch outcome {
		case checkFailed:
			failed = append(failed, name)
		case checkPending:
			pending = append(pending, name)
		}
	}
	sort.Strings(failed)
	sort.Strings(pending)
	return failed, pending
}
