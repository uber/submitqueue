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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
)

var (
	headSHA  = strings.Repeat("a", 40)
	movedSHA = strings.Repeat("b", 40)
)

func TestEvaluatePullRequest(t *testing.T) {
	cid := entitygithub.ChangeID{Scheme: "github", Host: "github.com", Org: "acme", Repo: "widgets", PRNumber: 7, HeadCommitSHA: headSHA}

	tests := []struct {
		name       string
		pr         pullRequestState
		wantReason rejectionReason
		wantDetail string
	}{
		{
			name: "clean pull request passes",
			pr:   pullRequestState{headSHA: headSHA, reviewDecision: "APPROVED", mergeStateStatus: "CLEAN"},
		},
		{
			name: "no review rule and no checks passes",
			pr:   pullRequestState{headSHA: headSHA, mergeStateStatus: "CLEAN"},
		},
		{
			name: "merge state still being computed passes",
			pr:   pullRequestState{headSHA: headSHA, mergeStateStatus: "UNKNOWN"},
		},
		{
			name: "failing check that is not required passes",
			pr: pullRequestState{headSHA: headSHA, mergeStateStatus: "UNSTABLE", checks: []check{
				{name: "lint", outcome: checkFailed},
			}},
		},
		{
			name: "required check that passed",
			pr: pullRequestState{headSHA: headSHA, mergeStateStatus: "CLEAN", checks: []check{
				{name: "ci", required: true, outcome: checkPassed},
			}},
		},
		{
			name: "merge states other stages own pass",
			pr:   pullRequestState{headSHA: headSHA, mergeStateStatus: "DIRTY"},
		},
		{
			name:       "moved head is stale",
			pr:         pullRequestState{headSHA: movedSHA, reviewDecision: "APPROVED", mergeStateStatus: "CLEAN"},
			wantReason: reasonStale,
			wantDetail: "head moved from " + headSHA + " to " + movedSHA,
		},
		{
			name:       "changes requested",
			pr:         pullRequestState{headSHA: headSHA, reviewDecision: "CHANGES_REQUESTED", mergeStateStatus: "BLOCKED"},
			wantReason: reasonNotApproved,
		},
		{
			name:       "required review missing",
			pr:         pullRequestState{headSHA: headSHA, reviewDecision: "REVIEW_REQUIRED", mergeStateStatus: "BLOCKED"},
			wantReason: reasonNotApproved,
		},
		{
			name: "required check failed",
			pr: pullRequestState{headSHA: headSHA, reviewDecision: "APPROVED", mergeStateStatus: "BLOCKED", checks: []check{
				{name: "ci", required: true, outcome: checkFailed},
				{name: "build", required: true, outcome: checkPassed},
			}},
			wantReason: reasonCheckFailed,
			wantDetail: "required checks failed: ci",
		},
		{
			name: "failed check outranks a pending one",
			pr: pullRequestState{headSHA: headSHA, mergeStateStatus: "BLOCKED", checks: []check{
				{name: "slow", required: true, outcome: checkPending},
				{name: "ci", required: true, outcome: checkFailed},
			}},
			wantReason: reasonCheckFailed,
		},
		{
			name: "required check still running",
			pr: pullRequestState{headSHA: headSHA, mergeStateStatus: "UNKNOWN", checks: []check{
				{name: "ci", required: true, outcome: checkPending},
			}},
			wantReason: reasonCheckPending,
			wantDetail: "required checks not finished: ci",
		},
		{
			name:       "blocked for a reason no rule sees",
			pr:         pullRequestState{headSHA: headSHA, reviewDecision: "APPROVED", mergeStateStatus: "BLOCKED"},
			wantReason: reasonBlocked,
		},
		{
			name: "stale outranks every other rule",
			pr: pullRequestState{headSHA: movedSHA, reviewDecision: "CHANGES_REQUESTED", mergeStateStatus: "BLOCKED", checks: []check{
				{name: "ci", required: true, outcome: checkFailed},
			}},
			wantReason: reasonStale,
		},
		{
			name: "review outranks checks",
			pr: pullRequestState{headSHA: headSHA, reviewDecision: "REVIEW_REQUIRED", mergeStateStatus: "BLOCKED", checks: []check{
				{name: "ci", required: true, outcome: checkFailed},
			}},
			wantReason: reasonNotApproved,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rej, rejected := evaluatePullRequest(cid, tt.pr)
			if tt.wantReason == "" {
				assert.False(t, rejected, "unexpected rejection: %v", rej)
				return
			}
			require.True(t, rejected)
			assert.Equal(t, tt.wantReason, rej.reason)
			assert.Equal(t, cid.String(), rej.uri)
			if tt.wantDetail != "" {
				assert.Equal(t, tt.wantDetail, rej.detail)
			}
		})
	}
}

func TestRequiredChecksNotPassed(t *testing.T) {
	tests := []struct {
		name        string
		checks      []check
		wantFailed  []string
		wantPending []string
	}{
		{
			name: "no checks",
		},
		{
			name: "checks that are not required are ignored",
			checks: []check{
				{name: "lint", outcome: checkFailed},
				{name: "docs", outcome: checkPending},
			},
		},
		{
			name: "a name reported twice takes its worst outcome",
			checks: []check{
				{name: "ci", required: true, outcome: checkPassed},
				{name: "ci", required: true, outcome: checkFailed},
				{name: "e2e", required: true, outcome: checkPassed},
				{name: "e2e", required: true, outcome: checkPending},
				{name: "lint", required: true, outcome: checkPending},
				{name: "lint", required: true, outcome: checkFailed},
			},
			wantFailed:  []string{"ci", "lint"},
			wantPending: []string{"e2e"},
		},
		{
			name: "a name whose every report passed passes",
			checks: []check{
				{name: "ci", required: true, outcome: checkPassed},
				{name: "ci", required: true, outcome: checkPassed},
			},
		},
		{
			name: "names are sorted",
			checks: []check{
				{name: "unit", required: true, outcome: checkFailed},
				{name: "build", required: true, outcome: checkFailed},
				{name: "slow", required: true, outcome: checkPending},
				{name: "e2e", required: true, outcome: checkPending},
			},
			wantFailed:  []string{"build", "unit"},
			wantPending: []string{"e2e", "slow"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failed, pending := requiredChecksNotPassed(tt.checks)
			assert.Equal(t, tt.wantFailed, failed)
			assert.Equal(t, tt.wantPending, pending)
		})
	}
}

func TestToCheck(t *testing.T) {
	tests := []struct {
		name string
		node checkContextNode
		want check
	}{
		{
			name: "successful check run",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "COMPLETED", Conclusion: "SUCCESS", IsRequired: true},
			want: check{name: "ci", required: true, outcome: checkPassed},
		},
		{
			name: "neutral check run passes",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "COMPLETED", Conclusion: "NEUTRAL"},
			want: check{name: "ci", outcome: checkPassed},
		},
		{
			name: "skipped check run passes",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "COMPLETED", Conclusion: "SKIPPED"},
			want: check{name: "ci", outcome: checkPassed},
		},
		{
			name: "check run in progress is pending",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "IN_PROGRESS"},
			want: check{name: "ci", outcome: checkPending},
		},
		{
			name: "queued check run is pending",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "QUEUED"},
			want: check{name: "ci", outcome: checkPending},
		},
		{
			name: "failed check run",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "COMPLETED", Conclusion: "FAILURE"},
			want: check{name: "ci", outcome: checkFailed},
		},
		{
			name: "cancelled check run fails",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "COMPLETED", Conclusion: "CANCELLED"},
			want: check{name: "ci", outcome: checkFailed},
		},
		{
			name: "check run needing action fails",
			node: checkContextNode{Typename: "CheckRun", Name: "ci", Status: "COMPLETED", Conclusion: "ACTION_REQUIRED"},
			want: check{name: "ci", outcome: checkFailed},
		},
		{
			name: "successful commit status",
			node: checkContextNode{Typename: "StatusContext", Context: "ci/legacy", State: "SUCCESS", IsRequired: true},
			want: check{name: "ci/legacy", required: true, outcome: checkPassed},
		},
		{
			name: "pending commit status",
			node: checkContextNode{Typename: "StatusContext", Context: "ci/legacy", State: "PENDING"},
			want: check{name: "ci/legacy", outcome: checkPending},
		},
		{
			name: "expected commit status is pending",
			node: checkContextNode{Typename: "StatusContext", Context: "ci/legacy", State: "EXPECTED"},
			want: check{name: "ci/legacy", outcome: checkPending},
		},
		{
			name: "errored commit status fails",
			node: checkContextNode{Typename: "StatusContext", Context: "ci/legacy", State: "ERROR"},
			want: check{name: "ci/legacy", outcome: checkFailed},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, toCheck(tt.node))
		})
	}
}
