package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entitygithub "github.com/uber/submitqueue/entity/github"
)

func TestValidatePRs(t *testing.T) {
	tests := []struct {
		name       string
		changeIDs  []entitygithub.ChangeID
		prInfoMap  map[int]PRInfo
		wantOK     bool
		wantReason string
		wantErr    bool
	}{
		{
			name: "single PR mergeable",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "abc123"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "abc123", State: PRStateOpen},
			},
			wantOK: true,
		},
		{
			name: "stack of three PRs all mergeable",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 2, HeadCommitSHA: "sha2"},
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 3, HeadCommitSHA: "sha3"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateOpen},
				2: {Number: 2, Mergeable: PRMergeableStateMergeable, BaseRefName: "feature-1", HeadRefName: "feature-2", HeadRefOid: "sha2", State: PRStateOpen},
				3: {Number: 3, Mergeable: PRMergeableStateMergeable, BaseRefName: "feature-2", HeadRefName: "feature-3", HeadRefOid: "sha3", State: PRStateOpen},
			},
			wantOK: true,
		},
		{
			name: "PR closed",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateClosed},
			},
			wantOK:     false,
			wantReason: "PR #1 is CLOSED",
		},
		{
			name: "PR already merged",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateMerged},
			},
			wantOK:     false,
			wantReason: "PR #1 is MERGED",
		},
		{
			name: "PR has conflicts",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateConflicting, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateOpen},
			},
			wantOK:     false,
			wantReason: "PR #1 has merge conflicts",
		},
		{
			name: "unknown mergeability returns error",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateUnknown, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateOpen},
			},
			wantOK:  false,
			wantErr: true,
		},
		{
			name: "stale SHA",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "old_sha"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "new_sha", State: PRStateOpen},
			},
			wantOK:     false,
			wantReason: "PR #1 head SHA changed: expected old_sha, got new_sha",
		},
		{
			name: "PR not found in map",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 999, HeadCommitSHA: "sha1"},
			},
			prInfoMap: map[int]PRInfo{},
			wantOK:    false,
			wantErr:   true,
		},
		{
			name: "second PR in stack conflicting",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 2, HeadCommitSHA: "sha2"},
			},
			prInfoMap: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateOpen},
				2: {Number: 2, Mergeable: PRMergeableStateConflicting, BaseRefName: "feature-1", HeadRefName: "feature-2", HeadRefOid: "sha2", State: PRStateOpen},
			},
			wantOK:     false,
			wantReason: "PR #2 has merge conflicts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, reason, err := validatePRs(tt.changeIDs, tt.prInfoMap)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}
