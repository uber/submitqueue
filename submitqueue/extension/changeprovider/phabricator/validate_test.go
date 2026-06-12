package phabricator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entityphab "github.com/uber/submitqueue/submitqueue/entity/phabricator"
)

func TestValidateChangeConsistency(t *testing.T) {
	testCases := []struct {
		name      string
		changeIDs []entityphab.ChangeID
		wantErr   string
	}{
		{
			name: "single change",
			changeIDs: []entityphab.ChangeID{
				{Scheme: "phab", RevisionID: 100, DiffID: 1},
			},
		},
		{
			name: "consistent stack",
			changeIDs: []entityphab.ChangeID{
				{Scheme: "phab", RevisionID: 100, DiffID: 1},
				{Scheme: "phab", RevisionID: 101, DiffID: 2},
				{Scheme: "phab", RevisionID: 102, DiffID: 3},
			},
		},
		{
			name: "different scheme",
			changeIDs: []entityphab.ChangeID{
				{Scheme: "phab", RevisionID: 100, DiffID: 1},
				{Scheme: "other", RevisionID: 101, DiffID: 2},
			},
			wantErr: "same change provider",
		},
		{
			name: "duplicate revision",
			changeIDs: []entityphab.ChangeID{
				{Scheme: "phab", RevisionID: 100, DiffID: 1},
				{Scheme: "phab", RevisionID: 100, DiffID: 2},
			},
			wantErr: "duplicate revision D100",
		},
		{
			name: "duplicate diff",
			changeIDs: []entityphab.ChangeID{
				{Scheme: "phab", RevisionID: 100, DiffID: 1},
				{Scheme: "phab", RevisionID: 101, DiffID: 1},
			},
			wantErr: "duplicate diff 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateChangeConsistency(tc.changeIDs)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateDiffResponse(t *testing.T) {
	testCases := []struct {
		name    string
		diffID  int
		diff    *diffResult
		wantErr string
	}{
		{
			name:   "valid response",
			diffID: 100,
			diff: &diffResult{
				Changes: []fileChange{{CurrentPath: "main.go"}},
			},
		},
		{
			name:   "no file changes",
			diffID: 100,
			diff: &diffResult{
				Changes: []fileChange{},
			},
			wantErr: "diff 100 has no file changes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDiffResponse(tc.diffID, tc.diff)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
