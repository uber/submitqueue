package github

import (
	"testing"

	"github.com/stretchr/testify/require"
	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
)

func TestValidateChangeConsistency(t *testing.T) {
	tests := []struct {
		name      string
		changeIDs []entitygithub.ChangeID
		wantErr   bool
	}{
		{
			name: "single PR",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 1},
			},
		},
		{
			name: "consistent stack",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 1},
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 2},
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 3},
			},
		},
		{
			name: "different repo",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 1},
				{Scheme: "github", Org: "uber", Repo: "other-repo", PRNumber: 2},
			},
			wantErr: true,
		},
		{
			name: "different org",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 1},
				{Scheme: "github", Org: "other-org", Repo: "submitqueue", PRNumber: 2},
			},
			wantErr: true,
		},
		{
			name: "different scheme",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 1},
				{Scheme: "other", Org: "uber", Repo: "submitqueue", PRNumber: 2},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateChangeConsistency(tt.changeIDs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidatePRStaleness(t *testing.T) {
	tests := []struct {
		name    string
		cid     entitygithub.ChangeID
		prData  pullRequestData
		wantErr bool
	}{
		{
			name:    "matching SHA",
			cid:     entitygithub.ChangeID{PRNumber: 1, HeadCommitSHA: "abc123"},
			prData:  pullRequestData{HeadRefOid: "abc123"},
			wantErr: false,
		},
		{
			name:    "mismatched SHA",
			cid:     entitygithub.ChangeID{PRNumber: 1, HeadCommitSHA: "oldsha"},
			prData:  pullRequestData{HeadRefOid: "newsha"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePRStaleness(tt.cid, &tt.prData)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
