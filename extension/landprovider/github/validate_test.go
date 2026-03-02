package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/landprovider"
)

func TestValidateSinglePR(t *testing.T) {
	validEntry := landprovider.LandEntry{
		Strategy: entity.RequestLandStrategyRebase,
		Change:   entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
	}

	tests := []struct {
		name    string
		entries []landprovider.LandEntry
		wantErr bool
	}{
		{
			name:    "single entry single URI",
			entries: []landprovider.LandEntry{validEntry},
		},
		{
			// len(entries) == 0
			name:    "no entries",
			entries: []landprovider.LandEntry{},
			wantErr: true,
		},
		{
			// totalURIs == 0
			name:    "entry with no URIs",
			entries: []landprovider.LandEntry{{Strategy: entity.RequestLandStrategyRebase, Change: entity.Change{URIs: []string{}}}},
			wantErr: true,
		},
		{
			// totalURIs > 1 via multiple entries
			name:    "multiple entries",
			entries: []landprovider.LandEntry{validEntry, validEntry},
			wantErr: true,
		},
		{
			// totalURIs > 1 via multiple URIs in one entry
			name: "multiple URIs in single entry",
			entries: []landprovider.LandEntry{{
				Strategy: entity.RequestLandStrategyRebase,
				Change:   entity.Change{URIs: []string{"github://uber/repo/1/sha1", "github://uber/repo/2/sha2"}},
			}},
			wantErr: true,
		},
		{
			// len(entries[0].Change.URIs) == 0 with URI in second entry
			name: "first entry empty second has URI",
			entries: []landprovider.LandEntry{
				{Strategy: entity.RequestLandStrategyRebase, Change: entity.Change{URIs: []string{}}},
				validEntry,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSinglePR(tt.entries)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
