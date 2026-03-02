package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity"
)

func TestMapStrategyToMergeMethod(t *testing.T) {
	tests := []struct {
		name     string
		strategy entity.RequestLandStrategy
		want     mergeMethod
		wantErr  bool
	}{
		{
			name:     "rebase strategy",
			strategy: entity.RequestLandStrategyRebase,
			want:     mergeMethodRebase,
		},
		{
			name:     "squash rebase strategy",
			strategy: entity.RequestLandStrategySquashRebase,
			want:     mergeMethodSquash,
		},
		{
			name:     "merge strategy",
			strategy: entity.RequestLandStrategyMerge,
			want:     mergeMethodMerge,
		},
		{
			name:     "unknown strategy",
			strategy: entity.RequestLandStrategyUnknown,
			wantErr:  true,
		},
		{
			name:     "invalid strategy",
			strategy: entity.RequestLandStrategy("invalid"),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mapStrategyToMergeMethod(tt.strategy)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
