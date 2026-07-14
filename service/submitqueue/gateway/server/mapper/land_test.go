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

package mapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestProtoToLandRequest(t *testing.T) {
	const uri = "github://github.example.com/uber/test-repo/pull/1/c3a4d5e6f7890123456789abcdef0123456789ab"

	tests := []struct {
		name        string
		req         *pb.LandRequest
		expected    entity.LandRequest
		expectedErr error
	}{
		{
			name: "maps all fields and leaves ID empty",
			req: &pb.LandRequest{
				Queue:    "test-queue",
				Change:   &changepb.Change{Uris: []string{uri}},
				Strategy: mergestrategypb.Strategy_SQUASH_REBASE,
			},
			// ID is not assigned by the mapper — the controller mints it.
			expected: entity.LandRequest{
				Queue:        "test-queue",
				Change:       change.Change{URIs: []string{uri}},
				LandStrategy: mergestrategy.MergeStrategySquashRebase,
			},
		},
		{
			name: "nil change yields empty URIs without erroring",
			req:  &pb.LandRequest{Queue: "test-queue", Change: nil},
			// The mapper does not validate; it leaves URIs empty for the controller to reject.
			expected: entity.LandRequest{
				Queue:        "test-queue",
				LandStrategy: mergestrategy.MergeStrategyRebase,
			},
		},
		{
			name: "unknown strategy errors",
			req: &pb.LandRequest{
				Queue:    "test-queue",
				Change:   &changepb.Change{Uris: []string{uri}},
				Strategy: mergestrategypb.Strategy(9999),
			},
			expectedErr: errUnknownStrategy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProtoToLandRequest(tt.req)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestResolveMergeStrategy(t *testing.T) {
	tests := []struct {
		name   string
		in     mergestrategypb.Strategy
		want   mergestrategy.MergeStrategy
		errMsg string
	}{
		{name: "default", in: mergestrategypb.Strategy_DEFAULT, want: mergestrategy.MergeStrategyRebase},
		{name: "rebase", in: mergestrategypb.Strategy_REBASE, want: mergestrategy.MergeStrategyRebase},
		{name: "squash_rebase", in: mergestrategypb.Strategy_SQUASH_REBASE, want: mergestrategy.MergeStrategySquashRebase},
		{name: "merge", in: mergestrategypb.Strategy_MERGE, want: mergestrategy.MergeStrategyMerge},
		{name: "unknown", in: mergestrategypb.Strategy(9999), errMsg: "unknown land strategy in proto message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveMergeStrategy(tt.in)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
