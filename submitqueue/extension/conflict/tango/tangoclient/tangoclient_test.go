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

package tangoclient

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tango/tangopb"
	"go.uber.org/yarpc"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
)

const testBaseRevision = "9f2c1d0e3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d"

var errTestFailure = errors.New("dependency down")

type fakeTango struct {
	tangopb.TangoYARPCClient
	stream  *fakeStream
	openErr error
	request *tangopb.GetChangedTargetsRequest
}

func (f *fakeTango) GetChangedTargets(_ context.Context, request *tangopb.GetChangedTargetsRequest, _ ...yarpc.CallOption) (tangopb.TangoServiceGetChangedTargetsYARPCClient, error) {
	f.request = request
	if f.openErr != nil {
		return nil, f.openErr
	}
	return f.stream, nil
}

type fakeStream struct {
	responses []*tangopb.GetChangedTargetsResponse
	endErr    error
	closed    bool
}

func (s *fakeStream) Context() context.Context { return context.Background() }

func (s *fakeStream) Recv(...yarpc.StreamOption) (*tangopb.GetChangedTargetsResponse, error) {
	if len(s.responses) == 0 {
		if s.endErr != nil {
			return nil, s.endErr
		}
		return nil, io.EOF
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response, nil
}

func (s *fakeStream) CloseSend(...yarpc.StreamOption) error {
	s.closed = true
	return nil
}

func changedTargets(targets ...*tangopb.ChangedTarget) *tangopb.GetChangedTargetsResponse {
	return &tangopb.GetChangedTargetsResponse{Item: &tangopb.GetChangedTargetsResponse_ChangedTargets{
		ChangedTargets: &tangopb.ChangedTargets{ChangedTargets: targets},
	}}
}

func metadata(labels map[int32]string) *tangopb.GetChangedTargetsResponse {
	return &tangopb.GetChangedTargetsResponse{Item: &tangopb.GetChangedTargetsResponse_Metadata{
		Metadata: &tangopb.Metadata{TargetIdMapping: labels},
	}}
}

func target(changeType tangopb.ChangeType, oldID, newID, distance int32) *tangopb.ChangedTarget {
	changed := &tangopb.ChangedTarget{ChangeType: changeType, Distance: distance}
	if oldID != 0 {
		changed.OldTarget = &tangopb.OptimizedTarget{Id: oldID}
	}
	if newID != 0 {
		changed.NewTarget = &tangopb.OptimizedTarget{Id: newID}
	}
	return changed
}

func TestClient_ChangedTargets(t *testing.T) {
	tests := []struct {
		name      string
		stream    *fakeStream
		openErr   error
		want      []tango.LabelDistance
		wantError bool
	}{
		{
			name: "labels targets across stream chunks",
			stream: &fakeStream{responses: []*tangopb.GetChangedTargetsResponse{
				changedTargets(target(tangopb.CHANGE_TYPE_CHANGED, 1, 1, 0), target(tangopb.CHANGE_TYPE_NEW, 0, 2, 0)),
				changedTargets(target(tangopb.CHANGE_TYPE_CHANGED, 3, 3, 2)),
				metadata(map[int32]string{1: "//app:lib", 2: "//app:new", 3: "//app:bin"}),
			}},
			want: []tango.LabelDistance{
				{Label: "//app:lib", Distance: 0},
				{Label: "//app:new", Distance: 0},
				{Label: "//app:bin", Distance: 2},
			},
		},
		{
			name: "deleted target is labeled by its old ID",
			stream: &fakeStream{responses: []*tangopb.GetChangedTargetsResponse{
				changedTargets(target(tangopb.CHANGE_TYPE_DELETED, 7, 0, 0)),
				metadata(map[int32]string{7: "//app:gone"}),
			}},
			want: []tango.LabelDistance{{Label: "//app:gone", Distance: 0}},
		},
		{
			name:   "empty result is valid",
			stream: &fakeStream{},
			want:   []tango.LabelDistance{},
		},
		{
			name: "metadata alone is an empty result",
			stream: &fakeStream{responses: []*tangopb.GetChangedTargetsResponse{
				metadata(map[int32]string{}),
			}},
			want: []tango.LabelDistance{},
		},
		{
			name:      "failure to open the stream fails",
			openErr:   errTestFailure,
			wantError: true,
		},
		{
			name: "stream error fails",
			stream: &fakeStream{
				responses: []*tangopb.GetChangedTargetsResponse{changedTargets(target(tangopb.CHANGE_TYPE_NEW, 0, 1, 0))},
				endErr:    errTestFailure,
			},
			wantError: true,
		},
		{
			name: "targets without metadata fail",
			stream: &fakeStream{responses: []*tangopb.GetChangedTargetsResponse{
				changedTargets(target(tangopb.CHANGE_TYPE_NEW, 0, 1, 0)),
			}},
			wantError: true,
		},
		{
			name: "target missing from the mapping fails",
			stream: &fakeStream{responses: []*tangopb.GetChangedTargetsResponse{
				changedTargets(target(tangopb.CHANGE_TYPE_NEW, 0, 1, 0)),
				metadata(map[int32]string{2: "//app:other"}),
			}},
			wantError: true,
		},
		{
			name: "changed target without a target fails",
			stream: &fakeStream{responses: []*tangopb.GetChangedTargetsResponse{
				changedTargets(target(tangopb.CHANGE_TYPE_NEW, 0, 0, 0)),
				metadata(map[int32]string{}),
			}},
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeTango{stream: tt.stream, openErr: tt.openErr}
			c := New(fake, Config{Remote: "org/repo", Strategy: tangopb.COMPUTATION_STRATEGY_NATIVE})

			got, err := c.ChangedTargets(context.Background(), tango.ChangedTargetsRequest{
				BaseRevision: testBaseRevision,
				ChangeURIs:   []string{"u1", "u2"},
			})

			if tt.stream != nil {
				assert.True(t, tt.stream.closed)
			}
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestClient_ChangedTargets_Request(t *testing.T) {
	fake := &fakeTango{stream: &fakeStream{}}
	c := New(fake, Config{Remote: "org/repo", Strategy: tangopb.COMPUTATION_STRATEGY_NATIVE})

	_, err := c.ChangedTargets(context.Background(), tango.ChangedTargetsRequest{
		BaseRevision: testBaseRevision,
		ChangeURIs:   []string{"u1", "u2"},
	})

	require.NoError(t, err)
	assert.Equal(t, &tangopb.GetChangedTargetsRequest{
		FirstRevision: &tangopb.BuildDescription{
			Remote:   "org/repo",
			BaseSha:  testBaseRevision,
			Requests: []*tangopb.Request{},
			Strategy: tangopb.COMPUTATION_STRATEGY_NATIVE,
		},
		SecondRevision: &tangopb.BuildDescription{
			Remote:   "org/repo",
			BaseSha:  testBaseRevision,
			Requests: []*tangopb.Request{{Url: "u1"}, {Url: "u2"}},
			Strategy: tangopb.COMPUTATION_STRATEGY_NATIVE,
		},
		OutputConfig: &tangopb.OutputConfig{MaxDistance: -1},
	}, fake.request)
}
