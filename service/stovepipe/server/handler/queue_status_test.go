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

package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/controller"
	"github.com/uber/submitqueue/stovepipe/core/queuepolicy"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeQueueStatusController struct {
	get func(context.Context, entity.GetQueueStatusRequest) (entity.GetQueueStatusResult, error)
}

func (f fakeQueueStatusController) GetQueueStatus(ctx context.Context, req entity.GetQueueStatusRequest) (entity.GetQueueStatusResult, error) {
	return f.get(ctx, req)
}

func TestGetQueueStatus(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code codes.Code
	}{
		{nil, codes.OK}, {controller.ErrInvalidRequest, codes.InvalidArgument},
		{controller.ErrQueuePolicyNotFound, codes.NotFound}, {controller.ErrCommitPolicyUnresolved, codes.FailedPrecondition},
		{queuepolicy.ErrInconsistentHistory, codes.Internal}, {context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded},
	} {
		wantErr := tt.err
		if wantErr == controller.ErrQueuePolicyNotFound || wantErr == controller.ErrCommitPolicyUnresolved {
			wantErr = errs.NewUserError(wantErr)
		}
		fake := fakeQueueStatusController{get: func(ctx context.Context, req entity.GetQueueStatusRequest) (entity.GetQueueStatusResult, error) {
			require.Equal(t, entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"}, req)
			return entity.GetQueueStatusResult{Queue: "repo/main", CurrentPolicy: entity.QueuePolicy{Revision: 2, State: entity.QueuePolicyStateEnabled}, HasPolicyForCommit: true, PolicyForCommit: entity.QueuePolicy{Revision: 1, State: entity.QueuePolicyStateDisabled}}, wantErr
		}}
		server := NewStovepipeServer(nil, nil, nil, nil, nil, fake)
		response, err := server.GetQueueStatus(context.Background(), &pb.GetQueueStatusRequest{Queue: "repo/main", ChangeUri: "C"})
		if wantErr != nil {
			require.Equal(t, tt.code, status.Code(err))
			require.Nil(t, response)
		} else {
			require.NoError(t, err)
			require.Equal(t, int64(2), response.CurrentPolicy.Revision)
			require.Equal(t, pb.QueuePolicyState_QUEUE_POLICY_STATE_DISABLED, response.PolicyForCommit.State)
		}
	}
}
