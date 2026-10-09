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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/controller"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type fakeListController struct {
	list func(context.Context, entity.ListRequest) (entity.ListResult, error)
}

var _ controller.ListController = (*fakeListController)(nil)

func (f *fakeListController) List(ctx context.Context, req entity.ListRequest) (entity.ListResult, error) {
	return f.list(ctx, req)
}

func TestList(t *testing.T) {
	page := entity.ListResult{
		Requests:      []entity.RequestSummary{{RequestID: "7", Queue: "queue", State: entity.RequestStateAccepted}},
		NextPageToken: "next-token",
	}
	consistencyErr := &controller.ListConsistencyError{
		Queue: "queue", RequestID: "7", Reason: "acceptance mapping has no summary", Err: storage.ErrNotFound,
	}
	for _, tt := range []struct {
		name   string
		result entity.ListResult
		err    error
	}{
		{name: "page with continuation", result: page},
		{name: "empty page"},
		{name: "validation error", result: page, err: controller.ErrInvalidRequest},
		{name: "consistency error", result: page, err: consistencyErr},
		{name: "storage failure", err: errors.New("storage failed")},
		{name: "cancellation", err: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fake := &fakeListController{list: func(gotCtx context.Context, req entity.ListRequest) (entity.ListResult, error) {
				assert.Same(t, ctx, gotCtx)
				assert.Equal(t, entity.ListRequest{
					Queue: "queue", PageSize: 25, PageToken: "token",
					HasAcceptedAtOrAfterMs: true, AcceptedBeforeMs: 1000, HasAcceptedBeforeMs: true,
				}, req)
				return tt.result, tt.err
			}}
			srv := NewStovepipeServer(nil, nil, nil, nil, fake, nil)
			response, err := srv.List(ctx, &pb.ListRequest{
				Queue: "queue", PageSize: 25, PageToken: "token",
				AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{},
				AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 1000},
			})
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Same(t, tt.err, err)
				assert.Nil(t, response)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Len(t, response.GetRequests(), len(tt.result.Requests))
			if len(tt.result.Requests) > 0 {
				assert.Equal(t, "7", response.GetRequests()[0].GetRequestId())
				assert.Equal(t, "queue", response.GetRequests()[0].GetQueue())
				assert.Equal(t, "accepted", response.GetRequests()[0].GetRequestState())
			}
			assert.Equal(t, tt.result.NextPageToken, response.GetNextPageToken())
		})
	}
}
