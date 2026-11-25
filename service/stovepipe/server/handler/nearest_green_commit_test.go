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
	"github.com/uber/submitqueue/stovepipe/controller"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nearestGreenLookupFunc func(context.Context, entity.NearestGreenCommitRequest) (entity.NearestGreenCommitResult, error)

func (f nearestGreenLookupFunc) NearestGreenCommitThatIncludes(ctx context.Context, req entity.NearestGreenCommitRequest) (entity.NearestGreenCommitResult, error) {
	return f(ctx, req)
}

func TestNearestGreenCommitThatIncludes(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result entity.NearestGreenCommitResult
		err    error
		code   codes.Code
	}{
		{name: "match", result: entity.NearestGreenCommitResult{CommitURI: "B", RequestID: "42"}},
		{name: "no match"},
		{name: "invalid selectors", err: controller.ErrInvalidRequest, code: codes.InvalidArgument},
		{name: "off trunk", err: sourcecontrol.ErrNotFound, code: codes.NotFound},
		{name: "lineage rebuilding", err: controller.ErrGreenProjectionNotReady, code: codes.Unavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := &StovepipeServer{nearestGreenController: nearestGreenLookupFunc(func(_ context.Context, req entity.NearestGreenCommitRequest) (entity.NearestGreenCommitResult, error) {
				require.Equal(t, entity.NearestGreenCommitRequest{Queue: "queue", DesiredCommitURI: "A", Project: "payments"}, req)
				return tt.result, tt.err
			})}
			got, err := srv.NearestGreenCommitThatIncludes(context.Background(), &pb.NearestGreenCommitThatIncludesRequest{Queue: "queue", DesiredCommitUri: "A", Project: "payments"})
			require.Equal(t, tt.code, status.Code(err))
			if tt.err == nil {
				require.Equal(t, tt.result.CommitURI, got.GetClosestContainingCommitUri())
				require.Equal(t, tt.result.RequestID, got.GetRequestId())
			}
		})
	}
}
