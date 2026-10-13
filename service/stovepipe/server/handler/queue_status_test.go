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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueueStatusContractIsUnimplemented(t *testing.T) {
	server := &StovepipeServer{}
	response, err := server.GetQueueStatus(context.Background(), &pb.GetQueueStatusRequest{Queue: "repo/main"})
	require.Nil(t, response)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
