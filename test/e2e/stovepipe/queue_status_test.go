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

package e2e_test

import (
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/core/queuepolicy"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const queuePolicyTestQueue = "e2e-stovepipe/queue-policy"

type policyStorageFactory struct {
	backend interface {
		For(string) (storage.Storage, error)
	}
}

func (f policyStorageFactory) For(cfg storage.Config) (storage.Storage, error) {
	return f.backend.For(cfg.QueueName)
}

type policySourceFactory struct{}

func (policySourceFactory) For(cfg sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return scfake.New(cfg, []string{"git://" + cfg.QueueName + "/HEAD"}), nil
}

func (s *StovepipeE2ESuite) TestQueueStatus_AppliedPolicyWithoutValidation() {
	t := s.T()
	queue := queuePolicyTestQueue
	uri := "git://" + queue + "/HEAD"
	initial, err := s.client.GetQueueStatus(s.ctx, &pb.GetQueueStatusRequest{Queue: queue})
	require.NoError(t, err)
	require.Nil(t, initial.PolicyForCommit)
	require.Equal(t, pb.QueuePolicyState_QUEUE_POLICY_STATE_DISABLED, initial.CurrentPolicy.State)
	require.Positive(t, initial.CurrentPolicy.ChangedAtMs)
	require.Equal(t, pb.QueueExecutionState_QUEUE_EXECUTION_STATE_RUNNING, initial.Execution.State)
	writer := queuepolicy.NewWriter(policyStorageFactory{backend: s.appStorage}, policySourceFactory{})
	for _, tt := range []struct {
		operation string
		expected  int64
		state     entity.QueuePolicyState
		wire      pb.QueuePolicyState
	}{
		{"enable", 1, entity.QueuePolicyStateEnabled, pb.QueuePolicyState_QUEUE_POLICY_STATE_ENABLED},
		{"disable", 2, entity.QueuePolicyStateDisabled, pb.QueuePolicyState_QUEUE_POLICY_STATE_DISABLED},
		{"reenable", 3, entity.QueuePolicyStateEnabled, pb.QueuePolicyState_QUEUE_POLICY_STATE_ENABLED},
	} {
		applied, err := writer.Apply(s.ctx, entity.ApplyQueuePolicyRequest{Queue: queue, OperationID: tt.operation, ExpectedRevision: tt.expected, State: tt.state, EffectiveFromCommitURI: uri})
		require.NoError(t, err)
		result, err := s.client.GetQueueStatus(s.ctx, &pb.GetQueueStatusRequest{Queue: queue, ChangeUri: uri})
		require.NoError(t, err)
		require.Equal(t, applied.Revision, result.CurrentPolicy.Revision)
		require.Equal(t, applied.Revision, result.PolicyForCommit.Revision)
		require.Equal(t, tt.wire, result.PolicyForCommit.State)
		require.Equal(t, uri, result.PolicyForCommit.EffectiveFromCommitUri)
	}
	var count int
	require.NoError(t, s.db.QueryRowContext(s.ctx, "SELECT COUNT(*) FROM request WHERE queue = ?", queue).Scan(&count))
	require.Zero(t, count)
	for _, tt := range []struct {
		request *pb.GetQueueStatusRequest
		code    codes.Code
	}{
		{&pb.GetQueueStatusRequest{}, codes.InvalidArgument},
		{&pb.GetQueueStatusRequest{Queue: "unregistered"}, codes.NotFound},
		{&pb.GetQueueStatusRequest{Queue: queue, ChangeUri: "unknown"}, codes.FailedPrecondition},
	} {
		result, err := s.client.GetQueueStatus(s.ctx, tt.request)
		require.Equal(t, tt.code, status.Code(err))
		require.Nil(t, result)
	}
}
