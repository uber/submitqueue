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
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/core/queuestate"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const queueStateTestQueue = "e2e-stovepipe/queue-state"

type queueStateStorageFactory struct{ backend *mysqlstorage.Storage }

func (f queueStateStorageFactory) For(cfg storage.Config) (storage.Storage, error) {
	return f.backend.For(cfg.QueueName)
}

type queueStateSourceFactory struct{}

func (queueStateSourceFactory) For(cfg sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return scfake.New(cfg, []string{"git://" + cfg.QueueName + "/HEAD"}), nil
}

func (s *StovepipeE2ESuite) TestQueueStatusWithoutValidation() {
	t := s.T()
	queue := queueStateTestQueue
	uri := "git://" + queue + "/HEAD"
	initial, err := s.client.GetQueueStatus(s.ctx, &pb.GetQueueStatusRequest{Queue: queue})
	require.NoError(t, err)
	require.NotNil(t, initial.Enablement)
	require.Equal(t, pb.QueueEnablement_QUEUE_ENABLEMENT_DISABLED, initial.Enablement.State)
	require.Equal(t, int64(1), initial.Enablement.Version)
	require.Positive(t, initial.Enablement.ChangedAtMs)
	manager := queuestate.NewTransitionController(queueStateStorageFactory{backend: s.appStorage}, queueStateSourceFactory{}, queuelog.NewMaterializer(tally.NoopScope))
	for _, tc := range []struct {
		name    string
		state   entity.QueueEnablement
		wire    pb.QueueEnablement
		version int64
	}{
		{"enable", entity.QueueEnablementEnabled, pb.QueueEnablement_QUEUE_ENABLEMENT_ENABLED, 2},
		{"disable", entity.QueueEnablementDisabled, pb.QueueEnablement_QUEUE_ENABLEMENT_DISABLED, 3},
		{"reenable", entity.QueueEnablementEnabled, pb.QueueEnablement_QUEUE_ENABLEMENT_ENABLED, 4},
	} {
		s.Run(tc.name, func() {
			t := s.T()
			change, err := manager.Prepare(s.ctx, queue, tc.state)
			require.NoError(t, err)
			applied, err := manager.Apply(s.ctx, change)
			require.NoError(t, err)
			result, err := s.client.GetQueueStatus(s.ctx, &pb.GetQueueStatusRequest{Queue: queue, ChangeUri: uri})
			require.NoError(t, err)
			require.NotNil(t, result.Enablement)
			require.Equal(t, tc.version, result.Enablement.Version)
			require.Equal(t, tc.wire, result.Enablement.State)
			require.Equal(t, uri, result.Enablement.EffectiveFromCommitUri)
			require.Equal(t, applied.ChangedAtMs, result.Enablement.ChangedAtMs)
		})
	}
	_, err = s.db.ExecContext(s.ctx, "DELETE FROM queue_log WHERE queue = ? AND log_id = ?", queue, entity.QueueLogID(4))
	require.NoError(t, err)
	result, err := s.client.GetQueueStatus(s.ctx, &pb.GetQueueStatusRequest{Queue: queue})
	require.Equal(t, codes.Internal, status.Code(err))
	require.Nil(t, result)
	_, err = manager.Initialize(s.ctx, queue)
	require.NoError(t, err)
	result, err = s.client.GetQueueStatus(s.ctx, &pb.GetQueueStatusRequest{Queue: queue})
	require.NoError(t, err)
	require.Equal(t, int64(4), result.Enablement.Version)
	bound, err := s.appStorage.For(queue)
	require.NoError(t, err)
	repaired, err := bound.GetQueueLogStore().Get(s.ctx, entity.QueueLogID(4))
	require.NoError(t, err)
	require.Equal(t, entity.QueueEnablementEnabled, repaired.State)
	var count int
	require.NoError(t, s.db.QueryRowContext(s.ctx, "SELECT COUNT(*) FROM request WHERE queue = ?", queue).Scan(&count))
	require.Zero(t, count)
}

func (s *StovepipeE2ESuite) TestQueueStatusErrors() {
	for _, tc := range []struct {
		name    string
		request *pb.GetQueueStatusRequest
		code    codes.Code
	}{
		{"missing queue", &pb.GetQueueStatusRequest{}, codes.InvalidArgument},
		{"unregistered queue", &pb.GetQueueStatusRequest{Queue: "unregistered"}, codes.NotFound},
		{"unknown commit", &pb.GetQueueStatusRequest{Queue: queueStateTestQueue, ChangeUri: "unknown"}, codes.FailedPrecondition},
	} {
		s.Run(tc.name, func() {
			response, err := s.client.GetQueueStatus(s.ctx, tc.request)
			require.Equal(s.T(), tc.code, status.Code(err))
			require.Nil(s.T(), response)
		})
	}
}
