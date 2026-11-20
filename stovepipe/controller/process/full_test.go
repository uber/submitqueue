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

package process

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestProcessForcedFullRequest(t *testing.T) {
	for _, tc := range []struct {
		name          string
		older         bool
		busy          bool
		supersedeRace bool
		admitRace     bool
	}{
		{name: "full override ignores incremental baseline"},
		{name: "older full request remains eligible", older: true},
		{name: "older full request waits for capacity", older: true, busy: true},
		{name: "force wins supersession race", older: true, supersedeRace: true},
		{name: "force wins admission race", admitRace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			id := testID
			if tc.older {
				id = testOlderID
			}
			request := acceptedRequest(id)
			if !tc.supersedeRace && !tc.admitRace {
				request.BuildStrategy = entity.BuildStrategyFull
			}
			m.reqStore.EXPECT().Get(gomock.Any(), id).Return(request, nil)
			queue := entity.Queue{
				Name: testQueue, LatestRequestID: testID,
				LastGreenURI: "git://repo/monorepo/main/green", Version: 1,
			}
			if tc.busy {
				queue.InFlightCount = 1
			}
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queue, nil)
			d := delivery(t, ctrl, processPayload(t, id))
			if tc.busy {
				d.EXPECT().Hold(int64(5000))
				require.NoError(t, c.Process(queueContext(testQueue), d))
				return
			}
			if tc.supersedeRace || tc.admitRace {
				first := request
				if tc.supersedeRace {
					first.State = entity.RequestStateSuperseded
				} else {
					m.sourceFactory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sourceControl, nil)
					m.sourceControl.EXPECT().IsAncestor(gomock.Any(), queue.LastGreenURI, testURI).Return(true, nil)
					first.State = entity.RequestStateProcessing
					first.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
					first.BaseURI = queue.LastGreenURI
				}
				write := m.reqStore.EXPECT().Update(gomock.Any(), first, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				request.BuildStrategy = entity.BuildStrategyFull
				request.Version = 2
				m.reqStore.EXPECT().Get(gomock.Any(), id).Return(request, nil).After(write)
			}
			admitted := request
			admitted.State = entity.RequestStateProcessing
			admitted.BuildStrategy = entity.BuildStrategyFull
			admitted.BaseURI = ""
			transition := m.reqStore.EXPECT().Update(gomock.Any(), admitted, request.Version, request.Version+1).Return(nil)
			queue.Version = 2
			queue.InFlightCount = 1
			count := m.queueStore.EXPECT().Update(gomock.Any(), queue, int32(2), int32(3)).Return(nil).After(transition)
			admitted.Version++
			expectProcessingLogAndHandoff(t, m, admitted).After(count)

			require.NoError(t, c.Process(queueContext(testQueue), d))
		})
	}
}
