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

package request

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"go.uber.org/mock/gomock"
)

func TestPublishTerminalRequestLog(t *testing.T) {
	for _, tc := range []struct {
		name       string
		state      entity.RequestState
		details    entity.RequestTerminalDetails
		wantStatus entity.RequestStatus
	}{
		{name: "landed", state: entity.RequestStateLanded, details: entity.RequestTerminalDetails{TimestampMs: 123}, wantStatus: entity.RequestStatusLanded},
		{name: "cancelled", state: entity.RequestStateCancelled, details: entity.RequestTerminalDetails{TimestampMs: 123, Metadata: map[string]string{"reason": "original"}}, wantStatus: entity.RequestStatusCancelled},
		{name: "legacy", state: entity.RequestStateError, wantStatus: entity.RequestStatusError},
		{name: "non-terminal rejected", state: entity.RequestStateStarted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, logs := recordingRegistry(t, gomock.NewController(t), nil)
			request := entity.Request{ID: "1", Queue: "q", State: tc.state, Version: 3, TerminalDetails: tc.details}
			err := PublishTerminalRequestLog(context.Background(), registry, request)
			if tc.wantStatus == entity.RequestStatusUnknown {
				require.Error(t, err)
				assert.Empty(t, *logs)
				return
			}
			require.NoError(t, err)
			require.Len(t, *logs, 1)
			log := (*logs)[0]
			assert.Equal(t, tc.wantStatus, log.Status)
			assert.Equal(t, request.Version, log.RequestVersion)
			if tc.details.TimestampMs == 0 {
				assert.Positive(t, log.TimestampMs)
				assert.Empty(t, log.LastError)
				assert.Equal(t, map[string]string{entity.TerminalDetailsUnavailable: "true"}, log.Metadata)
			} else {
				assert.Equal(t, tc.details.TimestampMs, log.TimestampMs)
				assert.Equal(t, tc.details.LastError, log.LastError)
				assert.Equal(t, len(tc.details.Metadata), len(log.Metadata))
				for key, value := range tc.details.Metadata {
					assert.Equal(t, value, log.Metadata[key])
				}
			}
		})
	}
}
