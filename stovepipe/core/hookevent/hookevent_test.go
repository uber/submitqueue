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

package hookevent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/stovepipe/entity"
)

const (
	testQueue = "monorepo/main"
	testID    = "request/monorepo/main/7"
)

func testRequest() entity.Request {
	return entity.Request{
		ID:      testID,
		Queue:   testQueue,
		URI:     "git://remote/monorepo/main/abc123",
		BaseURI: "git://remote/monorepo/main/def456",
	}
}

func TestConstructors(t *testing.T) {
	tests := []struct {
		name     string
		build    func(entity.Request) *basehook.HookEvent
		wantType Type
	}{
		{
			name:     "recorded",
			build:    NewValidationRepositoryRecorded,
			wantType: TypeValidationRepositoryRecorded,
		},
		{
			name:     "cancelled",
			build:    NewValidationRepositoryCancelled,
			wantType: TypeValidationRepositoryCancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := tt.build(testRequest())
			require.NoError(t, basehook.Validate(event))

			assert.Equal(t, Source, event.GetSource())
			assert.Equal(t, string(tt.wantType), event.GetType())
			assert.Positive(t, event.GetTimestampMs())

			// The wire field names a consumer in another repository reads. The
			// request's uri and base uri are deliberately absent: a hook
			// resolves those from storage.
			assert.Equal(t, map[string]any{
				"queue":      testQueue,
				"request_id": testID,
			}, event.GetPayload().AsMap())
		})
	}
}

// The id is derived from the transition rather than the clock, which is what
// lets the queue dedupe a redelivery.
func TestNewValidationRepositoryRecorded_IDIsStable(t *testing.T) {
	first := NewValidationRepositoryRecorded(testRequest())
	second := NewValidationRepositoryRecorded(testRequest())

	assert.Equal(t, first.GetId(), second.GetId())
}

func TestConstructors_IDsDifferByType(t *testing.T) {
	recorded := NewValidationRepositoryRecorded(testRequest())
	cancelled := NewValidationRepositoryCancelled(testRequest())

	assert.NotEqual(t, recorded.GetId(), cancelled.GetId())
}
