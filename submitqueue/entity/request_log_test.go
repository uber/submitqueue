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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRequestLog_NilMetadata(t *testing.T) {
	log := NewRequestStatusLog("queue1", "queue1/100", RequestStatusStarted, 0, "", nil)

	assert.NotNil(t, log.Metadata)
	assert.Empty(t, log.Metadata)
}

// An entry records a status or an event, never both. The constructors are what
// hold that up — the struct can express the invalid combinations, nothing is
// meant to build them — so this pins what each one sets and leaves unset.
func TestRequestLogConstructors(t *testing.T) {
	t.Run("status entry carries a status and no event", func(t *testing.T) {
		log := NewRequestStatusLog("q", "q/1", RequestStatusSpeculating, 4, "boom", map[string]string{"k": "v"})

		assert.Equal(t, RequestLogTypeStatus, log.Type)
		assert.Equal(t, RequestStatusSpeculating, log.Status)
		assert.Equal(t, RequestEventUnknown, log.Event)
		assert.Equal(t, int32(4), log.RequestVersion)
		assert.Equal(t, "boom", log.LastError)
		assert.Equal(t, "speculating", log.Value())
	})

	t.Run("event entry carries an event and no status", func(t *testing.T) {
		log := NewRequestEventLog("q", "q/1", RequestEventBuilding, map[string]string{"build_id": "b/7"})

		assert.Equal(t, RequestLogTypeEvent, log.Type)
		assert.Equal(t, RequestEventBuilding, log.Event)
		assert.Equal(t, RequestStatusUnknown, log.Status)
		assert.Equal(t, "building", log.Value())
		assert.Zero(t, log.RequestVersion)
	})
}
