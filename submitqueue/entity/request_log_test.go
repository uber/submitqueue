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
	"github.com/stretchr/testify/require"
)

func TestNewRequestLog_NilMetadata(t *testing.T) {
	log := NewRequestStatusLog("queue1", "queue1/100", RequestStatusStarted, 0, "", nil)

	assert.NotNil(t, log.Metadata)
	assert.Empty(t, log.Metadata)
}

func TestRequestLog_ToBytes(t *testing.T) {
	log := RequestLog{
		RequestID:      "test-queue/123",
		TimestampMs:    1709568000000,
		Status:         RequestStatusStarted,
		RequestVersion: 1,
		LastError:      "",
		Metadata:       map[string]string{"source": "gateway"},
	}

	data, err := log.ToBytes()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	jsonStr := string(data)
	assert.Contains(t, jsonStr, "test-queue/123")
	assert.Contains(t, jsonStr, "1709568000000")
	assert.Contains(t, jsonStr, "gateway")
}

func TestRequestLogFromBytes(t *testing.T) {
	original := RequestLog{
		RequestID:      "my-queue/999",
		TimestampMs:    1709568000000,
		Status:         RequestStatusSpeculating,
		RequestVersion: 3,
		LastError:      "timeout",
		Metadata:       map[string]string{"step": "validation", "attempt": "2"},
	}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := RequestLogFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original.RequestID, deserialized.RequestID)
	assert.Equal(t, original.TimestampMs, deserialized.TimestampMs)
	assert.Equal(t, original.Status, deserialized.Status)
	assert.Equal(t, original.RequestVersion, deserialized.RequestVersion)
	assert.Equal(t, original.LastError, deserialized.LastError)
	assert.Equal(t, original.Metadata, deserialized.Metadata)
}

func TestRequestLogFromBytes_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"invalid": json"}`)

	_, err := RequestLogFromBytes(invalidJSON)
	assert.Error(t, err)
}

func TestRequestLogFromBytes_EmptyData(t *testing.T) {
	emptyJSON := []byte(`{}`)

	log, err := RequestLogFromBytes(emptyJSON)
	require.NoError(t, err)

	assert.Empty(t, log.RequestID)
	assert.Equal(t, int64(0), log.TimestampMs)
	assert.Empty(t, log.Status)
	assert.Equal(t, int32(0), log.RequestVersion)
	assert.Empty(t, log.LastError)
	assert.NotNil(t, log.Metadata)
	assert.Empty(t, log.Metadata)
}

func TestRequestLog_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		log  RequestLog
	}{
		{
			name: "with all fields populated",
			log: RequestLog{
				RequestID:      "queue1/100",
				TimestampMs:    1709568000000,
				Type:           RequestLogTypeStatus,
				Status:         RequestStatusLanded,
				RequestVersion: 5,
				LastError:      "",
				Metadata:       map[string]string{"source": "orchestrator", "batch_id": "b-1"},
			},
		},
		{
			name: "with error",
			log: RequestLog{
				RequestID:      "queue2/200",
				TimestampMs:    1709568001000,
				Type:           RequestLogTypeStatus,
				Status:         RequestStatusError,
				RequestVersion: 2,
				LastError:      "merge conflict detected",
				Metadata:       map[string]string{},
			},
		},
		{
			name: "with zero version",
			log: RequestLog{
				RequestID:      "queue3/300",
				TimestampMs:    1709568002000,
				Type:           RequestLogTypeStatus,
				Status:         RequestStatusStarted,
				RequestVersion: 0,
				LastError:      "",
				Metadata:       map[string]string{"key": "value"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.log.ToBytes()
			require.NoError(t, err)

			deserialized, err := RequestLogFromBytes(data)
			require.NoError(t, err)

			assert.Equal(t, tt.log, deserialized)
		})
	}
}

// An entry records a status or an event, never both. The constructors are what
// hold that up — the struct can express the invalid combinations, nothing is
// meant to build them — so this pins what each one sets and leaves unset.
//
// The old classifier this replaces asked "is this status really an event?", a
// question the split makes unaskable: an event is a RequestEvent and cannot be
// assigned to Status or to RequestSummary.Status at all.
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
		// An event is not a state transition, so it can carry no version for a
		// reader to mistake for a reconcilable one.
		assert.Zero(t, log.RequestVersion)
	})
}

// An entry written before the type column existed recorded a status, so it must
// read back as one rather than as the zero type — otherwise every historical
// entry would stop counting towards the current status the moment this shipped.
func TestRequestLogFromBytes_UntypedEntryIsAStatus(t *testing.T) {
	legacy := []byte(`{"request_id":"q/1","queue":"q","timestamp_ms":10,"status":"landed","request_version":3}`)

	log, err := RequestLogFromBytes(legacy)
	require.NoError(t, err)

	assert.Equal(t, RequestLogTypeStatus, log.Type)
	assert.Equal(t, RequestStatusLanded, log.Status)
	assert.Equal(t, "landed", log.Value())
}

func TestRequestLog_EventSerializationRoundTrip(t *testing.T) {
	original := NewRequestEventLog("q", "q/1", RequestEventBuilt, map[string]string{"build_id": "b/7"})

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := RequestLogFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original, deserialized)
}
