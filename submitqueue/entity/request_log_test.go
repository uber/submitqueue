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
	log := NewRequestLog("queue1/100", RequestStatusStarted, 0, "", nil)

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
		Queue:          "my-queue",
		ChangeURIs:     []string{"github://uber/repo/pull/1/abcdef"},
		TimestampMs:    1709568000000,
		Status:         RequestStatusProcessing,
		RequestVersion: 3,
		LastError:      "timeout",
		Metadata:       map[string]string{"step": "validation", "attempt": "2"},
	}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := RequestLogFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original.RequestID, deserialized.RequestID)
	assert.Equal(t, original.Queue, deserialized.Queue)
	assert.Equal(t, original.ChangeURIs, deserialized.ChangeURIs)
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
				Queue:          "queue1",
				ChangeURIs:     []string{},
				TimestampMs:    1709568000000,
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
				Queue:          "queue2",
				ChangeURIs:     []string{},
				TimestampMs:    1709568001000,
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
				Queue:          "queue3",
				ChangeURIs:     []string{},
				TimestampMs:    1709568002000,
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

func TestQueueFromRequestID(t *testing.T) {
	assert.Equal(t, "queue", QueueFromRequestID("queue/100"))
	assert.Equal(t, "org/queue", QueueFromRequestID("org/queue/100"))
	assert.Empty(t, QueueFromRequestID("queue/not-a-number"))
	assert.Empty(t, QueueFromRequestID("queue"))
}
