package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRequestLog_NilMetadata(t *testing.T) {
	log := NewRequestLog("queue1/100", "new", 0, "", nil)

	assert.NotNil(t, log.Metadata)
	assert.Empty(t, log.Metadata)
}

func TestRequestLog_ToBytes(t *testing.T) {
	log := RequestLog{
		RequestID:      "test-queue/123",
		TimestampMs:    1709568000000,
		Status:         "new",
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
		Status:         "processing",
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
				Status:         "landed",
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
				Status:         "error",
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
				Status:         "new",
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
