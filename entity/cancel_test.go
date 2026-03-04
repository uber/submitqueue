package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancel_ToBytes(t *testing.T) {
	cancel := Cancel{
		Sqid: "test-queue/123",
	}

	data, err := cancel.ToBytes()
	require.NoError(t, err)
	assert.Contains(t, string(data), "test-queue/123")
}

func TestCancelFromBytes(t *testing.T) {
	original := Cancel{
		Sqid: "my-queue/999",
	}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := CancelFromBytes(data)
	require.NoError(t, err)
	assert.Equal(t, original, deserialized)
}

func TestCancelFromBytes_InvalidJSON(t *testing.T) {
	_, err := CancelFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}

func TestCancel_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		cancel Cancel
	}{
		{
			name:   "standard sqid",
			cancel: Cancel{Sqid: "queue1/100"},
		},
		{
			name:   "long sqid",
			cancel: Cancel{Sqid: "my-long-queue-name/999999"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.cancel.ToBytes()
			require.NoError(t, err)

			deserialized, err := CancelFromBytes(data)
			require.NoError(t, err)
			assert.Equal(t, tt.cancel, deserialized)
		})
	}
}
