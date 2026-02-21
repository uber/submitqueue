package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	id := "test-id"
	payload := []byte("test payload")

	msg := NewMessage(id, payload, "", nil)

	assert.Equal(t, id, msg.ID)
	assert.Equal(t, payload, msg.Payload)
	assert.Empty(t, msg.PartitionKey)
	assert.NotNil(t, msg.Metadata)
	assert.Empty(t, msg.Metadata)
	assert.NotZero(t, msg.PublishedAt)
}

func TestMessage_Copy(t *testing.T) {
	original := NewMessage("id-123", []byte("payload"), "partition-1", map[string]string{"key": "value"})

	copied := original.Copy()

	// Verify immutable fields are equal
	assert.Equal(t, original.ID, copied.ID)
	assert.Equal(t, original.PublishedAt, copied.PublishedAt)
	assert.Equal(t, original.PartitionKey, copied.PartitionKey)

	// Verify deep copy of payload
	assert.Equal(t, original.Payload, copied.Payload)
	original.Payload[0] = 'X'
	assert.NotEqual(t, original.Payload[0], copied.Payload[0])

	// Verify deep copy of metadata
	assert.Equal(t, original.Metadata, copied.Metadata)
	original.Metadata["new"] = "value"
	assert.NotContains(t, copied.Metadata, "new")
}

func TestMessage_Copy_EmptyPayload(t *testing.T) {
	original := NewMessage("id", []byte{}, "", nil)
	copied := original.Copy()

	assert.NotNil(t, copied.Payload)
	assert.Empty(t, copied.Payload)
	assert.Equal(t, original.Payload, copied.Payload)
}

func TestMessage_Fields(t *testing.T) {
	msg := NewMessage("id-123", []byte("payload"), "user-123", map[string]string{
		"trace-id": "xyz",
		"source":   "gateway",
	})

	// Test metadata
	assert.Equal(t, "xyz", msg.Metadata["trace-id"])
	assert.Equal(t, "gateway", msg.Metadata["source"])

	// Test partition key
	assert.Equal(t, "user-123", msg.PartitionKey)

	// Test PublishedAt can be overridden
	customTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	msg.PublishedAt = customTime
	assert.Equal(t, customTime, msg.PublishedAt)

	// Verify copy preserves fields
	copied := msg.Copy()
	assert.Equal(t, msg.ID, copied.ID)
	assert.Equal(t, msg.PartitionKey, copied.PartitionKey)
	assert.Equal(t, msg.PublishedAt, copied.PublishedAt)
	assert.Equal(t, msg.Metadata, copied.Metadata)
}

