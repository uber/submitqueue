package queue

import (
	"maps"
	"time"
)

// Message represents a queue message entity.
// Immutable - use Copy() for modifications.
type Message struct {
	// ID uniquely identifies the message for deduplication and tracing.
	ID string

	// Payload is the message body as raw bytes.
	Payload []byte

	// Metadata contains key-value pairs for headers and attributes.
	// Use for trace IDs, request IDs, and cross-service metadata.
	Metadata map[string]string

	// PartitionKey determines which partition/shard this message goes to.
	// Messages with the same PartitionKey are guaranteed ordered delivery.
	// Optional - if empty, backend may use round-robin distribution.
	PartitionKey string

	// PublishedAt is when the message was published (Unix milliseconds).
	PublishedAt int64
}

// NewMessage creates a new message with the given ID and payload.
// Metadata is initialized as an empty map.
// PublishedAt is set to the current time.
func NewMessage(id string, payload []byte) Message {
	return Message{
		ID:          id,
		Payload:     payload,
		Metadata:    make(map[string]string),
		PublishedAt: time.Now().UnixMilli(),
	}
}

// Copy creates a deep copy of the message.
// Safe to call concurrently.
func (m Message) Copy() Message {
	payloadCopy := make([]byte, len(m.Payload))
	copy(payloadCopy, m.Payload)

	return Message{
		ID:           m.ID,
		Payload:      payloadCopy,
		Metadata:     maps.Clone(m.Metadata),
		PartitionKey: m.PartitionKey,
		PublishedAt:  m.PublishedAt,
	}
}
