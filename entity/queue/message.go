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

// NewMessage creates a new message with the given ID, payload, partition key, and metadata.
// If metadata is nil, it will be initialized as an empty map.
// PublishedAt is set to the current time.
func NewMessage(id string, payload []byte, partitionKey string, metadata map[string]string) Message {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return Message{
		ID:           id,
		Payload:      payload,
		PartitionKey: partitionKey,
		Metadata:     metadata,
		PublishedAt:  time.Now().UnixMilli(),
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
