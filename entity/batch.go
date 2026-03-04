package entity

import "encoding/json"

// BatchState defines the possible states of a batch.
type BatchState string

const (
	// BatchStateUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	BatchStateUnknown BatchState = ""
	// BatchStateCreated is the state of a batch that has been created for processing.
	BatchStateCreated BatchState = "created"
	// BatchStateReady is the state of a batch that has been persisted and is ready for scoring.
	BatchStateReady BatchState = "ready"
	// BatchStateSpeculating is the state of a batch that is undergoing speculative execution.
	BatchStateSpeculating BatchState = "speculating"
	// BatchStateFinalizing is the state of a batch that is being finalized after speculative execution.
	BatchStateFinalizing BatchState = "finalizing"
	// BatchStateSucceeded is the terminal state of a batch that has been successfully landed.
	BatchStateSucceeded BatchState = "succeeded"
	// BatchStateFailed is the terminal state of a batch that has failed.
	BatchStateFailed BatchState = "failed"
	// BatchStateCancelled is the terminal state of a batch that was cancelled before completion.
	BatchStateCancelled BatchState = "cancelled"
)

// IsTerminal returns true if the batch state is a terminal state.
// Terminal states are states from which no further transitions are possible.
func (s BatchState) IsTerminal() bool {
	switch s {
	case BatchStateSucceeded, BatchStateFailed, BatchStateCancelled:
		return true
	default:
		return false
	}
}

// Batch represents a group of requests to land (merge into target branch of the source control repository).
type Batch struct {
	// ID is the globally unique identifier for the batch. Format: "<queue>/batch/<counter_value>".
	ID string
	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string
	// Contains is a list of land request IDs that are part of this batch.
	// Request IDs will always be part of the same queue.
	//
	// For e.g. - [queueA/1, queueA/2, queueA/3].
	//
	Contains []string
	// Dependencies is a list of batch IDs (and associated metadata) for this batch.
	// Dependencies will always be part of the same queue.
	//
	// For e.g - Consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 depend on queueA/batch/1
	//
	// In this case, the Dependencies field for -
	// - queueA/batch/1 will be empty
	// - queueA/batch/2 will contain queueA/batch/1
	// - queueA/batch/3 will contain queueA/batch/1
	//
	Dependencies []map[string]interface{}
	// The state of the batch lifecycle this batch is in.
	State BatchState
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}

// ToBytes serializes the Batch to JSON bytes for queue message payload.
func (b Batch) ToBytes() ([]byte, error) {
	return json.Marshal(b)
}

// BatchFromBytes deserializes a Batch from JSON bytes.
func BatchFromBytes(data []byte) (Batch, error) {
	var batch Batch
	err := json.Unmarshal(data, &batch)
	return batch, err
}
