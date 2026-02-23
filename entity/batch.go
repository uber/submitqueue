package entity

// Batch represents a group of requests to land (merge into target branch of the source control repository).
type Batch struct {
	// ID is the globally unique identifier for the batch. Format: "<queue>/batch/<counter_value>".
	ID string
	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string
	// Contains is a list of land request IDs that are part of this batch.
	// Request IDs will always be part of the same queue.
	// For e.g. - [queueA/1, queueA/2, queueA/3].
	Contains []string
	// Dependencies is a list of batch IDs (and associated metdata) for this batch.
	// Dependencies will always be part of the same queue.
	Dependencies []map[string]interface{}
	// The state of the batch lifecycle this batch is in.
	State string
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}
