package entity

// BatchDependent represents the downstream batches that depend on a given batch.
type BatchDependent struct {
	// BatchID is the globally unique identifier representing a batch.
	BatchID string
	// Dependents is a list of batch IDs that are dependents for this
	// batch.
	//
	// For e.g - Consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 depend on queueA/batch/1
	//
	// In this case, the Dependents field for -
	// - queueA/batch/1 will be [queueA/batch/2, queueA/batch/3]
	// - queueA/batch/2 will be empty
	// - queueA/batch/3 will be empty
	//
	Dependents []string
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}
