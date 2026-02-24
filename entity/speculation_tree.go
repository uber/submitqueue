package entity

// SpeculationTree represents the set of speculation paths constructed for a batch based on its dependency graph.
type SpeculationTree struct {
	// BatchID is the batch for which this speculation tree is constructed.
	BatchID string
	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string
	// Speculations is a list of speculation paths for this batch based on a graph of its
	// dependents.
	//
	// For e.g - Consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 depend on queueA/batch/1
	//
	// Speculations for queueA/batch/1 - [{"path": "queueA/batch/1", "state": "scheduled", "score": 0.1}]
	// Speculations for queueA/batch/2 - [{"path": "queueA/batch/2", "state": "scheduled", "score": 0.9}, {"path": "queueA/batch/1//queueA/batch/2", "state": "scheduled", "score": 0.3}]
	// Speculations for queueA/batch/3 - [{"path": "queueA/batch/3", "state": "scheduled", "score": 0.9}, {"path": "queueA/batch/1//queueA/batch/3", "state": "scheduled", "score": 0.3}]
	//
	// Note that the key value pairs within the map could have additional information about the speculation path if needed.
	//
	Speculations []map[string]string
}
