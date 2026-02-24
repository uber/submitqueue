package entity

// SpeculationInfo represents metadata about a single speculation path, including the path through the dependency graph, its current state, and the predicted build score.
type SpeculationInfo struct {
	Path string
	State string
	Score float32
}

// SpeculationTree represents the set of speculation paths constructed for a batch based on its dependency graph.
type SpeculationTree struct {
	// BatchID is the batch for which this speculation tree is constructed.
	BatchID string
	// Speculations is a list of speculation paths for this batch based on a graph of its
	// dependents.
	//
	// For e.g - Consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 depend on queueA/batch/1
	//
	// Speculations for queueA/batch/1 - [{Path: "queueA/batch/1", State: "scheduled", Score: 0.1}]
	// Speculations for queueA/batch/2 - [{Path: "queueA/batch/2", State: "scheduled", Score: 0.9}, {Path: "queueA/batch/1//queueA/batch/2", State: "scheduled", Score: 0.3}]
	// Speculations for queueA/batch/3 - [{Path: "queueA/batch/3", State: "scheduled", Score: 0.9}, {Path: "queueA/batch/1//queueA/batch/3", State: "scheduled", Score: 0.3}]
	//
	Speculations []SpeculationInfo
}
