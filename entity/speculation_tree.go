package entity

// SpeculationTree represents the set of speculation paths constructed for a batch based on its dependency graph.
type SpeculationTree struct {
	// BatchID is the batch for which this speculation tree is constructed.
	BatchID string
	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string
	// Speculations is a list of speculation paths for this batch based on a graph of its
	// dependents.
	Speculations []map[string]string
}
