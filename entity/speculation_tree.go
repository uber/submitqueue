package entity

// SpeculationPathAction defines the possible actions for a speculation path.
type SpeculationPathAction string

const (
	// SpeculationPathActionUnknown is the default zero value for SpeculationPathAction.
	SpeculationPathActionUnknown SpeculationPathAction = ""
	// TODO: Add comprehensive list of actions
)

// SpeculationInfo represents metadata about a single speculation path, including the path through the dependency graph, its current state, and the predicted build score.
type SpeculationInfo struct {
	// Path represents the speculation path; which is an ordered list of batches.
	Path []string
	// Action is a state that this path is in.
	Action SpeculationPathAction
	// Score is score for this speculation path.
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
	// Speculations for queueA/batch/1 - [{Path: []string{"queueA/batch/1"}, State: "scheduled", Score: 0.1}]
	// Speculations for queueA/batch/2 - [{Path: []string{"queueA/batch/2"}, State: "scheduled", Score: 0.9}, {Path: []string{"queueA/batch/1", "queueA/batch/2"}, State: "scheduled", Score: 0.3}]
	// Speculations for queueA/batch/3 - [{Path: []string{"queueA/batch/3"}, State: "scheduled", Score: 0.9}, {Path: []string{"queueA/batch/1", "queueA/batch/3"}, State: "scheduled", Score: 0.3}]
	//
	Speculations []SpeculationInfo
}
