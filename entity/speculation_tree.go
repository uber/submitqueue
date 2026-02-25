package entity

// SpeculationPathAction defines the possible actions for a speculation path.
type SpeculationPathAction string

const (
	// SpeculationPathActionUnknown is the default zero value for SpeculationPathAction.
	SpeculationPathActionUnknown SpeculationPathAction = ""
	// SpeculationPathActionSchedule indicates a build should be scheduled for this path.
	SpeculationPathActionSchedule SpeculationPathAction = "schedule"
	// SpeculationPathActionCancel indicates this path should be cancelled or not built.
	SpeculationPathActionCancel SpeculationPathAction = "cancel"
	// SpeculationPathActionLand indicates this path succeeded and should proceed to merge.
	SpeculationPathActionLand SpeculationPathAction = "land"
)

// SpeculationPath represents a speculation path through the dependency graph.
// Base contains the predecessor batch IDs assumed to have succeeded (in arrival order),
// and Head is the batch being tested.
type SpeculationPath struct {
	// Base is the ordered list of predecessor batch IDs assumed to have succeeded.
	Base []string
	// Head is the batch ID being tested along this path.
	Head string
}

// SpeculationInfo represents metadata about a single speculation path, including the path through the dependency graph, its current state, and the predicted build score.
type SpeculationInfo struct {
	// Path represents the speculation path as a base/head pair.
	Path SpeculationPath
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
	// Speculations for queueA/batch/1 - [{Path: {Base: [], Head: "queueA/batch/1"}, Action: "scheduled", Score: 0.1}]
	// Speculations for queueA/batch/2 - [{Path: {Base: [], Head: "queueA/batch/2"}, Action: "scheduled", Score: 0.9}, {Path: {Base: ["queueA/batch/1"], Head: "queueA/batch/2"}, Action: "scheduled", Score: 0.3}]
	// Speculations for queueA/batch/3 - [{Path: {Base: [], Head: "queueA/batch/3"}, Action: "scheduled", Score: 0.9}, {Path: {Base: ["queueA/batch/1"], Head: "queueA/batch/3"}, Action: "scheduled", Score: 0.3}]
	//
	Speculations []SpeculationInfo
}
