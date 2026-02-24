package entity

// BuildStatus defines the possible states of a build.
type BuildStatus string

const (
	// BuildStateUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	BuildStateUnknown BuildStatus = ""
	// TODO: Add comprehensive list of known build states.
)

// SpeculationPathInfo represents the base and head commits of a speculation path used in a build.
type SpeculationPathInfo struct {
	// Base represents the base state of this speculation path.
	Base string
	// Head represents the head commit of this speculation path.
	Head string
}

// Build represents a build scheduled for a batch along a specific speculation path.
// All fields except the Status are immutable after creation.
type Build struct {
	// ID represents the build ID. It is the responsibility of the caller to ensure
	// this is unique.
	ID string
	// BatchID is the batch for which this build is scheduled.
	BatchID string
	// SpeculationPath is the speculation path that represents this build. For
	// a given batch this path is crafted from the graph that is generated from the
	// dependencies of this batch.
	SpeculationPath SpeculationPathInfo
	// Score represents the build prediction score for this speculation path.
	Score float32
	// Status represents the state of the build lifecycle this build is in.
	Status BuildStatus
}
