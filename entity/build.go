package entity

// BuildStatus defines the possible states of a build.
type BuildStatus string

const (
	// BuildStatusUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	BuildStatusUnknown BuildStatus = ""

	// BuildStatusQueued indicates the build has been scheduled but not yet started.
	BuildStatusQueued BuildStatus = "queued"

	// BuildStatusAccepted indicates the build has been accepted by the CI provider.
	BuildStatusAccepted BuildStatus = "accepted"

	// BuildStatusRunning indicates the build is currently executing.
	BuildStatusRunning BuildStatus = "running"

	// BuildStatusPassed indicates the build completed successfully.
	// This is a terminal state.
	BuildStatusPassed BuildStatus = "passed"

	// BuildStatusFailed indicates the build completed with failures.
	// This is a terminal state.
	BuildStatusFailed BuildStatus = "failed"

	// BuildStatusCancelled indicates the build was cancelled before completion.
	// This is a terminal state.
	BuildStatusCancelled BuildStatus = "cancelled"
)

// IsTerminal returns true if the build state represents a final state (passed, failed, or cancelled).
// Terminal states indicate the build has finished and will not change state again.
func (s BuildStatus) IsTerminal() bool {
	return s == BuildStatusPassed || s == BuildStatusFailed || s == BuildStatusCancelled
}


// SpeculationPathInfo represents the base and head commits of a speculation path used in a build.
type SpeculationPathInfo struct {
	// Base is a list of batchIDs(in order) that form the base of this speculation path.
	Base []string
}

// Build represents a build scheduled for a batch along a specific speculation path.
// All fields except the Status are immutable after creation.
type Build struct {
	// ID represents the build ID. It is the responsibility of a build management system to ensure
	// that this is unique.
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

// BuildAction defines the action to perform on a change submitted to the build system.
type BuildAction string

const (
	// BuildActionUnknown is the sentinel value for uninitialized actions.
	BuildActionUnknown BuildAction = ""
	// BuildActionValidate runs validation/testing on the change without applying it.
	BuildActionValidate BuildAction = "validate"
	// BuildActionApply applies the change to the target branch.
	BuildActionApply BuildAction = "apply"
)

// BuildChange represents a code change to be processed by the build system.
// This is used by BuildManager to specify what changes to build and what action to perform.
type BuildChange struct {
	// ChangeID is the unique identifier for this change.
	// This is typically a diff ID (e.g., "D12345") or PR number (e.g., "42"),
	// depending on the source control provider.
	ChangeID string
	// Action specifies what operation to perform on this change.
	Action BuildAction
}
