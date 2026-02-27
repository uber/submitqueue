package entity

// BuildStatus defines the possible states of a build.
type BuildStatus string

const (
	// BuildStatusUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	BuildStatusUnknown BuildStatus = ""

	// BuildStatusAccepted indicates the build has been accepted by the CI provider.
	BuildStatusAccepted BuildStatus = "accepted"

	// BuildStatusSucceeded indicates the build completed successfully.
	// This is a terminal state.
	BuildStatusSucceeded BuildStatus = "succeeded"

	// BuildStatusFailed indicates the build completed with failures.
	// This is a terminal state.
	BuildStatusFailed BuildStatus = "failed"

	// BuildStatusCancelled indicates the build was cancelled by SubmitQueue.
	// This is a terminal state.
	// Note: If the build system cancels a build for external reasons (e.g., timeout, resource limits),
	// this should be reported as BuildStatusFailed, not BuildStatusCancelled.
	BuildStatusCancelled BuildStatus = "cancelled"
)

// IsTerminal returns true if the build state represents a final state (succeeded, failed, or cancelled).
// Terminal states indicate the build has finished and will not change state again.
func (s BuildStatus) IsTerminal() bool {
	return s == BuildStatusSucceeded || s == BuildStatusFailed || s == BuildStatusCancelled
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

// ChangeAction defines the action to perform on a change submitted to the build system.
type ChangeAction string

const (
	// ChangeActionUnknown is the sentinel value for uninitialized actions.
	ChangeActionUnknown ChangeAction = ""
	// ChangeActionApply applies the change to the target branch.
	ChangeActionApply ChangeAction = "apply"
	// ChangeActionValidate applies the change first, and then validates the change by running respective validation/test suites.
	ChangeActionValidate ChangeAction = "validate"
)

// BuildChange represents a code change to be processed by the build system.
// This is used by BuildManager to specify what changes to build and what action to perform.
type BuildChange struct {
	// Change is a list of URLs where the provider is encoded in the schema part.
	// Example: "github://uber/submitqueue/pull/123/abc123def"
	Change Change
	// Action specifies what operation to perform on this change.
	Action ChangeAction
}

// BuildMetadata contains additional metadata about a build returned by the build system.
// The specific keys and values are implementation-defined.
type BuildMetadata map[string]string
