package build

// BuildState represents the execution state of a build.
// This is a string enum following SubmitQueue's pattern of using string enums with sentinel values.
type BuildState string

const (
	// BuildStateUnknown is the sentinel value for unknown or unreachable build states.
	// This should only occur if the provider returns an unexpected state value.
	BuildStateUnknown BuildState = ""

	// BuildStateQueued indicates the build has been scheduled but not yet started.
	BuildStateQueued BuildState = "queued"

	// BuildStateRunning indicates the build is currently executing.
	BuildStateRunning BuildState = "running"

	// BuildStatePassed indicates the build completed successfully.
	// This is a terminal state.
	BuildStatePassed BuildState = "passed"

	// BuildStateFailed indicates the build completed with failures.
	// This is a terminal state.
	BuildStateFailed BuildState = "failed"

	// BuildStateCancelled indicates the build was cancelled before completion.
	// This is a terminal state.
	BuildStateCancelled BuildState = "cancelled"

	// BuildStateBlocked indicates the build is waiting for manual approval or unblocking.
	// Some CI systems (like BuildKite) support manual approval steps.
	BuildStateBlocked BuildState = "blocked"
)

// IsTerminal returns true if the build state represents a final state (passed, failed, or cancelled).
// Terminal states indicate the build has finished and will not change state again.
// Note: BuildStateBlocked is NOT terminal as blocked builds can be unblocked and continue execution.
func (s BuildState) IsTerminal() bool {
	return s == BuildStatePassed || s == BuildStateFailed || s == BuildStateCancelled
}
