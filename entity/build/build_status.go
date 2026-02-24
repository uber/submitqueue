package build

// BuildStatus represents the current state of a build as returned by BuildManager.Poll().
// It contains all information about the build's lifecycle, including timing, state, and URLs.
type BuildStatus struct {
	// ID is the unique identifier for this build.
	ID BuildID

	// State is the current execution state of the build (queued, running, passed, failed, etc.).
	State BuildState

	// QueuedAt is the Unix timestamp in milliseconds when the build was queued.
	// Zero if the build has not been queued yet.
	QueuedAt int64

	// StartedAt is the Unix timestamp in milliseconds when the build started executing.
	// Zero if the build has not started yet.
	StartedAt int64

	// FinishedAt is the Unix timestamp in milliseconds when the build finished (passed, failed, or cancelled).
	// Zero if the build has not finished yet.
	FinishedAt int64

	// WebURL is a link to view the build in the CI provider's web UI.
	// Empty string if not available.
	WebURL string

	// LogsURL is a direct link to the build's logs.
	// Empty string if not available.
	LogsURL string

	// ErrorMessage contains error details for failed builds.
	// Empty string for successful builds or builds that haven't finished.
	ErrorMessage string

	// Metadata contains provider-specific metadata that doesn't fit in the standard fields.
	// Examples: commit author, branch, test results summary, etc.
	// Never nil - always initialized to at least an empty map.
	Metadata map[string]string
}
