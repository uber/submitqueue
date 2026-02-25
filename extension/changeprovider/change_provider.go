package changeprovider

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// User represents the author of a change.
type User struct {
	// Name is the display name of the user.
	Name string
	// Email is the email address of the user.
	Email string
}

// ChangedFile represents a single file modification in a change.
type ChangedFile struct {
	// Path is the file path relative to the repository root.
	Path string
	// Patch is the diff patch content for this file.
	Patch string
	// LinesAdded is the number of lines added in this file.
	LinesAdded int
	// LinesDeleted is the number of lines deleted in this file.
	LinesDeleted int
	// LinesModified is the number of lines modified in this file.
	LinesModified int
}

// ChangeInfo contains metadata and file changes for a code change.
type ChangeInfo struct {
	// ID is the change identifier (e.g., "PR: uber-code/go-code/1" or "diff: uber-code/go-code/D1").
	ID string
	// User is the author of the change.
	User User
	// ChangedFiles is the list of files modified in this change. Order is unspecified.
	ChangedFiles []ChangedFile
}

// ChangeProvider fetches change metadata from external systems
// Each implementation is configured for a specific provider (GitHub, GitLab, Phabricator).
type ChangeProvider interface {
	// Get retrieves change information for the provided Change.
	// Returns the change info containing metadata and file changes.
	Get(ctx context.Context, change entity.Change) (ChangeInfo, error)
}
