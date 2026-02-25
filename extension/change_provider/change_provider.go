package changeprovider

import "context"

// FileChangeStatus defines the type of modification made to a file.
type FileChangeStatus string

const (
	// FileChangeStatusAdded indicates a new file was added.
	FileChangeStatusAdded FileChangeStatus = "added"
	// FileChangeStatusModified indicates an existing file was modified.
	FileChangeStatusModified FileChangeStatus = "modified"
	// FileChangeStatusDeleted indicates a file was deleted.
	FileChangeStatusDeleted FileChangeStatus = "deleted"
)

// FileChange represents a single file modification in a change.
type FileChange struct {
	// Path is the file path relative to the repository root.
	Path string
	// Status is the type of change made to the file.
	Status FileChangeStatus
}

// ChangeProvider fetches the list of changed files from external code review providers.
// Each implementation is configured for a specific provider (GitHub, GitLab, Phabricator).
type ChangeProvider interface {
	// Get retrieves the list of files changed across all provided URIs.
	// The URI format is provider-specific:
	//   - GitHub: "owner/repo/pr_number@commit_hash" (e.g., "uber/submitqueue/123@abc123def")
	//   - Phabricator: "revision_id@commit_hash" (e.g., "D12345@abc123def")
	// Returns the aggregated list of changed files across all URIs.
	Get(ctx context.Context, uris []string) ([]FileChange, error)
}
