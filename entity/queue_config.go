package entity

// QueueConfig holds the configuration for a single submit queue.
// Each queue maps a repository + destination to a processing pipeline.
// A repository can have multiple queues, but each queue has exactly one destination.
// Immutable after creation.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	// Referenced by Request.Queue.
	Name string

	// RepositoryID is the platform-specific identifier for the source control repository.
	// Opaque to the system; meaningful only to the change provider.
	// Examples:
	//   - GitHub/GitLab: "owner/repo" (e.g., "uber/submitqueue")
	//   - Perforce: depot path (e.g., "//depot/project")
	//   - SVN: repository URL (e.g., "https://svn.example.com/repos/project")
	RepositoryID string

	// DestinationRef is the VCS-specific reference for the landing target.
	// Opaque to the system; meaningful only to the change provider.
	// Examples:
	//   - Git: branch name (e.g., "main", "release/v2")
	//   - Perforce: stream or depot path (e.g., "//depot/main/...")
	//   - SVN: repository path (e.g., "trunk/")
	//   - Mercurial: bookmark name (e.g., "main")
	DestinationRef string

	// ChangeProviderNames identifies which change providers to use (e.g., "github", "gerrit").
	// A queue may use multiple providers simultaneously (e.g., "github" and "phabricator").
	// Resolved to ChangeProvider instances at wiring time.
	ChangeProviderNames []string
}

// NewQueueConfig creates a new QueueConfig with the given parameters.
func NewQueueConfig(
	name string,
	repositoryID string,
	destinationRef string,
	changeProviderNames []string,
) QueueConfig {
	return QueueConfig{
		Name:                name,
		RepositoryID:        repositoryID,
		DestinationRef:      destinationRef,
		ChangeProviderNames: changeProviderNames,
	}
}
