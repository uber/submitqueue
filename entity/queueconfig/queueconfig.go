package queueconfig

// Repository identifies a source control repository.
// The interpretation of ID is platform-specific and resolved by the change provider:
//   - GitHub/GitLab: "owner/repo" (e.g., "uber/submitqueue")
//   - Perforce: depot path (e.g., "//depot/project")
//   - SVN: repository URL (e.g., "https://svn.example.com/repos/project")
//
// Immutable after creation.
type Repository struct {
	// ID is the platform-specific identifier for the repository.
	// Opaque to the system; meaningful only to the change provider.
	ID string
}

// Destination identifies the landing target in a version control system.
// The interpretation of Ref is VCS-specific and resolved by the change provider:
//   - Git: branch name (e.g., "main", "release/v2")
//   - Perforce: stream or depot path (e.g., "//depot/main/...")
//   - SVN: repository path (e.g., "trunk/")
//   - Mercurial: bookmark name (e.g., "main")
//
// Immutable after creation.
type Destination struct {
	// Ref is the version-control-specific reference for the landing target.
	// Opaque to the system; meaningful only to the change provider.
	Ref string
}

// QueueConfig holds the configuration for a single submit queue.
// Each queue maps a repository + destination to a processing pipeline.
// A repository can have multiple queues, but each queue has exactly one destination.
// Immutable after creation.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	// Referenced by entity.Request.Queue.
	Name string

	// Repository identifies the source control repository this queue operates on.
	Repository Repository

	// Destination is the landing target where changes are merged.
	Destination Destination

	// ChangeProviderName identifies which change provider to use (e.g., "github", "gerrit").
	// Resolved to a ChangeProvider instance at wiring time.
	ChangeProviderName string

	// ChangeProvider is the change provider to use for this queue.
	// TODO:
	// ChangeProvider ChangeProvider to be defined in the changeprovider extension package
}

// NewQueueConfig creates a new QueueConfig with the given parameters.
func NewQueueConfig(
	name string,
	repository Repository,
	destination Destination,
	changeProviderName string,
) QueueConfig {
	// Create instance of ChangeProvider instance from the change provider name
	return QueueConfig{
		Name:               name,
		Repository:         repository,
		Destination:        destination,
		ChangeProviderName: changeProviderName,
	}
}
