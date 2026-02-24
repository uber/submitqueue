package entity

// QueueConfig holds the configuration for a single submit queue.
// Each queue maps a VCS repository + target to a processing pipeline.
// A repository can have multiple queues, but each queue has exactly one target.
// Immutable after creation.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	// Referenced by Request.Queue.
	Name string

	// VCSType identifies the version control system (e.g., "git", "svn", "perforce").
	// A queue operates on exactly one VCS.
	VCSType string

	// VCSAddress identifies the repository in the version control system.
	// The format is VCS-specific:
	//   - Git: remote URL (e.g., "git@github.com:uber/submitqueue.git")
	//   - Perforce: depot path (e.g., "//depot/project")
	//   - SVN: repository URL (e.g., "https://svn.example.com/repos/project")
	VCSAddress string

	// Target is the landing target where changes are merged.
	// The format is VCS-specific:
	//   - Git: branch ref (e.g., "main", "release/v2")
	//   - Perforce: stream or depot path (e.g., "//depot/main/...")
	//   - SVN: repository path (e.g., "trunk/")
	Target string
}

// NewQueueConfig creates a new QueueConfig with the given parameters.
func NewQueueConfig(
	name string,
	vcsType string,
	vcsAddress string,
	target string,
) QueueConfig {
	return QueueConfig{
		Name:       name,
		VCSType:    vcsType,
		VCSAddress: vcsAddress,
		Target:     target,
	}
}
