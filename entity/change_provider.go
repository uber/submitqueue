package entity

// ChangeProvider represents a code change from an external provider (e.g., a GitHub pull request or Gerrit changelist)
// along with its associated metadata. The object is immutable after creation.
type ChangeProvider struct {
	// RequestID is the globally unique identifier for the land request. Format: "<queue>/<counter_value>".
	RequestID string
	// ChangeProviderSrc defines the source of the change. For e.g. - Github, Gitlab etc.
	ChangeProviderSrc string
	// ChangeProviderID is the identifier specified by the change provider source. For e.g. - Github PR ID etc.
	ChangeProviderID string
	// Metadata is the interesting data from the change provider that we want to store.
	// This is a freeform JSON object.
	Metadata map[string]string
}
