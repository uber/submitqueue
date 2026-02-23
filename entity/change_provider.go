package entity

type ChangeProvider struct {
	// ID is the globally unique identifier for the land request. Format: "<queue>/<counter_value>".
	ID string
	// Queue is the name of the queue processing the land request.
	// This is defined in the configuration and should be unique within the system.
	Queue string
	// ChangeProviderSrc defines the source of the change. For e.g. - Github, Gitlab etc.
	ChangeProviderSrc string
	// ChangeProviderID is the identifier specified by the change provider source. For e.g. - Github PR ID etc.
	ChangeProviderID string
	// Metadata is the interesting data from the change provider that we want to store.
	// This is a freeform JSON object.
	Metadata map[string]any
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}
