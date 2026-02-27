package entity

import "encoding/json"


// RequestLandStrategy defines the possible source control integration methods.
type RequestLandStrategy string

const (
	// RequestLandStrategyUnknown is the unknown strategy. It is set by default when the structure is initialized. It should never be seen in the system and used for error control.
	RequestLandStrategyUnknown RequestLandStrategy = ""
	// RequestLandStrategyRebase rebases commits onto the target branch before landing.
	RequestLandStrategyRebase RequestLandStrategy = "rebase"
	// RequestLandStrategySquashRebase squashes commits into a single commit before rebasing on top of the target branch.
	RequestLandStrategySquashRebase RequestLandStrategy = "squash_rebase"
	// RequestLandStrategyMerge merges commits into the target branch by creating a separate merge commit, preserving the commit history along with hashes.
	RequestLandStrategyMerge RequestLandStrategy = "merge"
)

// RequestState defines the possible states of a land request.
type RequestState string

const (
	// RequestStateUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	RequestStateUnknown RequestState = ""
	// RequestStateNew is the initial state of a land request. It is confirmed by the system but the processing is not started yet.
	RequestStateNew RequestState = "new"
	// RequestStateValidated indicates that the request has been validated (duplicate check, merge check etc.).
	RequestStateValidated RequestState = "validated"
	// RequestStateProcessing is the state of a land request that is being processed.
	RequestStateProcessing RequestState = "processing"
	// RequestStateLanded is the state of a land request that has been successfully processed and landed. This is the final state.
	RequestStateLanded RequestState = "landed"
	// RequestStateError is the state of a land request that has encountered an error. This is the final state.
	RequestStateError RequestState = "error"
)

// Change represents a code change identified by URIs from a code change provider (e.g., GitHub Pull Request, Phabricator Diff).
// The provider is extracted from the URI scheme. The object is immutable after creation.
type Change struct {
	// URIs identifies the change(s) to land (RFC 3986 compliant).
	// The scheme identifies the change provider, and the path contains provider-specific resource identifiers.
	//
	// GitHub is supported by default (though other providers can be added):
	//   Template: "github://<org>/<repo>/pull/<pr>/<hash>"
	//   Example: "github://uber/submitqueue/pull/123/abc123def"
	//
	URIs []string `json:"uris"`
}

// Request defines a request to land (merge into target branch of the source control repository) a set of code changes.
// The object is immutable after creation.
type Request struct {
	// ****************
	// Immutable fields, fixed at request entity creation
	// ****************

	// ID is the globally unique identifier for the land request. Format: "<queue>/<counter_value>".
	ID string `json:"id"`
	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string `json:"queue"`
	// Change is a number of code changes (such as pull requests) to land into the target branch. Target branch is defined by the queue configuration.
	Change Change `json:"change"`
	// LandStrategy is the source control integration strategy to use for this land operation.
	LandStrategy RequestLandStrategy `json:"land_strategy"`

	// ****************
	// Following fields could be changed throughout the lifecycle of the request
	// ****************

	// State is the current state of the land request.
	State RequestState `json:"state"`
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32 `json:"version"`
}

// ToBytes serializes the Request to JSON bytes for queue message payload.
func (r Request) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// RequestFromBytes deserializes a Request from JSON bytes.
func RequestFromBytes(data []byte) (Request, error) {
	var req Request
	err := json.Unmarshal(data, &req)
	return req, err
}
