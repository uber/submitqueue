package entities

import (
	"fmt"
	"strconv"
	"strings"
)

// RequestLandStrategy defines the possible source control integration methods.
type RequestLandStrategy int

const (
	// RequestLandStrategyDefault lets the server decide based on configuration.
	RequestLandStrategyDefault RequestLandStrategy = 0
	// RequestLandStrategyRebase rebases commits onto the target branch before landing.
	RequestLandStrategyRebase = 1
	// RequestLandStrategySquashRebase squashes commits into a single commit before rebase.
	RequestLandStrategySquashRebase = 2
	// RequestLandStrategyMerge merges commits into the target branch by creating a separate merge commit, preserving the commit history along with hashes.
	RequestLandStrategyMerge = 3
)

type RequestState int

// TODO: define all states
const (
	// RequestStateUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	RequestStateUnknown RequestState = 0
	// RequestStateNew is the initial state of a land request. It is confirmed by the system but the processing is not started yet.
	RequestStateNew RequestState = 1
	// RequestStateProcessing is the state of a land request that is being processed.
	RequestStateProcessing = 2
	// RequestStateLanded is the state of a land request that has been successfully processed and landed. This is the final state.
	RequestStateLanded = 3
	// RequestStateError is the state of a land request that has encountered an error. This is the final state.
	RequestStateError = 4
)

// Change represents a set of related code changes identified by one or more IDs from a particular code change provider, like Github Pull Requests.
// The object is immutable after creation.
type Change struct {
	// Source is the code change provider (e.g., "github", "gerrit", "phabricator").
	Source string
	// IDs is a list of change IDs, in a format specific to the code change provider, that should be landed together.
	IDs []string
}

// Request defines a request to land (merge into target branch of the source control repository) a set of code changes.
// The object is immutable after creation.
type Request struct {
	// ****************
	// Immutable fields, fixed at request entity creation
	// ****************

	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string
	// Seq is an autoincrementing integer identifier for the land request. It is unique within the queue.
	Seq int64
	// Change is a number of code changes (such as pull requests) to land into the target branch. Target branch is defined by the queue configuration.
	Change Change
	// LandStrategy is the source control integration strategy to use for this land operation. If not specified, the default queue strategy is used.
	LandStrategy RequestLandStrategy

	// ****************
	// Following fields could be changed throughout the lifecycle of the request
	// ****************

	// State is the current state of the land request.
	State RequestState
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}

// GetID returns the globally unique identifier for the land request.
func (r *Request) GetID() string {
	return fmt.Sprintf("%s/%d", r.Queue, r.Seq)
}

// ParseRequestID parses the globally unique identifier for the land request and returns the queue name and sequence number.
func ParseRequestID(id string) (queue string, seq int64, err error) {
	parts := strings.Split(id, "/")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid format of the request ID: %s; expected format: <queue>/<seq>", id)
	}

	seq, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid sequence number in the request ID: %s; expected format: <queue>/<seq>; parsing error: %w", id, err)
	}

	if seq <= 0 {
		return "", 0, fmt.Errorf("invalid sequence number in the request ID: %s; expected format: <queue>/<seq>; sequence number must be greater than 0 but got %d", id, seq)
	}

	return parts[0], seq, nil
}
