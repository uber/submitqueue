// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package lander defines the pluggable interface for version-control land
// operations that Runway performs on behalf of its callers. Implementations
// resolve change URIs, apply changes to a land target, and (for a committing
// land) push the result and finalize the change lifecycle (e.g. close PRs).
package lander

//go:generate mockgen -source=lander.go -destination=mock/lander_mock.go -package=mock

import (
	"context"
	"errors"

	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
)

// ErrConflict signals that the ordered steps could not be applied cleanly.
// Controllers treat this as an expected outcome (ack + publish a failure
// result), not an infrastructure error.
var ErrConflict = errors.New("land conflict")

// ErrInvalidRequest signals that the request can never be applied as written —
// an unknown land strategy, a malformed change URI, or an invalid strategy
// composition (e.g. PROMOTE mixed with other steps). Like ErrConflict it is a
// terminal outcome: retrying never succeeds, so controllers ack and publish a
// failure result rather than nacking into an infinite retry / dead-letter.
var ErrInvalidRequest = errors.New("invalid land request")

// IsTerminal reports whether err is a terminal land outcome — one the client
// must be told about via a FAILED result rather than retried. Controllers use
// it to decide between publishing a FAILED result (ack) and nacking for retry.
func IsTerminal(err error) bool {
	return errors.Is(err, ErrConflict) || errors.Is(err, ErrInvalidRequest)
}

// Lander performs version-control operations against a single land target.
// Both methods accept the same LandRequest payload; the behavioral difference
// is whether the result is committed to the remote.
type Lander interface {
	// CheckLandability performs a dry-run land without committing. The
	// returned LandResult reports per-step landability; Outputs are empty.
	CheckLandability(ctx context.Context, req *runwaymq.LandRequest) (*runwaymq.LandResult, error)
	// Land applies the ordered steps, commits the result to the remote, and
	// reports per-step Outputs (the VCS-neutral revision identifiers produced).
	Land(ctx context.Context, req *runwaymq.LandRequest) (*runwaymq.LandResult, error)
}

// Config identifies the land target a Lander instance operates on. The factory
// resolves deployment-specific details (remote URL, credentials) from this.
type Config struct {
	// QueueName is the caller-provided queue name from the LandRequest.
	QueueName string
}

// Factory creates Lander instances bound to a land target.
type Factory interface {
	// For returns a Lander instance configured for the given land target.
	For(cfg Config) (Lander, error)
}
