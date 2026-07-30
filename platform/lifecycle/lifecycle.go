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

// Package lifecycle provides the Component interface and Group type for
// managing ordered start/stop lifecycles. Every runnable subsystem (consumer,
// publisher, server) implements Component; Group composes them into a single
// Component with deterministic ordering and rollback on partial failure.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
)

// Component is anything with a lifecycle. Construct returns one; hosts drive it.
type Component interface {
	// Start initializes and starts the component. The context governs the
	// start-up phase (e.g. connecting, subscribing); long-running work may
	// outlive the context and must be terminated by calling Stop.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the component. The context provides a
	// deadline for the shutdown; implementations should respect it and
	// return promptly when the context is cancelled.
	Stop(ctx context.Context) error
}

// Group runs an ordered list of Components as one Component.
//
//   - Start: members in order; if member i fails to start, members i-1…0 are
//     stopped in reverse and the error is returned — no half-started state.
//   - Stop: members in REVERSE order (work-acceptors drain before the
//     connections under them close); errors joined, none swallowed.
type Group struct {
	members []Component
}

// NewGroup creates a Group from the given components. Nil members are silently
// skipped so callers can pass optional components without nil-checking.
func NewGroup(members ...Component) *Group {
	filtered := make([]Component, 0, len(members))
	for _, m := range members {
		if m != nil {
			filtered = append(filtered, m)
		}
	}
	return &Group{members: filtered}
}

// Start starts all members in order. If any member fails to start, all
// previously started members are stopped in reverse order and the original
// start error is returned. The stop errors from rollback, if any, are joined
// with the start error.
func (g *Group) Start(ctx context.Context) error {
	for i, m := range g.members {
		if err := m.Start(ctx); err != nil {
			// Rollback: stop members i-1…0 in reverse order.
			rollbackErr := g.stopRange(ctx, i-1)
			return errors.Join(fmt.Errorf("component %d failed to start: %w", i, err), rollbackErr)
		}
	}
	return nil
}

// Stop stops all members in reverse order. All stop errors are joined so
// none is swallowed; a single member's failure does not prevent the others
// from being stopped.
func (g *Group) Stop(ctx context.Context) error {
	return g.stopRange(ctx, len(g.members)-1)
}

// stopRange stops members from index hi down to 0 (inclusive), collecting
// all errors. A negative hi is a no-op.
func (g *Group) stopRange(ctx context.Context, hi int) error {
	var errs []error
	for i := hi; i >= 0; i-- {
		if err := g.members[i].Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("component %d failed to stop: %w", i, err))
		}
	}
	return errors.Join(errs...)
}
