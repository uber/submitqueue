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

// Package sourcecontrol defines the contract through which Stovepipe talks to a
// version control system. It is the sole owner of URI semantics: a URI is an
// opaque, VCS-agnostic locator of a commit (e.g. "git://remote/repo/ref/.../<sha>"
// for the reference git backend, but a Mercurial or Perforce backend would mint
// its own scheme). Nothing outside an implementation parses a URI — it is a token
// handed back to ask questions ("what is the latest commit of this ref?", "is A an
// ancestor of B?"). A SourceControl is bound to a single queue (a repo+ref) at
// construction by its Factory, so its methods take no queue argument.
package sourcecontrol

//go:generate mockgen -source=sourcecontrol.go -destination=mock/sourcecontrol_mock.go -package=mock

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/platform/base/page"
)

// ErrNotFound is returned when a queue, ref, or URI cannot be resolved by the
// implementation (for example an unknown queue, or an ancestry query referencing
// a URI that is not on the ref).
var ErrNotFound = errors.New("source control reference not found")

// IsNotFound returns true if any error in the error chain is an ErrNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// WrapNotFound wraps ErrNotFound with the original error from the implementation.
func WrapNotFound(err error) error {
	return fmt.Errorf("%w: %w", ErrNotFound, err)
}

// SourceControl resolves and compares commit URIs for the single queue it is
// bound to. Implementations interpret URIs; callers treat them as opaque tokens.
type SourceControl interface {
	// Latest returns the URI of the latest commit on the queue's ref — the
	// commit a new validation Request is minted against. Returns ErrNotFound if
	// the queue or ref cannot be resolved.
	Latest(ctx context.Context) (string, error)

	// IsAncestor reports whether ancestor is an ancestor of descendant in the
	// queue's history. Stovepipe uses it to decide the build strategy: when the
	// last-green URI is no longer an ancestor of the latest commit (false),
	// history was rewritten and a full build is required instead of an
	// incremental one. Returns ErrNotFound if either URI is unknown to the ref.
	IsAncestor(ctx context.Context, ancestor, descendant string) (bool, error)

	// History returns a bounded page of the queue's commit URIs, newest first.
	// It is paginated with an opaque cursor so a remote backend stays cheap:
	// callers pass an empty cursor for the first (newest) page and the page's
	// NextCursor to walk further back, stopping when NextCursor is empty. limit
	// caps the page size; a limit of zero or less lets the implementation choose a
	// default. Callers join the returned URIs against the request store to render
	// the greenness/status of each commit. Returns ErrNotFound if the cursor does
	// not refer to a position on the ref.
	History(ctx context.Context, cursor string, limit int) (page.Page[string], error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (the VCS endpoint,
// credentials, the ref it maps to) is injected at construction by the integrator.
type Config struct {
	// QueueName identifies the queue (a repo+ref) this SourceControl serves.
	QueueName string
}

// Factory builds the SourceControl for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction. The
// per-queue routing adapter lives in the wiring layer, not here.
type Factory interface {
	// For returns the SourceControl for the given queue.
	For(cfg Config) (SourceControl, error)
}
