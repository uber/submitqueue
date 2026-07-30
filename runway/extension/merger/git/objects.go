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

package git

import (
	"context"
	"fmt"
	"strings"

	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/runway/extension/merger"
)

// ensureObjects makes every commit the request references available locally,
// for all steps, before any step is applied. Front-loading it means a request
// naming an unreachable commit fails without having mutated the checkout.
//
// It matters because the default fetch refspec is +refs/heads/*, which does not
// cover a provider's change refs: a pull request head that was never also
// pushed as a branch (the normal case for a fork) is simply absent locally, and
// the apply would then fail with git's "bad object" — indistinguishable, to the
// apply paths, from a merge conflict.
func (m *gitMerger) ensureObjects(ctx context.Context, refs []changeRef) error {
	for _, ref := range refs {
		if err := m.ensureObject(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

// ensureObject guarantees a single commit is present, fetching it if needed.
//
// The commit is requested by SHA, which relies on the server allowing a want
// for an object that is reachable but not advertised (uploadpack
// .allowReachableSHA1InWant, which github.com enables). The provider's
// canonical ref is the fallback for a server that does not. Neither fetch is
// shallow: the apply paths need the commit's ancestry, not just the commit.
func (m *gitMerger) ensureObject(ctx context.Context, ref changeRef) error {
	if m.hasCommit(ctx, ref.SHA) {
		return nil
	}

	if _, err := m.run(ctx, nil, "fetch", m.remote, ref.SHA); err == nil && m.hasCommit(ctx, ref.SHA) {
		return nil
	}
	if ref.Ref != "" {
		if _, err := m.run(ctx, nil, "fetch", m.remote, ref.Ref); err == nil && m.hasCommit(ctx, ref.SHA) {
			return nil
		}
	}

	// Both fetches failed. Distinguish "the remote is unreachable right now"
	// (infrastructure, worth retrying) from "the remote is fine and simply does
	// not have this commit" (terminal — no number of retries conjures it).
	if _, err := m.run(ctx, nil, "ls-remote", "--exit-code", m.remote, "HEAD"); err != nil {
		return fmt.Errorf("remote %s unreachable while resolving commit %s: %w", m.remote, ref.SHA, err)
	}

	coremetrics.NamedCounter(m.metricsScope, "merge", "object_unavailable", 1)
	// Name the change, not just the commit. A bare "commit not available"
	// sends the reader looking for a deleted commit, when the likelier cause is
	// a change this remote was never going to be able to serve.
	return fmt.Errorf("%w: commit %s of %s is not available from remote %s (tried by SHA and via %q)",
		merger.ErrInvalidRequest, ref.SHA, ref.Label, m.remote, ref.Ref)
}

// hasCommit reports whether sha names a commit object in the local checkout.
func (m *gitMerger) hasCommit(ctx context.Context, sha string) bool {
	_, err := m.run(ctx, nil, "cat-file", "-e", sha+"^{commit}")
	return err == nil
}

// checkStale reports whether any change has been superseded since its URI was
// minted. Fetching by SHA guarantees the merger applies exactly the commit the
// URI names, but not that the commit is still the change's head — a force-push
// leaves the old commit fetchable for some time on most hosts, so a successful
// fetch says nothing about freshness.
//
// The comparison costs one ref advertisement and transfers no objects. A ref
// that no longer exists yields no verdict rather than a failure: the change may
// legitimately have been closed, and ensureObjects has already established that
// the commit itself is present.
func (m *gitMerger) checkStale(ctx context.Context, refs []changeRef) error {
	if !m.checkStaleness {
		return nil
	}
	for _, ref := range refs {
		if ref.Ref == "" {
			continue
		}
		out, err := m.run(ctx, nil, "ls-remote", m.remote, ref.Ref)
		if err != nil {
			return fmt.Errorf("git ls-remote %s %s: %w", m.remote, ref.Ref, err)
		}
		fields := strings.Fields(string(out))
		if len(fields) == 0 {
			continue
		}
		if current := fields[0]; current != ref.SHA {
			coremetrics.NamedCounter(m.metricsScope, "merge", "stale_changes", 1)
			return fmt.Errorf("%w: change is stale: %s names commit %s but %s now points at %s",
				merger.ErrInvalidRequest, ref.Label, ref.SHA, ref.Ref, current)
		}
	}
	return nil
}
