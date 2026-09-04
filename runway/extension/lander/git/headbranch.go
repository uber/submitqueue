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
)

// headBranchPrefix is the ref namespace a change's head branch lives in. Only
// branches are candidates: a provider's own change refs (refs/pull/*/head,
// refs/merge-requests/*/head) are published by the provider and not writable.
const headBranchPrefix = "refs/heads/"

// headUpdate pairs a change with the commit that now represents it on the
// target, so its head branch can be moved there before the target is pushed.
type headUpdate struct {
	// ref is the change as resolved from its URI, pinning the commit the head
	// branch is expected to still be at.
	ref changeRef
	// newSHA is the commit produced on the target for this change — the last
	// commit of its replayed range, or the single commit it squashed to.
	newSHA string
}

// headBranchTracker records, per change, the branch its head was found on and
// the commit this lander last pushed that branch to. It is keyed by the commit
// the change's URI pins, which is fixed for the whole request and so survives
// the retries the value has to cross.
//
// It exists because a branch only answers to the pinned SHA until the first time
// it is moved. An attempt that moves the branch and then fails to push the
// target leaves the branch on a commit that never landed: the next attempt would
// find nothing at the pinned SHA and skip the change as if it had no branch,
// stranding it there. What that attempt learned — which branch, and what value
// the next lease must name — is only available from here.
type headBranchTracker map[string]trackedBranch

// trackedBranch is one change's resolved head branch and the commit this lander
// last pushed it to.
type trackedBranch struct {
	// branch is the fully-qualified ref resolved on the first attempt.
	branch string
	// sha is the commit the branch was last pushed to, and therefore the value
	// the next attempt's lease must name.
	sha string
}

// lookup returns the branch and lease value recorded for a change, if any.
func (t headBranchTracker) lookup(pinnedSHA string) (branch, lease string, ok bool) {
	tb, ok := t[pinnedSHA]
	return tb.branch, tb.sha, ok
}

// record notes where a change's head branch now sits. A nil tracker discards the
// record, which is what a caller driving a single update directly wants.
func (t headBranchTracker) record(pinnedSHA, branch, sha string) {
	if t == nil {
		return
	}
	t[pinnedSHA] = trackedBranch{branch: branch, sha: sha}
}

// updateHeadBranches moves each change's head branch to the commit that now
// represents it on the target, as its own push before the target is pushed.
//
// A provider decides whether a change merged while it processes the push to the
// target branch, comparing the change's recorded head against what that push
// makes reachable. The rewriting strategies break that comparison: the commits
// pushed to the target are new objects, so the change's own head appears nowhere
// in the target's history. Repointing the head branch at the commit the change
// became restores it — but only if the provider has already recorded the new
// head by the time it sees the target move. That is why this is a separate,
// earlier push: moving the branch after the target has been pushed, or in the
// same atomic push, both leave the provider comparing against the pre-land head
// and it records the change as closed rather than merged.
//
// This is deliberately provider-neutral. It matches a SHA against the remote's
// branch tips rather than parsing a change number or calling an API, so it works
// the same for a GitHub pull request, a GitLab merge request, or a plain branch.
//
// A failure to move a branch fails the land, before the target is pushed.
// Landing a change while knowing its head could not be moved produces exactly
// the partially landed state the option exists to prevent, so the land stops instead
// of completing into it. Having nothing to move is not a failure: a change
// proposed from a fork, or one whose head several branches share, is skipped and
// the land carries on.
func (m *gitLander) updateHeadBranches(ctx context.Context, updates []headUpdate, tracked headBranchTracker) error {
	// Resolved lazily: an attempt whose changes were all resolved by an earlier
	// one needs no advertisement, and asking for one only adds a way to fail.
	var tips map[string][]string
	for _, u := range updates {
		if u.newSHA == "" || u.newSHA == u.ref.SHA {
			continue
		}
		if _, _, known := tracked.lookup(u.ref.SHA); !known && tips == nil {
			var err error
			if tips, err = m.remoteBranchTips(ctx); err != nil {
				coremetrics.NamedCounter(m.metricsScope, "head_branch", "list_errors", 1)
				return fmt.Errorf("list branches on remote %s: %w", m.remote, err)
			}
		}
		if err := m.updateHeadBranchFor(ctx, u, tips, tracked); err != nil {
			return err
		}
	}
	return nil
}

// updateHeadBranchFor moves one change's head branch. It reports an error only
// when a move was attempted and failed; a change with no branch of ours to move
// is a skip, not a failure.
func (m *gitLander) updateHeadBranchFor(ctx context.Context, u headUpdate, tips map[string][]string, tracked headBranchTracker) error {
	if u.newSHA == "" || u.newSHA == u.ref.SHA {
		return nil
	}

	// A branch an earlier attempt already moved no longer sits at the SHA the URI
	// pinned, so the tips will not name it and only the tracker can.
	branch, lease, known := tracked.lookup(u.ref.SHA)
	if !known {
		candidates := tips[u.ref.SHA]
		switch len(candidates) {
		case 1:
			branch, lease = candidates[0], u.ref.SHA
		case 0:
			// Nothing on this remote is at the change's head. The ordinary cause is
			// a change proposed from a fork, whose head branch lives in another
			// repository entirely and is not ours to move; a deleted or already
			// advanced branch lands here too. All are left alone.
			coremetrics.NamedCounter(m.metricsScope, "head_branch", "no_branch", 1)
			m.logger.Debugw("no head branch on this remote for change; skipping",
				"change", u.ref.Label, "head_sha", u.ref.SHA)
			return nil
		default:
			// Several branches sit on the same commit and nothing in the URI says
			// which one the change was proposed from. Guessing risks rewriting an
			// unrelated branch, so decline.
			coremetrics.NamedCounter(m.metricsScope, "head_branch", "ambiguous", 1)
			m.logger.Warnw("several branches point at the change head; skipping",
				"change", u.ref.Label, "head_sha", u.ref.SHA, "branches", candidates)
			return nil
		}
	}

	// --force-with-lease names the remote ref and the value it must still hold —
	// the SHA the change's URI pinned, or, once an earlier attempt has moved the
	// branch, whatever that attempt left it at. An author who pushed while the
	// batch was building fails this check, and the land stops rather than
	// discarding their push. The explicit form is what makes the check
	// independent of whatever stale remote-tracking refs this checkout holds.
	leaseArg := fmt.Sprintf("--force-with-lease=%s:%s", branch, lease)
	refspec := u.newSHA + ":" + branch
	if _, err := m.run(ctx, nil, "push", leaseArg, m.remote, refspec); err != nil {
		coremetrics.NamedCounter(m.metricsScope, "head_branch", "push_errors", 1)
		return fmt.Errorf("move head branch %s of change %s from %s to %s: %w",
			branch, u.ref.Label, lease, u.newSHA, err)
	}
	tracked.record(u.ref.SHA, branch, u.newSHA)

	coremetrics.NamedCounter(m.metricsScope, "head_branch", "updated", 1)
	m.logger.Infow("moved change head branch to its landed commit",
		"change", u.ref.Label, "branch", branch, "from", lease, "to", u.newSHA)
	return nil
}

// remoteBranchTips maps each commit on the remote's branches to the branches
// pointing at it, in one ref advertisement rather than one per change.
//
// The target branch is excluded. It is not a change's head branch, and a change
// whose head happens to equal the target tip would otherwise make this rewrite
// the very branch the operation just landed on.
func (m *gitLander) remoteBranchTips(ctx context.Context) (map[string][]string, error) {
	out, err := m.run(ctx, nil, "ls-remote", "--heads", m.remote)
	if err != nil {
		return nil, fmt.Errorf("git ls-remote --heads %s: %w", m.remote, err)
	}

	targetRef := headBranchPrefix + m.target
	tips := make(map[string][]string)
	for _, line := range strings.Split(string(out), "\n") {
		sha, ref, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok {
			continue
		}
		ref = strings.TrimSpace(ref)
		if ref == targetRef || !strings.HasPrefix(ref, headBranchPrefix) {
			continue
		}
		tips[sha] = append(tips[sha], ref)
	}
	return tips, nil
}
