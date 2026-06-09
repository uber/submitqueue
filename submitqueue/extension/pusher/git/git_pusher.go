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

// Package git is a simple Pusher implementation backed by a local git
// checkout.
//
// On every Push call the implementation:
//
//  1. Fetches the configured remote.
//  2. Resets the checkout's HEAD to refs/remotes/<remote>/<target>.
//  3. Cherry-picks the head SHA of every URI of every Change, in order.
//     A pick that produces no new content (because the change is already
//     present on the target branch) is what git surfaces as "rebased out"
//     and is recorded as OutcomeStatusAlreadyExisted.
//     A pick that fails to apply cleanly is treated as a conflict.
//  4. Pushes HEAD to refs/heads/<target> on the remote.
//
// Atomicity: nothing is published to the remote until step 4 succeeds. If
// any cherry-pick fails the in-progress pick is aborted and Push returns an
// error without ever invoking step 4.
//
// Contention: if step 4 fails because the remote tip moved between step 2
// and step 4 (typically a concurrent push from another writer), the whole
// fetch/reset/cherry-pick/push cycle is retried. Detection works by
// re-fetching the remote tip after a push failure and comparing it to the
// SHA we reset to in step 2: if it advanced, the failure is treated as
// contention. Other push failures (network, auth, hook reject without ref
// change) propagate immediately. Retries are capped at
// Params.MaxPushAttempts (default 10) to bound the worst case.
//
// Construction takes the path to an existing checkout, the remote name, and
// the target branch — the implementation owns the working tree at that path
// for the duration of any in-flight Push call and serializes concurrent
// invocations.
//
// Change URIs are parsed using the github-family URI format (see
// entity/github), so each URI's last segment is interpreted as the head
// commit SHA. The SHA must be reachable from a ref on the remote so that
// `git fetch` makes it available locally.
package git

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	coremetrics "github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	entitygithub "github.com/uber/submitqueue/submitqueue/entity/github"
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
)

// defaultMaxPushAttempts caps the retry loop in Push when the remote tip
// keeps moving under us. Bounded to prevent an infinite loop against a
// pathologically busy remote.
const defaultMaxPushAttempts = 10

// Params holds the dependencies for the git Pusher.
type Params struct {
	// CheckoutPath is the absolute path to an existing git checkout that the
	// Pusher will operate against. The Pusher owns this working tree.
	CheckoutPath string
	// Remote is the name of the git remote to fetch from and push to
	// (e.g. "origin").
	Remote string
	// Target is the destination branch ref on the remote (e.g. "main").
	Target string
	// Resolver resolves each batch's changes.
	Resolver changeset.Resolver
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
	// MaxPushAttempts caps how many times Push retries the full
	// fetch/reset/cherry-pick/push cycle when the remote tip moves under
	// it. Defaults to defaultMaxPushAttempts when zero or negative.
	MaxPushAttempts int
}

// gitPusher implements pusher.Pusher by shelling out to the `git` CLI
// against a local checkout.
type gitPusher struct {
	checkoutPath    string
	remote          string
	target          string
	resolver        changeset.Resolver
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	maxPushAttempts int

	// mu serializes concurrent Push calls — the underlying checkout cannot
	// be safely shared between operations.
	mu sync.Mutex
}

// Verify gitPusher implements pusher.Pusher at compile time.
var _ pusher.Pusher = (*gitPusher)(nil)

// NewPusher constructs a new git-backed Pusher operating against the given
// checkout. The checkout must already exist and have the configured remote.
func NewPusher(params Params) pusher.Pusher {
	maxAttempts := params.MaxPushAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxPushAttempts
	}
	return &gitPusher{
		checkoutPath:    params.CheckoutPath,
		remote:          params.Remote,
		target:          params.Target,
		resolver:        params.Resolver,
		logger:          params.Logger.Named("git_pusher"),
		metricsScope:    params.MetricsScope.SubScope("git_pusher"),
		maxPushAttempts: maxAttempts,
	}
}

// Push fulfils the pusher.Pusher contract.
func (p *gitPusher) Push(ctx context.Context, batches []entity.Batch) (ret entity.PushResult, retErr error) {
	op := coremetrics.Begin(p.metricsScope, "push")
	defer func() { op.Complete(retErr) }()

	// Resolve each batch's changes, keeping per-batch counts so the flat
	// outcomes can be regrouped per batch on success.
	perBatch := make([][]entity.Change, len(batches))
	var changes []entity.Change
	for i, b := range batches {
		cs, err := p.resolver.ChangesForBatch(ctx, b)
		if err != nil {
			return entity.PushResult{}, fmt.Errorf("resolve batch %s: %w", b.ID, err)
		}
		perBatch[i] = cs
		changes = append(changes, cs...)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(changes) == 0 {
		coremetrics.NamedCounter(p.metricsScope, "push", "empty_changes", 1)
		return entity.PushResult{}, fmt.Errorf("push called with no changes")
	}

	p.logger.Debugw("starting push",
		"target", p.target,
		"remote", p.remote,
		"change_count", len(changes),
	)

	var lastErr error
	for attempt := 1; attempt <= p.maxPushAttempts; attempt++ {
		baseSHA, outcomes, err := p.tryPush(ctx, changes)
		if err == nil {
			p.logger.Debugw("push complete",
				"target", p.target,
				"outcomes", outcomes,
			)
			return entity.PushResult{Batches: groupByBatch(batches, perBatch, outcomes)}, nil
		}

		// Was the failure caused by the remote tip moving under us between
		// reset and push (concurrent-push contention)? That's the only push
		// failure worth retrying; everything else (network, auth, hook
		// reject without ref change) is fatal. Detection is by re-fetching
		// the remote tip and comparing to baseSHA — robust against varying
		// git error message formats. baseSHA is empty when the failure
		// happened before reset captured a base; treat those as fatal too.
		if baseSHA == "" {
			return entity.PushResult{}, err
		}
		currentSHA, refetchErr := p.refetchTipSHA(ctx)
		if refetchErr != nil {
			return entity.PushResult{}, fmt.Errorf("refetch after push failure failed: %v (original push error: %w)", refetchErr, err)
		}
		if currentSHA == baseSHA {
			return entity.PushResult{}, err
		}

		coremetrics.NamedCounter(p.metricsScope, "push", "stale_base_retries", 1)
		p.logger.Warnw("remote tip moved during push, retrying",
			"attempt", attempt,
			"max_attempts", p.maxPushAttempts,
			"base_sha", baseSHA,
			"current_sha", currentSHA,
			"err", err,
		)
		lastErr = err
	}

	coremetrics.NamedCounter(p.metricsScope, "push", "stale_base_giveup", 1)
	return entity.PushResult{}, fmt.Errorf("exceeded %d push attempts due to remote contention: %w", p.maxPushAttempts, lastErr)
}

// groupByBatch splits the flat, apply-ordered outcomes back into one
// BatchOutcome per input batch, using each batch's resolved change count.
func groupByBatch(batches []entity.Batch, perBatch [][]entity.Change, outcomes []entity.ChangeOutcome) []entity.BatchOutcome {
	result := make([]entity.BatchOutcome, len(batches))
	pos := 0
	for i, b := range batches {
		n := len(perBatch[i])
		result[i] = entity.BatchOutcome{BatchID: b.ID, Outcomes: outcomes[pos : pos+n]}
		pos += n
	}
	return result
}

// tryPush runs one full reset+cherry-pick+push cycle. The returned baseSHA
// is the SHA the cycle was based on (set as soon as resetToRemote completes)
// so the caller can distinguish concurrent-push contention from other push
// failures. baseSHA is empty when the failure happened before reset
// produced a base.
func (p *gitPusher) tryPush(ctx context.Context, changes []entity.Change) (string, []entity.ChangeOutcome, error) {
	if err := p.resetToRemote(ctx); err != nil {
		coremetrics.NamedCounter(p.metricsScope, "push", "reset_errors", 1)
		return "", nil, err
	}
	baseSHA, err := p.headSHA(ctx)
	if err != nil {
		return "", nil, err
	}

	outcomes, err := p.cherryPickAll(ctx, changes)
	if err != nil {
		// Best-effort cleanup so the next attempt starts from a known state.
		// The next attempt starts with resetToRemote regardless, so we don't
		// care if --abort itself fails (e.g., no pick is in progress).
		_, _ = p.run(ctx, nil, "cherry-pick", "--abort")
		return baseSHA, nil, err
	}

	if err := p.push(ctx); err != nil {
		coremetrics.NamedCounter(p.metricsScope, "push", "git_push_errors", 1)
		return baseSHA, nil, err
	}
	return baseSHA, outcomes, nil
}

// headSHA returns the SHA at HEAD in the local checkout.
func (p *gitPusher) headSHA(ctx context.Context) (string, error) {
	out, err := p.run(ctx, nil, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// refetchTipSHA fetches the remote and returns the current SHA at
// refs/remotes/<remote>/<target>. Used after a push failure to detect
// whether the remote tip moved under us.
func (p *gitPusher) refetchTipSHA(ctx context.Context) (string, error) {
	if _, err := p.run(ctx, nil, "fetch", p.remote); err != nil {
		return "", fmt.Errorf("git fetch %s: %w", p.remote, err)
	}
	remoteRef := p.remote + "/" + p.target
	out, err := p.run(ctx, nil, "rev-parse", remoteRef)
	if err != nil {
		return "", fmt.Errorf("git rev-parse %s: %w", remoteRef, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// resetToRemote fetches the configured remote and hard-resets the checkout's
// HEAD to refs/remotes/<remote>/<target>, producing a clean working tree
// pinned to the latest remote tip.
func (p *gitPusher) resetToRemote(ctx context.Context) error {
	if _, err := p.run(ctx, nil, "fetch", p.remote); err != nil {
		return fmt.Errorf("git fetch %s: %w", p.remote, err)
	}
	remoteRef := p.remote + "/" + p.target
	if _, err := p.run(ctx, nil, "reset", "--hard", remoteRef); err != nil {
		return fmt.Errorf("git reset --hard %s: %w", remoteRef, err)
	}
	if _, err := p.run(ctx, nil, "clean", "-fdx"); err != nil {
		return fmt.Errorf("git clean: %w", err)
	}
	return nil
}

// cherryPickAll walks the changes in order, cherry-picking every URI's head
// SHA, and returns one ChangeOutcome per Change in the same order.
func (p *gitPusher) cherryPickAll(ctx context.Context, changes []entity.Change) ([]entity.ChangeOutcome, error) {
	outcomes := make([]entity.ChangeOutcome, 0, len(changes))
	for _, change := range changes {
		commits, err := p.cherryPickChange(ctx, change)
		if err != nil {
			return nil, err
		}
		status := entity.OutcomeStatusCommitted
		if len(commits) == 0 {
			status = entity.OutcomeStatusAlreadyExisted
		}
		outcomes = append(outcomes, entity.ChangeOutcome{
			Change:     change,
			Status:     status,
			CommitSHAs: commits,
		})
	}
	return outcomes, nil
}

// cherryPickChange parses each URI in the change, fetches the referenced
// SHA, and cherry-picks it. It returns the list of new commit SHAs
// produced for this change (empty if every pick was a no-op because the
// content was already on the target branch).
func (p *gitPusher) cherryPickChange(ctx context.Context, change entity.Change) ([]string, error) {
	var commits []string
	for _, uri := range change.URIs {
		cid, err := entitygithub.ParseChangeID(uri)
		if err != nil {
			return nil, fmt.Errorf("invalid change URI %q: %w", uri, err)
		}

		sha, picked, err := p.cherryPickSHA(ctx, cid.HeadCommitSHA)
		if err != nil {
			return nil, fmt.Errorf("cherry-pick %s (uri %q): %w", cid.HeadCommitSHA, uri, err)
		}
		if picked {
			commits = append(commits, sha)
		}
	}
	return commits, nil
}

// cherryPickSHA cherry-picks a single SHA. It returns:
//   - the new commit SHA and picked=true on a successful pick that produced
//     a non-empty commit;
//   - "" and picked=false when the pick is a no-op (the change is already
//     on the target branch); the empty commit is rolled back so it doesn't
//     get pushed;
//   - an error: pusher.ErrConflict when the pick fails to apply because of a conflict (generally a user-caused failure),
//     or any other error when the pick fails for any other reason (potentially recoverable).
//
// `--allow-empty` covers the case where the source commit itself was
// originally empty. For redundant picks (the change is already on target,
// so applying it produces no new diff) git refuses with "previous
// cherry-pick is now empty"; we recover by running `cherry-pick --skip`
// and reporting the change as already-existed (what git would call
// "rebased out").
func (p *gitPusher) cherryPickSHA(ctx context.Context, sha string) (string, bool, error) {
	out, err := p.runCombined(ctx, nil, "cherry-pick", "--allow-empty", sha)
	if err != nil {
		if isRedundantCherryPick(out) {
			if _, skipErr := p.run(ctx, nil, "cherry-pick", "--skip"); skipErr != nil {
				return "", false, fmt.Errorf("git cherry-pick --skip after redundant pick: %w", skipErr)
			}
			return "", false, nil
		}
		coremetrics.NamedCounter(p.metricsScope, "push", "cherry_pick_conflicts", 1)
		return "", false, fmt.Errorf("%w: git cherry-pick %s: %s", pusher.ErrConflict, sha, strings.TrimSpace(string(out)))
	}

	// `--allow-empty` lets a genuinely empty source commit through as an
	// empty commit on target. Detect and roll that back so it doesn't get
	// pushed, and report it as already-existed.
	empty, err := p.isEmptyHEADCommit(ctx)
	if err != nil {
		return "", false, err
	}
	if empty {
		if _, err := p.run(ctx, nil, "reset", "--hard", "HEAD^"); err != nil {
			return "", false, fmt.Errorf("git reset --hard HEAD^ after empty pick: %w", err)
		}
		return "", false, nil
	}

	headOut, err := p.run(ctx, nil, "rev-parse", "HEAD")
	if err != nil {
		return "", false, fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(headOut)), true, nil
}

// isEmptyHEADCommit returns true when HEAD's tree matches HEAD^'s tree —
// i.e. the most recent commit introduces no changes.
func (p *gitPusher) isEmptyHEADCommit(ctx context.Context) (bool, error) {
	headTree, err := p.commitTreeSHA(ctx, "HEAD")
	if err != nil {
		return false, err
	}
	parentTree, err := p.commitTreeSHA(ctx, "HEAD^")
	if err != nil {
		return false, err
	}
	return headTree == parentTree, nil
}

// commitTreeSHA returns the tree SHA recorded in the commit object at ref.
// It reads the raw commit via `git cat-file commit` and parses the leading
// `tree <sha>` line — the same data `rev-parse <ref>^{tree}` would peel to,
// but without depending on revision-syntax magic.
func (p *gitPusher) commitTreeSHA(ctx context.Context, ref string) (string, error) {
	out, err := p.run(ctx, nil, "cat-file", "commit", ref)
	if err != nil {
		return "", fmt.Errorf("git cat-file commit %s: %w", ref, err)
	}
	firstLine, _, _ := strings.Cut(string(out), "\n")
	const prefix = "tree "
	if !strings.HasPrefix(firstLine, prefix) {
		return "", fmt.Errorf("git cat-file commit %s: unexpected first line %q", ref, firstLine)
	}
	return strings.TrimSpace(firstLine[len(prefix):]), nil
}

// push pushes the current HEAD to refs/heads/<target> on the remote.
func (p *gitPusher) push(ctx context.Context) error {
	refspec := "HEAD:refs/heads/" + p.target
	if _, err := p.run(ctx, nil, "push", p.remote, refspec); err != nil {
		return fmt.Errorf("git push %s %s: %w", p.remote, refspec, err)
	}
	return nil
}

// run executes a `git` command in the checkout. Returns captured stdout and
// an error that includes captured stderr for diagnostics.
func (p *gitPusher) run(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = p.checkoutPath
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// runCombined is like run but returns combined stdout+stderr both on success
// and failure. Used when the caller needs to inspect git's diagnostic
// output (e.g., to detect "previous cherry-pick is now empty").
func (p *gitPusher) runCombined(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = p.checkoutPath
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	return cmd.CombinedOutput()
}

// isRedundantCherryPick reports whether git's cherry-pick output indicates
// the pick was rejected because the change is already present on target
// (i.e. applying it would produce no diff).
func isRedundantCherryPick(out []byte) bool {
	s := string(out)
	return strings.Contains(s, "previous cherry-pick is now empty") ||
		strings.Contains(s, "nothing to commit")
}
