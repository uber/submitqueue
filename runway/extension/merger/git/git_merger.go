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

// Package git is a Merger implementation backed by a local git checkout. It
// applies a MergeRequest's ordered steps onto a target branch, honoring each
// step's merge strategy, and (for a committing Merge) pushes the result.
//
// Strategy → git operation:
//
//   - REBASE:        cherry-pick each URI's head commit onto the target tip.
//   - DEFAULT:       resolved to the instance's configured DefaultStrategy
//     before any step runs.
//
// REBASE is the only strategy implemented so far. A step naming any other
// strategy is rejected as merger.ErrInvalidRequest until its apply path lands.
//
// Atomicity: for a committing merge nothing reaches the remote until the final
// push. A step that fails to apply aborts the in-progress git operation and
// returns without pushing.
//
// Contention: if the push fails because the remote tip moved between reset and
// push, the whole reset/apply/push cycle is retried up to Params.MaxPushAttempts
// (default 10). Detection re-fetches the remote tip after a push failure and
// compares it to the SHA reset to at the start of the cycle.
//
// Dry-run (CheckMergeability) applies the exact same steps but never pushes;
// intermediate steps are committed locally so a cumulative multi-step check
// sees the same conflict surface, then the checkout is reset to discard them.
// Reported StepResults carry no Outputs.
//
// Change URIs are resolved through resolveChange (see changeref.go), which
// reduces any supported provider URI to the commit to apply, the ref that
// commit canonically lives under, and a label. github:// and git:// are
// supported; an unrecognized scheme is a terminal invalid request.
//
// Object availability: every referenced commit is fetched and verified before
// any step is applied. A change is replayed as the full range from its merge
// base with the target up to its head, because a URI pins a pull request to a
// single head SHA while the pull request itself is routinely several commits.
package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/runway/extension/merger"
)

// defaultMaxPushAttempts caps the retry loop when the remote tip keeps moving
// under us. Bounded to prevent an infinite loop against a busy remote.
const defaultMaxPushAttempts = 10

// Default committer identity used when Params leaves it unset. The scrubbed
// environment (GIT_CONFIG_NOSYSTEM, GIT_CONFIG_GLOBAL=/dev/null) leaves no
// ambient identity, so the commit-creating REBASE strategy needs one supplied
// explicitly.
const (
	defaultCommitterName  = "SubmitQueue Runway"
	defaultCommitterEmail = "runway@submitqueue.invalid"
)

// GitRuntime identifies the explicitly provided Git runtime used by the Merger.
type GitRuntime struct {
	// Executable is the absolute path to the Git executable.
	Executable string
	// ExecPath is the absolute directory containing Git's helper executables.
	ExecPath string
	// TemplateDir is the absolute directory containing Git's repository
	// templates.
	TemplateDir string
	// PassthroughEnv names additional environment variables to inherit from
	// the parent process, on top of the auth and transport ones always passed
	// through. For a deployment whose remote needs something unusual; leave
	// empty otherwise. Names that could alter merge semantics do not belong
	// here — the scrubbed environment is what keeps a merge reproducible.
	PassthroughEnv []string
}

// Params holds the dependencies for the git Merger.
type Params struct {
	// CheckoutPath is the absolute path to an existing git checkout that the
	// Merger will operate against. The Merger owns this working tree.
	CheckoutPath string
	// Remote is the name of the git remote to fetch from and push to
	// (e.g. "origin").
	Remote string
	// Target is the destination branch ref on the remote (e.g. "main").
	Target string
	// DefaultStrategy resolves a step whose strategy is DEFAULT. Must be a
	// concrete strategy (currently only REBASE).
	DefaultStrategy mergestrategypb.Strategy
	// Runtime is the pinned Git runtime used for every invocation.
	Runtime GitRuntime
	// MaxPushAttempts caps how many times a committing merge retries the full
	// reset/apply/push cycle when the remote tip moves under it. Defaults to
	// defaultMaxPushAttempts when zero or negative.
	MaxPushAttempts int
	// FetchRefspecs are extra refspecs fetched on every cycle. Only needed for
	// a remote that refuses to serve an unadvertised object by SHA; leave empty
	// otherwise, since fetching broad patterns such as
	// "+refs/pull/*/head:refs/remotes/origin/pr/*" costs a full ref
	// advertisement on every attempt.
	FetchRefspecs []string
	// CheckStaleness enables verifying, before applying, that each change's
	// canonical ref still points at the commit its URI names.
	CheckStaleness bool
	// CommitterName / CommitterEmail identify the author/committer of
	// service-created commits. Defaults are used when empty.
	CommitterName  string
	CommitterEmail string
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

// gitMerger implements merger.Merger by shelling out to the `git` CLI against a
// local checkout.
type gitMerger struct {
	checkoutPath    string
	remote          string
	target          string
	defaultStrategy mergestrategypb.Strategy
	runtime         GitRuntime
	maxPushAttempts int
	fetchRefspecs   []string
	checkStaleness  bool
	committerName   string
	committerEmail  string
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope

	// mu serializes concurrent operations — the underlying checkout cannot be
	// safely shared between operations.
	mu sync.Mutex
}

// Verify gitMerger implements merger.Merger at compile time.
var _ merger.Merger = (*gitMerger)(nil)

// resolvedStep pairs a request step with its resolved (non-DEFAULT) strategy
// and the changes it names, already parsed.
type resolvedStep struct {
	step     *runwaymq.MergeStep
	strategy mergestrategypb.Strategy
	// refs holds one changeRef per URI of the step's change, in application
	// order. Resolution happens once, during validation, so the apply paths
	// work from parsed refs rather than re-parsing the URIs they came from.
	refs []changeRef
}

// NewMerger constructs a git-backed Merger operating against the given checkout.
// The checkout must already exist and have the configured remote. Runtime paths
// must be absolute and DefaultStrategy must be a concrete strategy.
func NewMerger(params Params) (merger.Merger, error) {
	if err := params.Runtime.validate(); err != nil {
		return nil, err
	}
	if !isConcreteStrategy(params.DefaultStrategy) {
		return nil, fmt.Errorf("default strategy must be concrete (currently only REBASE), got %v", params.DefaultStrategy)
	}
	maxAttempts := params.MaxPushAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxPushAttempts
	}
	committerName := params.CommitterName
	if committerName == "" {
		committerName = defaultCommitterName
	}
	committerEmail := params.CommitterEmail
	if committerEmail == "" {
		committerEmail = defaultCommitterEmail
	}
	return &gitMerger{
		checkoutPath:    params.CheckoutPath,
		remote:          params.Remote,
		target:          params.Target,
		defaultStrategy: params.DefaultStrategy,
		runtime:         params.Runtime,
		maxPushAttempts: maxAttempts,
		fetchRefspecs:   params.FetchRefspecs,
		checkStaleness:  params.CheckStaleness,
		committerName:   committerName,
		committerEmail:  committerEmail,
		logger:          params.Logger.Named("git_merger"),
		metricsScope:    params.MetricsScope.SubScope("git_merger"),
	}, nil
}

func (r GitRuntime) validate() error {
	for name, path := range map[string]string{
		"executable":   r.Executable,
		"exec path":    r.ExecPath,
		"template dir": r.TemplateDir,
	} {
		if path == "" {
			return fmt.Errorf("git runtime %s is required", name)
		}
		if !filepath.IsAbs(path) {
			return fmt.Errorf("git runtime %s must be absolute: %q", name, path)
		}
	}
	return nil
}

// CheckMergeability applies the request's steps as a dry run: it verifies each
// step applies cleanly without committing to the remote, and returns per-step
// results with empty Outputs.
func (m *gitMerger) CheckMergeability(ctx context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
	return m.process(ctx, req, false)
}

// Merge applies the request's steps and commits the result to the remote,
// returning per-step Outputs (the revisions produced on the target).
func (m *gitMerger) Merge(ctx context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
	return m.process(ctx, req, true)
}

// process is the shared entry point for both Merge and CheckMergeability. The
// commit flag selects whether the result is pushed to the remote (Merge) or
// applied locally and discarded (CheckMergeability).
func (m *gitMerger) process(ctx context.Context, req *runwaymq.MergeRequest, commit bool) (ret *runwaymq.MergeResult, retErr error) {
	opName := "check"
	if commit {
		opName = "merge"
	}
	op := coremetrics.Begin(m.metricsScope, opName, coremetrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	steps, err := m.resolveAndValidate(req)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Debugw("starting merge",
		"id", req.GetId(),
		"target", m.target,
		"remote", m.remote,
		"step_count", len(steps),
		"commit", commit,
	)

	return m.applyTransforming(ctx, req, steps, commit)
}

// resolveAndValidate normalizes DEFAULT strategies to the configured default
// and resolves every change URI, once, for the apply paths to work from. All failures here are terminal (merger.ErrInvalidRequest): retrying never
// succeeds, so the controller publishes a FAILED result rather than nacking.
func (m *gitMerger) resolveAndValidate(req *runwaymq.MergeRequest) ([]resolvedStep, error) {
	if len(req.GetSteps()) == 0 {
		return nil, fmt.Errorf("%w: request has no steps", merger.ErrInvalidRequest)
	}

	resolved := make([]resolvedStep, 0, len(req.GetSteps()))
	for _, step := range req.GetSteps() {
		strategy := step.GetStrategy()
		if strategy == mergestrategypb.Strategy_DEFAULT {
			strategy = m.defaultStrategy
		}
		if !isConcreteStrategy(strategy) {
			return nil, fmt.Errorf("%w: unsupported strategy %v", merger.ErrInvalidRequest, step.GetStrategy())
		}
		ch := step.GetChange()
		if ch == nil || len(ch.GetUris()) == 0 {
			return nil, fmt.Errorf("%w: step %q has no change URIs", merger.ErrInvalidRequest, step.GetStepId())
		}
		refs := make([]changeRef, 0, len(ch.GetUris()))
		for _, uri := range ch.GetUris() {
			ref, err := resolveChange(uri)
			if err != nil {
				return nil, err
			}
			refs = append(refs, ref)
		}
		resolved = append(resolved, resolvedStep{step: step, strategy: strategy, refs: refs})
	}

	return resolved, nil
}

// applyTransforming runs the reset/apply/push cycle for the transforming
// strategies, retrying on remote contention when committing. For a dry run it applies the steps locally then discards them.
func (m *gitMerger) applyTransforming(ctx context.Context, req *runwaymq.MergeRequest, steps []resolvedStep, commit bool) (*runwaymq.MergeResult, error) {
	// Fetch and vet every change the request names before the first attempt, so
	// an unusable request fails without having touched the checkout. This sits
	// outside the retry loop deliberately: the refs are fixed for the whole
	// request, and re-checking them per attempt would re-query the remote to
	// learn what it already told us.
	refs := stepChangeRefs(steps)
	if err := m.ensureObjects(ctx, refs); err != nil {
		return nil, err
	}
	if err := m.checkStale(ctx, refs); err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= m.maxPushAttempts; attempt++ {
		baseSHA, stepResults, err := m.tryApply(ctx, steps, commit)
		if err == nil {
			if !commit {
				// Discard the local commits the dry run created so the checkout
				// is clean for the next operation, and report empty Outputs.
				if derr := m.resetToRemote(ctx); derr != nil {
					return nil, fmt.Errorf("discard after dry-run: %w", derr)
				}
				stripOutputs(stepResults)
			}
			m.logger.Debugw("merge complete", "id", req.GetId(), "target", m.target, "commit", commit)
			return successResult(req, stepResults), nil
		}

		// A conflict is terminal — no retry. Discard any partial dry-run state.
		if errors.Is(err, merger.ErrConflict) {
			if !commit {
				_ = m.resetToRemote(ctx)
			}
			return nil, err
		}

		// Only a push failure caused by the remote tip moving under us (between
		// reset and push) is worth retrying; everything else is fatal. baseSHA
		// is empty when the failure happened before reset captured a base.
		if !commit || baseSHA == "" {
			return nil, err
		}
		currentSHA, refetchErr := m.refetchTipSHA(ctx)
		if refetchErr != nil {
			return nil, fmt.Errorf("refetch after push failure failed: %v (original push error: %w)", refetchErr, err)
		}
		if currentSHA == baseSHA {
			return nil, err
		}

		coremetrics.NamedCounter(m.metricsScope, "merge", "stale_base_retries", 1)
		m.logger.Warnw("remote tip moved during merge, retrying",
			"attempt", attempt,
			"max_attempts", m.maxPushAttempts,
			"base_sha", baseSHA,
			"current_sha", currentSHA,
			"err", err,
		)
		lastErr = err
	}

	coremetrics.NamedCounter(m.metricsScope, "merge", "stale_base_giveup", 1)
	return nil, fmt.Errorf("exceeded %d merge attempts due to remote contention: %w", m.maxPushAttempts, lastErr)
}

// tryApply runs one full reset+apply(+push) cycle. The returned baseSHA is the
// SHA the cycle was based on (set as soon as resetToRemote completes) so the
// caller can distinguish concurrent-push contention from other failures.
func (m *gitMerger) tryApply(ctx context.Context, steps []resolvedStep, commit bool) (string, []*runwaymq.StepResult, error) {
	if err := m.resetToRemote(ctx); err != nil {
		coremetrics.NamedCounter(m.metricsScope, "merge", "reset_errors", 1)
		return "", nil, err
	}
	baseSHA, err := m.headSHA(ctx)
	if err != nil {
		return "", nil, err
	}

	stepResults, err := m.applySteps(ctx, steps)
	if err != nil {
		// The failing apply function aborts its own in-progress git operation;
		// the next attempt starts with resetToRemote regardless.
		return baseSHA, nil, err
	}

	if commit {
		if err := m.push(ctx); err != nil {
			coremetrics.NamedCounter(m.metricsScope, "merge", "git_push_errors", 1)
			return baseSHA, nil, err
		}
	}
	return baseSHA, stepResults, nil
}

// applySteps dispatches each step by its resolved strategy, in order, building
// up local HEAD and collecting one StepResult per step.
func (m *gitMerger) applySteps(ctx context.Context, steps []resolvedStep) ([]*runwaymq.StepResult, error) {
	results := make([]*runwaymq.StepResult, 0, len(steps))
	for _, rs := range steps {
		var (
			outputs []*runwaymq.StepOutput
			err     error
		)
		switch rs.strategy {
		case mergestrategypb.Strategy_REBASE:
			outputs, err = m.applyRebase(ctx, rs)
		default:
			// resolveAndValidate rejects anything else; defensive.
			return nil, fmt.Errorf("%w: unsupported strategy %v", merger.ErrInvalidRequest, rs.strategy)
		}
		if err != nil {
			return nil, err
		}
		results = append(results, &runwaymq.StepResult{StepId: rs.step.GetStepId(), Outputs: outputs})
	}
	return results, nil
}

// applyRebase cherry-picks the head SHA of every URI of every change in the
// step, in order, returning one StepOutput per newly-created commit.
func (m *gitMerger) applyRebase(ctx context.Context, rs resolvedStep) ([]*runwaymq.StepOutput, error) {
	picked, err := m.pickStepChanges(ctx, rs)
	if err != nil {
		return nil, err
	}
	return toOutputs(picked), nil
}

// pickStepChanges applies every change in the step, in order, returning the
// SHAs of the commits created on the target (empty for a change whose content
// was already present).
func (m *gitMerger) pickStepChanges(ctx context.Context, rs resolvedStep) ([]string, error) {
	var picked []string
	for _, ref := range rs.refs {
		created, err := m.pickRange(ctx, ref)
		if err != nil {
			return nil, err
		}
		picked = append(picked, created...)
	}
	return picked, nil
}

// pickRange replays every commit the change introduces, not just its head.
//
// A change URI pins a pull request to its head commit, and a pull request is
// routinely more than one commit. Cherry-picking the head alone applies only
// that commit's diff against its own parent, which either conflicts against
// context the earlier commits would have established, or — worse, when the
// commits touch different files — succeeds while silently dropping everything
// before the head. The unit to replay is therefore the range from the change's
// merge base with the target up to its head.
func (m *gitMerger) pickRange(ctx context.Context, ref changeRef) ([]string, error) {
	base, err := m.mergeBase(ctx, "HEAD", ref.SHA)
	if err != nil {
		return nil, err
	}
	if base == "" {
		// No common ancestor: there is no range to compute, and replaying an
		// imported history commit-by-commit would rewrite every SHA in it,
		// which is the opposite of what such a request wants.
		return nil, fmt.Errorf("%w: change %s shares no history with the merge target; use the MERGE strategy to integrate an unrelated history",
			merger.ErrInvalidRequest, ref.Label)
	}

	// A change wholly contained in the target has an empty range — its merge
	// base with HEAD is its own head. That is the already-landed case, and it
	// is a no-op rather than an error.
	pending, err := m.revList(ctx, base, ref.SHA)
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return nil, nil
	}

	preSHA, err := m.headSHA(ctx)
	if err != nil {
		return nil, err
	}
	if err := m.cherryPickRange(ctx, base, ref.SHA); err != nil {
		return nil, err
	}
	return m.revList(ctx, preSHA, "HEAD")
}

// cherryPickRange cherry-picks base..head, resuming past commits git declines
// to apply because they would be empty.
//
// A multi-commit range runs through git's sequencer, which changes the control
// flow versus a single pick: the operation stops on the offending commit and
// --skip advances to the next one rather than ending the operation. Two kinds
// of commit stop it harmlessly — one whose content is already on the target
// (the change was partially landed) and one that was empty to begin with — and
// both should be skipped rather than treated as failures. Anything else ends
// the attempt, and the in-progress pick is aborted so the checkout stays
// usable for the next request.
func (m *gitMerger) cherryPickRange(ctx context.Context, base, head string) error {
	out, err := m.runCombined(ctx, nil, "cherry-pick", base+".."+head)
	for err != nil {
		if isRedundantCherryPick(out) {
			out, err = m.runCombined(ctx, nil, "cherry-pick", "--skip")
			continue
		}
		// Ask the index what happened before aborting clears it. A non-zero
		// exit alone does not mean the change conflicts — a missing object, an
		// unreadable repository or a killed process all land here too, and
		// calling those conflicts tells the client its change collides with
		// the target and permanently fails a request that a retry might well
		// have completed.
		conflicted := m.hasUnmergedPaths(ctx)
		_, _ = m.run(ctx, nil, "cherry-pick", "--abort")
		detail := strings.TrimSpace(string(out))
		if !conflicted {
			coremetrics.NamedCounter(m.metricsScope, "merge", "cherry_pick_errors", 1)
			return fmt.Errorf("git cherry-pick %s..%s: %w: %s", base, head, err, detail)
		}
		coremetrics.NamedCounter(m.metricsScope, "merge", "cherry_pick_conflicts", 1)
		return fmt.Errorf("%w: git cherry-pick %s..%s: %s", merger.ErrConflict, base, head, detail)
	}
	return nil
}

// hasUnmergedPaths reports whether the index holds conflicted entries. That is
// what separates a genuine content conflict from any other reason a git
// invocation exited non-zero, and it has to be read before the operation is
// aborted, since aborting clears the index.
func (m *gitMerger) hasUnmergedPaths(ctx context.Context) bool {
	out, err := m.run(ctx, nil, "ls-files", "--unmerged")
	return err == nil && len(bytes.TrimSpace(out)) > 0
}

// mergeBase returns the best common ancestor of two commits, or "" when they
// share no history. git exits 1 for the latter, which is an answer rather than
// a failure.
func (m *gitMerger) mergeBase(ctx context.Context, a, b string) (string, error) {
	out, err := m.run(ctx, nil, "merge-base", a, b)
	if err == nil {
		return strings.TrimSpace(string(out)), nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return "", nil
	}
	return "", fmt.Errorf("git merge-base %s %s: %w", a, b, err)
}

// revList lists the commits reachable from `to` but not from `from`, oldest
// first. The commits an apply produced are derived from the checkout this way
// rather than tracked per-invocation, which stays correct when the sequencer
// drops some of them.
func (m *gitMerger) revList(ctx context.Context, from, to string) ([]string, error) {
	out, err := m.run(ctx, nil, "rev-list", "--reverse", from+".."+to)
	if err != nil {
		return nil, fmt.Errorf("git rev-list %s..%s: %w", from, to, err)
	}
	return strings.Fields(string(out)), nil
}

// resetToRemote fetches the configured remote and hard-resets the checkout's
// HEAD to refs/remotes/<remote>/<target>, producing a clean working tree pinned
// to the latest remote tip.
func (m *gitMerger) resetToRemote(ctx context.Context) error {
	if _, err := m.run(ctx, nil, "fetch", m.remote); err != nil {
		return fmt.Errorf("git fetch %s: %w", m.remote, err)
	}
	// Deployment-supplied refspecs, for a server that refuses a fetch for an
	// unadvertised object and so cannot satisfy the by-SHA fetch in
	// ensureObject. Normally empty.
	for _, refspec := range m.fetchRefspecs {
		if _, err := m.run(ctx, nil, "fetch", m.remote, refspec); err != nil {
			return fmt.Errorf("git fetch %s %s: %w", m.remote, refspec, err)
		}
	}
	remoteRef := m.remote + "/" + m.target
	if _, err := m.run(ctx, nil, "reset", "--hard", remoteRef); err != nil {
		return fmt.Errorf("git reset --hard %s: %w", remoteRef, err)
	}
	if _, err := m.run(ctx, nil, "clean", "-fdx"); err != nil {
		return fmt.Errorf("git clean: %w", err)
	}
	return nil
}

// headSHA returns the SHA at HEAD in the local checkout.
func (m *gitMerger) headSHA(ctx context.Context) (string, error) {
	out, err := m.run(ctx, nil, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// refetchTipSHA fetches the remote and returns the current SHA at
// refs/remotes/<remote>/<target>. Used after a push failure to detect whether
// the remote tip moved under us.
func (m *gitMerger) refetchTipSHA(ctx context.Context) (string, error) {
	if _, err := m.run(ctx, nil, "fetch", m.remote); err != nil {
		return "", fmt.Errorf("git fetch %s: %w", m.remote, err)
	}
	remoteRef := m.remote + "/" + m.target
	out, err := m.run(ctx, nil, "rev-parse", remoteRef)
	if err != nil {
		return "", fmt.Errorf("git rev-parse %s: %w", remoteRef, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// push pushes the current HEAD to refs/heads/<target> on the remote.
func (m *gitMerger) push(ctx context.Context) error {
	refspec := "HEAD:refs/heads/" + m.target
	if _, err := m.run(ctx, nil, "push", m.remote, refspec); err != nil {
		return fmt.Errorf("git push %s %s: %w", m.remote, refspec, err)
	}
	return nil
}

// run executes a `git` command in the checkout. Returns captured stdout and an
// error that includes captured stderr for diagnostics.
func (m *gitMerger) run(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := m.command(ctx, args...)
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
// and failure. Used when the caller needs to inspect git's diagnostic output.
func (m *gitMerger) runCombined(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := m.command(ctx, args...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	return cmd.CombinedOutput()
}

// command builds a git command with the committer identity injected via -c
// flags, on top of the pinned runtime and scrubbed environment.
func (m *gitMerger) command(ctx context.Context, args ...string) *exec.Cmd {
	withIdentity := make([]string, 0, len(args)+6)
	withIdentity = append(withIdentity,
		"-c", "user.name="+m.committerName,
		"-c", "user.email="+m.committerEmail,
		"-c", "commit.gpgsign=false",
	)
	withIdentity = append(withIdentity, args...)
	return newGitCommand(ctx, m.runtime, m.checkoutPath, withIdentity...)
}

// newGitCommand constructs a Git command without inheriting the caller's
// environment. The executable and helper paths come from the pinned runtime;
// repository-local configuration remains an intentional input.
func newGitCommand(ctx context.Context, runtime GitRuntime, dir string, args ...string) *exec.Cmd {
	gitArgs := make([]string, 0, len(args)+3)
	gitArgs = append(gitArgs,
		"--exec-path="+runtime.ExecPath,
		"-c", "init.templateDir="+runtime.TemplateDir,
	)
	gitArgs = append(gitArgs, args...)

	cmd := exec.CommandContext(ctx, runtime.Executable, gitArgs...)
	cmd.Dir = dir
	cmd.Env = []string{
		"HOME=" + filepath.Join(dir, ".submitqueue-git-home"),
		"XDG_CONFIG_HOME=" + filepath.Join(dir, ".submitqueue-git-home", "xdg"),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_ATTR_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_PAGER=cat",
		"GIT_EDITOR=:",
		"GIT_EXEC_PATH=" + runtime.ExecPath,
		"GIT_TEMPLATE_DIR=" + runtime.TemplateDir,
		"LC_ALL=C",
		"LANG=C",
	}
	cmd.Env = append(cmd.Env, passthroughEnv(runtime.PassthroughEnv)...)
	return cmd
}

// authEnvNames are the variables inherited from the parent process when set.
//
// Scrubbing the environment is about denying git ambient *configuration* that
// could change what a merge produces. Reaching the remote is a separate
// concern, and these carry it: an SSH remote authenticates through the agent
// socket, git locates its ssh and credential helpers through PATH, and TLS and
// proxy settings decide whether an HTTPS remote is reachable at all. Dropping
// them does not make the merge more hermetic, it just makes fetch and push
// fail — and at Uber, where a uSSH certificate lives in the agent, it fails as
// an opaque authentication error rather than an obviously missing variable.
//
// None of these can influence merge semantics, which is what keeps them
// compatible with the scrubbing above.
var authEnvNames = []string{
	"SSH_AUTH_SOCK",
	"SSH_AGENT_PID",
	"PATH",
	"GIT_SSH",
	"GIT_SSH_COMMAND",
	"GIT_SSH_VARIANT",
	"GIT_SSL_CAINFO",
	"GIT_SSL_CAPATH",
	"SSL_CERT_DIR",
	"SSL_CERT_FILE",
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
	"http_proxy", "https_proxy", "no_proxy",
}

// passthroughEnv returns "NAME=value" entries for the auth and transport
// variables that are actually set, plus any extra names the deployment asked
// for. An unset variable is omitted rather than exported empty, which for
// SSH_AUTH_SOCK is the difference between "use the agent" and "there is no
// agent".
func passthroughEnv(extra []string) []string {
	names := make([]string, 0, len(authEnvNames)+len(extra))
	names = append(names, authEnvNames...)
	names = append(names, extra...)

	seen := make(map[string]bool, len(names))
	env := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	return env
}

// isConcreteStrategy reports whether s names a concrete integration strategy
// (i.e. not DEFAULT and not an unknown value). Only REBASE is implemented so
// far; the remaining strategies are rejected as invalid requests until their
// apply paths land.
func isConcreteStrategy(s mergestrategypb.Strategy) bool {
	switch s {
	case mergestrategypb.Strategy_REBASE:
		return true
	default:
		return false
	}
}

// isRedundantCherryPick reports whether git's cherry-pick output indicates the
// pick was rejected because the change is already present on target.
func isRedundantCherryPick(out []byte) bool {
	s := string(out)
	return strings.Contains(s, "previous cherry-pick is now empty") ||
		strings.Contains(s, "nothing to commit")
}

// toOutputs wraps commit SHAs as StepOutputs in order.
func toOutputs(shas []string) []*runwaymq.StepOutput {
	if len(shas) == 0 {
		return nil
	}
	outputs := make([]*runwaymq.StepOutput, 0, len(shas))
	for _, sha := range shas {
		outputs = append(outputs, &runwaymq.StepOutput{Id: sha})
	}
	return outputs
}

// stripOutputs clears the Outputs of every step result — used for a dry-run
// check, which reports mergeability only.
func stripOutputs(steps []*runwaymq.StepResult) {
	for _, s := range steps {
		s.Outputs = nil
	}
}

// successResult builds a SUCCEEDED MergeResult echoing the request id.
func successResult(req *runwaymq.MergeRequest, steps []*runwaymq.StepResult) *runwaymq.MergeResult {
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}
}
