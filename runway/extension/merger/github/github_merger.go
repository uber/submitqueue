// Copyright (c) 2026 Uber Technologies, Inc.
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

// Package github implements merger.Merger on top of the GitHub REST API. Each
// request step is one GitHub pull request stack (a single pull request being
// the one-element case) and is landed by GitHub's asynchronous merge API, so
// branch rules, the merged pull request state and stack rebasing stay
// GitHub's. See README.md for the model and its guarantees.
package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/runway/extension/merger"
)

// ErrMergePending signals that GitHub had not settled a merge, or a pull
// request's mergeability, within the merger's poll budget. The work is still in
// flight on GitHub's side, so the delivery should be retried: a redelivery
// adopts the in-flight request or finds the pull requests already merged.
var ErrMergePending = errors.New("github merge still pending")

const (
	defaultPollInterval    = 2 * time.Second
	defaultMaxPollDuration = 10 * time.Minute
)

// Params holds the dependencies for the GitHub Merger.
type Params struct {
	// HTTPClient sends every API call. Its transport resolves relative paths
	// against the API root (e.g. platform/http.NewClient("https://api.github.com"))
	// and authenticates them; the merger adds neither.
	HTTPClient *http.Client
	// Host is the GitHub instance change URIs must name (e.g. "github.com").
	Host string
	// Owner and Repo name the repository this merger lands into.
	Owner string
	Repo  string
	// Target is the trunk branch every stack must be based on (e.g. "main").
	Target string
	// DefaultStrategy resolves a step whose strategy is DEFAULT. Must be
	// REBASE, SQUASH_REBASE or MERGE.
	DefaultStrategy mergestrategypb.Strategy
	// BypassRules asks GitHub to merge past branch rules. The token must
	// belong to an actor allowed to bypass them.
	BypassRules bool
	// PollInterval is the wait between status reads. Zero uses the default; a
	// negative value polls without waiting.
	PollInterval time.Duration
	// MaxPollDuration bounds how long one call waits for GitHub to settle
	// before returning ErrMergePending. Zero uses the default.
	MaxPollDuration time.Duration
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

type githubMerger struct {
	client          *client
	host            string
	owner           string
	repo            string
	target          string
	defaultStrategy mergestrategypb.Strategy
	bypassRules     bool
	pollInterval    time.Duration
	maxPollDuration time.Duration
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
}

var _ merger.Merger = (*githubMerger)(nil)

// stepPull is one URI of a step together with the pull request it names.
type stepPull struct {
	ref changeRef
	pr  pullRequest
}

// New constructs a GitHub-backed Merger for one repository.
func New(params Params) (merger.Merger, error) {
	if params.HTTPClient == nil {
		return nil, errors.New("github merger: HTTPClient is required")
	}
	for name, v := range map[string]string{"host": params.Host, "owner": params.Owner, "repo": params.Repo, "target": params.Target} {
		if v == "" {
			return nil, fmt.Errorf("github merger: %s is required", name)
		}
	}
	if _, ok := mergeMethods[params.DefaultStrategy]; !ok {
		return nil, fmt.Errorf("github merger: default strategy must be REBASE, SQUASH_REBASE or MERGE, got %v", params.DefaultStrategy)
	}
	pollInterval := params.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}
	maxPollDuration := params.MaxPollDuration
	if maxPollDuration <= 0 {
		maxPollDuration = defaultMaxPollDuration
	}
	return &githubMerger{
		client:          &client{httpClient: params.HTTPClient, owner: params.Owner, repo: params.Repo},
		host:            params.Host,
		owner:           params.Owner,
		repo:            params.Repo,
		target:          params.Target,
		defaultStrategy: params.DefaultStrategy,
		bypassRules:     params.BypassRules,
		pollInterval:    pollInterval,
		maxPollDuration: maxPollDuration,
		logger:          params.Logger.Named("github_merger"),
		metricsScope:    params.MetricsScope.SubScope("github_merger"),
	}, nil
}

// mergeMethods maps the strategies this merger supports onto GitHub merge
// methods. PROMOTE has no pull request equivalent.
var mergeMethods = map[mergestrategypb.Strategy]string{
	mergestrategypb.Strategy_REBASE:        "rebase",
	mergestrategypb.Strategy_SQUASH_REBASE: "squash",
	mergestrategypb.Strategy_MERGE:         "merge",
}

// CheckMergeability verifies, without writing anything, that every step is a
// GitHub stack based on the target and that none of its pull requests
// conflicts with its base.
func (m *githubMerger) CheckMergeability(ctx context.Context, req *runwaymq.MergeRequest) (ret *runwaymq.MergeResult, retErr error) {
	op := coremetrics.Begin(m.metricsScope, "check", coremetrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := m.validateRequest(req); err != nil {
		return nil, err
	}
	results := make([]*runwaymq.StepResult, 0, len(req.GetSteps()))
	for _, step := range req.GetSteps() {
		pulls, err := m.loadValidStepPulls(ctx, step)
		if err != nil {
			return nil, err
		}
		for _, p := range unmergedPulls(pulls) {
			if err := m.checkPullMergeable(ctx, step, p); err != nil {
				return nil, err
			}
		}
		results = append(results, &runwaymq.StepResult{StepId: step.GetStepId()})
	}
	return successResult(req, results), nil
}

// Merge lands each step's stack in request order, one GitHub merge per step,
// and reports the merge commit of every pull request as that URI's output.
func (m *githubMerger) Merge(ctx context.Context, req *runwaymq.MergeRequest) (ret *runwaymq.MergeResult, retErr error) {
	op := coremetrics.Begin(m.metricsScope, "merge", coremetrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := m.validateRequest(req); err != nil {
		return nil, err
	}
	results := make([]*runwaymq.StepResult, 0, len(req.GetSteps()))
	for _, step := range req.GetSteps() {
		outputs, err := m.mergeStep(ctx, step)
		if err != nil {
			if !merger.IsTerminal(err) {
				return nil, err
			}
			// Ordering cannot be guaranteed past a failed step, so later steps
			// are not attempted; the ones before it stay landed.
			results = append(results, &runwaymq.StepResult{StepId: step.GetStepId(), Reason: err.Error()})
			return &runwaymq.MergeResult{
				Id:      req.GetId(),
				Outcome: runwaypb.Outcome_FAILED,
				Reason:  err.Error(),
				Steps:   results,
			}, err
		}
		results = append(results, &runwaymq.StepResult{StepId: step.GetStepId(), Outputs: outputs})
	}
	return successResult(req, results), nil
}

// loadValidStepPulls loads a step's pull requests and rejects the step unless
// GitHub would land them as one stack onto the target.
func (m *githubMerger) loadValidStepPulls(ctx context.Context, step *runwaymq.MergeStep) ([]stepPull, error) {
	pulls, err := m.loadStepPulls(ctx, step)
	if err != nil {
		return nil, err
	}
	if err := m.validateStepIsGitHubStack(ctx, step, pulls); err != nil {
		return nil, err
	}
	return pulls, nil
}

func (m *githubMerger) mergeStep(ctx context.Context, step *runwaymq.MergeStep) ([]*runwaymq.StepOutput, error) {
	pulls, err := m.loadValidStepPulls(ctx, step)
	if err != nil {
		return nil, err
	}
	pending := unmergedPulls(pulls)
	if len(pending) > 0 {
		top := pending[len(pending)-1]
		method := mergeMethods[m.resolveStrategy(step)]
		m.logger.Infow("merging github stack",
			"step_id", step.GetStepId(),
			"top_pull", top.ref.PRNumber,
			"pull_count", len(pending),
			"merge_method", method,
		)
		if err := m.mergeStack(ctx, step, top, method); err != nil {
			return nil, err
		}
	}
	return m.awaitMergeCommits(ctx, pulls)
}

// awaitMergeCommits returns each pull request's merge commit, re-reading the
// ones GitHub has not recorded yet: a stack merge settles before every pull
// request in it shows its merge, so a lagging read is waited out rather than
// failed. Only the merge record is read — a stack merge may rewrite the heads
// of pull requests that a lagging read still shows open, so revalidating them
// here would reject a merge that already happened.
func (m *githubMerger) awaitMergeCommits(ctx context.Context, pulls []stepPull) ([]*runwaymq.StepOutput, error) {
	deadline := time.Now().Add(m.maxPollDuration)
	outputs := make([]*runwaymq.StepOutput, 0, len(pulls))
	for _, p := range pulls {
		for {
			sha, ok, err := m.client.getMergeCommit(ctx, p.ref.PRNumber)
			if err != nil {
				return nil, err
			}
			if ok {
				outputs = append(outputs, &runwaymq.StepOutput{Id: sha})
				break
			}
			if !time.Now().Before(deadline) {
				return nil, fmt.Errorf("%w: pull request %s not yet reported merged", ErrMergePending, p.ref.Label)
			}
			if err := m.waitPollInterval(ctx); err != nil {
				return nil, err
			}
		}
	}
	return outputs, nil
}

// mergeStack submits the async merge for the top pull request, which GitHub
// applies to every unmerged pull request below it as one operation, and waits
// for it to settle.
func (m *githubMerger) mergeStack(ctx context.Context, step *runwaymq.MergeStep, top stepPull, method string) error {
	status, outcome, err := m.client.submitMerge(ctx, top.ref.PRNumber, asyncMergeRequest{
		SHA:         top.ref.SHA,
		MergeMethod: method,
		MergeAction: mergeActionDirect,
		BypassRules: m.bypassRules,
	})
	if err != nil {
		return err
	}
	if outcome == submitRejected {
		return fmt.Errorf("%w: step %q: GitHub refused to merge %s: %s", merger.ErrInvalidRequest, step.GetStepId(), top.ref.Label, status.Details.Message)
	}
	if outcome == submitConflicting {
		return fmt.Errorf("%w: step %q: %s already has a merge in flight for head %s by %s, not head %s by %s",
			merger.ErrInvalidRequest, step.GetStepId(), top.ref.Label, status.Details.ExpectedHeadSHA, status.Details.MergeMethod, top.ref.SHA, method)
	}

	deadline := time.Now().Add(m.maxPollDuration)
	for status.Status == mergeStatusPending {
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: merge %s of %s", ErrMergePending, status.Details.UUID, top.ref.Label)
		}
		if err := m.waitPollInterval(ctx); err != nil {
			return err
		}
		uuid := status.Details.UUID
		status, err = m.client.getMergeStatus(ctx, top.ref.PRNumber, uuid)
		if err != nil {
			return err
		}
		if status.Details.UUID == "" {
			status.Details.UUID = uuid
		}
	}

	switch status.Status {
	case mergeStatusMerged:
		return nil
	case mergeStatusFailed:
		// GitHub does not separate a conflict from a rule or head-moved
		// failure here; all are terminal, and its message is the reason.
		return fmt.Errorf("%w: step %q: GitHub failed to merge %s: %s", merger.ErrConflict, step.GetStepId(), top.ref.Label, status.Details.Message)
	default:
		return fmt.Errorf("unexpected merge status %q for %s", status.Status, top.ref.Label)
	}
}

// checkPullMergeable reads GitHub's mergeability verdict for one pull request
// against its own base, re-reading while GitHub is still computing it.
func (m *githubMerger) checkPullMergeable(ctx context.Context, step *runwaymq.MergeStep, p stepPull) error {
	pr := p.pr
	deadline := time.Now().Add(m.maxPollDuration)
	for pr.Mergeable == nil {
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: mergeability of %s", ErrMergePending, p.ref.Label)
		}
		if err := m.waitPollInterval(ctx); err != nil {
			return err
		}
		var err error
		pr, err = m.client.getPull(ctx, p.ref.PRNumber)
		if err != nil {
			return err
		}
	}
	if !*pr.Mergeable || pr.MergeableState == mergeableStateDirty {
		return fmt.Errorf("%w: step %q: %s conflicts with %s", merger.ErrConflict, step.GetStepId(), p.ref.Label, pr.Base.Ref)
	}
	return nil
}

// validateRequest checks the parts of a request that need no API call.
func (m *githubMerger) validateRequest(req *runwaymq.MergeRequest) error {
	if len(req.GetSteps()) == 0 {
		return fmt.Errorf("%w: request has no steps", merger.ErrInvalidRequest)
	}
	for _, step := range req.GetSteps() {
		if len(step.GetChange().GetUris()) == 0 {
			return fmt.Errorf("%w: step %q has no change URIs", merger.ErrInvalidRequest, step.GetStepId())
		}
		if _, ok := mergeMethods[m.resolveStrategy(step)]; !ok {
			return fmt.Errorf("%w: step %q: strategy %v is not supported by the GitHub merger", merger.ErrInvalidRequest, step.GetStepId(), step.GetStrategy())
		}
		for _, uri := range step.GetChange().GetUris() {
			if _, err := m.resolveChange(uri); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *githubMerger) resolveStrategy(step *runwaymq.MergeStep) mergestrategypb.Strategy {
	if step.GetStrategy() == mergestrategypb.Strategy_DEFAULT {
		return m.defaultStrategy
	}
	return step.GetStrategy()
}

// loadStepPulls reads the pull request behind every URI of a step and rejects
// one that can no longer be landed as the URI describes it.
func (m *githubMerger) loadStepPulls(ctx context.Context, step *runwaymq.MergeStep) ([]stepPull, error) {
	uris := step.GetChange().GetUris()
	pulls := make([]stepPull, 0, len(uris))
	for _, uri := range uris {
		ref, err := m.resolveChange(uri)
		if err != nil {
			return nil, err
		}
		pr, err := m.client.getPull(ctx, ref.PRNumber)
		if err != nil {
			return nil, err
		}
		if pr.Merged {
			// No head check: landing a stack can rewrite the heads of the
			// pull requests it merged, and a merged one is final either way.
			pulls = append(pulls, stepPull{ref: ref, pr: pr})
			continue
		}
		if pr.Head.SHA != ref.SHA {
			return nil, fmt.Errorf("%w: step %q: %s head moved from %s to %s", merger.ErrInvalidRequest, step.GetStepId(), ref.Label, ref.SHA, pr.Head.SHA)
		}
		if pr.State != pullStateOpen {
			return nil, fmt.Errorf("%w: step %q: %s is closed without being merged", merger.ErrInvalidRequest, step.GetStepId(), ref.Label)
		}
		if pr.Draft {
			return nil, fmt.Errorf("%w: step %q: %s is a draft", merger.ErrInvalidRequest, step.GetStepId(), ref.Label)
		}
		pulls = append(pulls, stepPull{ref: ref, pr: pr})
	}
	return pulls, nil
}

// validateStepIsGitHubStack rejects a step whose URIs GitHub would not land as
// one stack onto the target. Merged pull requests must form a prefix — GitHub
// merges bottom-up — and are skipped. The unmerged rest must be the bottom of a
// GitHub stack based on the target, in order and with no gap: merging the top
// lands everything below it, so a pull request the step does not name would
// otherwise land with it, and independent pull requests listed together would
// never land as one.
func (m *githubMerger) validateStepIsGitHubStack(ctx context.Context, step *runwaymq.MergeStep, pulls []stepPull) error {
	pending := unmergedPulls(pulls)
	if len(pending) == 0 {
		return nil
	}
	for _, p := range pulls[:len(pulls)-len(pending)] {
		if !p.pr.Merged {
			return fmt.Errorf("%w: step %q: %s is unmerged but listed below merged pull requests", merger.ErrInvalidRequest, step.GetStepId(), p.ref.Label)
		}
	}

	if len(pending) == 1 {
		if base := pending[0].pr.Base.Ref; base != m.target {
			return fmt.Errorf("%w: step %q: %s targets %q, not %q; a stacked pull request must be listed with the pull requests below it", merger.ErrInvalidRequest, step.GetStepId(), pending[0].ref.Label, base, m.target)
		}
		return nil
	}

	top := pending[len(pending)-1]
	s, ok, err := m.client.getStackForPull(ctx, top.ref.PRNumber)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: step %q lists %s, which are not a GitHub stack", merger.ErrInvalidRequest, step.GetStepId(), pullLabels(pending))
	}
	if s.Base.Ref != m.target {
		return fmt.Errorf("%w: step %q: stack #%d is based on %q, not %q", merger.ErrInvalidRequest, step.GetStepId(), s.Number, s.Base.Ref, m.target)
	}
	var stackPending []int
	for _, sp := range s.PullRequests {
		if sp.unmerged() {
			stackPending = append(stackPending, sp.Number)
		}
	}
	for i, p := range pending {
		if i >= len(stackPending) || stackPending[i] != p.ref.PRNumber {
			return fmt.Errorf("%w: step %q lists %s, but the unmerged bottom of stack #%d is %s", merger.ErrInvalidRequest, step.GetStepId(), pullLabels(pending), s.Number, numbersLabel(stackPending))
		}
	}
	return nil
}

func (m *githubMerger) waitPollInterval(ctx context.Context) error {
	if m.pollInterval < 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(m.pollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func unmergedPulls(pulls []stepPull) []stepPull {
	for i, p := range pulls {
		if !p.pr.Merged {
			return pulls[i:]
		}
	}
	return nil
}

func pullLabels(pulls []stepPull) string {
	labels := make([]string, 0, len(pulls))
	for _, p := range pulls {
		labels = append(labels, p.ref.Label)
	}
	return strings.Join(labels, ", ")
}

func numbersLabel(numbers []int) string {
	labels := make([]string, 0, len(numbers))
	for _, n := range numbers {
		labels = append(labels, "#"+strconv.Itoa(n))
	}
	return "[" + strings.Join(labels, ", ") + "]"
}

func successResult(req *runwaymq.MergeRequest, steps []*runwaymq.StepResult) *runwaymq.MergeResult {
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}
}
