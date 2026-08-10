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

// Package sampler implements buildrunner.BuildRunner by splitting builds across
// two other BuildRunners: a baseline that takes most of the traffic and a
// candidate that takes a configured percentage of it. It is the mechanism for
// rolling a new build backend out gradually, or for comparing two backends on
// live traffic, without deploying separate queues.
//
// The sample is drawn per Trigger, so the percentage is a rate over many builds
// rather than a guarantee about any fixed number of them. Because the split is
// per build and not per queue, a sampler is composed rather than routed to: the
// wiring layer decides which queues get a sampler, and the sampler decides which
// builds within those queues get the candidate.
//
// Status and Cancel must reach the same runner that minted a build's id, and a
// BuildRunner id is opaque — nothing else can tell whose it is. The sampler
// therefore tags each id with the runner that produced it and strips the tag
// before delegating. Tagging keeps the sampler stateless the same way the fake
// runner's encoded outcome does: any sampler instance, in any process, routes an
// id the same way, so redelivery and multi-replica deployments need no shared
// bookkeeping. Ids that carry no tag — minted before the sampler was wired, or by
// a runner used directly — route to the baseline.
package sampler

import (
	"context"
	"fmt"
	mathrand "math/rand/v2"
	"strings"

	"go.uber.org/zap"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

// _idPrefix introduces the routing tag on every BuildID the sampler mints:
// "sampler-<slot>-<runner's own id>". The delegate's id keeps its own shape
// inside the tag, so a delegate that is itself a sampler nests cleanly.
const _idPrefix = "sampler-"

// Slot names identifying which delegate minted a build, carried in the id.
const (
	_slotBaseline  = "baseline"
	_slotCandidate = "candidate"
)

// _percentScale is the draw range for a percentage sample: a draw in [0,100)
// compared against a percentage in [0,100] yields that percentage of hits.
const _percentScale = 100

// Params holds the dependencies and split for a sampling BuildRunner.
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config

	// Baseline runs every build not sampled into the candidate. Required.
	Baseline buildrunner.BuildRunner

	// Candidate runs the sampled share of builds. Required even at a
	// CandidatePercent of 0, because Status and Cancel still have to reach it
	// for builds a previous, higher percentage sent its way.
	Candidate buildrunner.BuildRunner

	// CandidatePercent is the share of builds, in percent, routed to Candidate.
	// Valid range is 0-100; the zero value sends everything to Baseline.
	CandidatePercent int

	// Logger is the structured logger. Required.
	Logger *zap.SugaredLogger
}

// runner implements buildrunner.BuildRunner.
type runner struct {
	// cfg is the per-queue identity this runner was built for.
	cfg              buildrunner.Config
	baseline         buildrunner.BuildRunner
	candidate        buildrunner.BuildRunner
	candidatePercent int
	logger           *zap.SugaredLogger

	// intn draws the routing sample from [0,n). A field rather than a direct
	// call to math/rand so tests can pin the draw.
	intn func(n int) int
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// New constructs a BuildRunner that routes params.CandidatePercent of builds to
// the candidate and the rest to the baseline. It rejects a missing delegate or a
// percentage outside 0-100 so a misconfigured split fails at wiring time rather
// than at the first build.
func New(params Params) (buildrunner.BuildRunner, error) {
	if params.Baseline == nil {
		return nil, fmt.Errorf("sampler: baseline build runner is required")
	}
	if params.Candidate == nil {
		return nil, fmt.Errorf("sampler: candidate build runner is required")
	}
	if params.CandidatePercent < 0 || params.CandidatePercent > _percentScale {
		return nil, fmt.Errorf("sampler: candidate percent %d outside 0-100", params.CandidatePercent)
	}
	if params.Logger == nil {
		return nil, fmt.Errorf("sampler: logger is required")
	}
	return &runner{
		cfg:              params.Config,
		baseline:         params.Baseline,
		candidate:        params.Candidate,
		candidatePercent: params.CandidatePercent,
		logger:           params.Logger.Named("sampler_buildrunner"),
		intn:             mathrand.IntN,
	}, nil
}

// Trigger draws a delegate for this build and returns its id tagged with the
// delegate that minted it. A delegate's error is propagated rather than retried
// against the other delegate: a sampled rollout exists to surface the sampled
// backend's failures, and silently covering for it would hide the very signal
// the split was set up to collect.
func (r *runner) Trigger(ctx context.Context, baseURI, headURI string, metadata entity.BuildMetadata) (entity.BuildID, error) {
	delegate, slot := r.draw()
	buildID, err := delegate.Trigger(ctx, baseURI, headURI, metadata)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("sampler: %s trigger: %w", slot, err)
	}

	tagged := entity.BuildID{ID: _idPrefix + slot + "-" + buildID.ID}
	r.logger.Debugw("routed build",
		"queue", r.cfg.QueueName,
		"slot", slot,
		"candidate_percent", r.candidatePercent,
		"build_id", tagged.ID,
	)
	return tagged, nil
}

// Status delegates to the runner that minted buildID, with the routing tag
// stripped so the delegate sees the id it issued.
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	delegate, slot, delegateID := r.route(buildID)
	status, metadata, err := delegate.Status(ctx, delegateID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("sampler: %s status: %w", slot, err)
	}
	return status, metadata, nil
}

// Cancel delegates to the runner that minted buildID, with the routing tag
// stripped so the delegate sees the id it issued.
func (r *runner) Cancel(ctx context.Context, buildID entity.BuildID) error {
	delegate, slot, delegateID := r.route(buildID)
	if err := delegate.Cancel(ctx, delegateID); err != nil {
		return fmt.Errorf("sampler: %s cancel: %w", slot, err)
	}
	return nil
}

// draw samples the delegate for a new build.
func (r *runner) draw() (buildrunner.BuildRunner, string) {
	if r.candidatePercent > 0 && r.intn(_percentScale) < r.candidatePercent {
		return r.candidate, _slotCandidate
	}
	return r.baseline, _slotBaseline
}

// route resolves the delegate that minted buildID from the tag Trigger added,
// returning it alongside the delegate's own untagged id. An untagged or
// unrecognized id routes to the baseline unchanged: the baseline is the runner
// that handled traffic before the sampler was wired, so it is the only delegate
// that can plausibly own an id the sampler never minted.
func (r *runner) route(buildID entity.BuildID) (buildrunner.BuildRunner, string, entity.BuildID) {
	if tag, found := strings.CutPrefix(buildID.ID, _idPrefix); found {
		if slot, delegateID, split := strings.Cut(tag, "-"); split {
			switch slot {
			case _slotBaseline:
				return r.baseline, _slotBaseline, entity.BuildID{ID: delegateID}
			case _slotCandidate:
				return r.candidate, _slotCandidate, entity.BuildID{ID: delegateID}
			}
		}
	}
	return r.baseline, _slotBaseline, buildID
}
