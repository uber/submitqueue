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

// Package fake provides a merger.Merger whose outcome is driven by the request
// payload. With no marker it succeeds like the noop merger, behaving as a
// best-case stub for wiring and baselines. A failure can be injected end-to-end
// (e.g. from an e2e merge request) by embedding a marker token in a change URI
// of the form "sq-fake=<token>":
//
//	sq-fake=merge-conflict -> merger.ErrConflict
//	sq-fake=merge-invalid  -> merger.ErrInvalidRequest
//	sq-fake=merge-error    -> a plain (non-retryable) error
//
// The first token found across the request's steps, in order, decides the
// outcome for the whole request. This lets a single running stack exercise
// Runway's terminal-failure and dead-letter paths purely by varying request
// payloads. It is intended for examples and tests only, never production.
package fake

import (
	"context"
	"fmt"
	"sync/atomic"

	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/fakemarker"
	"github.com/uber/submitqueue/runway/extension/merger"
)

// Recognized marker tokens. See the package doc for the convention.
const (
	tokenConflict = "merge-conflict"
	tokenInvalid  = "merge-invalid"
	tokenError    = "merge-error"
)

var _ merger.Merger = (*Merger)(nil)

// Merger is a Merger that succeeds unless a marker token in a change URI
// requests otherwise.
type Merger struct {
	// cfg is the per-queue identity this merger was built for.
	cfg merger.Config
	// seq mints the synthetic revision ids returned by Merge. It is supplied by
	// the caller so a factory that builds one Merger per queue can still hand
	// out ids that are unique across every queue in the process.
	seq *atomic.Uint64
}

// New returns a Merger bound to the queue named in cfg that defaults to success
// and honors marker tokens embedded in change URIs. seq must be non-nil and is
// shared, not owned: callers minting ids from more than one Merger must pass the
// same counter to each.
func New(cfg merger.Config, seq *atomic.Uint64) *Merger {
	return &Merger{cfg: cfg, seq: seq}
}

// CheckMergeability reports the request as mergeable unless a recognized marker
// token asks for a failure. Outputs are empty, as for any dry run.
func (m *Merger) CheckMergeability(_ context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
	if err := injectedFailure(req); err != nil {
		return nil, err
	}

	steps := make([]*runwaymq.StepResult, len(req.GetSteps()))
	for i, s := range req.GetSteps() {
		steps[i] = &runwaymq.StepResult{StepId: s.GetStepId()}
	}
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}, nil
}

// Merge reports the request as merged unless a recognized marker token asks for
// a failure, producing one synthetic revision id per step.
func (m *Merger) Merge(_ context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
	if err := injectedFailure(req); err != nil {
		return nil, err
	}

	steps := make([]*runwaymq.StepResult, len(req.GetSteps()))
	for i, s := range req.GetSteps() {
		n := m.seq.Add(1)
		steps[i] = &runwaymq.StepResult{
			StepId:  s.GetStepId(),
			Outputs: []*runwaymq.StepOutput{{Id: fmt.Sprintf("%040x", n)}},
		}
	}
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}, nil
}

// injectedFailure returns the error the request's marker token asks for, or nil
// when no step carries a recognized token.
func injectedFailure(req *runwaymq.MergeRequest) error {
	for _, s := range req.GetSteps() {
		switch fakemarker.Token(s.GetChange().GetUris()) {
		case tokenConflict:
			return fmt.Errorf("fake: marked conflicting on step %s: %w", s.GetStepId(), merger.ErrConflict)
		case tokenInvalid:
			return fmt.Errorf("fake: marked invalid on step %s: %w", s.GetStepId(), merger.ErrInvalidRequest)
		case tokenError:
			return fmt.Errorf("fake: marked failing on step %s", s.GetStepId())
		}
	}
	return nil
}
