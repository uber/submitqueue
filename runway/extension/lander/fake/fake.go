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

// Package fake provides a lander.Lander whose outcome is driven by the request
// payload. With no marker it succeeds like the noop lander, behaving as a
// best-case stub for wiring and baselines. A failure can be injected end-to-end
// (e.g. from an e2e land request) by embedding a marker token in a change URI
// of the form "sq-fake=<token>":
//
//	sq-fake=land-conflict -> lander.ErrConflict
//	sq-fake=land-invalid  -> lander.ErrInvalidRequest
//	sq-fake=land-error    -> a plain (non-retryable) error
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
	"github.com/uber/submitqueue/runway/extension/lander"
)

// Recognized marker tokens. See the package doc for the convention.
const (
	tokenConflict = "land-conflict"
	tokenInvalid  = "land-invalid"
	tokenError    = "land-error"
)

var _ lander.Lander = (*Lander)(nil)

// Lander is a Lander that succeeds unless a marker token in a change URI
// requests otherwise.
type Lander struct {
	// cfg is the per-queue identity this lander was built for.
	cfg lander.Config
	// seq mints the synthetic revision ids returned by Land. It is supplied by
	// the caller so a factory that builds one Lander per queue can still hand
	// out ids that are unique across every queue in the process.
	seq *atomic.Uint64
}

// New returns a Lander bound to the queue named in cfg that defaults to success
// and honors marker tokens embedded in change URIs. seq must be non-nil and is
// shared, not owned: callers minting ids from more than one Lander must pass the
// same counter to each.
func New(cfg lander.Config, seq *atomic.Uint64) *Lander {
	return &Lander{cfg: cfg, seq: seq}
}

// CheckLandability reports the request as landable unless a recognized marker
// token asks for a failure. Outputs are empty, as for any dry run.
func (m *Lander) CheckLandability(_ context.Context, req *runwaymq.LandRequest) (*runwaymq.LandResult, error) {
	if err := injectedFailure(req); err != nil {
		return nil, err
	}

	steps := make([]*runwaymq.StepResult, len(req.GetSteps()))
	for i, s := range req.GetSteps() {
		steps[i] = &runwaymq.StepResult{StepId: s.GetStepId()}
	}
	return &runwaymq.LandResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}, nil
}

// Land reports the request as landed unless a recognized marker token asks for
// a failure, producing one synthetic revision id per step.
func (m *Lander) Land(_ context.Context, req *runwaymq.LandRequest) (*runwaymq.LandResult, error) {
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
	return &runwaymq.LandResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}, nil
}

// injectedFailure returns the error the request's marker token asks for, or nil
// when no step carries a recognized token.
func injectedFailure(req *runwaymq.LandRequest) error {
	for _, s := range req.GetSteps() {
		switch fakemarker.Token(s.GetChange().GetUris()) {
		case tokenConflict:
			return fmt.Errorf("fake: marked conflicting on step %s: %w", s.GetStepId(), lander.ErrConflict)
		case tokenInvalid:
			return fmt.Errorf("fake: marked invalid on step %s: %w", s.GetStepId(), lander.ErrInvalidRequest)
		case tokenError:
			return fmt.Errorf("fake: marked failing on step %s", s.GetStepId())
		}
	}
	return nil
}
