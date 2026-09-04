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

// Package noop provides a no-op Lander implementation for local development and
// testing. CheckLandability always reports success; Land produces synthetic
// output IDs from an atomic counter.
package noop

import (
	"context"
	"fmt"
	"sync/atomic"

	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/runway/extension/lander"
)

var _ lander.Lander = (*Lander)(nil)

// Lander is a no-op implementation that always succeeds.
type Lander struct {
	// cfg is the per-queue identity this lander was built for.
	cfg lander.Config
	// seq mints the synthetic revision ids returned by Land. It is supplied by
	// the caller so a factory that builds one Lander per queue can still hand
	// out ids that are unique across every queue in the process.
	seq *atomic.Uint64
}

// New returns a no-op Lander bound to the queue named in cfg. seq must be
// non-nil and is shared, not owned: callers minting ids from more than
// Lander must pass the same counter to each.
func New(cfg lander.Config, seq *atomic.Uint64) *Lander {
	return &Lander{cfg: cfg, seq: seq}
}

func (v *Lander) CheckLandability(_ context.Context, req *runwaymq.LandRequest) (*runwaymq.LandResult, error) {
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

func (v *Lander) Land(_ context.Context, req *runwaymq.LandRequest) (*runwaymq.LandResult, error) {
	steps := make([]*runwaymq.StepResult, len(req.GetSteps()))
	for i, s := range req.GetSteps() {
		n := v.seq.Add(1)
		steps[i] = &runwaymq.StepResult{
			StepId: s.GetStepId(),
			Outputs: []*runwaymq.StepOutput{
				{Id: fmt.Sprintf("%040x", n)},
			},
		}
	}
	return &runwaymq.LandResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}, nil
}
