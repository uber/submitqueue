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

// Package noop provides a no-op Merger implementation for local development and
// testing. CheckMergeability always reports success; Merge produces synthetic
// output IDs from an atomic counter.
package noop

import (
	"context"
	"fmt"
	"sync/atomic"

	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/runway/extension/merger"
)

var _ merger.Merger = (*Merger)(nil)

// Merger is a no-op implementation that always succeeds.
type Merger struct {
	seq atomic.Uint64
}

// New returns a new no-op Merger instance.
func New() *Merger { return &Merger{} }

func (v *Merger) CheckMergeability(_ context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
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

func (v *Merger) Merge(_ context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
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
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}, nil
}
