// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package merger adapts SQSim merge behavior to the production Runway extension.
package merger

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"time"

	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	mergerext "github.com/uber/submitqueue/runway/extension/merger"
	"github.com/uber/submitqueue/sqsim/adapter/internal/operationlock"
	simentity "github.com/uber/submitqueue/sqsim/entity"
	"github.com/uber/submitqueue/sqsim/model"
	"google.golang.org/protobuf/proto"
)

var _ mergerext.Merger = (*Merger)(nil)

// Merger executes SQSim merge-conflict-check and committing merge behavior.
type Merger struct {
	runtime    *model.Runtime
	queue      string
	operations *operationlock.Locker
	checks     *model.State[checkResult]
	merges     *model.State[*runwaymq.MergeResult]
}

type checkResult struct {
	result   *runwaymq.MergeResult
	conflict bool
}

// New returns an SQSim Merger bound to one queue.
func New(runtime *model.Runtime, queue string) *Merger {
	return &Merger{
		runtime:    runtime,
		queue:      queue,
		operations: operationlock.New(),
		checks:     model.NewState[checkResult](),
		merges:     model.NewState[*runwaymq.MergeResult](),
	}
}

// CheckMergeability executes the next modeled dry-run invocation.
func (m *Merger) CheckMergeability(ctx context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
	changeURI, err := m.resolveBehavior(req, true)
	if err != nil {
		return nil, err
	}
	key := "check:" + req.GetId()
	unlock := m.operations.Lock(key)
	defer unlock()

	if retained, ok := m.checks.Get(key); ok {
		if retained.conflict {
			return nil, mergerext.ErrConflict
		}
		return cloneResult(retained.result), nil
	}
	invocation, err := m.runtime.NextMergeConflictCheck(changeURI)
	if err != nil {
		return nil, fmt.Errorf("next merge-conflict-check invocation: %w", err)
	}
	if err := m.runtime.Clock().Wait(ctx, time.Duration(invocation.DelayMs)*time.Millisecond); err != nil {
		return nil, err
	}
	if err := model.ErrorForFault(invocation.Fault); err != nil {
		return nil, err
	}
	if invocation.Outcome == simentity.MergeConflict {
		m.checks.Set(key, checkResult{conflict: true})
		return nil, mergerext.ErrConflict
	}
	result := successfulCheckResult(req)
	m.checks.Set(key, checkResult{result: result})
	return cloneResult(result), nil
}

// Merge executes the next modeled committing merge invocation.
func (m *Merger) Merge(ctx context.Context, req *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
	changeURI, err := m.resolveBehavior(req, false)
	if err != nil {
		return nil, err
	}
	key := "merge:" + req.GetId()
	unlock := m.operations.Lock(key)
	defer unlock()

	if retained, ok := m.merges.Get(key); ok {
		return cloneResult(retained), nil
	}
	invocation, err := m.runtime.NextMerge(changeURI)
	if err != nil {
		return nil, fmt.Errorf("next Merge invocation: %w", err)
	}
	if err := m.runtime.Clock().Wait(ctx, time.Duration(invocation.DelayMs)*time.Millisecond); err != nil {
		return nil, err
	}
	if invocation.Fault.Phase == simentity.FaultBeforeSideEffect {
		return nil, model.ErrorForFault(invocation.Fault)
	}

	result := successfulMergeResult(req)
	m.merges.Set(key, result)
	if err := model.ErrorForFault(invocation.Fault); err != nil {
		return nil, err
	}
	return cloneResult(result), nil
}

func (m *Merger) resolveBehavior(req *runwaymq.MergeRequest, check bool) (string, error) {
	if req == nil {
		return "", fmt.Errorf("merge request is required")
	}
	if req.GetId() == "" {
		return "", fmt.Errorf("merge request ID is required")
	}
	if req.GetQueueName() != m.queue {
		return "", fmt.Errorf("merge request queue %q does not match SQSim Merger queue %q", req.GetQueueName(), m.queue)
	}
	var selectedURI string
	var selected any
	for _, step := range req.GetSteps() {
		for _, change := range step.GetChanges() {
			for _, changeURI := range change.GetUris() {
				land, err := m.runtime.Resolve(changeURI)
				if err != nil {
					return "", fmt.Errorf("resolve merge change URI %q: %w", changeURI, err)
				}
				if land.Queue != m.queue {
					return "", fmt.Errorf("land %q queue %q does not match SQSim Merger queue %q", land.Name, land.Queue, m.queue)
				}
				var behavior any = land.Behavior.Merge
				if check {
					behavior = land.Behavior.MergeConflictCheck
				}
				if selectedURI == "" {
					selectedURI = changeURI
					selected = behavior
					continue
				}
				if !reflect.DeepEqual(selected, behavior) {
					return "", fmt.Errorf("merge request %q contains incompatible SQSim behavior", req.GetId())
				}
			}
		}
	}
	if selectedURI == "" {
		return "", fmt.Errorf("merge request %q has no SQSim change URI", req.GetId())
	}
	return selectedURI, nil
}

func successfulCheckResult(req *runwaymq.MergeRequest) *runwaymq.MergeResult {
	steps := make([]*runwaymq.StepResult, len(req.GetSteps()))
	for i, step := range req.GetSteps() {
		steps[i] = &runwaymq.StepResult{StepId: step.GetStepId()}
	}
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}
}

func successfulMergeResult(req *runwaymq.MergeRequest) *runwaymq.MergeResult {
	steps := make([]*runwaymq.StepResult, len(req.GetSteps()))
	for i, step := range req.GetSteps() {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", req.GetId(), step.GetStepId(), i)))
		steps[i] = &runwaymq.StepResult{
			StepId: step.GetStepId(),
			Outputs: []*runwaymq.StepOutput{{
				Id: fmt.Sprintf("%x", sum[:20]),
			}},
		}
	}
	return &runwaymq.MergeResult{
		Id:      req.GetId(),
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   steps,
	}
}

func cloneResult(result *runwaymq.MergeResult) *runwaymq.MergeResult {
	if result == nil {
		return nil
	}
	return proto.Clone(result).(*runwaymq.MergeResult)
}
