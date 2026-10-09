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

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/queuepolicy"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueexecution"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

var (
	// ErrQueuePolicyNotFound means the queue has no explicitly registered policy.
	ErrQueuePolicyNotFound = errors.New("queue policy not found")
	// ErrCommitPolicyUnresolved means the commit or the retained policy lineage cannot be resolved.
	ErrCommitPolicyUnresolved = errors.New("commit policy unresolved")
)

// GetQueueStatusController resolves applied policy independently of validation requests.
type GetQueueStatusController interface {
	GetQueueStatus(context.Context, entity.GetQueueStatusRequest) (entity.GetQueueStatusResult, error)
}

type getQueueStatusController struct {
	logger        *zap.SugaredLogger
	scope         tally.Scope
	stores        storage.Factory
	sourceControl sourcecontrol.Factory
	execution     queueexecution.Reader
	now           func() time.Time
}

// NewGetQueueStatusController creates a read-only lookup over retained policy and queue-level execution control.
func NewGetQueueStatusController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, sourceControl sourcecontrol.Factory, execution queueexecution.Reader) GetQueueStatusController {
	return &getQueueStatusController{logger: logger, scope: scope.SubScope("queue_status_controller"), stores: stores, sourceControl: sourceControl, execution: execution, now: time.Now}
}

func (c *getQueueStatusController) GetQueueStatus(ctx context.Context, req entity.GetQueueStatusRequest) (result entity.GetQueueStatusResult, retErr error) {
	op := metrics.Begin(c.scope, "get_queue_status", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return result, err
	}
	if req.ChangeURI != "" {
		if err := validateHistoryIdentifier("change_uri", req.ChangeURI); err != nil {
			return result, err
		}
	}
	stores, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve queue policy storage: %w", err)
	}
	store := stores.GetQueuePolicyStore()
	_, current, err := queuepolicy.LoadCurrent(ctx, store, req.Queue)
	if err != nil {
		if storage.IsNotFound(err) {
			return result, errs.NewUserError(fmt.Errorf("queue %q: %w", req.Queue, ErrQueuePolicyNotFound))
		}
		return result, err
	}
	result.Queue, result.CurrentPolicy = req.Queue, current.Policy
	if req.ChangeURI != "" {
		result.PolicyForCommit, err = c.resolveCommitPolicy(ctx, req, store, current)
		if err != nil {
			return entity.GetQueueStatusResult{}, err
		}
		result.HasPolicyForCommit = true
	}
	state, err := c.execution.GetQueueExecutionState(ctx, req.Queue)
	if ctx.Err() != nil {
		return entity.GetQueueStatusResult{}, ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return entity.GetQueueStatusResult{}, err
	}
	if err != nil || (state != entity.QueueExecutionStateRunning && state != entity.QueueExecutionStatePaused) {
		state = entity.QueueExecutionStateUnknown
		c.scope.Counter("execution_unknown").Inc(1)
		c.logger.Warnw("queue execution state unavailable", "queue", req.Queue, "error", err)
	}
	result.Execution = entity.QueueExecutionStatus{State: state, ObservedAtMs: c.now().UnixMilli()}
	return result, nil
}

func (c *getQueueStatusController) resolveCommitPolicy(ctx context.Context, req entity.GetQueueStatusRequest, store storage.QueuePolicyStore, current entity.QueuePolicyTransition) (entity.QueuePolicy, error) {
	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: req.Queue})
	if err != nil {
		return entity.QueuePolicy{}, classifyCommitPolicyError(err)
	}
	tip, err := sc.Latest(ctx)
	if err != nil {
		return entity.QueuePolicy{}, classifyCommitPolicyError(err)
	}
	belongs, err := sc.IsAncestor(ctx, req.ChangeURI, tip)
	if err != nil {
		return entity.QueuePolicy{}, classifyCommitPolicyError(err)
	}
	if !belongs {
		return entity.QueuePolicy{}, errs.NewUserError(ErrCommitPolicyUnresolved)
	}
	// A rewritten tip must not turn a formerly enabled commit into initial DISABLED.
	if current.Policy.EffectiveFromCommitURI != "" {
		belongs, err = sc.IsAncestor(ctx, current.Policy.EffectiveFromCommitURI, tip)
		if err != nil {
			return entity.QueuePolicy{}, classifyCommitPolicyError(err)
		}
		if !belongs {
			return entity.QueuePolicy{}, errs.NewUserError(ErrCommitPolicyUnresolved)
		}
	}
	for {
		if current.Policy.Revision == 1 {
			return current.Policy, nil
		}
		covered, err := sc.IsAncestor(ctx, current.Policy.EffectiveFromCommitURI, req.ChangeURI)
		if err != nil {
			return entity.QueuePolicy{}, classifyCommitPolicyError(err)
		}
		if covered {
			return current.Policy, nil
		}
		previous, err := queuepolicy.LoadPrevious(ctx, store, current)
		if err != nil {
			return entity.QueuePolicy{}, err
		}
		if previous.Policy.EffectiveFromCommitURI != "" {
			forward, err := sc.IsAncestor(ctx, previous.Policy.EffectiveFromCommitURI, current.Policy.EffectiveFromCommitURI)
			if err != nil {
				return entity.QueuePolicy{}, classifyCommitPolicyError(err)
			}
			if !forward {
				return entity.QueuePolicy{}, queuepolicy.ErrInconsistentHistory
			}
		}
		current = previous
	}
}

func classifyCommitPolicyError(err error) error {
	if sourcecontrol.IsNotFound(err) {
		return errs.NewUserError(fmt.Errorf("%w: %w", ErrCommitPolicyUnresolved, err))
	}
	return fmt.Errorf("resolve commit policy: %w", err)
}
