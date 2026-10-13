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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

var (
	// ErrQueueStateNotFound means the queue has no materialized summary.
	ErrQueueStateNotFound = errors.New("queue state not found")
	// ErrCommitStateUnresolved means the commit or the retained state lineage cannot be resolved.
	ErrCommitStateUnresolved = errors.New("commit state unresolved")
)

// GetQueueStatusController resolves applied state independently of validation requests.
type GetQueueStatusController interface {
	GetQueueStatus(context.Context, entity.GetQueueStatusRequest) (entity.GetQueueStatusResult, error)
}

type getQueueStatusController struct {
	scope         tally.Scope
	stores        storage.Factory
	sourceControl sourcecontrol.Factory
}

// NewGetQueueStatusController creates a lookup over applied state and committed history.
func NewGetQueueStatusController(scope tally.Scope, stores storage.Factory, sourceControl sourcecontrol.Factory) GetQueueStatusController {
	return &getQueueStatusController{scope: scope.SubScope("queue_status_controller"), stores: stores, sourceControl: sourceControl}
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
		return result, fmt.Errorf("resolve queue state storage: %w", err)
	}
	store := stores.GetQueueLogStore()
	current, err := queuelog.LoadSummary(ctx, stores, req.Queue)
	if err != nil {
		if storage.IsNotFound(err) {
			return result, errs.NewUserError(fmt.Errorf("queue summary %q: %w", req.Queue, ErrQueueStateNotFound))
		}
		return result, err
	}
	result.Queue, result.State = req.Queue, current
	if req.ChangeURI != "" {
		result.State, err = c.resolveCommitState(ctx, req, store, current)
		if err != nil {
			return entity.GetQueueStatusResult{}, err
		}
	}
	if ctx.Err() != nil {
		return entity.GetQueueStatusResult{}, ctx.Err()
	}
	return result, nil
}

func (c *getQueueStatusController) resolveCommitState(ctx context.Context, req entity.GetQueueStatusRequest, store storage.QueueLogStore, current entity.QueueState) (entity.QueueState, error) {
	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: req.Queue})
	if err != nil {
		return entity.QueueState{}, classifyCommitStateError(err)
	}
	tip, err := sc.Latest(ctx)
	if err != nil {
		return entity.QueueState{}, classifyCommitStateError(err)
	}
	belongs, err := sc.IsAncestor(ctx, req.ChangeURI, tip)
	if err != nil {
		return entity.QueueState{}, classifyCommitStateError(err)
	}
	if !belongs {
		return entity.QueueState{}, errs.NewUserError(ErrCommitStateUnresolved)
	}
	// A rewritten tip must not turn a formerly enabled commit into initial DISABLED.
	if current.EffectiveFromCommitURI != "" {
		belongs, err = sc.IsAncestor(ctx, current.EffectiveFromCommitURI, tip)
		if err != nil {
			return entity.QueueState{}, classifyCommitStateError(err)
		}
		if !belongs {
			return entity.QueueState{}, errs.NewUserError(ErrCommitStateUnresolved)
		}
	}
	for {
		if current.Version == 1 {
			return current, nil
		}
		covered, err := sc.IsAncestor(ctx, current.EffectiveFromCommitURI, req.ChangeURI)
		if err != nil {
			return entity.QueueState{}, classifyCommitStateError(err)
		}
		if covered {
			return current, nil
		}
		beforeBoundary, err := sc.IsAncestor(ctx, req.ChangeURI, current.EffectiveFromCommitURI)
		if err != nil {
			return entity.QueueState{}, classifyCommitStateError(err)
		}
		if !beforeBoundary {
			return entity.QueueState{}, errs.NewUserError(ErrCommitStateUnresolved)
		}
		previous, err := queuelog.LoadVersion(ctx, store, req.Queue, current.Version-1)
		if err != nil {
			return entity.QueueState{}, err
		}
		if previous.State == current.State {
			return entity.QueueState{}, queuelog.ErrInconsistentHistory
		}
		if previous.EffectiveFromCommitURI != "" {
			forward, err := sc.IsAncestor(ctx, previous.EffectiveFromCommitURI, current.EffectiveFromCommitURI)
			if err != nil {
				return entity.QueueState{}, classifyCommitStateError(err)
			}
			if !forward {
				return entity.QueueState{}, queuelog.ErrInconsistentHistory
			}
		}
		current = previous
	}
}

func classifyCommitStateError(err error) error {
	if sourcecontrol.IsNotFound(err) {
		return errs.NewUserError(fmt.Errorf("%w: %w", ErrCommitStateUnresolved, err))
	}
	return fmt.Errorf("resolve commit state: %w", err)
}
