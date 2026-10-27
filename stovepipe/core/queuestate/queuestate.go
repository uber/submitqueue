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

// Package queuestate applies enablement to internal queue snapshots and repairs their read models.
package queuestate

//go:generate mockgen -source=queuestate.go -destination=mock/queuestate_mock.go -package=mock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// ErrInvalidChange identifies an invalid enablement decision or boundary.
var ErrInvalidChange = errors.New("invalid queue state change")

// PreparedChange captures one decision and its immutable boundary for application retries.
type PreparedChange struct {
	previous entity.QueueState
	next     entity.QueueState
}

// TransitionController changes internal queue enablement and repairs its history and summary.
type TransitionController interface {
	// Initialize explicitly registers disabled enablement while preserving existing state.
	Initialize(ctx context.Context, queue string) (state entity.QueueState, err error)
	// Prepare pins the enablement sequence at the supplied commit URI; same-state decisions are no-ops.
	Prepare(ctx context.Context, queue string, enablement entity.QueueEnablement, boundaryURI string) (change PreparedChange, err error)
	// Apply commits a prepared change; competing stale decisions are never rebased.
	Apply(ctx context.Context, change PreparedChange) (state entity.QueueState, err error)
}

type transitionController struct {
	stores        storage.Factory
	sourceControl sourcecontrol.Factory
	materializer  queuelog.Materializer
	now           func() time.Time
}

func NewTransitionController(stores storage.Factory, sc sourcecontrol.Factory, materializer queuelog.Materializer) TransitionController {
	return &transitionController{stores: stores, sourceControl: sc, materializer: materializer, now: time.Now}
}

func (c *transitionController) Initialize(ctx context.Context, name string) (entity.QueueState, error) {
	stores, err := c.resolveStores(ctx, name)
	if err != nil {
		return entity.QueueState{}, err
	}
	store := stores.GetQueueStore()
	for {
		if err := ctx.Err(); err != nil {
			return entity.QueueState{}, err
		}
		queue, err := store.Get(ctx, name)
		missing := storage.IsNotFound(err)
		if err != nil && !missing {
			return entity.QueueState{}, err
		}
		if !missing {
			if queue.Name != name || queue.Version <= 0 {
				return entity.QueueState{}, fmt.Errorf("initialize queue %q: invalid stored queue %q at row version %d: %w", name, queue.Name, queue.Version, queuelog.ErrInconsistentHistory)
			}
			if queue.EnablementVersion != 0 || queue.Enablement != entity.QueueEnablementUnknown || queue.EffectiveFromCommitURI != "" || queue.EnablementChangedAtMs != 0 {
				return c.repairQueueReadModels(ctx, stores, queue)
			}
		} else {
			queue = entity.Queue{Name: name, Version: 1}
		}
		updated := queue
		updated.Enablement, updated.EnablementVersion = entity.QueueEnablementDisabled, 1
		updated.EnablementChangedAtMs = c.now().UnixMilli()
		if err := updated.EnablementState().Validate(); err != nil {
			return entity.QueueState{}, fmt.Errorf("initialize queue %q: invalid enablement: %w: %v", name, ErrInvalidChange, err)
		}
		if missing {
			if err := store.Create(ctx, updated); errors.Is(err, storage.ErrAlreadyExists) {
				continue
			} else if err != nil {
				return entity.QueueState{}, err
			}
		} else {
			if queue.Version == math.MaxInt32 {
				return entity.QueueState{}, fmt.Errorf("initialize queue %q: row version %d exhausted: %w", name, queue.Version, ErrInvalidChange)
			}
			newVersion := queue.Version + 1
			if err := store.Update(ctx, updated, queue.Version, newVersion); errors.Is(err, storage.ErrVersionMismatch) {
				continue
			} else if err != nil {
				return entity.QueueState{}, err
			}
			updated.Version = newVersion
		}
		return c.repairQueueReadModels(ctx, stores, updated)
	}
}

func (c *transitionController) Prepare(ctx context.Context, queue string, enablement entity.QueueEnablement, boundary string) (PreparedChange, error) {
	if enablement != entity.QueueEnablementEnabled && enablement != entity.QueueEnablementDisabled {
		return PreparedChange{}, fmt.Errorf("prepare queue %q: unsupported enablement %q: %w", queue, enablement, ErrInvalidChange)
	}
	stores, err := c.resolveStores(ctx, queue)
	if err != nil {
		return PreparedChange{}, err
	}
	internal, err := c.loadInternalQueue(ctx, stores, queue)
	if err != nil {
		return PreparedChange{}, err
	}
	current := internal.EnablementState()
	change := PreparedChange{previous: current, next: current}
	if current.State == enablement {
		return change, nil
	}
	if current.Version == math.MaxInt64 {
		return PreparedChange{}, fmt.Errorf("prepare queue %q: enablement version %d exhausted: %w", queue, current.Version, ErrInvalidChange)
	}
	if boundary == "" || len(boundary) > 255 {
		return PreparedChange{}, fmt.Errorf("prepare queue %q: invalid boundary URI %q (length %d): %w", queue, boundary, len(boundary), ErrInvalidChange)
	}
	if current.EffectiveFromCommitURI != "" {
		sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: queue})
		if err != nil {
			return PreparedChange{}, err
		}
		forward, err := sc.IsAncestor(ctx, current.EffectiveFromCommitURI, boundary)
		if err != nil {
			return PreparedChange{}, err
		}
		if !forward {
			return PreparedChange{}, fmt.Errorf("prepare queue %q: boundary %q does not include previous boundary %q: %w", queue, boundary, current.EffectiveFromCommitURI, ErrInvalidChange)
		}
	}
	change.next.State, change.next.EffectiveFromCommitURI = enablement, boundary
	change.next.ChangedAtMs = c.now().UnixMilli()
	if change.next.ChangedAtMs <= 0 {
		return PreparedChange{}, fmt.Errorf("prepare queue %q: invalid enablement timestamp %d: %w", queue, change.next.ChangedAtMs, ErrInvalidChange)
	}
	return change, nil
}

func (c *transitionController) Apply(ctx context.Context, change PreparedChange) (entity.QueueState, error) {
	if err := change.previous.Validate(); err != nil {
		return entity.QueueState{}, fmt.Errorf("apply queue %q: invalid prepared state version %d: %w: %v", change.previous.Queue, change.previous.Version, ErrInvalidChange, err)
	}
	stores, err := c.resolveStores(ctx, change.previous.Queue)
	if err != nil {
		return entity.QueueState{}, err
	}
	for {
		internal, err := c.loadInternalQueue(ctx, stores, change.previous.Queue)
		if err != nil {
			return entity.QueueState{}, err
		}
		current := internal.EnablementState()
		if change.next == change.previous {
			if current == change.previous {
				return current, nil
			}
			retained, err := queuelog.LoadVersion(ctx, stores.GetQueueLogStore(), current.Queue, change.previous.Version)
			if err != nil {
				return entity.QueueState{}, err
			}
			if retained != change.previous {
				return entity.QueueState{}, storage.ErrVersionMismatch
			}
			return retained, nil
		}
		if change.previous.Version == math.MaxInt64 || internal.Version == math.MaxInt32 {
			return entity.QueueState{}, fmt.Errorf("apply queue %q: cannot advance enablement version %d or row version %d: %w", current.Queue, change.previous.Version, internal.Version, ErrInvalidChange)
		}
		stateVersion := change.previous.Version + 1
		if current.Version != change.previous.Version {
			return reconcileChange(ctx, stores.GetQueueLogStore(), current, change, stateVersion)
		}
		if current != change.previous {
			return entity.QueueState{}, storage.ErrVersionMismatch
		}
		if err := c.validatePreparedBoundary(ctx, change); err != nil {
			return entity.QueueState{}, err
		}
		// An enablement change must preserve concurrent pipeline updates.
		updated := internal
		updated.Enablement, updated.EnablementVersion = change.next.State, stateVersion
		updated.EffectiveFromCommitURI, updated.EnablementChangedAtMs = change.next.EffectiveFromCommitURI, change.next.ChangedAtMs
		newVersion := internal.Version + 1
		if err := stores.GetQueueStore().Update(ctx, updated, internal.Version, newVersion); err != nil {
			observed, readErr := c.loadInternalQueue(ctx, stores, current.Queue)
			if readErr != nil {
				return entity.QueueState{}, errors.Join(err, readErr)
			}
			if observed.EnablementState() == change.previous && errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			applied, reconcileErr := reconcileChange(ctx, stores.GetQueueLogStore(), observed.EnablementState(), change, stateVersion)
			if reconcileErr != nil {
				if errors.Is(reconcileErr, storage.ErrVersionMismatch) {
					return entity.QueueState{}, err
				}
				return entity.QueueState{}, reconcileErr
			}
			return applied, nil
		}
		updated.Version = newVersion
		return c.repairQueueReadModels(ctx, stores, updated)
	}
}

func (c *transitionController) resolveStores(ctx context.Context, queue string) (storage.Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if queue == "" || len(queue) > 255 {
		return nil, fmt.Errorf("invalid queue name %q (length %d): %w", queue, len(queue), ErrInvalidChange)
	}
	stores, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return nil, fmt.Errorf("resolve queue state storage: %w", err)
	}
	return stores, nil
}

func (c *transitionController) loadInternalQueue(ctx context.Context, stores storage.Storage, name string) (entity.Queue, error) {
	if err := ctx.Err(); err != nil {
		return entity.Queue{}, err
	}
	queue, err := stores.GetQueueStore().Get(ctx, name)
	if err != nil {
		return entity.Queue{}, err
	}
	if queue.Name != name || queue.Version <= 0 {
		return entity.Queue{}, fmt.Errorf("load queue %q: invalid stored queue %q at row version %d: %w", name, queue.Name, queue.Version, queuelog.ErrInconsistentHistory)
	}
	if queue.EnablementVersion == 0 && queue.Enablement == entity.QueueEnablementUnknown && queue.EffectiveFromCommitURI == "" && queue.EnablementChangedAtMs == 0 {
		return entity.Queue{}, storage.ErrNotFound
	}
	if _, err := c.repairQueueReadModels(ctx, stores, queue); err != nil {
		return entity.Queue{}, err
	}
	return queue, nil
}

func (c *transitionController) repairQueueReadModels(ctx context.Context, stores storage.Storage, queue entity.Queue) (entity.QueueState, error) {
	state := queue.EnablementState()
	if err := state.Validate(); err != nil {
		return entity.QueueState{}, fmt.Errorf("queue %q: invalid internal enablement at state version %d, row version %d: %w: %v", queue.Name, state.Version, queue.Version, queuelog.ErrInconsistentHistory, err)
	}
	return state, c.materializer.PersistLog(ctx, stores, queuelog.NewQueueStateLog(queue))
}

func (c *transitionController) validatePreparedBoundary(ctx context.Context, change PreparedChange) error {
	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: change.previous.Queue})
	if err != nil {
		return err
	}
	tip, err := sc.Latest(ctx)
	if err != nil {
		return err
	}
	belongs, err := sc.IsAncestor(ctx, change.next.EffectiveFromCommitURI, tip)
	if err != nil {
		return err
	}
	if !belongs {
		return fmt.Errorf("queue %q: prepared boundary %q is not an ancestor of current tip %q: %w", change.previous.Queue, change.next.EffectiveFromCommitURI, tip, ErrInvalidChange)
	}
	if change.previous.EffectiveFromCommitURI != "" {
		forward, err := sc.IsAncestor(ctx, change.previous.EffectiveFromCommitURI, change.next.EffectiveFromCommitURI)
		if err != nil {
			return err
		}
		if !forward {
			return fmt.Errorf("queue %q: prepared boundary %q does not include previous boundary %q: %w", change.previous.Queue, change.next.EffectiveFromCommitURI, change.previous.EffectiveFromCommitURI, ErrInvalidChange)
		}
	}
	return nil
}

func reconcileChange(ctx context.Context, logs storage.QueueLogStore, current entity.QueueState, change PreparedChange, version int64) (entity.QueueState, error) {
	if current.Version < version {
		return entity.QueueState{}, storage.ErrVersionMismatch
	}
	applied := current
	if current.Version > version {
		var err error
		applied, err = queuelog.LoadVersion(ctx, logs, current.Queue, version)
		if err != nil {
			return entity.QueueState{}, err
		}
	}
	if applied.Queue != change.next.Queue || applied.State != change.next.State || applied.EffectiveFromCommitURI != change.next.EffectiveFromCommitURI || applied.ChangedAtMs != change.next.ChangedAtMs {
		return entity.QueueState{}, storage.ErrVersionMismatch
	}
	return applied, nil
}
