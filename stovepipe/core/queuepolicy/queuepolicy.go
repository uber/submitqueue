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

// Package queuepolicy owns applied policy transitions and their committed history.
// A transition is created before its versioned head pointer is advanced. Only
// occurrences reachable from that pointer are committed, so a lost CAS leaves an
// unreachable proposal rather than a second applied policy at one revision.
package queuepolicy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	initialTransitionID = "initial"
	maxIdentifierBytes  = 255
)

var (
	// ErrInvalidChange identifies an invalid state, operation identity, or commit boundary.
	ErrInvalidChange = errors.New("invalid queue policy change")
	// ErrOperationConflict identifies an idempotency key reused with different content.
	ErrOperationConflict = errors.New("queue policy operation conflict")
	// ErrInconsistentHistory identifies a broken committed policy pointer or predecessor chain.
	ErrInconsistentHistory = errors.New("inconsistent queue policy history")
)

// Writer applies policy changes under optimistic concurrency without publishing notifications.
type Writer interface {
	// Initialize creates the initial disabled policy for an explicitly registered queue; retries preserve existing state.
	Initialize(ctx context.Context, queue string) (entity.QueuePolicy, error)
	// Apply commits a caller-owned operation against ExpectedRevision. A committed retry returns its original policy.
	Apply(ctx context.Context, req entity.ApplyQueuePolicyRequest) (entity.QueuePolicy, error)
}

type writer struct {
	stores        storage.Factory
	sourceControl sourcecontrol.Factory
	now           func() time.Time
}

// NewWriter constructs the write path used by host configuration or administration.
func NewWriter(stores storage.Factory, sourceControl sourcecontrol.Factory) Writer {
	return &writer{stores: stores, sourceControl: sourceControl, now: time.Now}
}

func (w *writer) Initialize(ctx context.Context, queue string) (entity.QueuePolicy, error) {
	if err := validateIdentifier(queue); err != nil {
		return entity.QueuePolicy{}, err
	}
	stores, err := w.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return entity.QueuePolicy{}, fmt.Errorf("resolve policy storage for %q: %w", queue, err)
	}
	store := stores.GetQueuePolicyStore()
	_, current, err := LoadCurrent(ctx, store, queue)
	if err == nil {
		return current.Policy, nil
	}
	if !storage.IsNotFound(err) {
		return entity.QueuePolicy{}, err
	}
	initial := entity.QueuePolicyTransition{
		ID: initialTransitionID, Queue: queue,
		Policy: entity.QueuePolicy{Revision: 1, State: entity.QueuePolicyStateDisabled, ChangedAtMs: w.now().UnixMilli()},
	}
	if err := store.CreateTransition(ctx, initial); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return entity.QueuePolicy{}, fmt.Errorf("create initial policy for %q: %w", queue, err)
		}
	}
	initial, err = readTransition(ctx, store, queue, initialTransitionID, 1)
	if err != nil {
		return entity.QueuePolicy{}, err
	}
	head := entity.QueuePolicyHead{Queue: queue, TransitionID: initial.ID, Revision: 1, Version: 1}
	if err := store.CreateCurrent(ctx, head); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return entity.QueuePolicy{}, fmt.Errorf("create current policy for %q: %w", queue, err)
	}
	_, current, err = LoadCurrent(ctx, store, queue)
	return current.Policy, err
}

func (w *writer) Apply(ctx context.Context, req entity.ApplyQueuePolicyRequest) (entity.QueuePolicy, error) {
	if err := validateChange(req); err != nil {
		return entity.QueuePolicy{}, err
	}
	stores, err := w.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.QueuePolicy{}, fmt.Errorf("resolve policy storage for %q: %w", req.Queue, err)
	}
	store := stores.GetQueuePolicyStore()
	head, current, err := LoadCurrent(ctx, store, req.Queue)
	if err != nil {
		return entity.QueuePolicy{}, err
	}
	candidate, err := store.GetTransition(ctx, req.OperationID)
	exists := err == nil
	if err != nil && !storage.IsNotFound(err) {
		return entity.QueuePolicy{}, fmt.Errorf("read policy operation %q: %w", req.OperationID, err)
	}
	if exists {
		if !matchesOperation(candidate, req) {
			return entity.QueuePolicy{}, errs.NewUserError(ErrOperationConflict)
		}
		committed, err := isCommitted(ctx, store, current, candidate)
		if err != nil {
			return entity.QueuePolicy{}, err
		}
		if committed {
			return candidate.Policy, nil
		}
	}
	if head.Revision != req.ExpectedRevision || (exists && candidate.PreviousID != head.TransitionID) {
		return entity.QueuePolicy{}, storage.ErrVersionMismatch
	}
	if current.Policy.State == req.State {
		return entity.QueuePolicy{}, errs.NewUserError(fmt.Errorf("policy is already %s: %w", req.State, ErrInvalidChange))
	}
	if head.Version == math.MaxInt64 {
		return entity.QueuePolicy{}, fmt.Errorf("policy storage version exhausted: %w", ErrInconsistentHistory)
	}
	if err := w.validateBoundary(ctx, req, current.Policy); err != nil {
		return entity.QueuePolicy{}, err
	}
	if !exists {
		candidate = entity.QueuePolicyTransition{
			ID: req.OperationID, Queue: req.Queue, PreviousID: current.ID,
			Policy: entity.QueuePolicy{
				Revision: head.Revision + 1, State: req.State,
				EffectiveFromCommitURI: req.EffectiveFromCommitURI, ChangedAtMs: w.now().UnixMilli(),
			},
		}
		if err := store.CreateTransition(ctx, candidate); err != nil {
			if !errors.Is(err, storage.ErrAlreadyExists) {
				return entity.QueuePolicy{}, fmt.Errorf("create policy operation %q: %w", req.OperationID, err)
			}
			candidate, err = readTransition(ctx, store, req.Queue, req.OperationID, head.Revision+1)
			if err != nil {
				return entity.QueuePolicy{}, err
			}
			if !matchesOperation(candidate, req) || candidate.PreviousID != head.TransitionID {
				return entity.QueuePolicy{}, errs.NewUserError(ErrOperationConflict)
			}
		}
	}
	updated := head
	updated.TransitionID, updated.Revision = candidate.ID, candidate.Policy.Revision
	newVersion := head.Version + 1
	if err := store.UpdateCurrent(ctx, updated, head.Version, newVersion); err != nil {
		if !errors.Is(err, storage.ErrVersionMismatch) {
			return entity.QueuePolicy{}, fmt.Errorf("commit policy operation %q: %w", req.OperationID, err)
		}
		_, canonical, readErr := LoadCurrent(ctx, store, req.Queue)
		if readErr != nil {
			return entity.QueuePolicy{}, readErr
		}
		committed, readErr := isCommitted(ctx, store, canonical, candidate)
		if readErr != nil {
			return entity.QueuePolicy{}, readErr
		}
		if !committed {
			return entity.QueuePolicy{}, err
		}
	}
	return candidate.Policy, nil
}

// LoadCurrent pins a pointer and validates the immutable occurrence it names.
func LoadCurrent(ctx context.Context, store storage.QueuePolicyStore, queue string) (entity.QueuePolicyHead, entity.QueuePolicyTransition, error) {
	head, err := store.GetCurrent(ctx)
	if err != nil {
		return entity.QueuePolicyHead{}, entity.QueuePolicyTransition{}, err
	}
	if head.Queue != queue || head.Revision <= 0 || head.Version <= 0 || head.TransitionID == "" {
		return entity.QueuePolicyHead{}, entity.QueuePolicyTransition{}, ErrInconsistentHistory
	}
	transition, err := readTransition(ctx, store, queue, head.TransitionID, head.Revision)
	return head, transition, err
}

// LoadPrevious validates the adjacent revision in the committed predecessor chain.
func LoadPrevious(ctx context.Context, store storage.QueuePolicyStore, current entity.QueuePolicyTransition) (entity.QueuePolicyTransition, error) {
	if current.Policy.Revision <= 1 || current.PreviousID == "" {
		return entity.QueuePolicyTransition{}, ErrInconsistentHistory
	}
	previous, err := readTransition(ctx, store, current.Queue, current.PreviousID, current.Policy.Revision-1)
	if err != nil {
		return entity.QueuePolicyTransition{}, err
	}
	if previous.Policy.State == current.Policy.State {
		return entity.QueuePolicyTransition{}, ErrInconsistentHistory
	}
	return previous, nil
}

func readTransition(ctx context.Context, store storage.QueuePolicyStore, queue, id string, revision int64) (entity.QueuePolicyTransition, error) {
	transition, err := store.GetTransition(ctx, id)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.QueuePolicyTransition{}, fmt.Errorf("missing referenced policy %q: %w", id, ErrInconsistentHistory)
		}
		return entity.QueuePolicyTransition{}, fmt.Errorf("read policy %q: %w", id, err)
	}
	policy := transition.Policy
	if transition.Queue != queue || transition.ID != id || policy.Revision != revision || policy.ChangedAtMs <= 0 ||
		(policy.State != entity.QueuePolicyStateEnabled && policy.State != entity.QueuePolicyStateDisabled) {
		return entity.QueuePolicyTransition{}, ErrInconsistentHistory
	}
	if revision == 1 {
		if id != initialTransitionID || transition.PreviousID != "" || policy.State != entity.QueuePolicyStateDisabled || policy.EffectiveFromCommitURI != "" {
			return entity.QueuePolicyTransition{}, ErrInconsistentHistory
		}
	} else if revision < 1 || id == initialTransitionID || transition.PreviousID == "" || policy.EffectiveFromCommitURI == "" {
		return entity.QueuePolicyTransition{}, ErrInconsistentHistory
	}
	return transition, nil
}

func isCommitted(ctx context.Context, store storage.QueuePolicyStore, current, candidate entity.QueuePolicyTransition) (bool, error) {
	for current.Policy.Revision > candidate.Policy.Revision {
		previous, err := LoadPrevious(ctx, store, current)
		if err != nil {
			return false, err
		}
		current = previous
	}
	return current.ID == candidate.ID && current.Policy.Revision == candidate.Policy.Revision, nil
}

func matchesOperation(transition entity.QueuePolicyTransition, req entity.ApplyQueuePolicyRequest) bool {
	return transition.ID == req.OperationID && transition.Queue == req.Queue &&
		transition.Policy.Revision == req.ExpectedRevision+1 && transition.Policy.State == req.State &&
		transition.Policy.EffectiveFromCommitURI == req.EffectiveFromCommitURI && transition.Policy.ChangedAtMs > 0
}

func (w *writer) validateBoundary(ctx context.Context, req entity.ApplyQueuePolicyRequest, current entity.QueuePolicy) error {
	sc, err := w.sourceControl.For(sourcecontrol.Config{QueueName: req.Queue})
	if err != nil {
		return fmt.Errorf("resolve policy source control for %q: %w", req.Queue, err)
	}
	tip, err := sc.Latest(ctx)
	if err != nil {
		return fmt.Errorf("resolve policy queue tip for %q: %w", req.Queue, err)
	}
	onBranch, err := sc.IsAncestor(ctx, req.EffectiveFromCommitURI, tip)
	if err != nil {
		return fmt.Errorf("resolve policy boundary on queue %q: %w", req.Queue, err)
	}
	if !onBranch {
		return errs.NewUserError(fmt.Errorf("policy boundary is not on the queue's branch: %w", ErrInvalidChange))
	}
	if current.EffectiveFromCommitURI != "" {
		forward, err := sc.IsAncestor(ctx, current.EffectiveFromCommitURI, req.EffectiveFromCommitURI)
		if err != nil {
			return fmt.Errorf("compare policy boundaries for %q: %w", req.Queue, err)
		}
		if !forward {
			return errs.NewUserError(fmt.Errorf("policy boundary moves backward or changes lineage: %w", ErrInvalidChange))
		}
	}
	return nil
}

func validateChange(req entity.ApplyQueuePolicyRequest) error {
	for _, value := range []string{req.Queue, req.OperationID, req.EffectiveFromCommitURI} {
		if err := validateIdentifier(value); err != nil {
			return err
		}
	}
	if req.OperationID == initialTransitionID || req.ExpectedRevision <= 0 || req.ExpectedRevision == math.MaxInt64 ||
		(req.State != entity.QueuePolicyStateEnabled && req.State != entity.QueuePolicyStateDisabled) {
		return errs.NewUserError(ErrInvalidChange)
	}
	return nil
}

func validateIdentifier(value string) error {
	if value == "" || len(value) > maxIdentifierBytes {
		return errs.NewUserError(ErrInvalidChange)
	}
	return nil
}
