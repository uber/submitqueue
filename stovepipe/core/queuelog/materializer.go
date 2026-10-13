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

// Package queuelog retains queue history and projects its current status summary.
package queuelog

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// ErrInconsistentHistory identifies missing or conflicting committed occurrences or projections.
var ErrInconsistentHistory = errors.New("inconsistent queue history")

// Materializer retains committed queue occurrences and advances their derived summary.
type Materializer interface {
	// PersistLog idempotently retains an occurrence, then projects its state.
	PersistLog(context.Context, storage.Storage, entity.QueueLog) error
}

type materializer struct{ scope tally.Scope }

// NewMaterializer creates a queue history materializer with bounded outcome metrics.
func NewMaterializer(scope tally.Scope) Materializer {
	return &materializer{scope: scope.SubScope("queue_log_materializer")}
}

// NewQueueStateLog represents the committed enablement occurrence in an internal queue snapshot.
func NewQueueStateLog(queue entity.Queue) entity.QueueLog {
	return entity.QueueLog{ID: entity.QueueLogID(queue.EnablementVersion), Queue: queue.Name,
		State: queue.Enablement, QueueStateVersion: queue.EnablementVersion,
		EffectiveFromCommitURI: queue.EffectiveFromCommitURI, TimestampMs: queue.EnablementChangedAtMs}
}

func (m *materializer) PersistLog(ctx context.Context, stores storage.Storage, log entity.QueueLog) (retErr error) {
	op := metrics.Begin(m.scope, "persist_log", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := log.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInconsistentHistory, err)
	}
	logs := stores.GetQueueLogStore()
	if err := logs.Create(ctx, log); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("retain queue log: %w", err)
		}
		retained, err := logs.Get(ctx, log.ID)
		if err != nil {
			return fmt.Errorf("reconcile queue log: %w", err)
		}
		if retained != log {
			return ErrInconsistentHistory
		}
		log = retained
	}
	return materializeQueueSummary(ctx, stores.GetQueueSummaryStore(), log)
}

func materializeQueueSummary(ctx context.Context, store storage.QueueSummaryStore, log entity.QueueLog) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := store.Get(ctx)
		if storage.IsNotFound(err) {
			initial := entity.QueueSummary{Queue: log.Queue, State: log.State,
				QueueStateVersion: log.QueueStateVersion, EffectiveFromCommitURI: log.EffectiveFromCommitURI,
				StateTimestampMs: log.TimestampMs, Version: 1}
			if err := store.Create(ctx, initial); errors.Is(err, storage.ErrAlreadyExists) {
				continue
			} else {
				return err
			}
		}
		if err != nil {
			return err
		}
		if current.Queue != log.Queue {
			return ErrInconsistentHistory
		}
		if err := current.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInconsistentHistory, err)
		}
		if log.QueueStateVersion < current.QueueStateVersion {
			return nil
		}
		if log.QueueStateVersion == current.QueueStateVersion {
			if current.Snapshot() != log.Snapshot() {
				return ErrInconsistentHistory
			}
			return nil
		}
		if current.Version == math.MaxInt64 {
			return ErrInconsistentHistory
		}
		updated := current
		updated.State, updated.QueueStateVersion = log.State, log.QueueStateVersion
		updated.EffectiveFromCommitURI, updated.StateTimestampMs = log.EffectiveFromCommitURI, log.TimestampMs
		newVersion := current.Version + 1
		if err := store.Update(ctx, updated, current.Version, newVersion); errors.Is(err, storage.ErrVersionMismatch) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
}
