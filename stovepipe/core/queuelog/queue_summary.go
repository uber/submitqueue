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

package queuelog

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// LoadSummary requires the summary to match its retained history entry.
func LoadSummary(ctx context.Context, stores storage.Storage, queue string) (entity.QueueState, error) {
	if err := ctx.Err(); err != nil {
		return entity.QueueState{}, err
	}
	summary, err := stores.GetQueueSummaryStore().Get(ctx)
	if err != nil {
		return entity.QueueState{}, err
	}
	if summary.Queue != queue {
		return entity.QueueState{}, fmt.Errorf("queue %q: summary belongs to queue %q at projection version %d: %w", queue, summary.Queue, summary.Version, ErrInconsistentHistory)
	}
	if err := summary.Validate(); err != nil {
		return entity.QueueState{}, fmt.Errorf("queue %q: invalid summary at projection version %d, state version %d: %w: %v", queue, summary.Version, summary.QueueStateVersion, ErrInconsistentHistory, err)
	}
	retained, err := LoadVersion(ctx, stores.GetQueueLogStore(), queue, summary.QueueStateVersion)
	if err != nil {
		return entity.QueueState{}, err
	}
	if retained != summary.Snapshot() {
		return entity.QueueState{}, fmt.Errorf("queue %q: summary state version %d at projection version %d conflicts with retained history: %w", queue, summary.QueueStateVersion, summary.Version, ErrInconsistentHistory)
	}
	return summary.Snapshot(), nil
}

// LoadVersion returns ErrInconsistentHistory for missing or invalid history.
func LoadVersion(ctx context.Context, logs storage.QueueLogStore, queue string, version int64) (entity.QueueState, error) {
	logID := entity.QueueLogID(version)
	log, err := logs.Get(ctx, logID)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.QueueState{}, fmt.Errorf("queue %q: missing log %q for state version %d: %w", queue, logID, version, ErrInconsistentHistory)
		}
		return entity.QueueState{}, err
	}
	if log.Queue != queue || log.QueueStateVersion != version {
		return entity.QueueState{}, fmt.Errorf("queue %q: log %q for state version %d belongs to queue %q at state version %d: %w", queue, logID, version, log.Queue, log.QueueStateVersion, ErrInconsistentHistory)
	}
	if err := log.Validate(); err != nil {
		return entity.QueueState{}, fmt.Errorf("queue %q: invalid log %q at state version %d: %w: %v", queue, logID, version, ErrInconsistentHistory, err)
	}
	return log.Snapshot(), nil
}
