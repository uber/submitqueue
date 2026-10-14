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
		return entity.QueueState{}, ErrInconsistentHistory
	}
	if err := summary.Validate(); err != nil {
		return entity.QueueState{}, fmt.Errorf("%w: %v", ErrInconsistentHistory, err)
	}
	retained, err := LoadVersion(ctx, stores.GetQueueLogStore(), queue, summary.QueueStateVersion)
	if err != nil {
		return entity.QueueState{}, err
	}
	if retained != summary.Snapshot() {
		return entity.QueueState{}, ErrInconsistentHistory
	}
	return summary.Snapshot(), nil
}

// LoadVersion returns ErrInconsistentHistory for missing or invalid history.
func LoadVersion(ctx context.Context, logs storage.QueueLogStore, queue string, version int64) (entity.QueueState, error) {
	log, err := logs.Get(ctx, entity.QueueLogID(version))
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.QueueState{}, fmt.Errorf("missing state version %d: %w", version, ErrInconsistentHistory)
		}
		return entity.QueueState{}, err
	}
	if log.Queue != queue || log.QueueStateVersion != version {
		return entity.QueueState{}, ErrInconsistentHistory
	}
	if err := log.Validate(); err != nil {
		return entity.QueueState{}, fmt.Errorf("%w: %v", ErrInconsistentHistory, err)
	}
	return log.Snapshot(), nil
}
