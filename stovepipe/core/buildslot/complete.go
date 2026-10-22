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

// Package buildslot reserves and completes request-owned build capacity and baselines.
package buildslot

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// Complete releases a terminal request's slot and advances its successful baseline
// in one queue update. The durable Request outcome must be written first.
// Repeated calls cannot release another request's capacity.
func Complete(ctx context.Context, store storage.Storage, request entity.Request) error {
	if !request.State.IsTerminal() || request.ID == "" || request.Queue == "" {
		return fmt.Errorf("build slot completion requires a terminal request with an ID and queue")
	}
	queueStore := store.GetQueueStore()
	for {
		queue, err := queueStore.Get(ctx, request.Queue)
		if err != nil {
			return fmt.Errorf("load queue %q for build slot completion: %w", request.Queue, err)
		}
		if err := queue.ValidateBuildSlots(); err != nil {
			return err
		}
		index := slices.Index(queue.InFlightRequestIDs, request.ID)
		if index < 0 {
			return nil
		}

		updated := queue
		updated.InFlightRequestIDs = slices.Delete(slices.Clone(queue.InFlightRequestIDs), index, index+1)
		if len(updated.InFlightRequestIDs) == 0 {
			updated.InFlightRequestIDs = nil
		}
		updated.InFlightCount = int32(len(updated.InFlightRequestIDs))
		if request.State == entity.RequestStateSucceeded {
			if request.URI == "" {
				return fmt.Errorf("successful request %q has no baseline URI", request.ID)
			}
			newer := queue.LastGreenRequestID == ""
			if !newer {
				comparison, err := entity.CompareRequestID(request.ID, queue.LastGreenRequestID)
				if err != nil {
					return fmt.Errorf("compare build baseline for queue %q: %w", request.Queue, err)
				}
				newer = comparison > 0
			}
			if newer {
				updated.LastGreenURI = request.URI
				updated.LastGreenRequestID = request.ID
			}
		}
		if err := queueStore.Update(ctx, updated, queue.Version, queue.Version+1); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("complete build slot for request %q: %w", request.ID, err)
		}
		return nil
	}
}
