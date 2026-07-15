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

// Package dlq contains controllers that consume messages from a pipeline stage's
// dead-letter topic and reconcile the affected request into a terminal state.
//
// Background. The consumer framework moves a message to its DLQ after the controller
// for the original topic returns a non-retryable error or exhausts retries on a
// retryable error. Without DLQ reconciliation the affected request would remain stuck
// in a non-terminal state (accepted, processing) forever — a caller gating deployments
// on greenness would see that indistinguishably from "not yet validated" (see
// doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work).
//
// Reconciliation strategy. Each DLQ topic carries the same payload as its originating
// topic (the queue framework preserves the bytes verbatim under a new `{topic}_dlq`
// name). The DLQ controller decodes that payload to recover the affected request, then
// transitions it to RequestStateRecordedNotGreen — the conservative not-green verdict
// for gating (see entity.RequestState) — with an idempotent optimistic-locking write so
// concurrent activity (a late successful pipeline transition) wins cleanly. If the request had
// already been admitted (processing) and was holding a concurrency slot, the
// reconciler also releases it by CAS-decrementing the queue's in_flight_count, per
// doc/rfc/stovepipe/steps/process.md#in_flight_count-integrity.
package dlq

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// topicSuffix is appended to a primary topic key to derive the corresponding DLQ topic
// key. The queue extension's DefaultSubscriptionConfig also uses "_dlq" as the DLQ
// topic suffix; keeping both in sync is intentional so a registered DLQ subscription's
// topic name matches the controller's TopicKey().
const topicSuffix = "_dlq"

// TopicKey returns the DLQ topic key for the given primary pipeline topic. It is
// exported so the stovepipe wiring layer can build matching pairs without duplicating
// the suffix literal.
func TopicKey(main consumer.TopicKey) consumer.TopicKey {
	return consumer.TopicKey(string(main) + topicSuffix)
}

// failRequest transitions request to RequestStateRecordedNotGreen if it is not already
// in a terminal state. If the request had reached RequestStateProcessing — meaning process's
// admit step already CAS-incremented the queue's in_flight_count for it — the queue's
// slot is released first. Queue and Request are separate entities with no cross-entity
// transaction, so the two writes cannot be atomic and the ordering picks which crash
// failure mode we accept: a crash between the writes leaves the request non-terminal,
// redelivery re-runs reconciliation, and releaseSlot (which tracks no per-request slot
// ownership) decrements again — transiently over-admitting by one slot until the
// under-count re-converges at releaseSlot's zero clamp. The reverse order would leak
// the slot instead: redelivery skips terminal requests, permanently shrinking the
// queue's capacity toward a wedge. Over-admission is the failure mode we prefer. See
// doc/rfc/stovepipe/steps/process.md#in_flight_count-integrity for the broader
// counter-drift story.
func failRequest(ctx context.Context, store storage.Storage, logger *zap.SugaredLogger, requestID string) error {
	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			logger.Warnw("dlq reconcile: request not found, skipping",
				"request_id", requestID,
			)
			return nil
		}
		return fmt.Errorf("failed to get request %s: %w", requestID, err)
	}

	if request.State.IsTerminal() {
		logger.Infow("dlq reconcile: request already terminal, skipping",
			"request_id", requestID,
			"state", string(request.State),
		)
		return nil
	}

	if request.State == entity.RequestStateProcessing {
		if err := releaseSlot(ctx, store, logger, request.Queue); err != nil {
			return fmt.Errorf("failed to release queue slot for request %s: %w", requestID, err)
		}
	}

	updated := request
	updated.State = entity.RequestStateRecordedNotGreen
	newVersion := request.Version + 1
	if err := store.GetRequestStore().Update(ctx, updated, request.Version, newVersion); err != nil {
		return fmt.Errorf("failed to update request %s state to recorded_not_green: %w", requestID, err)
	}
	logger.Infow("dlq reconcile: request forced terminal not-green",
		"request_id", requestID,
		"previous_state", string(request.State),
	)
	return nil
}

// releaseSlot CAS-decrements the queue's in_flight_count, retrying on version
// conflicts, mirroring process.Controller's own CAS-retry loop for queue updates.
func releaseSlot(ctx context.Context, store storage.Storage, logger *zap.SugaredLogger, queueName string) error {
	queueStore := store.GetQueueStore()

	for {
		queueRow, err := queueStore.Get(ctx, queueName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				logger.Warnw("dlq reconcile: queue not found, skipping slot release",
					"queue", queueName,
				)
				return nil
			}
			return fmt.Errorf("failed to get queue %s: %w", queueName, err)
		}

		if queueRow.InFlightCount <= 0 {
			logger.Warnw("dlq reconcile: queue in_flight_count already at zero, skipping slot release",
				"queue", queueName,
			)
			return nil
		}

		updated := queueRow
		updated.InFlightCount--
		newVersion := queueRow.Version + 1
		if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("failed to release slot for queue %s: %w", queueName, err)
		}
		logger.Infow("dlq reconcile: released queue slot",
			"queue", queueName,
			"in_flight_count", updated.InFlightCount,
		)
		return nil
	}
}
