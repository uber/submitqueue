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
// transitions it to RequestStateFailed — the conservative unsuccessful outcome, which
// whatever records greenness treats as not-green for gating (see entity.RequestState) —
// with an idempotent optimistic-locking write so
// concurrent activity (a late successful pipeline transition) wins cleanly. Before that write
// the reconciler releases the request's build slot by removing its ID from the queue's
// in_flight_request_ids. Ownership is keyed by request ID, so the release is idempotent and
// runs for any nonterminal request; a crash between process's claim and admit is repaired
// when the redelivery recognizes its own claim. See
// doc/rfc/stovepipe/steps/process.md#in_flight_request_ids-integrity.
package dlq

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
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

// failRequest marks a nonterminal request failed, releasing its build slot first: a terminal
// request is skipped on redelivery, so a slot left behind would never be released.
func failRequest(
	ctx context.Context,
	store storage.Storage,
	materializer requestlog.Materializer,
	logger *zap.SugaredLogger,
	requestID string,
	reason entity.RequestOutcomeReason,
) error {
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
		if request.State == entity.RequestStateFailed {
			// The originating DLQ remains the retry trigger after the state write. Its stage-specific
			// reason repairs that write's missing log; PersistLog rejects an already-retained conflict.
			return persistFailureLog(ctx, store, materializer, request, reason)
		}
		logger.Infow("dlq reconcile: request already terminal, skipping",
			"request_id", requestID,
			"state", string(request.State),
		)
		return nil
	}

	if err := releaseSlot(ctx, store, logger, request); err != nil {
		return fmt.Errorf("failed to release queue slot for request %s: %w", requestID, err)
	}

	updated := request
	updated.State = entity.RequestStateFailed
	newVersion := request.Version + 1
	if err := store.GetRequestStore().Update(ctx, updated, request.Version, newVersion); err != nil {
		return fmt.Errorf("failed to update request %s state to failed: %w", requestID, err)
	}
	updated.Version = newVersion
	if err := persistFailureLog(ctx, store, materializer, updated, reason); err != nil {
		return err
	}
	logger.Infow("dlq reconcile: request forced terminal failed",
		"request_id", requestID,
		"previous_state", string(request.State),
	)
	return nil
}

func persistFailureLog(
	ctx context.Context,
	store storage.Storage,
	materializer requestlog.Materializer,
	request entity.Request,
	reason entity.RequestOutcomeReason,
) error {
	log := requestlog.NewRequestStateLog(request, reason)
	if err := materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record failed state for request %s: %w", request.ID, err)
	}
	return nil
}

// releaseSlot CAS-removes request's ID from the queue's in_flight_request_ids, writing
// nothing when it is absent and retrying on version conflicts.
func releaseSlot(ctx context.Context, store storage.Storage, logger *zap.SugaredLogger, request entity.Request) error {
	queueStore := store.GetQueueStore()

	for {
		queueRow, err := queueStore.Get(ctx, request.Queue)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				logger.Warnw("dlq reconcile: queue not found, skipping slot release",
					"queue", request.Queue,
				)
				return nil
			}
			return fmt.Errorf("failed to get queue %s: %w", request.Queue, err)
		}

		ownerIndex := slices.Index(queueRow.InFlightRequestIDs, request.ID)
		if ownerIndex < 0 {
			return nil
		}

		updated := queueRow
		updated.InFlightRequestIDs = slices.Delete(slices.Clone(queueRow.InFlightRequestIDs), ownerIndex, ownerIndex+1)
		newVersion := queueRow.Version + 1
		if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("failed to release slot for request %s in queue %s: %w", request.ID, request.Queue, err)
		}
		logger.Infow("dlq reconcile: released queue slot",
			"queue", request.Queue,
			"request_id", request.ID,
			"in_flight_request_ids", updated.InFlightRequestIDs,
		)
		return nil
	}
}
