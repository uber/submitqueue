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

// Package dlq contains controllers that consume messages from per-topic
// dead-letter queues and reconcile the affected request and batch entities
// into a terminal failed state.
//
// Background. The consumer framework moves a message to its DLQ after the
// controller for the original topic returns a non-retryable error or exhausts
// retries on a retryable error. Without DLQ reconciliation the affected
// request would remain stuck in a non-terminal state (e.g. Validated, Batched,
// Processing) forever — the gateway would still report it as "in progress"
// even though no pipeline stage is going to advance it.
//
// Reconciliation strategy. Each DLQ topic carries the same payload as its
// originating topic (the queue framework preserves the bytes verbatim under a
// new `{topic}_dlq` name). The DLQ controllers decode that payload to recover
// the affected request or batch, then transition it to a terminal failed
// state — Error for requests, Failed for batches — with an idempotent
// optimistic-locking write so concurrent activity (a late merge, a cancel
// race) wins cleanly. Batch failures also fan out to the member requests so
// the gateway no longer reports them as in-progress.
package dlq

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	requestcore "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// topicSuffix is appended to a primary topic key to derive the corresponding
// DLQ topic key. The queue extension's DefaultSubscriptionConfig also uses
// "_dlq" as the DLQ topic suffix; keeping both in sync is intentional so
// that a registered DLQ subscription's topic name matches the controller's
// TopicKey().
const topicSuffix = "_dlq"

// TopicKey returns the DLQ topic key for the given primary pipeline topic.
// The returned key is meant to be used both when registering the DLQ topic
// with the topic registry and when the corresponding DLQ controller advertises
// its TopicKey(). It is exported so the orchestrator wiring layer can build
// matching pairs without duplicating the suffix literal.
func TopicKey(main consumer.TopicKey) consumer.TopicKey {
	return consumer.TopicKey(string(main) + topicSuffix)
}

// failRequest transitions a non-terminal request to RequestStateError and
// appends the matching RequestStatusError log. Redelivery for an existing Error
// state repeats materialization to repair a previous partial attempt. A
// different terminal outcome is left unchanged.
// lastError is the failure reason preserved by the queue in DLQ delivery
// metadata and is exposed through Status and History for diagnosis.
//
// A request in RequestStateCancelling is reconciled to RequestStateError, not
// left in place: DLQ means the pipeline failed to converge, so we cannot
// confirm the cancel completed cleanly. Writing Error is the honest signal and
// keeps the request from being stuck in a non-terminal state forever.
func failRequest(ctx context.Context, store storage.Storage, registry consumer.TopicRegistry, logger *zap.SugaredLogger, requestID, lastError string) error {
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

	logVersion := request.Version
	switch request.State {
	case entity.RequestStateError:
		logger.Infow("dlq reconcile: request already failed, republishing terminal log",
			"request_id", requestID,
		)
	case entity.RequestStateLanded, entity.RequestStateCancelled:
		logger.Infow("dlq reconcile: request has a different terminal outcome, skipping",
			"request_id", requestID,
			"state", string(request.State),
		)
		return nil
	default:
		newVersion := request.Version + 1
		if err := store.GetRequestStore().UpdateState(ctx, requestID, request.Version, newVersion, entity.RequestStateError); err != nil {
			return fmt.Errorf("failed to update request %s state to error: %w", requestID, err)
		}
		logVersion = newVersion
		logger.Infow("dlq reconcile: request marked terminal error",
			"request_id", requestID,
			"previous_state", string(request.State),
		)
	}

	// Publish the terminal Error status through the log topic so Gateway remains
	// the sole writer of request logs and public projections. An existing Error
	// state republishes the same logical event so a previous attempt that changed
	// the entity but failed to publish can be repaired.
	logEntry := entity.NewRequestLog(requestID, entity.RequestStatusError, logVersion, lastError, nil)
	if err := requestcore.PublishLog(ctx, registry, logEntry, requestID); err != nil {
		return fmt.Errorf("failed to publish request log for %s: %w", requestID, err)
	}

	return nil
}

// failBatch transitions a batch to BatchStateFailed if it is not already in a
// terminal state, then fans out by transitioning each member request to
// RequestStateError. The fan-out mirrors what the conclude controller would do
// for a normally-completed batch, but skips re-publishing to the conclude
// topic — for DLQ messages there is no guarantee that conclude would ever run,
// so the reconciliation has to drive each request directly.
//
// A batch in BatchStateCancelling is reconciled to BatchStateFailed for the
// same reason failRequest reconciles Cancelling requests: DLQ means we cannot
// confirm the cancel completed, so the batch must reach a terminal state.
//
// Idempotency: an existing Failed batch repeats fan-out because a previous
// attempt may have crashed after updating the batch. Succeeded and Cancelled
// are different terminal outcomes and do not fan out errors.
// lastError is propagated to each member request's terminal Error log.
func failBatch(ctx context.Context, store storage.Storage, registry consumer.TopicRegistry, logger *zap.SugaredLogger, batchID, lastError string) error {
	batch, err := store.GetBatchStore().Get(ctx, batchID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			logger.Warnw("dlq reconcile: batch not found, skipping",
				"batch_id", batchID,
			)
			return nil
		}
		return fmt.Errorf("failed to get batch %s: %w", batchID, err)
	}

	switch batch.State {
	case entity.BatchStateFailed:
		logger.Infow("dlq reconcile: batch already failed, repairing request fan-out",
			"batch_id", batchID,
		)
	case entity.BatchStateSucceeded, entity.BatchStateCancelled:
		logger.Infow("dlq reconcile: batch has a different terminal outcome, skipping",
			"batch_id", batchID,
			"state", string(batch.State),
		)
		return nil
	default:
		newVersion := batch.Version + 1
		if err := store.GetBatchStore().UpdateState(ctx, batchID, batch.Version, newVersion, entity.BatchStateFailed); err != nil {
			return fmt.Errorf("failed to update batch %s state to failed: %w", batchID, err)
		}
		logger.Infow("dlq reconcile: batch marked failed",
			"batch_id", batchID,
			"previous_state", string(batch.State),
		)
	}

	for _, requestID := range batch.Contains {
		if err := failRequest(ctx, store, registry, logger, requestID, lastError); err != nil {
			return fmt.Errorf("fan-out for batch %s: %w", batchID, err)
		}
	}
	return nil
}
