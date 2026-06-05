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

	"github.com/uber/submitqueue/submitqueue/core/consumer"
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

// failRequest transitions a request to RequestStateError if it is not already
// in a terminal state, and unconditionally appends a RequestStatusError row to
// the request log. The state transition is idempotent — a request already in a
// terminal state skips the UpdateState CAS — but the log insert runs on every
// successful call so that a prior attempt that wrote the Error state but then
// failed to insert the log is repaired on redelivery.
//
// A request in RequestStateCancelling is reconciled to RequestStateError, not
// left in place: DLQ means the pipeline failed to converge, so we cannot
// confirm the cancel completed cleanly. Writing Error is the honest signal and
// keeps the request from being stuck in a non-terminal state forever.
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

	logVersion := request.Version
	if entity.IsRequestStateTerminal(request.State) {
		logger.Infow("dlq reconcile: request already terminal, ensuring log entry",
			"request_id", requestID,
			"state", string(request.State),
		)
	} else {
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

	// Append the terminal Error status to the request log directly via the
	// RequestLogStore, bypassing the log topic and the primary log controller.
	// DLQ controllers must not call back into the primary pipeline (publishing
	// to a primary topic) — the primary pipeline is what failed and routed this
	// message to the DLQ in the first place, and re-entering it would risk the
	// same failure mode that put us here. The log store is the same storage
	// backend the primary log controller eventually writes to, so a direct
	// insert produces an equivalent record without the round-trip.
	//
	// The log entry is written unconditionally — even when the state was already
	// terminal on entry — so a previous DLQ attempt that succeeded in flipping
	// the state but then failed to insert the log is repaired on redelivery.
	// A duplicate log entry from such a retry is the accepted trade-off; a
	// missing one would leave the gateway-visible status divergent from the
	// entity state.
	logEntry := entity.NewRequestLog(requestID, entity.RequestStatusError, logVersion, "", nil)
	if err := store.GetRequestLogStore().Insert(ctx, logEntry); err != nil {
		return fmt.Errorf("failed to insert request log for %s: %w", requestID, err)
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
// Idempotency: if the batch is already terminal the function still fans out
// to the member requests, because a previous attempt may have transitioned
// the batch but crashed before completing the fan-out. Per-request fan-out
// is itself idempotent via failRequest.
func failBatch(ctx context.Context, store storage.Storage, logger *zap.SugaredLogger, batchID string) error {
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

	if batch.State.IsTerminal() {
		logger.Infow("dlq reconcile: batch already terminal, fanning out only",
			"batch_id", batchID,
			"state", string(batch.State),
		)
	} else {
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
		if err := failRequest(ctx, store, logger, requestID); err != nil {
			return fmt.Errorf("fan-out for batch %s: %w", batchID, err)
		}
	}
	return nil
}
