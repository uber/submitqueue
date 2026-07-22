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
// into terminal states.
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
// the affected request or batch. Normal pipeline DLQs reconcile requests to
// Error and batches to Failed. The conclude DLQ preserves the terminal batch
// outcome while repairing request fanout. Optimistic locking and idempotent log
// publication let concurrent activity win cleanly.
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

func dlqFailureOutcome(metadata map[string]string) requestcore.TerminalOutcome {
	logMetadata := make(map[string]string, 3)
	for _, key := range []string{"dlq.original_topic", "dlq.failure_count", "dlq.failed_at"} {
		if value := metadata[key]; value != "" {
			logMetadata[key] = value
		}
	}
	return requestcore.TerminalOutcome{
		State:     entity.RequestStateError,
		LastError: metadata["dlq.last_error"],
		Metadata:  logMetadata,
	}
}

// reconcileRequest converges a request on the caller-selected terminal outcome.
// Redelivery for the same terminal state republishes the log to repair a
// previous partial attempt. A different terminal outcome is left unchanged.
//
// Normal pipeline DLQs select RequestStateError for a request in
// RequestStateCancelling because the failed pipeline cannot confirm that
// cancellation completed cleanly.
func reconcileRequest(
	ctx context.Context,
	store storage.Storage,
	registry consumer.TopicRegistry,
	logger *zap.SugaredLogger,
	requestID string,
	outcome requestcore.TerminalOutcome,
) error {
	requestStore := store.GetRequestStore()
	request, err := requestStore.Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			logger.Warnw("dlq reconcile: request not found, skipping",
				"request_id", requestID,
			)
			return nil
		}
		return fmt.Errorf("failed to get request %s: %w", requestID, err)
	}

	reconciled, err := requestcore.ReconcileTerminalState(
		ctx,
		requestStore,
		registry,
		request,
		outcome,
	)
	if err != nil {
		return fmt.Errorf("failed to reconcile request %s: %w", requestID, err)
	}
	if !reconciled {
		logger.Infow("dlq reconcile: request has a different terminal outcome, skipping",
			"request_id", requestID,
			"state", string(request.State),
			"target_state", string(outcome.State),
		)
	}

	return nil
}

// failBatch transitions a batch to BatchStateFailed if it is not already in a
// terminal state, then fans out by transitioning each member request to
// RequestStateError. The fan-out mirrors what the conclude controller would do
// for a normally-completed batch, but skips re-publishing to the conclude
// topic. For DLQ messages there is no guarantee that conclude would ever run,
// so reconciliation drives each request directly.
//
// A batch in BatchStateCancelling is reconciled to BatchStateFailed because the
// failed pipeline cannot confirm that cancellation completed cleanly.
//
// Idempotency: an existing Failed batch repeats fan-out because a previous
// attempt may have crashed after updating the batch. Succeeded and Cancelled
// are different terminal outcomes and do not fan out errors.
// outcome is propagated to each member request's terminal Error log.
func failBatch(
	ctx context.Context,
	store storage.Storage,
	registry consumer.TopicRegistry,
	logger *zap.SugaredLogger,
	batchID string,
	metadata map[string]string,
) error {
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

	outcome := dlqFailureOutcome(metadata)
	for _, requestID := range batch.Contains {
		if err := reconcileRequest(ctx, store, registry, logger, requestID, outcome); err != nil {
			return fmt.Errorf("fan-out for batch %s: %w", batchID, err)
		}
	}
	return nil
}

// concludeBatch preserves a terminal batch outcome and repairs request fanout.
func concludeBatch(
	ctx context.Context,
	store storage.Storage,
	registry consumer.TopicRegistry,
	logger *zap.SugaredLogger,
	batchID string,
	_ map[string]string,
) error {
	batch, err := store.GetBatchStore().Get(ctx, batchID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			logger.Warnw("dlq reconcile: batch not found, skipping", "batch_id", batchID)
			return nil
		}
		return fmt.Errorf("failed to get batch %s: %w", batchID, err)
	}

	requestState, err := terminalRequestState(batch.State)
	if err != nil {
		return fmt.Errorf("cannot reconcile conclude dlq for batch %s: %w", batchID, err)
	}
	outcome := requestcore.TerminalOutcome{
		State:    requestState,
		Metadata: map[string]string{"batch_id": batch.ID},
	}
	for _, requestID := range batch.Contains {
		if err := reconcileRequest(ctx, store, registry, logger, requestID, outcome); err != nil {
			return fmt.Errorf("conclude fan-out for batch %s: %w", batchID, err)
		}
	}
	return nil
}

func terminalRequestState(state entity.BatchState) (entity.RequestState, error) {
	switch state {
	case entity.BatchStateSucceeded:
		return entity.RequestStateLanded, nil
	case entity.BatchStateFailed:
		return entity.RequestStateError, nil
	case entity.BatchStateCancelled:
		return entity.RequestStateCancelled, nil
	default:
		return entity.RequestStateUnknown, fmt.Errorf("batch state %s is not terminal", state)
	}
}
