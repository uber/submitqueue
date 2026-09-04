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
// optimistic-locking write so concurrent activity (a late land, a cancel
// race) wins cleanly. Batch failures also fan out to the member requests so
// the gateway no longer reports them as in-progress.
package dlq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/uber/submitqueue/platform/consumer"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
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

// failureContext reads everything the queue recorded about a dead-lettered
// message: the human-readable reason, and a metadata map to carry alongside it
// on the terminal request log.
//
// The map is what makes a dead letter diagnosable after the fact. The queue
// already hands the reconciler the failure count, the topic it failed on, and
// when — and, when the producer attributed it, which entities it was about.
// All of that used to be logged and then dropped; the request log is where a
// user can actually see it, through the gateway's status and history.
//
// Values are flattened to strings because RequestLog.Metadata and the gateway's
// wire contract are both string maps. Nested detail becomes dotted keys, which
// read well in a display surface where a JSON blob would not.
func failureContext(delivery consumer.Delivery) (string, map[string]string) {
	dmeta := delivery.Metadata()
	metadata := make(map[string]string, len(dmeta))
	for _, key := range []string{"dlq.original_topic", "dlq.failure_count", "dlq.failed_at"} {
		if v, ok := dmeta[key]; ok && v != "" {
			metadata[key] = v
		}
	}

	lastError := dmeta["dlq.last_error"]

	f, failed := delivery.Failure()
	if !failed {
		return lastError, metadata
	}
	if f.Message != "" {
		lastError = f.Message
	}

	// One key per subject type, so several batches at fault read as a list
	// rather than overwriting each other.
	byType := make(map[string][]string, len(f.Subjects))
	for _, s := range f.Subjects {
		byType[s.Type] = append(byType[s.Type], s.ID)
	}
	for subjectType, ids := range byType {
		metadata["dlq.subject."+subjectType] = strings.Join(ids, ",")
	}

	flattenDetail("dlq.detail", f.Detail, metadata)

	return lastError, metadata
}

// flattenDetail writes a JSON-shaped document into a string map, joining nested
// keys with dots. Values are rendered with %v: this is a display surface, not a
// contract, so a readable rendering beats a faithful one.
func flattenDetail(prefix string, detail map[string]any, out map[string]string) {
	for k, v := range detail {
		key := prefix + "." + k
		if nested, ok := v.(map[string]any); ok {
			flattenDetail(key, nested, out)
			continue
		}
		out[key] = fmt.Sprintf("%v", v)
	}
}

// failRequest transitions a non-terminal request to RequestStateError and
// appends the matching RequestStatusError log. Redelivery for an existing Error
// state repeats materialization to repair a previous partial attempt. A
// different terminal outcome is left unchanged.
// lastError is the failure reason preserved by the queue in DLQ delivery
// metadata and is exposed through Status and History for diagnosis, alongside
// metadata carrying the rest of the failure context.
//
// A request in RequestStateCancelling is reconciled to RequestStateError, not
// left in place: DLQ means the pipeline failed to converge, so we cannot
// confirm the cancel completed cleanly. Writing Error is the honest signal and
// keeps the request from being stuck in a non-terminal state forever.
func failRequest(ctx context.Context, store storage.Storage, registry consumer.TopicRegistry, logger *zap.SugaredLogger, requestID, lastError string, metadata map[string]string) error {
	res, err := requestcore.TerminateRequest(ctx, store, registry, requestID, entity.RequestStateError, lastError, metadata)
	if err != nil {
		return fmt.Errorf("dlq reconcile request %s failed: %w", requestID, err)
	}

	switch res.Outcome {
	case requestcore.TerminationOutcomeSuccess:
		logger.Infow("dlq reconcile: request marked terminal error",
			"request_id", requestID,
			"previous_state", string(res.BeforeState),
		)
	case requestcore.TerminationOutcomeAlreadyInTargetState:
		logger.Infow("dlq reconcile: request already failed, republished terminal log", "request_id", requestID)
	case requestcore.TerminationOutcomeDiverged:
		logger.Infow("dlq reconcile: request has a different terminal outcome, skipping",
			"request_id", requestID,
			"state", string(res.BeforeState),
		)
	case requestcore.TerminationOutcomeNotFound:
		logger.Warnw("dlq reconcile: request not found, skipping", "request_id", requestID)
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
// lastError and metadata are propagated to each member request's terminal
// Error log.
//
// It reports whether it transitioned the batch. A caller that only wants to act
// on real progress — republishing to wake the queue, say — can then tell a
// first reconcile from a redelivery of one already done, and avoid doing it
// again forever.
func failBatch(ctx context.Context, store storage.Storage, registry consumer.TopicRegistry, logger *zap.SugaredLogger, batchID, lastError string, metadata map[string]string) (bool, error) {
	batch, err := store.GetBatchStore().Get(ctx, batchID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			logger.Warnw("dlq reconcile: batch not found, skipping",
				"batch_id", batchID,
			)
			return false, nil
		}
		return false, fmt.Errorf("failed to get batch %s: %w", batchID, err)
	}

	transitioned := false

	switch batch.State {
	case entity.BatchStateFailed:
		logger.Infow("dlq reconcile: batch already failed, repairing request fan-out",
			"batch_id", batchID,
		)
		// A prior attempt may have CAS'd to Failed without completing the
		// membership record move; repair it alongside the fan-out.
		if err := corebatch.EnsureRecord(ctx, store, batch); err != nil {
			return false, err
		}
	case entity.BatchStateSucceeded, entity.BatchStateCancelled:
		logger.Infow("dlq reconcile: batch has a different terminal outcome, skipping",
			"batch_id", batchID,
			"state", string(batch.State),
		)
		return false, nil
	default:
		previousState := batch.State
		updated, err := corebatch.Transition(ctx, store, batch, entity.BatchStateFailed)
		if err != nil {
			return false, err
		}
		batch = updated
		transitioned = true
		logger.Infow("dlq reconcile: batch marked failed",
			"batch_id", batchID,
			"previous_state", string(previousState),
		)
	}

	for _, requestID := range batch.Contains {
		if err := failRequest(ctx, store, registry, logger, requestID, lastError, metadata); err != nil {
			return transitioned, fmt.Errorf("fan-out for batch %s: %w", batchID, err)
		}
	}
	return transitioned, nil
}
