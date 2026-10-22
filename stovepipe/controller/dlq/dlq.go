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
// Reconciliation preserves the first terminal outcome, then completes the request-owned
// build slot and successful baseline. Terminal requests remain repairable on replay.
package dlq

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/stovepipe/core/buildslot"
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

// failRequest preserves a concurrent terminal outcome and completes its owned slot.
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

	for !request.State.IsTerminal() {
		updated := request
		updated.State = entity.RequestStateFailed
		newVersion := request.Version + 1
		if err := store.GetRequestStore().Update(ctx, updated, request.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				request, err = store.GetRequestStore().Get(ctx, requestID)
				if err != nil {
					return fmt.Errorf("reload request %s during dlq completion: %w", requestID, err)
				}
				continue
			}
			return fmt.Errorf("failed to update request %s state to failed: %w", requestID, err)
		}
		updated.Version = newVersion
		request = updated
	}
	if err := buildslot.Complete(ctx, store, request); err != nil {
		return err
	}
	switch request.State {
	case entity.RequestStateSucceeded:
		reason = entity.RequestOutcomeReasonBuildSucceeded
	case entity.RequestStateCancelled:
		reason = entity.RequestOutcomeReasonBuildCancelled
	case entity.RequestStateSuperseded:
		reason = entity.RequestOutcomeReasonSupersededByNewerHead
	case entity.RequestStateFailed:
		if request.TerminalBuildID != "" {
			reason = entity.RequestOutcomeReasonBuildFailed
		}
	}
	return persistOutcomeLog(ctx, store, materializer, request, reason)
}

func persistOutcomeLog(
	ctx context.Context,
	store storage.Storage,
	materializer requestlog.Materializer,
	request entity.Request,
	reason entity.RequestOutcomeReason,
) error {
	log := requestlog.NewRequestStateLog(request, reason)
	if request.TerminalBuildID != "" {
		log.Metadata[requestlog.MetadataKeyBuildID] = request.TerminalBuildID
	}
	if err := materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record terminal state for request %s: %w", request.ID, err)
	}
	return nil
}
