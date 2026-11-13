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

package batch

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	corerequest "github.com/uber/submitqueue/submitqueue/orchestrator/core/request"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	"go.uber.org/zap"
)

// ConcludeBatchRequests reconciles each member with the authoritative terminal batch outcome.
// Matching terminal requests repair their status logs; different terminal winners remain unchanged.
// A missing member or failed write/publish is an error so redelivery retains unfinished work.
// failureReason applies only to Failed batches, not successful or cancelled outcomes.
func ConcludeBatchRequests(ctx context.Context, store orchstorage.Storage, registry consumer.TopicRegistry, logger *zap.SugaredLogger, scope tally.Scope, batch entity.Batch, failureReason string) error {
	requestState, err := batchStateToRequestState(batch.State)
	if err != nil {
		metrics.NamedCounter(scope, "process", "unexpected_state_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("unexpected batch state %q for batch %s: %w", batch.State, batch.ID, err)
	}

	if requestState != entity.RequestStateError {
		failureReason = ""
	}
	for _, requestID := range batch.Contains {
		res, err := corerequest.TerminateRequest(ctx, store, registry, requestID, requestState, failureReason, map[string]string{
			"batch_id": batch.ID,
		})
		if err != nil {
			metrics.NamedCounter(scope, "process", "terminate_errors", 1, metrics.TagsFromContext(ctx)...)
			return fmt.Errorf("failed to terminate request %s for batch %s: %w", requestID, batch.ID, err)
		}

		switch res.Outcome {
		case corerequest.TerminationOutcomeSuccess:
			logger.Infow("updated request state",
				"batch_id", batch.ID,
				"request_id", requestID,
				"new_state", string(res.AfterState),
			)
		case corerequest.TerminationOutcomeAlreadyInTargetState:
			metrics.NamedCounter(scope, "process", "already_reconciled", 1, metrics.TagsFromContext(ctx)...)
		case corerequest.TerminationOutcomeDiverged:
			logger.Warnw("request already in different terminal state, skipping reconcile",
				"batch_id", batch.ID,
				"request_id", requestID,
				"expected_state", string(requestState),
				"actual_state", string(res.BeforeState),
			)
			metrics.NamedCounter(scope, "process", "terminal_state_divergence", 1, metrics.TagsFromContext(ctx)...)
		case corerequest.TerminationOutcomeNotFound:
			metrics.NamedCounter(scope, "process", "request_store_errors", 1, metrics.TagsFromContext(ctx)...)
			return fmt.Errorf("failed to get request %s for batch %s: %w", requestID, batch.ID, storage.ErrNotFound)
		}
	}

	return nil
}

// batchStateToRequestState maps a terminal batch state to the corresponding request state.
func batchStateToRequestState(state entity.BatchState) (entity.RequestState, error) {
	switch state {
	case entity.BatchStateSucceeded:
		return entity.RequestStateLanded, nil
	case entity.BatchStateFailed:
		return entity.RequestStateError, nil
	case entity.BatchStateCancelled:
		return entity.RequestStateCancelled, nil
	default:
		return entity.RequestStateUnknown, fmt.Errorf("non-terminal batch state: %s", state)
	}
}
