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

package request

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// TerminationOutcome describes what TerminateRequest did to the request.
// The zero value (TerminationOutcomeUnknown) is only produced alongside a non-nil error.
type TerminationOutcome int

const (
	// TerminationOutcomeUnknown is the zero value, produced only when TerminateRequest also returns a non-nil error.
	TerminationOutcomeUnknown TerminationOutcome = iota
	// TerminationOutcomeSuccess means the request was transitioned from a non-terminal
	// state to the target terminal state and a terminal log entry was published.
	TerminationOutcomeSuccess
	// TerminationOutcomeAlreadyInTargetState means the request was already in the target terminal
	// state. No state write occurred, but the terminal log entry was re-published to
	// repair a possible prior attempt that wrote the state but failed before publishing.
	TerminationOutcomeAlreadyInTargetState
	// TerminationOutcomeDiverged means the request had already reached a different terminal state -
	// a concurrent path won the race and owns the terminal log for the state it wrote.
	// Nothing was written or published.
	TerminationOutcomeDiverged
	// TerminationOutcomeNotFound means the request does not exist. Nothing was done.
	TerminationOutcomeNotFound
)

// TerminationResult reports what TerminateRequest observed and did, so callers can
// log and emit metrics for the outcome at their own level and granularity without re-fetching the request.
// It is returned alongside a separate error:
// TerminationResult describes the expected variation (reconciled / already-terminal / diverged / not-found).
// Error signals an infrastructure failure.
type TerminationResult struct {
	// Outcome is what happened to the request.
	Outcome TerminationOutcome
	// BeforeState is the request's state as observed before any write.
	// On a divergence it is the (different) terminal state the request actually reached.
	// On a success it is the prior non-terminal state.
	// Empty when the request was not found or the call failed.
	BeforeState entity.RequestState
	// AfterState is the request's state after the operation.
	// Equal to the target state on success and already-terminal.
	// Equal to BeforeState on divergence.
	// Empty when the request was not found or the call failed.
	AfterState entity.RequestState
}

// TerminateRequest transitions a request to the given terminal state and publishes
// the corresponding terminal log entry, idempotently under at-least-once delivery.
// It is the shared primitive behind concluding a batch's requests, dead-letter
// reconciliation, and rejecting an invalid request at validation time.
//
// targetState must be one of the terminal request states (Landed, Error, Cancelled).
// The write follows the immutability / optimistic-locking contract:
// caller-owned version arithmetic (newVersion = version+1) guards a pure
// conditional store write, and the in-memory version is only advanced after the store call succeeds.
//
// Idempotency has three shapes, reported via TerminationResult.Outcome:
//   - the request is already in targetState: the state write is skipped but the
//     terminal log is re-published (TerminationOutcomeAlreadyInTargetState);
//   - the request is in a different terminal state: nothing is written or
//     published, since the other writer owns that state's terminal log
//     (TerminationOutcomeDiverged);
//   - the request is not found: nothing is done (TerminationOutcomeNotFound).
//
// The caller owns all logging and metrics — TerminationResult carries the state context needed to do so.
// lastError and metadata are attached to the published RequestLog for diagnosis.
//
// A storage.ErrVersionMismatch from the conditional write is returned as-is (it is
// intrinsically retryable) so the caller's next attempt re-reads and re-evaluates.
func TerminateRequest(
	ctx context.Context,
	store storage.Storage,
	registry consumer.TopicRegistry,
	requestID string,
	targetState entity.RequestState,
	lastError string,
	metadata map[string]string,
) (TerminationResult, error) {
	status, err := terminalStateToStatus(targetState)
	if err != nil {
		return TerminationResult{}, err
	}

	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return TerminationResult{Outcome: TerminationOutcomeNotFound}, nil
		}
		return TerminationResult{}, fmt.Errorf("failed to get request %s: %w", requestID, err)
	}

	// logVersion is the request version reflected in the published terminal log.
	// It stays at the current version on the idempotent same-state path and
	// advances to the new version only after a successful reconciling write.
	beforeState := request.State
	logVersion := request.Version
	outcome := TerminationOutcomeSuccess
	switch {
	case request.State == targetState:
		// Idempotent retry: a prior attempt already wrote the terminal state.
		// Skip the CAS and fall through to re-publish the terminal log.
		outcome = TerminationOutcomeAlreadyInTargetState
	case entity.IsRequestStateTerminal(request.State):
		// Divergent terminal state — a concurrent path reached terminal first
		// and owns the terminal log entry for the state it actually wrote.
		return TerminationResult{
			Outcome:     TerminationOutcomeDiverged,
			BeforeState: request.State,
			AfterState:  request.State,
		}, nil
	default:
		newVersion := request.Version + 1
		request.State = targetState
		if err := store.GetRequestStore().Update(ctx, request, request.Version, newVersion); err != nil {
			return TerminationResult{}, fmt.Errorf("failed to update request %s state to %s: %w", requestID, targetState, err)
		}
		request.Version = newVersion
		logVersion = request.Version
	}

	logEntry := entity.NewRequestLog(requestID, status, logVersion, lastError, metadata)
	if err := PublishLog(ctx, registry, logEntry, requestID); err != nil {
		return TerminationResult{}, fmt.Errorf("failed to publish request log for %s: %w", requestID, err)
	}

	return TerminationResult{
		Outcome:     outcome,
		BeforeState: beforeState,
		AfterState:  targetState,
	}, nil
}

// terminalStatusByState maps each terminal request state to the customer-facing status published on the terminal log.
// A state absent from this map is not a valid termination target.
var terminalStatusByState = map[entity.RequestState]entity.RequestStatus{
	entity.RequestStateLanded:    entity.RequestStatusLanded,
	entity.RequestStateError:     entity.RequestStatusError,
	entity.RequestStateCancelled: entity.RequestStatusCancelled,
}

// terminalStateToStatus maps a terminal request state to the corresponding customer-facing log status.
// It rejects non-terminal states so callers cannot terminate a request into a state that is not final.
func terminalStateToStatus(state entity.RequestState) (entity.RequestStatus, error) {
	status, ok := terminalStatusByState[state]
	if !ok {
		return entity.RequestStatusUnknown, fmt.Errorf("non-terminal request state: %s", state)
	}
	return status, nil
}
