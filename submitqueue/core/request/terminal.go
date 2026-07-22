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

package request

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// TerminalOutcome describes the terminal request state and public log context
// selected by the controller that owns the business or reconciliation decision.
type TerminalOutcome struct {
	// State is the desired terminal request state.
	State entity.RequestState
	// LastError is the failure context exposed through request status and history.
	LastError string
	// Metadata is display and debugging context persisted with the terminal log.
	Metadata map[string]string
}

// ReconcileTerminalState converges a request entity and its public log on the
// caller-selected terminal outcome. It returns false without writing when the
// request already has a different terminal state.
func ReconcileTerminalState(
	ctx context.Context,
	requestStore storage.RequestStore,
	registry consumer.TopicRegistry,
	request entity.Request,
	outcome TerminalOutcome,
) (bool, error) {
	status, err := terminalStatus(outcome.State)
	if err != nil {
		return false, err
	}

	switch {
	case request.State == outcome.State:
	case entity.IsRequestStateTerminal(request.State):
		return false, nil
	default:
		newVersion := request.Version + 1
		if err := requestStore.UpdateState(ctx, request.ID, request.Version, newVersion, outcome.State); err != nil {
			return false, fmt.Errorf(
				"failed to update request %s to terminal state %s: %w",
				request.ID,
				outcome.State,
				err,
			)
		}
		request.Version = newVersion
	}

	logEntry := entity.NewRequestLog(request.ID, status, request.Version, outcome.LastError, outcome.Metadata)
	if err := PublishLog(ctx, registry, logEntry, request.ID); err != nil {
		return false, fmt.Errorf("failed to publish terminal request log for %s: %w", request.ID, err)
	}
	return true, nil
}

func terminalStatus(state entity.RequestState) (entity.RequestStatus, error) {
	switch state {
	case entity.RequestStateLanded:
		return entity.RequestStatusLanded, nil
	case entity.RequestStateError:
		return entity.RequestStatusError, nil
	case entity.RequestStateCancelled:
		return entity.RequestStatusCancelled, nil
	default:
		return entity.RequestStatusUnknown, fmt.Errorf("request state %s is not terminal", state)
	}
}
