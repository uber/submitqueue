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
	"fmt"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
)

// CurrentState holds the current request status obtained from the request log. It is eventually consistent with the request status in the request store. It might take some time to converge, typically no more than a few seconds.
type CurrentState struct {
	// Status is the current request status obtained from the request log.
	Status string
	// LastError is the last error associated with the current status.
	LastError string
	// Metadata is the metadata associated with the current status.
	Metadata map[string]string
}

// GetCurrentStateFromRequestLog returns the current reconciled state for a request by reading the
// request log. Returns ErrNotFound if the request ID has no records in the log database. The state is eventually consistent with the request status in the request store. It might take some time to converge, typically no more than a few seconds.
func GetCurrentStateFromRequestLog(ctx context.Context, store storage.RequestLogStore, requestID string) (CurrentState, error) {
	logs, err := store.List(ctx, requestID)
	if err != nil {
		return CurrentState{}, fmt.Errorf("failed to list request logs for request_id=%s: %w", requestID, err)
	}

	// Reconciliation strategy:
	//
	// Timestamps in request log records are client-generated and may not be consistent with the
	// actual order of state modifications (e.g. clock skew, concurrent writers). Therefore we
	// cannot rely on timestamps alone to determine the most current status.
	//
	// Records that originate from the Request entity carry a RequestVersion, which is
	// monotonically incremented by the storage layer under optimistic locking. Version ordering
	// is authoritative and guaranteed by the Request data model.
	//
	// The algorithm:
	// 1. If any record has a terminal status (landed, error) AND a version (RequestVersion > 0),
	//    pick the one with the highest version. Timestamp breaks ties between equal versions, even though it should not happen.
	// 2. Otherwise, fall back to the record with the largest timestamp.

	var bestTerminal *entity.RequestLog
	var bestLatest *entity.RequestLog

	for i := range logs {
		// iterate over all log records, storage contract guarantees that the records are ordered by timestamp ascending.
		log := &logs[i]

		// Track the record with the largest timestamp as fallback.
		if bestLatest == nil || log.TimestampMs > bestLatest.TimestampMs {
			bestLatest = log
		}

		// A terminal candidate must have a version from the Request entity and a terminal status.
		if log.RequestVersion > 0 && entity.IsRequestStateTerminal(entity.RequestState(log.Status)) {
			if bestTerminal == nil ||
				log.RequestVersion > bestTerminal.RequestVersion ||
				(log.RequestVersion == bestTerminal.RequestVersion && log.TimestampMs > bestTerminal.TimestampMs) {
				bestTerminal = log
			}
		}
	}

	winner := bestLatest
	if bestTerminal != nil {
		winner = bestTerminal
	}

	return CurrentState{
		Status:    winner.Status,
		LastError: winner.LastError,
		Metadata:  winner.Metadata,
	}, nil
}
