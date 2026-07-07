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

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

const maxSummaryProjectionAttempts = 8

// CreateContext creates immutable admission data and the initial List projection.
func CreateContext(ctx context.Context, store storage.Storage, requestContext entity.RequestContext) error {
	if err := store.GetRequestContextStore().Create(ctx, requestContext); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to create request context request_id=%s: %w", requestContext.RequestID, err)
		}
		existing, getErr := store.GetRequestContextStore().Get(ctx, requestContext.RequestID)
		if getErr != nil {
			return fmt.Errorf("failed to verify existing request context request_id=%s: %w", requestContext.RequestID, getErr)
		}
		if !equalRequestContext(existing, requestContext) {
			return fmt.Errorf("request context conflicts with existing immutable context request_id=%s", requestContext.RequestID)
		}
	}

	summary := entity.RequestSummary{
		RequestID:         requestContext.RequestID,
		Queue:             requestContext.Queue,
		ChangeURIs:        append([]string(nil), requestContext.ChangeURIs...),
		Status:            entity.RequestStatusAccepted,
		Metadata:          map[string]string{},
		StartedAtMs:       requestContext.AdmittedAtMs,
		UpdatedAtMs:       requestContext.AdmittedAtMs,
		StatusTimestampMs: requestContext.AdmittedAtMs,
		Version:           1,
	}
	if err := store.GetRequestSummaryStore().Create(ctx, summary); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("failed to create request summary request_id=%s: %w", requestContext.RequestID, err)
	}
	return nil
}

// PersistLog appends one immutable status event and updates its summary projection. A projection error is returned so queue delivery retries.
func PersistLog(ctx context.Context, store storage.Storage, log entity.RequestLog) error {
	if err := store.GetRequestLogStore().Insert(ctx, log); err != nil {
		return fmt.Errorf("failed to insert request log request_id=%s: %w", log.RequestID, err)
	}
	if err := ProjectLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to project request log request_id=%s: %w", log.RequestID, err)
	}
	return nil
}

// ProjectLog applies a status event to an existing summary using optimistic concurrency.
func ProjectLog(ctx context.Context, store storage.Storage, log entity.RequestLog) error {
	requestContext, err := store.GetRequestContextStore().Get(ctx, log.RequestID)
	if err != nil {
		return fmt.Errorf("failed to get request context request_id=%s: %w", log.RequestID, err)
	}

	for attempt := 0; attempt < maxSummaryProjectionAttempts; attempt++ {
		existing, err := store.GetRequestSummaryStore().Get(ctx, requestContext.Queue, log.RequestID)
		if err != nil {
			return fmt.Errorf("failed to get request summary request_id=%s: %w", log.RequestID, err)
		}
		next := MergeSummary(existing, log)
		newVersion := existing.Version + 1
		if err := store.GetRequestSummaryStore().Update(ctx, next, existing.Version, newVersion); err == nil {
			return nil
		} else if !errors.Is(err, storage.ErrVersionMismatch) {
			return fmt.Errorf("failed to update request summary request_id=%s: %w", log.RequestID, err)
		}
	}
	return fmt.Errorf("request summary projection did not converge request_id=%s: %w", log.RequestID, storage.ErrVersionMismatch)
}

// MergeSummary returns the summary that results when log is reconciled against existing.
func MergeSummary(existing entity.RequestSummary, log entity.RequestLog) entity.RequestSummary {
	next := existing
	if !shouldReplaceWinner(existing, log) {
		return next
	}
	next.Status = log.Status
	next.LastError = log.LastError
	next.Metadata = cloneMetadata(log.Metadata)
	next.UpdatedAtMs = log.TimestampMs
	next.RequestVersion = log.RequestVersion
	next.StatusTimestampMs = log.TimestampMs
	next.WinnerTerminalVersion = isTerminalVersion(log)
	if entity.IsRequestStateTerminal(entity.RequestState(string(log.Status))) {
		next.CompletedAtMs = log.TimestampMs
	} else {
		next.CompletedAtMs = 0
	}
	return next
}

func shouldReplaceWinner(existing entity.RequestSummary, log entity.RequestLog) bool {
	incomingTerminalVersion := isTerminalVersion(log)
	if incomingTerminalVersion {
		if !existing.WinnerTerminalVersion {
			return true
		}
		return log.RequestVersion > existing.RequestVersion || (log.RequestVersion == existing.RequestVersion && log.TimestampMs > existing.StatusTimestampMs)
	}
	if existing.WinnerTerminalVersion {
		return false
	}
	return log.TimestampMs > existing.StatusTimestampMs
}

func isTerminalVersion(log entity.RequestLog) bool {
	return log.RequestVersion > 0 && entity.IsRequestStateTerminal(entity.RequestState(string(log.Status)))
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	clone := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func equalRequestContext(left, right entity.RequestContext) bool {
	return left.RequestID == right.RequestID &&
		left.Queue == right.Queue &&
		left.AdmittedAtMs == right.AdmittedAtMs &&
		equalChangeURIs(left.ChangeURIs, right.ChangeURIs)
}

func equalChangeURIs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
