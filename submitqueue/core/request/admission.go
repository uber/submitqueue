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
	"maps"
	"slices"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// AdmissionWriter creates immutable request context and initial read-model projections.
// Storage implementations remain mechanical; this type decides whether duplicate creates are identical retries or conflicts.
type AdmissionWriter struct {
	store storage.Storage
}

// NewAdmissionWriter creates a request receipt projection writer.
func NewAdmissionWriter(store storage.Storage) *AdmissionWriter {
	return &AdmissionWriter{store: store}
}

// Create writes immutable request context and initial accepted projections.
// A duplicate for the same request ID is accepted only when its immutable context matches exactly.
func (m *AdmissionWriter) Create(ctx context.Context, summary entity.RequestSummary) error {
	if err := m.createRequestSummary(ctx, summary); err != nil {
		return err
	}

	for _, changeURI := range summary.ChangeURIs {
		mapping := entity.RequestURI{
			ChangeURI:    changeURI,
			ReceivedAtMs: summary.ReceivedAtMs,
			RequestID:    summary.RequestID,
		}
		if err := m.store.GetRequestURIStore().Create(ctx, mapping); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to create request URI mapping request_id=%s change_uri=%s: %w", summary.RequestID, changeURI, err)
		}
	}

	queueSummary := queueSummaryFromSummary(summary)
	if err := m.store.GetRequestQueueSummaryStore().Create(ctx, queueSummary); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to create queue summary request_id=%s: %w", summary.RequestID, err)
		}
		existing, getErr := m.store.GetRequestQueueSummaryStore().Get(ctx, summary.Queue, summary.ReceivedAtMs, summary.RequestID)
		if getErr != nil {
			return fmt.Errorf("failed to get duplicate queue summary request_id=%s: %w", summary.RequestID, getErr)
		}
		if !sameQueueSummaryIdentity(existing, queueSummary) {
			return fmt.Errorf("conflicting queue summary request_id=%s: %w", summary.RequestID, storage.ErrAlreadyExists)
		}
	}

	return nil
}

func (m *AdmissionWriter) createRequestSummary(ctx context.Context, summary entity.RequestSummary) error {
	if err := m.store.GetRequestSummaryStore().Create(ctx, summary); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to create request summary request_id=%s: %w", summary.RequestID, err)
		}
		existing, getErr := m.store.GetRequestSummaryStore().Get(ctx, summary.RequestID)
		if getErr != nil {
			return fmt.Errorf("failed to get duplicate request summary request_id=%s: %w", summary.RequestID, getErr)
		}
		if !sameRequestSummaryIdentity(existing, summary) {
			return fmt.Errorf("conflicting request summary request_id=%s: %w", summary.RequestID, storage.ErrAlreadyExists)
		}
	}
	return nil
}

func queueSummaryFromSummary(summary entity.RequestSummary) entity.RequestQueueSummary {
	return entity.RequestQueueSummary{
		RequestID:    summary.RequestID,
		Queue:        summary.Queue,
		ChangeURIs:   slices.Clone(summary.ChangeURIs),
		ReceivedAtMs: summary.ReceivedAtMs,
		Status:       summary.Status,
		Version:      summary.Version,
		LastError:    summary.LastError,
		Metadata:     cloneMetadata(summary.Metadata),
	}
}

func sameRequestSummaryIdentity(left, right entity.RequestSummary) bool {
	return left.RequestID == right.RequestID &&
		left.Queue == right.Queue &&
		left.ReceivedAtMs == right.ReceivedAtMs &&
		slices.Equal(left.ChangeURIs, right.ChangeURIs)
}

func sameQueueSummaryIdentity(left, right entity.RequestQueueSummary) bool {
	return left.RequestID == right.RequestID &&
		left.Queue == right.Queue &&
		left.ReceivedAtMs == right.ReceivedAtMs &&
		slices.Equal(left.ChangeURIs, right.ChangeURIs)
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return maps.Clone(metadata)
}
