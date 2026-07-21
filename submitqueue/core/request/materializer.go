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

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Materializer appends request logs and projects the winning public request state.
// It owns winner selection, optimistic concurrency, and public projection repair.
type Materializer struct {
	store storage.Storage
}

// NewMaterializer creates a request read-model materializer.
func NewMaterializer(store storage.Storage) *Materializer {
	return &Materializer{store: store}
}

// PersistLog appends one audit log and materializes its winning state.
// Projection errors are returned so queue deliveries are retried rather than silently dropping the side write.
// Because the append happens first, retrying after a projection failure may retain another copy of the event in History.
func (m *Materializer) PersistLog(ctx context.Context, log entity.RequestLog) error {
	if err := m.store.GetRequestLogStore().Insert(ctx, log); err != nil {
		return fmt.Errorf("failed to insert request log request_id=%s: %w", log.RequestID, err)
	}

	for {
		summary, err := m.store.GetRequestSummaryStore().Get(ctx, log.RequestID)
		if err != nil {
			return fmt.Errorf("failed to get request summary request_id=%s: %w", log.RequestID, err)
		}

		if logWins(log, summary) {
			oldVersion := summary.Version
			newVersion := oldVersion + 1
			updated := summary
			updated.Status = log.Status
			updated.RequestVersion = log.RequestVersion
			updated.StatusTimestampMs = log.TimestampMs
			updated.LastError = log.LastError
			updated.Metadata = cloneMetadata(log.Metadata)

			if err := m.store.GetRequestSummaryStore().Update(ctx, updated, oldVersion, newVersion); err != nil {
				if errors.Is(err, errs.ErrVersionMismatch) {
					continue
				}
				return fmt.Errorf("failed to update request summary request_id=%s: %w", log.RequestID, err)
			}
			updated.Version = newVersion
			summary = updated
		}

		if err := m.repairPublicProjections(ctx, summary); err != nil {
			return err
		}
		return nil
	}
}

// repairPublicProjections activates and repairs the public query projections.
// URI mappings are created before the queue summary, which acts as the marker that activation completed.
func (m *Materializer) repairPublicProjections(ctx context.Context, authoritative entity.RequestSummary) error {
	desired := queueSummaryFromSummary(authoritative)
	for {
		current, err := m.store.GetRequestQueueSummaryStore().Get(ctx, desired.Queue, desired.ReceivedAtMs, desired.RequestID)
		if errors.Is(err, errs.ErrNotFound) {
			if err := m.createURIMappings(ctx, authoritative); err != nil {
				return err
			}
			if err := m.store.GetRequestQueueSummaryStore().Create(ctx, desired); err != nil {
				if errors.Is(err, storage.ErrAlreadyExists) {
					continue
				}
				return fmt.Errorf("failed to recreate queue summary request_id=%s: %w", desired.RequestID, err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get queue summary request_id=%s: %w", desired.RequestID, err)
		}
		if current.Version == desired.Version {
			return nil
		}
		if current.Version > desired.Version {
			// Another materializer already projected a newer authoritative snapshot.
			return nil
		}
		if err := m.store.GetRequestQueueSummaryStore().Update(ctx, desired, current.Version, desired.Version); err != nil {
			if errors.Is(err, errs.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("failed to update queue summary request_id=%s: %w", desired.RequestID, err)
		}
		return nil
	}
}

func (m *Materializer) createURIMappings(ctx context.Context, summary entity.RequestSummary) error {
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
	return nil
}

// logWins keeps accepting internal, treats accepted as the lowest public state,
// then applies versioned-terminal precedence and timestamp ordering.
func logWins(log entity.RequestLog, summary entity.RequestSummary) bool {
	if summary.Status == entity.RequestStatusAccepting {
		return log.Status != entity.RequestStatusAccepting
	}
	if log.Status == entity.RequestStatusAccepting {
		return false
	}
	if log.Status == entity.RequestStatusAccepted && summary.Status != entity.RequestStatusAccepted {
		return false
	}

	incomingTerminal := isVersionedTerminal(log)
	currentTerminal := isVersionedTerminalSummary(summary)
	if incomingTerminal != currentTerminal {
		return incomingTerminal
	}
	if incomingTerminal {
		if log.RequestVersion != summary.RequestVersion {
			return log.RequestVersion > summary.RequestVersion
		}
	}
	return log.TimestampMs > summary.StatusTimestampMs
}

func isVersionedTerminalSummary(summary entity.RequestSummary) bool {
	return summary.RequestVersion > 0 && entity.IsRequestStateTerminal(entity.RequestState(summary.Status))
}

func isVersionedTerminal(log entity.RequestLog) bool {
	return log.RequestVersion > 0 && entity.IsRequestStateTerminal(entity.RequestState(log.Status))
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

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return maps.Clone(metadata)
}
