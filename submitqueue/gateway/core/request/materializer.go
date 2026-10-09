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

	"github.com/uber/submitqueue/submitqueue/entity"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	gwstorage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
)

// Materializer appends request logs and projects the winning public request state.
// It owns winner selection, optimistic concurrency, and public projection repair.
// Every store it touches is queue-scoped, so each call resolves the aggregate once
// from the queue carried on the log being persisted.
type Materializer struct {
	stores gwstorage.Factory
}

// NewMaterializer creates a request read-model materializer.
func NewMaterializer(stores gwstorage.Factory) *Materializer {
	return &Materializer{stores: stores}
}

// PersistLog appends one audit log and materializes its winning state.
// Projection errors are returned so queue deliveries are retried rather than silently dropping the side write.
// Because the append happens first, retrying after a projection failure may retain another copy of the event in History.
func (m *Materializer) PersistLog(ctx context.Context, log entity.RequestLog) error {
	stores, err := m.stores.For(gwstorage.Config{QueueName: log.Queue})
	if err != nil {
		return fmt.Errorf("failed to resolve storage for queue %q: %w", log.Queue, err)
	}
	logs := stores.GetRequestLogStore()
	summaries := stores.GetRequestSummaryStore()

	if err := logs.Insert(ctx, log); err != nil {
		return fmt.Errorf("failed to insert request log request_id=%s: %w", log.RequestID, err)
	}

	for {
		summary, err := summaries.Get(ctx, log.RequestID)
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

			if err := summaries.Update(ctx, updated, oldVersion, newVersion); err != nil {
				if errors.Is(err, basestorage.ErrVersionMismatch) {
					continue
				}
				return fmt.Errorf("failed to update request summary request_id=%s: %w", log.RequestID, err)
			}
			updated.Version = newVersion
			summary = updated
		}

		if summary.Status == entity.RequestStatusAccepting {
			return nil
		}
		if err := ensureRequestURIMappings(ctx, stores.GetRequestURIStore(), summary); err != nil {
			return err
		}
		if err := ensureRequestReceiptMapping(ctx, stores.GetRequestReceiptStore(), summary); err != nil {
			return err
		}
		return nil
	}
}

// ensureRequestURIMappings retries every immutable mapping, including after partial activation.
func ensureRequestURIMappings(ctx context.Context, uris basestorage.RequestURIStore, summary entity.RequestSummary) error {
	for _, changeURI := range summary.ChangeURIs {
		mapping := entity.RequestURI{
			ChangeURI:    changeURI,
			Queue:        summary.Queue,
			ReceivedAtMs: summary.ReceivedAtMs,
			RequestID:    summary.RequestID,
		}
		if err := uris.Create(ctx, mapping); err != nil && !errors.Is(err, basestorage.ErrAlreadyExists) {
			return fmt.Errorf("failed to create request URI mapping request_id=%s change_uri=%s: %w", summary.RequestID, changeURI, err)
		}
	}
	return nil
}

// logWins keeps accepting internal, treats accepted as the lowest public state,
// then applies versioned-terminal precedence and timestamp ordering.
//
// Only status entries are candidates. An event describes work happening
// underneath the request's position — a build starting or finishing on one of
// several concurrent speculation paths — so it has no status to project, and its
// Status field is unset.
func logWins(log entity.RequestLog, summary entity.RequestSummary) bool {
	if log.Type != entity.RequestLogTypeStatus {
		return false
	}
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

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return maps.Clone(metadata)
}
