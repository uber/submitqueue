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
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	gwstorage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
)

// Materializer appends request logs and projects the winning public request state.
// It owns winner selection, optimistic concurrency, and public projection repair.
// Every store it touches is queue-scoped, so each call resolves the aggregate once
// from the queue carried on the log being persisted.
type Materializer struct {
	stores       gwstorage.Factory
	metricsScope tally.Scope
}

// MaterializerOption configures a Materializer.
type MaterializerOption func(*Materializer)

// WithMetricsScope emits request lifecycle metrics on scope. Without it the
// materializer emits none.
func WithMetricsScope(scope tally.Scope) MaterializerOption {
	return func(m *Materializer) { m.metricsScope = scope }
}

// NewMaterializer creates a request read-model materializer.
func NewMaterializer(stores gwstorage.Factory, opts ...MaterializerOption) *Materializer {
	m := &Materializer{stores: stores, metricsScope: tally.NoopScope}
	for _, opt := range opts {
		opt(m)
	}
	return m
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

	// previous is the status this delivery moved the summary away from, when it moved it.
	var previous entity.RequestStatus
	moved := false
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
			m.recordLifecycleTransition(summary, updated)
			previous, moved = summary.Status, true
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
		return fileStatusRecord(ctx, stores.GetQueueRequestStatusStore(), summary, previous, moved)
	}
}

// fileStatusRecord keeps the request's status membership record in step with its summary.
//
// It runs on every delivery, like the immutable mappings: putting the record for the
// summary's current status repairs a delivery that failed after the summary update, and is
// a no-op when the record already exists. When this delivery moved the summary, the record
// under the previous status is deleted only after the new one is filed, so a request in the
// queue is never without a record. A record left under an old status by a delivery that
// failed before that delete is not found again here — the redelivery no longer knows the old
// status — which is why records are advisory and readers confirm them against the summary.
func fileStatusRecord(ctx context.Context, records basestorage.QueueRequestStatusStore, summary entity.RequestSummary, previous entity.RequestStatus, moved bool) error {
	if entity.IsRequestStatusInQueue(summary.Status) {
		record := entity.QueueRequestStatus{
			Queue:        summary.Queue,
			Status:       summary.Status,
			RequestID:    summary.RequestID,
			ReceivedAtMs: summary.ReceivedAtMs,
			EnteredAtMs:  summary.StatusTimestampMs,
		}
		if err := records.Put(ctx, record); err != nil {
			return fmt.Errorf("failed to file request status record request_id=%s status=%s: %w", summary.RequestID, summary.Status, err)
		}
	}
	if moved && previous != summary.Status && entity.IsRequestStatusInQueue(previous) {
		if err := records.Delete(ctx, previous, summary.RequestID); err != nil {
			return fmt.Errorf("failed to remove request status record request_id=%s status=%s: %w", summary.RequestID, previous, err)
		}
	}
	return nil
}

// recordLifecycleTransition counts a request's public lifecycle milestones from
// one successful summary update.
//
// Each milestone is recorded on the update that crosses it, never on a winning
// log that merely restates it: an orchestrator retry re-publishes a terminal log
// with the same request version and a newer timestamp, which wins logWins and
// rewrites the summary again. Comparing the summary before and after the update
// is what keeps each request counted once.
func (m *Materializer) recordLifecycleTransition(before, after entity.RequestSummary) {
	queueTag := metrics.NewTag("queue", after.Queue)
	if before.Status == entity.RequestStatusAccepting && after.Status != entity.RequestStatusAccepting {
		metrics.NamedCounter(m.metricsScope, "request_lifecycle", "accepted", 1, queueTag)
	}
	if isTerminalStatus(before.Status) || !isTerminalStatus(after.Status) {
		return
	}
	statusTag := metrics.NewTag("status", string(after.Status))
	metrics.NamedCounter(m.metricsScope, "request_lifecycle", "terminated", 1,
		queueTag, statusTag, metrics.NewTag("from_status", string(before.Status)))
	if before.ReceivedAtMs <= 0 {
		return
	}
	// Clock skew between the gateway and the orchestrator can put the terminal timestamp before receipt.
	elapsed := max(time.Duration(after.StatusTimestampMs-before.ReceivedAtMs)*time.Millisecond, 0)
	metrics.NamedHistogram(m.metricsScope, "request_lifecycle", "end_to_end_latency", metrics.LandLatencyBuckets,
		queueTag, statusTag).RecordDuration(elapsed)
}

func isTerminalStatus(status entity.RequestStatus) bool {
	return entity.IsRequestStateTerminal(entity.RequestState(status))
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
