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

// Package requestlog retains request occurrences and projects request status read models.
package requestlog

//go:generate mockgen -source=materializer.go -destination=mock/materializer_mock.go -package=mock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	_occurrenceKindEvent = "event"
	_occurrenceKindState = "state"

	// MetadataKeyBuildID identifies the build associated with a request event.
	MetadataKeyBuildID = "build_id"
	// MetadataKeyFactDegree records the degree established by a validation fact.
	MetadataKeyFactDegree = "fact_degree"
	// MetadataKeyBaseURI records the selected build baseline; an empty value means a full build.
	MetadataKeyBaseURI = "base_uri"
)

// Materializer retains request-log occurrences and advances their derived read models.
type Materializer interface {
	// PersistLog retains one occurrence idempotently and updates projections derived from it.
	PersistLog(context.Context, storage.Storage, entity.RequestLog) error
}

type materializer struct {
	scope tally.Scope
	now   func() time.Time
}

var _ Materializer = (*materializer)(nil)

// NewMaterializer creates a request-log materializer.
func NewMaterializer(scope tally.Scope) Materializer {
	return &materializer{
		scope: scope.SubScope("request_log_materializer"),
		now:   time.Now,
	}
}

// NewRequestStateLog constructs the occurrence representing the request's current durable state.
func NewRequestStateLog(request entity.Request, outcomeReason entity.RequestOutcomeReason) entity.RequestLog {
	metadata := make(map[string]string)
	switch request.BuildStrategy {
	case entity.BuildStrategyFull:
		metadata[MetadataKeyBaseURI] = ""
	case entity.BuildStrategyIncrementalSinceGreen:
		metadata[MetadataKeyBaseURI] = request.BaseURI
	}
	return entity.RequestLog{
		ID:             publish.IntentID(_occurrenceKindState, strconv.FormatInt(int64(request.Version), 10)),
		Queue:          request.Queue,
		RequestID:      request.ID,
		State:          request.State,
		RequestVersion: request.Version,
		OutcomeReason:  outcomeReason,
		Metadata:       metadata,
	}
}

// NewRequestEventLog constructs a stable occurrence for a durable request lifecycle event.
func NewRequestEventLog(
	request entity.Request,
	event entity.RequestEvent,
	occurrence string,
	metadata map[string]string,
) entity.RequestLog {
	return entity.RequestLog{
		ID:        publish.IntentID(_occurrenceKindEvent, string(event), occurrence),
		Queue:     request.Queue,
		RequestID: request.ID,
		Event:     event,
		Metadata:  metadata,
	}
}

func (m *materializer) PersistLog(ctx context.Context, stores storage.Storage, log entity.RequestLog) error {
	if log.TimestampMs == 0 {
		log.TimestampMs = m.now().UnixMilli()
	}

	if err := log.Validate(); err != nil {
		m.count(ctx, "validation_failure")
		return fmt.Errorf("invalid request log occurrence: %w", err)
	}
	retained, err := m.retainRequestLog(ctx, stores.GetRequestLogStore(), log)
	if err != nil {
		return err
	}

	if retained.State == entity.RequestStateUnknown {
		return nil
	}
	if err := materializeRequestSummary(ctx, stores, retained); err != nil {
		m.count(ctx, "projection_failure")
		return err
	}
	return nil
}

func (m *materializer) retainRequestLog(
	ctx context.Context,
	requestLogs storage.RequestLogStore,
	log entity.RequestLog,
) (entity.RequestLog, error) {
	// SubmitQueue deduplicates the message that hands a log to its materializer. Stovepipe has no
	// log topic, so the retained occurrence ID is the retry boundary instead.
	err := requestLogs.Create(ctx, log)
	if err == nil {
		m.count(ctx, "created")
		return log, nil
	}
	if !errors.Is(err, storage.ErrAlreadyExists) {
		m.count(ctx, "storage_failure")
		return entity.RequestLog{}, fmt.Errorf("failed to create request log request_id=%q log_id=%q: %w", log.RequestID, log.ID, err)
	}

	stored, err := requestLogs.Get(ctx, log.RequestID, log.ID)
	if err != nil {
		m.count(ctx, "storage_failure")
		return entity.RequestLog{}, fmt.Errorf("failed to reconcile request log request_id=%q log_id=%q: %w", log.RequestID, log.ID, err)
	}
	if !sameSemanticOccurrence(stored, log) {
		m.count(ctx, "conflict")
		return entity.RequestLog{}, fmt.Errorf("request log conflicts with retained occurrence request_id=%q log_id=%q", log.RequestID, log.ID)
	}
	m.count(ctx, "identical_existing")
	return stored, nil
}

func (m *materializer) count(ctx context.Context, counter string) {
	metrics.NamedCounter(m.scope, "persist", counter, 1, metrics.TagsFromContext(ctx)...)
}

func sameSemanticOccurrence(stored, candidate entity.RequestLog) bool {
	// This list is the compatibility boundary for duplicate reconciliation. New entity fields do not
	// become conflict-sensitive until they are deliberately added here.
	return stored.ID == candidate.ID &&
		stored.Queue == candidate.Queue &&
		stored.RequestID == candidate.RequestID &&
		stored.State == candidate.State &&
		stored.Event == candidate.Event &&
		stored.RequestVersion == candidate.RequestVersion &&
		stored.OutcomeReason == candidate.OutcomeReason &&
		metadataCompatible(stored.Metadata, candidate.Metadata)
}

func metadataCompatible(stored, candidate map[string]string) bool {
	// Keys present on only one side permit additive rollout against older immutable rows. Values
	// emitted by both versions must still agree.
	for key, storedValue := range stored {
		if candidateValue, ok := candidate[key]; ok && candidateValue != storedValue {
			return false
		}
	}
	return true
}
