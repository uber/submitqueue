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

// Package requestlog retains the request occurrences exposed by Stovepipe's history API.
package requestlog

//go:generate mockgen -source=recorder.go -destination=mock/recorder_mock.go -package=mock

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	_occurrenceKindState = "state"
)

// Recorder retains idempotent request-state occurrences.
type Recorder interface {
	// RecordRequestState retains the request's current durable state and version.
	RecordRequestState(context.Context, storage.RequestLogStore, entity.Request, entity.RequestOutcomeReason) error
}

type recorder struct {
	scope tally.Scope
	now   func() time.Time
}

// NewRecorder creates a request-log recorder.
func NewRecorder(scope tally.Scope) Recorder {
	return &recorder{
		scope: scope.SubScope("request_log_recorder"),
		now:   time.Now,
	}
}

func (r *recorder) RecordRequestState(ctx context.Context, store storage.RequestLogStore, request entity.Request, outcomeReason entity.RequestOutcomeReason) error {
	log := entity.RequestLog{
		ID:             occurrenceID(request.Queue, request.ID, _occurrenceKindState, strconv.FormatInt(int64(request.Version), 10)),
		Queue:          request.Queue,
		RequestID:      request.ID,
		State:          request.State,
		RequestVersion: request.Version,
		OutcomeReason:  outcomeReason,
	}
	return r.record(ctx, store, log)
}

func (r *recorder) record(ctx context.Context, store storage.RequestLogStore, log entity.RequestLog) error {
	log.TimestampMs = r.now().UnixMilli()

	if err := log.Validate(); err != nil {
		r.count(ctx, "validation_failure", log)
		return fmt.Errorf("invalid request log occurrence: %w", err)
	}

	if err := store.Create(ctx, log); err == nil {
		r.count(ctx, "created", log)
		return nil
	} else if !errors.Is(err, storage.ErrAlreadyExists) {
		r.count(ctx, "storage_failure", log)
		return fmt.Errorf("failed to create request log request_id=%q log_id=%q: %w", log.RequestID, log.ID, err)
	}

	stored, err := store.Get(ctx, log.RequestID, log.ID)
	if err != nil {
		r.count(ctx, "storage_failure", log)
		return fmt.Errorf("failed to reconcile request log request_id=%q log_id=%q: %w", log.RequestID, log.ID, err)
	}
	if !sameOccurrence(stored, log) {
		r.count(ctx, "conflict", log)
		return fmt.Errorf("request log conflicts with retained occurrence request_id=%q log_id=%q", log.RequestID, log.ID)
	}

	r.count(ctx, "identical_existing", log)
	return nil
}

func (r *recorder) count(ctx context.Context, counter string, log entity.RequestLog) {
	metrics.NamedCounter(r.scope, "record", counter, 1, metrics.TagsFromContext(ctx, occurrenceTag(log))...)
}

func occurrenceID(queue, requestID string, identity ...string) string {
	hash := sha256.New()
	parts := append([]string{queue, requestID}, identity...)
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(part))
	}
	return "log/" + hex.EncodeToString(hash.Sum(nil))
}

func sameOccurrence(stored, candidate entity.RequestLog) bool {
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
	// One-sided keys permit additive metadata rollout without making a retry conflict with an older
	// immutable row. Values emitted by both versions must still agree.
	for key, storedValue := range stored {
		if candidateValue, ok := candidate[key]; ok && candidateValue != storedValue {
			return false
		}
	}
	return true
}

func occurrenceTag(log entity.RequestLog) metrics.Tag {
	value := "invalid"
	switch log.State {
	case entity.RequestStateAccepted,
		entity.RequestStateProcessing,
		entity.RequestStateSuperseded,
		entity.RequestStateSucceeded,
		entity.RequestStateFailed,
		entity.RequestStateCancelled:
		value = string(log.State)
	}
	return metrics.NewTag("occurrence", value)
}
