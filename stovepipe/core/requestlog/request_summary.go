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

package requestlog

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func materializeRequestSummary(ctx context.Context, stores storage.Storage, log entity.RequestLog) (entity.RequestSummary, error) {
	summaryStore := stores.GetRequestSummaryStore()
	// Create and update races reload the winner; request versions make each retry converge.
	for {
		currentSummary, err := summaryStore.Get(ctx, log.RequestID)
		if storage.IsNotFound(err) {
			initial, createErr := createInitialRequestSummary(ctx, stores.GetRequestStore(), summaryStore, log)
			if errors.Is(createErr, storage.ErrAlreadyExists) {
				continue
			}
			return initial, createErr
		}
		if err != nil {
			return entity.RequestSummary{}, fmt.Errorf("failed to get request summary request_id=%q: %w", log.RequestID, err)
		}

		updated, updateErr := updateExistingRequestSummary(ctx, summaryStore, currentSummary, log)
		if errors.Is(updateErr, storage.ErrVersionMismatch) {
			continue
		}
		return updated, updateErr
	}
}

func createInitialRequestSummary(
	ctx context.Context,
	requestStore storage.RequestStore,
	summaryStore storage.RequestSummaryStore,
	log entity.RequestLog,
) (entity.RequestSummary, error) {
	request, err := requestStore.Get(ctx, log.RequestID)
	if err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to get request for summary request_id=%q: %w", log.RequestID, err)
	}
	if request.ID != log.RequestID || request.Queue != log.Queue {
		return entity.RequestSummary{}, fmt.Errorf(
			"request identity conflicts with state log log_request_id=%q request_id=%q log_queue=%q request_queue=%q",
			log.RequestID, request.ID, log.Queue, request.Queue,
		)
	}
	if request.URI == "" {
		return entity.RequestSummary{}, fmt.Errorf("request URI is empty for summary request_id=%q", log.RequestID)
	}

	initial := requestSummaryFromRequestAndLog(request, log)
	initial.Version = 1
	if err := summaryStore.Create(ctx, initial); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to create request summary request_id=%q: %w", log.RequestID, err)
	}
	return initial, nil
}

func updateExistingRequestSummary(
	ctx context.Context,
	summaryStore storage.RequestSummaryStore,
	current entity.RequestSummary,
	log entity.RequestLog,
) (entity.RequestSummary, error) {
	updated, err := projectRequestSummary(current, log)
	if err != nil {
		return entity.RequestSummary{}, err
	}
	if updated == current {
		return current, nil
	}
	oldVersion := current.Version
	newVersion := oldVersion + 1
	if err := summaryStore.Update(ctx, updated, oldVersion, newVersion); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to update request summary request_id=%q: %w", log.RequestID, err)
	}
	updated.Version = newVersion
	return updated, nil
}

func projectRequestSummary(current entity.RequestSummary, log entity.RequestLog) (entity.RequestSummary, error) {
	updated, err := projectRequestSummaryFacts(current, log)
	if err != nil {
		return entity.RequestSummary{}, err
	}
	return projectRequestSummaryState(updated, log)
}

// Facts may be filled from any retained version; known values must agree.
func projectRequestSummaryFacts(summary entity.RequestSummary, log entity.RequestLog) (entity.RequestSummary, error) {
	if summary.RequestID != log.RequestID || summary.Queue != log.Queue {
		return entity.RequestSummary{}, fmt.Errorf(
			"request summary identity conflicts with state log log_request_id=%q summary_request_id=%q log_queue=%q summary_queue=%q",
			log.RequestID, summary.RequestID, log.Queue, summary.Queue,
		)
	}
	if log.State != entity.RequestStateAccepted {
		return summary, nil
	}
	if summary.AcceptedAtMs != 0 && summary.AcceptedAtMs != log.TimestampMs {
		return entity.RequestSummary{}, fmt.Errorf("request summary acceptance time conflicts with retained log request_id=%q", log.RequestID)
	}
	summary.AcceptedAtMs = log.TimestampMs
	return summary, nil
}

func projectRequestSummaryState(summary entity.RequestSummary, log entity.RequestLog) (entity.RequestSummary, error) {
	if log.RequestVersion < summary.RequestVersion {
		return summary, nil
	}
	if log.RequestVersion == summary.RequestVersion {
		if !requestSummaryStateMatchesLog(summary, log) {
			return entity.RequestSummary{}, fmt.Errorf("request summary conflicts with state log request_id=%q version=%d", log.RequestID, log.RequestVersion)
		}
		summary.OutcomeReason = log.OutcomeReason
		return summary, nil
	}
	summary.State = log.State
	summary.RequestVersion = log.RequestVersion
	summary.StateTimestampMs = log.TimestampMs
	summary.OutcomeReason = log.OutcomeReason
	if baseURI, ok := log.Metadata[MetadataKeyBaseURI]; ok {
		summary.BaseURI = baseURI
	}
	return summary, nil
}

func requestSummaryStateMatchesLog(summary entity.RequestSummary, log entity.RequestLog) bool {
	if summary.State != log.State || summary.StateTimestampMs != log.TimestampMs ||
		(summary.OutcomeReason != entity.RequestOutcomeReasonUnknown && summary.OutcomeReason != log.OutcomeReason) {
		return false
	}
	baseURI, updatesBaseURI := log.Metadata[MetadataKeyBaseURI]
	return !updatesBaseURI || summary.BaseURI == baseURI
}

func requestSummaryFromRequestAndLog(request entity.Request, log entity.RequestLog) entity.RequestSummary {
	summary := entity.RequestSummary{
		RequestID:        log.RequestID,
		Queue:            log.Queue,
		URI:              request.URI,
		State:            log.State,
		RequestVersion:   log.RequestVersion,
		StateTimestampMs: log.TimestampMs,
		OutcomeReason:    log.OutcomeReason,
	}
	if log.State == entity.RequestStateAccepted {
		summary.AcceptedAtMs = log.TimestampMs
	}
	if baseURI, ok := log.Metadata[MetadataKeyBaseURI]; ok {
		summary.BaseURI = baseURI
	}
	return summary
}
