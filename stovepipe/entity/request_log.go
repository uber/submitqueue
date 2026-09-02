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

package entity

import "fmt"

// RequestEvent identifies a retained occurrence that does not change request state.
type RequestEvent string

const (
	// RequestEventUnknown is the unset event value.
	RequestEventUnknown RequestEvent = ""
	// RequestEventBuildTriggered records that a build was durably accepted.
	RequestEventBuildTriggered RequestEvent = "build_triggered"
	// RequestEventBuildFinished records that a build first reached a terminal status.
	RequestEventBuildFinished RequestEvent = "build_finished"
	// RequestEventValidationFactRecorded records that an immutable validation verdict was established.
	RequestEventValidationFactRecorded RequestEvent = "validation_fact_recorded"
)

// RequestOutcomeReason identifies the durable domain reason for a terminal request state.
type RequestOutcomeReason string

const (
	// RequestOutcomeReasonUnknown is the unset outcome reason.
	RequestOutcomeReasonUnknown RequestOutcomeReason = ""
	// RequestOutcomeReasonBuildSucceeded indicates that the request's build succeeded.
	RequestOutcomeReasonBuildSucceeded RequestOutcomeReason = "build_succeeded"
	// RequestOutcomeReasonBuildFailed indicates that the request's build failed.
	RequestOutcomeReasonBuildFailed RequestOutcomeReason = "build_failed"
	// RequestOutcomeReasonBuildCancelled indicates that the request's build was cancelled.
	RequestOutcomeReasonBuildCancelled RequestOutcomeReason = "build_cancelled"
	// RequestOutcomeReasonProcessingFailed indicates that validation could not be prepared.
	RequestOutcomeReasonProcessingFailed RequestOutcomeReason = "processing_failed"
	// RequestOutcomeReasonBuildPollingExhausted indicates that build status could not be resolved.
	RequestOutcomeReasonBuildPollingExhausted RequestOutcomeReason = "build_polling_exhausted"
	// RequestOutcomeReasonValidationTimeout indicates that validation exceeded its allowed duration.
	RequestOutcomeReasonValidationTimeout RequestOutcomeReason = "validation_timeout"
	// RequestOutcomeReasonSupersededByNewerHead indicates that a newer request replaced this one.
	RequestOutcomeReasonSupersededByNewerHead RequestOutcomeReason = "superseded_by_newer_head"
)

// RequestLog is one immutable request state or explanatory lifecycle occurrence.
type RequestLog struct {
	// ID is the stable identity of the occurrence within the request.
	ID string `json:"id"`
	// Queue is the logical queue containing the request and scopes RequestID.
	Queue string `json:"queue"`
	// RequestID identifies the request whose log contains this record.
	RequestID string `json:"request_id"`
	// TimestampMs is when the occurrence was first retained, in Unix milliseconds.
	TimestampMs int64 `json:"timestamp_ms"`
	// State is the durable request state recorded by a state record and is unset on an event record.
	State RequestState `json:"state"`
	// Event identifies the occurrence recorded by an event record and is unset on a state record.
	Event RequestEvent `json:"event"`
	// RequestVersion is the durable request version recorded by a state record and is zero on an event record.
	RequestVersion int32 `json:"request_version"`
	// OutcomeReason is the durable domain reason for a terminal request state and is otherwise unset.
	OutcomeReason RequestOutcomeReason `json:"outcome_reason"`
	// Metadata contains optional occurrence context; nil and empty maps are equivalent.
	Metadata map[string]string `json:"metadata"`
}

// Validate verifies the invariants required for a newly persisted request log.
func (e RequestLog) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("request log ID must not be empty")
	}
	if e.Queue == "" {
		return fmt.Errorf("request log queue must not be empty")
	}
	if e.RequestID == "" {
		return fmt.Errorf("request log request ID must not be empty")
	}
	if e.TimestampMs <= 0 {
		return fmt.Errorf("request log timestamp must be positive")
	}
	if (e.State == RequestStateUnknown) == (e.Event == RequestEventUnknown) {
		return fmt.Errorf("request log must contain exactly one of state and event")
	}
	if e.State != RequestStateUnknown {
		return e.validateState()
	}
	return e.validateEvent()
}

func (e RequestLog) validateState() error {
	if e.RequestVersion <= 0 {
		return fmt.Errorf("state log must have a positive request version")
	}
	switch e.State {
	case RequestStateAccepted, RequestStateProcessing:
		if e.OutcomeReason != RequestOutcomeReasonUnknown {
			return fmt.Errorf("non-terminal state log must not contain terminal context")
		}
	case RequestStateSuperseded:
		if e.OutcomeReason != RequestOutcomeReasonSupersededByNewerHead {
			return fmt.Errorf("superseded state log has invalid outcome context")
		}
	case RequestStateSucceeded:
		if e.OutcomeReason != RequestOutcomeReasonBuildSucceeded {
			return fmt.Errorf("succeeded state log has invalid outcome context")
		}
	case RequestStateFailed:
		if !isFailureReason(e.OutcomeReason) {
			return fmt.Errorf("failed state log has invalid outcome context")
		}
	case RequestStateCancelled:
		if e.OutcomeReason != RequestOutcomeReasonBuildCancelled {
			return fmt.Errorf("cancelled state log has invalid outcome context")
		}
	default:
		return fmt.Errorf("unknown request state %q", e.State)
	}
	return nil
}

func (e RequestLog) validateEvent() error {
	if e.RequestVersion != 0 || e.OutcomeReason != RequestOutcomeReasonUnknown {
		return fmt.Errorf("event log must not contain request-state context")
	}
	switch e.Event {
	case RequestEventBuildTriggered, RequestEventBuildFinished, RequestEventValidationFactRecorded:
	default:
		return fmt.Errorf("unknown request event %q", e.Event)
	}
	return nil
}

func isFailureReason(reason RequestOutcomeReason) bool {
	switch reason {
	case RequestOutcomeReasonBuildFailed,
		RequestOutcomeReasonProcessingFailed,
		RequestOutcomeReasonBuildPollingExhausted,
		RequestOutcomeReasonValidationTimeout:
		return true
	default:
		return false
	}
}
