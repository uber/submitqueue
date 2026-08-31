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

import (
	"encoding/json"
	"time"
)

// RequestStatus is where a request is in the pipeline. It is customer-friendly and can be displayed to the user.
// It is different from the request state, which is internal and used to implement a state machine. Request statuses can be
// generally added freely by the system without breaking the state machine.
// Some statuses correspond to the request state, in which case they should be supplemented with the request state version to be used for reconciliation.
// Other statuses are purely informational and can be added freely.
// Every status may be accompanied by a last error message and free-formmetadata in the Request Log. It will only be used for display or debugging purposes.
//
// Exactly one status is a request's current position at any moment. Things that merely
// happen to a request while it sits at one — a build starting, a build finishing — are
// not statuses and are not in this vocabulary; they are RequestEvent.
type RequestStatus string

const (
	// RequestStatusUnknown is the unknown sentinel status. It is set by default when the structure is initialized. It should never be seen in the system.
	RequestStatusUnknown RequestStatus = ""

	// RequestStatusAccepting is the internal status of a persisted Land receipt that has not yet been published to the processing pipeline.
	// Public read APIs must not expose requests that remain in this status.
	RequestStatusAccepting RequestStatus = "accepting"

	// RequestStatusAccepted indicates that the request has been published to the processing pipeline.
	RequestStatusAccepted RequestStatus = "accepted"

	// RequestStatusStarted is the initial status of a request. It corresponds to the RequestStateStarted state and typically set by the orchestrator service when the request is received and persisted to the operating database.
	RequestStatusStarted RequestStatus = "started"

	// RequestStatusValidating indicates that the request is currently being validated (e.g., duplicate check, merge check, etc.).
	RequestStatusValidating RequestStatus = "validating"

	// RequestStatusValidated indicates that the request has been validated (duplicate check, merge check etc.) successfully. It corresponds to the RequestStateValidated state.
	RequestStatusValidated RequestStatus = "validated"

	// RequestStatusBatching indicates that a batch has been created for the request and is resolving what it must serialize behind.
	RequestStatusBatching RequestStatus = "batching"

	// RequestStatusBatched indicates that the request has been included in a new batch and will be sent to speculation.
	RequestStatusBatched RequestStatus = "batched"

	// RequestStatusSpeculating indicates that the batch containing the request is in speculation:
	// planning, building, or waiting until either its dependencies settle or passed paths cover every possible outcome.
	RequestStatusSpeculating RequestStatus = "speculating"

	// RequestStatusSpeculated indicates that the batch containing the request has finished speculating:
	// either a passed path's assumptions all held, or passed paths cover every outcome of its unsettled dependencies.
	RequestStatusSpeculated RequestStatus = "speculated"

	// RequestStatusLanding indicates that the request is actively being landed (e.g., source control operation is in progress to push the change to the target branch).
	RequestStatusLanding RequestStatus = "landing"

	// RequestStatusLanded indicates that the request has been successfully processed and landed. It corresponds to the RequestStateLanded state.
	RequestStatusLanded RequestStatus = "landed"

	// RequestStatusError indicates that the request has encountered an error. It corresponds to the RequestStateError state.
	RequestStatusError RequestStatus = "error"

	// RequestStatusCancelling indicates that the user has requested cancellation but the request has not yet transitioned
	// to the RequestStateCancelled state. Cancellation is best-effort: a request that has already been merged or that
	// races to completion before the cancel propagates through the pipeline may still land. Observers should treat this
	// as intent only and rely on RequestStatusCancelled (or RequestStatusLanded) for the terminal outcome. Emitted by
	// the gateway when the Cancel RPC is received.
	RequestStatusCancelling RequestStatus = "cancelling"

	// RequestStatusCancelled indicates that the request was cancelled by the user before it could land. It corresponds to the RequestStateCancelled state.
	RequestStatusCancelled RequestStatus = "cancelled"
)

// RequestEvent is something that happened to a request while it sat at a status,
// rather than a status of its own.
//
// Speculation is what the distinction exists for. A batch funds several paths at
// once and each is built separately, so a build starting or finishing, or one
// path passing and later being contradicted, says nothing about where the request
// as a whole is — it is still speculating. Were these statuses, one build
// succeeding while its siblings ran would report the request as finished, and go
// on reporting it that way until the batch resolved.
//
// Events are not unique per request: each names one path or build, and a batch
// may be re-planned many times. They belong in a request's history and are never
// its current status — which is enforced by the type, since a RequestEvent cannot
// be assigned to RequestSummary.Status.
type RequestEvent string

const (
	// RequestEventUnknown is the unknown sentinel event. It is set by default when the structure is initialized. It should never be seen in the system.
	RequestEventUnknown RequestEvent = ""

	// RequestEventBuilding indicates that one build verifying one speculation path of the batch containing the request has started.
	RequestEventBuilding RequestEvent = "building"

	// RequestEventBuilt indicates that one build verifying one speculation path of the batch containing the request finished successfully.
	// A build that fails or is cancelled records nothing.
	RequestEventBuilt RequestEvent = "built"

	// RequestEventWaiting indicates that one speculation path of the batch containing the request passed,
	// leaving the batch nothing of its own to run and waiting on its dependencies to settle.
	RequestEventWaiting RequestEvent = "waiting"

	// RequestEventInvalidated indicates that a dependency resolved against the guess made by the passed path
	// the batch containing the request was waiting on, so that path can no longer carry it.
	RequestEventInvalidated RequestEvent = "invalidated"
)

// RequestLogType is what a log entry records: the request reaching a status, or
// an event that happened while it was at one.
type RequestLogType string

const (
	// RequestLogTypeStatus is an entry that records a status. Its Status field is
	// set, and it is a candidate for the request's current status.
	RequestLogTypeStatus RequestLogType = "status"

	// RequestLogTypeEvent is an entry that records an event. Its Event field is
	// set, it carries no request version, and it never becomes a current status.
	RequestLogTypeEvent RequestLogType = "event"
)

// RequestLog is an append-only record that captures a point-in-time snapshot of a request's status
// for reconciliation purposes. It is stored in a separate database from the request store to support
// eventual consistency reconciliation.
//
// An entry records either a status or an event, never both, and Type says which.
// Build the two through NewRequestStatusLog and NewRequestEventLog rather than as
// a literal, so the unset half stays unset.
type RequestLog struct {
	// RequestID is the ID of the request this log entry belongs to. References entity.Request.ID.
	RequestID string `json:"request_id"`
	// Queue is the name of the queue processing the request. It is unique together
	// with RequestID: a request ID is only unique within its own queue.
	Queue string `json:"queue"`
	// TimestampMs is the time this log entry was created, in milliseconds since Unix epoch.
	TimestampMs int64 `json:"timestamp_ms"`
	// Type is what this entry records. An entry read back without one predates the
	// field and records a status.
	Type RequestLogType `json:"type"`
	// Status is the request status this entry records. Set only when Type is RequestLogTypeStatus.
	Status RequestStatus `json:"status"`
	// Event is the event this entry records. Set only when Type is RequestLogTypeEvent.
	Event RequestEvent `json:"event"`
	// RequestVersion is the version of the request at the time this log entry was created.
	// Zero if the version is not available, and always zero on an event.
	RequestVersion int32 `json:"request_version"`
	// LastError is the last error message associated with the status at the time of this log entry.
	// Empty string if no error.
	LastError string `json:"last_error"`
	// Metadata is a set of key-value pairs providing additional context for this log entry.
	// Empty map if no metadata.
	Metadata map[string]string `json:"metadata"`
}

// NewRequestStatusLog creates a RequestLog recording that a request reached a status.
// TimestampMs is set to the current time. If metadata is nil, it will be initialized as an empty map.
// queue is the queue processing the request; it scopes requestID, which is only unique within it.
// requestVersion is the version of the request entity, should only be set if reporting a request state as a status, otherwise it should be 0.
// lastError is the last error message associated with the status at the time of this log entry, empty string if no error.
// metadata is a set of key-value pairs providing additional context for this log entry. Not constrained to any specific format or schema, used for display or debugging purposes.
func NewRequestStatusLog(queue string, requestID string, status RequestStatus, requestVersion int32, lastError string, metadata map[string]string) RequestLog {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return RequestLog{
		RequestID:      requestID,
		Queue:          queue,
		TimestampMs:    time.Now().UnixMilli(),
		Type:           RequestLogTypeStatus,
		Status:         status,
		RequestVersion: requestVersion,
		LastError:      lastError,
		Metadata:       metadata,
	}
}

// NewRequestEventLog creates a RequestLog recording that something happened to a
// request while it sat at its current status.
//
// It takes no request version because an event is not a state transition: carrying
// one would let a reader mistake it for a reconcilable status. Arguments otherwise
// match NewRequestStatusLog.
func NewRequestEventLog(queue string, requestID string, event RequestEvent, metadata map[string]string) RequestLog {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return RequestLog{
		RequestID:   requestID,
		Queue:       queue,
		TimestampMs: time.Now().UnixMilli(),
		Type:        RequestLogTypeEvent,
		Event:       event,
		Metadata:    metadata,
	}
}

// Value returns what this entry recorded — its status or its event — as the
// single string that identifies it for a message ID, a log line, or display.
func (r RequestLog) Value() string {
	if r.Type == RequestLogTypeEvent {
		return string(r.Event)
	}
	return string(r.Status)
}

// ToBytes serializes the RequestLog to JSON bytes for queue message payload.
func (r RequestLog) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// RequestLogFromBytes deserializes a RequestLog from JSON bytes.
// If metadata is absent from the JSON, it will be initialized as an empty map.
// An entry without a type predates the field, when every entry recorded a status.
func RequestLogFromBytes(data []byte) (RequestLog, error) {
	var log RequestLog
	err := json.Unmarshal(data, &log)
	if err != nil {
		return log, err
	}
	if log.Metadata == nil {
		log.Metadata = make(map[string]string)
	}
	if log.Type == "" {
		log.Type = RequestLogTypeStatus
	}
	return log, nil
}
