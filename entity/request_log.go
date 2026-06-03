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

// RequestLogStatus defines the possible status of a request. Status is customer-friendly and can be displayed to the user.
// It is different from the request state, which is internal and used to implement a state machine. Request statuses can be
// generally added freely by the system without breaking the state machine.
// Some statuses correspond to the request state, in which case they should be supplemented with the request state version to be used for reconciliation.
// Other statuses are purely informational and can be added freely.
// Every status may be accompanied by a last error message and free-formmetadata in the Request Log. It will only be used for display or debugging purposes.
type RequestStatus string

const (
	// RequestStatusUnknown is the unknown sentinel status. It is set by default when the structure is initialized. It should never be seen in the system.
	RequestStatusUnknown RequestStatus = ""

	// RequestStatusAccepted indicates that the request has been accepted by the system. Typically a gateway service will set this status when the land request is received and persisted to the logging database.
	RequestStatusAccepted RequestStatus = "accepted"

	// RequestStatusStarted is the initial status of a request. It corresponds to the RequestStateStarted state and typically set by the orchestrator service when the request is received and persisted to the operating database.
	RequestStatusStarted RequestStatus = "started"

	// RequestStatusValidating indicates that the request is currently being validated (e.g., duplicate check, merge check, etc.).
	RequestStatusValidating RequestStatus = "validating"

	// RequestStatusValidated indicates that the request has been validated (duplicate check, merge check etc.) successfully. It corresponds to the RequestStateValidated state.
	RequestStatusValidated RequestStatus = "validated"

	// RequestStatusBatching indicates that the request is waiting to be included in a batch.
	RequestStatusBatching RequestStatus = "batching"

	// RequestStatusBatched indicates that the request has been included in a new batch and will be sent to speculation.
	RequestStatusBatched RequestStatus = "batched"

	// RequestStatusScored indicates that the batch containing the request has been scored for build success probability.
	RequestStatusScored RequestStatus = "scored"

	// RequestStatusSpeculating indicates that the request is currently being speculated (e.g., speculative merge/rebase, etc.).
	RequestStatusSpeculating RequestStatus = "speculating"

	// RequestStatusSpeculated indicates that the request has been successfully speculated and is ready to be validated via a build system.
	RequestStatusSpeculated RequestStatus = "speculated"

	// RequestStatusBuilding indicates that the request is currently being built (e.g., CI/CD system is building the change on top of the speculation path).
	RequestStatusBuilding RequestStatus = "building"

	// RequestStatusBuilt indicates that the request has finished the build step successfully and can move to the next phase, either wait for other requests to finish or move to the land phase.
	RequestStatusBuilt RequestStatus = "built"

	// RequestStatusWaitingPath indicates that the request is waiting for other preceiding request in the same speculation path to finish.
	RequestStatusWaitingPath RequestStatus = "waitingpath"

	// RequestStatusLanding indicates that the request is actively being landed (e.g., source control operation is in progress to push the change to the target branch).
	RequestStatusLanding RequestStatus = "landing"

	// RequestStatusProcessing is the status of a request that is being processed. It corresponds to the RequestStateProcessing state.
	RequestStatusProcessing RequestStatus = "processing"

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

// RequestLog is an append-only record that captures a point-in-time snapshot of a request's status
// for reconciliation purposes. It is stored in a separate database from the request store to support
// eventual consistency reconciliation.
type RequestLog struct {
	// RequestID is the ID of the request this log entry belongs to. References entity.Request.ID.
	RequestID string `json:"request_id"`
	// TimestampMs is the time this log entry was created, in milliseconds since Unix epoch.
	TimestampMs int64 `json:"timestamp_ms"`
	// Status is the request status at the time this log entry was created. It may contain requests states from the state machine and also display-friendly intermediate statuses.
	Status RequestStatus `json:"status"`
	// RequestVersion is the version of the request at the time this log entry was created.
	// Zero if the version is not available.
	RequestVersion int32 `json:"request_version"`
	// LastError is the last error message associated with the status at the time of this log entry.
	// Empty string if no error.
	LastError string `json:"last_error"`
	// Metadata is a set of key-value pairs providing additional context for this log entry.
	// Empty map if no metadata.
	Metadata map[string]string `json:"metadata"`
}

// NewRequestLog creates a new RequestLog with the given fields.
// TimestampMs is set to the current time. If metadata is nil, it will be initialized as an empty map.
// requestVersion is the version of the request entity, should only be set if reporting a request state as a status, otherwise it should be 0.
// lastError is the last error message associated with the status at the time of this log entry, empty string if no error.
// metadata is a set of key-value pairs providing additional context for this log entry. Not constrained to any specific format or schema, used for display or debugging purposes.
func NewRequestLog(requestID string, status RequestStatus, requestVersion int32, lastError string, metadata map[string]string) RequestLog {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return RequestLog{
		RequestID:      requestID,
		TimestampMs:    time.Now().UnixMilli(),
		Status:         status,
		RequestVersion: requestVersion,
		LastError:      lastError,
		Metadata:       metadata,
	}
}

// ToBytes serializes the RequestLog to JSON bytes for queue message payload.
func (r RequestLog) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// RequestLogFromBytes deserializes a RequestLog from JSON bytes.
// If metadata is absent from the JSON, it will be initialized as an empty map.
func RequestLogFromBytes(data []byte) (RequestLog, error) {
	var log RequestLog
	err := json.Unmarshal(data, &log)
	if err != nil {
		return log, err
	}
	if log.Metadata == nil {
		log.Metadata = make(map[string]string)
	}
	return log, nil
}
