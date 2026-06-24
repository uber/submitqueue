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

// RequestStatus defines the possible customer-friendly status of a request. Status is display-friendly
// and can be shown to callers; it is different from the internal RequestState used to implement the state
// machine. Request statuses can generally be added freely by the system without breaking the state machine.
// Some statuses correspond to a request state, in which case they should be supplemented with the request
// state version for reconciliation. Other statuses are purely informational and can be added freely. Every
// status may be accompanied by a last error message and free-form metadata in the request log; those are
// used for display or debugging purposes only.
type RequestStatus string

const (
	// RequestStatusUnknown is the unknown sentinel status. It is set by default when the structure is
	// initialized and should never be seen in the system.
	RequestStatusUnknown RequestStatus = ""

	// RequestStatusAccepted indicates that the request has been accepted by the system. The gateway sets
	// this status when the ingest request is received and persisted to the logging database.
	RequestStatusAccepted RequestStatus = "accepted"

	// RequestStatusStarted is the initial status of a request. It corresponds to the RequestStateStarted
	// state and is typically set by the orchestrator when the commit is recorded in the operating database.
	RequestStatusStarted RequestStatus = "started"

	// RequestStatusValidating indicates that the request's commit metadata is currently being resolved for
	// ordering and batching.
	RequestStatusValidating RequestStatus = "validating"

	// RequestStatusValidated indicates that the request has been validated successfully. It corresponds to
	// the RequestStateValidated state.
	RequestStatusValidated RequestStatus = "validated"

	// RequestStatusBatching indicates that the request is waiting to be included in a validation batch.
	RequestStatusBatching RequestStatus = "batching"

	// RequestStatusBatched indicates that the request has been included in a validation batch. It
	// corresponds to the RequestStateBatched state.
	RequestStatusBatched RequestStatus = "batched"

	// RequestStatusBuilding indicates that the batch containing the request is being built and tested. It
	// corresponds to the RequestStateBuilding state.
	RequestStatusBuilding RequestStatus = "building"

	// RequestStatusBuilt indicates that the batch containing the request has finished building and can move
	// to the next phase.
	RequestStatusBuilt RequestStatus = "built"

	// RequestStatusSucceeded indicates that the request's commit validated green. It corresponds to the
	// RequestStateSucceeded state.
	RequestStatusSucceeded RequestStatus = "succeeded"

	// RequestStatusFailed indicates that the request's commit was found to break a target. It corresponds
	// to the RequestStateFailed state.
	RequestStatusFailed RequestStatus = "failed"

	// RequestStatusError indicates that the request encountered an infrastructure error and could not be
	// validated. It corresponds to the RequestStateError state.
	RequestStatusError RequestStatus = "error"
)

// RequestLog is an append-only record that captures a point-in-time snapshot of a request's status for
// reconciliation purposes. It is stored in a separate database from the request store to support eventual
// consistency reconciliation.
type RequestLog struct {
	// RequestID is the ID of the request this log entry belongs to. References entity.Request.ID.
	RequestID string `json:"request_id"`
	// TimestampMs is the time this log entry was created, in milliseconds since Unix epoch.
	TimestampMs int64 `json:"timestamp_ms"`
	// Status is the request status at the time this log entry was created. It may contain request states
	// from the state machine and also display-friendly intermediate statuses.
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
// requestVersion is the version of the request entity; it should only be set when reporting a request
// state as a status, otherwise it should be 0.
// lastError is the last error message associated with the status at the time of this log entry, empty
// string if no error.
// metadata is a set of key-value pairs providing additional context for this log entry. It is not
// constrained to any specific format or schema and is used for display or debugging purposes.
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
