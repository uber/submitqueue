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

// RequestLog is an append-only record that captures a point-in-time snapshot of a request's status
// for reconciliation purposes. It is stored in a separate database from the request store to support
// eventual consistency reconciliation.
type RequestLog struct {
	// RequestID is the ID of the request this log entry belongs to. References entity.Request.ID.
	RequestID string `json:"request_id"`
	// TimestampMs is the time this log entry was created, in milliseconds since Unix epoch.
	TimestampMs int64 `json:"timestamp_ms"`
	// Status is the request status at the time this log entry was created. It does not have to correspond to the request status. For example, it may contain intermediate statuses like "validated" or "processing".
	Status string `json:"status"`
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
func NewRequestLog(requestID string, status string, requestVersion int32, lastError string, metadata map[string]string) RequestLog {
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
