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

import "encoding/json"

// CancelRequest represents a cancellation request sent over the queue from the gateway to the orchestrator.
// It identifies the request to cancel by its ID and carries an optional human-readable reason for observability.
type CancelRequest struct {
	// ID is the globally unique identifier of the request to cancel. Format: "<queue>/<counter_value>".
	ID string `json:"id"`
	// Reason is an optional free-form explanation of why the cancellation was requested.
	Reason string `json:"reason"`
}

// ToBytes serializes the CancelRequest to JSON bytes for queue message payload.
func (r CancelRequest) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// CancelRequestFromBytes deserializes a CancelRequest from JSON bytes.
func CancelRequestFromBytes(data []byte) (CancelRequest, error) {
	var req CancelRequest
	err := json.Unmarshal(data, &req)
	return req, err
}
