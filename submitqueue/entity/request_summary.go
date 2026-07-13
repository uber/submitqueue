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

// RequestSummary is the gateway-owned materialized current view of a request.
// RequestID is exposed as sqid by the gateway API.
type RequestSummary struct {
	// RequestID is the globally unique request identifier.
	RequestID string
	// Queue is the queue supplied at receipt.
	Queue string
	// ChangeURIs are the change URIs supplied at receipt in caller order.
	ChangeURIs []string
	// ReceivedAtMs is the immutable receipt timestamp in Unix milliseconds.
	ReceivedAtMs int64
	// Status is the current customer-facing request status.
	Status RequestStatus
	// RequestVersion is the orchestrator request version carried by the winning log entry, or zero when unavailable.
	RequestVersion int32
	// StatusTimestampMs is the timestamp of the winning log entry in Unix milliseconds.
	StatusTimestampMs int64
	// Version is the optimistic-lock version of this materialized view.
	Version int32
	// LastError is the error associated with the current status, or empty when absent.
	LastError string
	// Metadata is display and debugging metadata associated with the current status.
	Metadata map[string]string
}

// RequestQueueSummary is the queue-ordered projection returned by List.
type RequestQueueSummary struct {
	// RequestID is the globally unique request identifier.
	RequestID string
	// Queue is the queue supplied at receipt.
	Queue string
	// ChangeURIs are the change URIs supplied at receipt in caller order.
	ChangeURIs []string
	// ReceivedAtMs is the immutable receipt timestamp in Unix milliseconds.
	ReceivedAtMs int64
	// Status is the current customer-facing request status.
	Status RequestStatus
	// Version is copied from the authoritative RequestSummary and guards stale projection writers.
	Version int32
	// LastError is the error associated with the current status, or empty when absent.
	LastError string
	// Metadata is display and debugging metadata associated with the current status.
	Metadata map[string]string
}

// RequestURI maps one change URI to one received request.
type RequestURI struct {
	// ChangeURI is the exact canonical URI supplied at receipt.
	ChangeURI string
	// ReceivedAtMs is the immutable receipt timestamp in Unix milliseconds.
	ReceivedAtMs int64
	// RequestID is the globally unique request identifier.
	RequestID string
}
