// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package entity defines immutable SQSim scenario data.
package entity

// Scenario is the complete workload executed against one fresh SubmitQueue stack.
type Scenario struct {
	// TimeoutMs is the maximum wall-clock duration of the run in milliseconds.
	TimeoutMs int64 `json:"timeout_ms"`
	// Lands are the requests submitted by the run in declaration order.
	Lands []Land `json:"lands"`
}

// Land describes one Gateway Land call and its modeled external behavior.
type Land struct {
	// Name identifies the Land within its scenario.
	Name string `json:"name"`
	// Queue is the SubmitQueue queue receiving the request.
	Queue string `json:"queue"`
	// SubmitAfterMs is the delay from run start before submission in milliseconds.
	SubmitAfterMs int64 `json:"submit_after_ms"`
	// Behavior describes the external systems encountered by this request.
	Behavior Behavior `json:"behavior"`
	// Expectation describes the required public terminal outcome.
	Expectation Expectation `json:"expectation"`
}

// Expectation describes the public terminal outcome required by a Land.
type Expectation struct {
	// Status is the expected public terminal request status.
	Status ExpectedRequestStatus `json:"status"`
}

// ExpectedRequestStatus is a terminal public request status expected by SQSim.
type ExpectedRequestStatus string

const (
	// RequestLanded expects the request to land successfully.
	RequestLanded ExpectedRequestStatus = "landed"
	// RequestError expects the request to reach terminal error.
	RequestError ExpectedRequestStatus = "error"
	// RequestCancelled expects the request to be cancelled.
	RequestCancelled ExpectedRequestStatus = "cancelled"
)
