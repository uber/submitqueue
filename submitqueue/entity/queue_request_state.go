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

package entity

// QueueRequestState is a membership record filing one in-queue request under its current
// state. Exactly one record exists for a request from its creation until it reaches a
// terminal state, and none afterwards.
//
// The (Queue, State, RequestID) triple is the record's identity. The record mirrors the
// State of the Request identified by RequestID and changes together with it.
type QueueRequestState struct {
	// Queue is the name of the queue the request belongs to. Queue name is defined in the
	// configuration and should be unique within the system.
	Queue string
	// State is the request's current state. Always one of InQueueRequestStates().
	State RequestState
	// RequestID is the request's identifier, unique within Queue.
	RequestID string
	// EnteredAtMs is when the request entered State, in Unix milliseconds. A version-only
	// update that keeps the state does not change it.
	EnteredAtMs int64
}
