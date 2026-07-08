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

// Queue holds per-queue pipeline coordination state for a named repo+ref (e.g. "monorepo/main").
type Queue struct {
	// ****************
	// Immutable fields, fixed at queue entity creation
	// ****************

	// Name is the stable logical id for the queue. It should be unique within the system.
	Name string `json:"name"`

	// ****************
	// Following fields could be changed throughout the lifecycle of the queue
	// ****************

	// LastGreenURI is the queue's last-known-good commit: the most recent head at which
	// whole-repo validation recorded green (health degree 0). Empty until the first such outcome.
	LastGreenURI string `json:"last_green_uri"`

	// InFlightCount is the number of trunk validations admitted by process but not yet terminal.
	InFlightCount int32 `json:"in_flight_count"`

	// LatestRequestSeq is the highest request sequence accepted for this queue. Used to coalesce
	// backlog so intermediate heads are skipped without superseding the newest head.
	LatestRequestSeq int64 `json:"latest_request_seq"`

	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32 `json:"version"`
}
