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

// QueueBatchState is a membership record filing one in-queue batch under one lifecycle
// state bucket of its queue. A record exists for every batch from its creation until it
// exits the queue — through terminal states, not just while in flight.
//
// The (Queue, State, BatchID) triple is the record's identity; records carry no other
// data and are never updated in place — a batch changes buckets by a record appearing
// under the new state and the old record disappearing.
//
// Records are advisory. The authoritative state is the State field of the Batch
// identified by BatchID; a record may transiently file a batch under a bucket the batch
// has already left, and a batch may transiently have records in more than one bucket. A
// batch is never without at least one record while it is in the queue.
type QueueBatchState struct {
	// Queue is the name of the queue the batch belongs to. Queue name is defined in the
	// configuration and should be unique within the system.
	Queue string

	// State is the lifecycle state bucket this record files the batch under. Advisory:
	// the batch's authoritative state lives on the Batch entity and may differ transiently.
	State BatchState

	// BatchID is the globally unique identifier of the batch. Format: "<queue>/batch/<counter_value>".
	BatchID string
}
