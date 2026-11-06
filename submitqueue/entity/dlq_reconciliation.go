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

// DLQReconciliationSource identifies a logical message intent within a queue.
// Its values are immutable and compared verbatim, including trailing spaces.
type DLQReconciliationSource struct {
	// Topic is the logical source topic, not a backend-specific topic name.
	Topic string
	// PartitionKey is the original opaque source partition key.
	PartitionKey string
	// MessageID is the opaque logical-cause ID within the topic and partition.
	MessageID string
}

// DLQReconciliation is a versioned snapshot of a final message's queue repair.
// Queue and Source together form its immutable primary key.
type DLQReconciliation struct {
	// ID is an immutable positive canonical decimal, unique within Queue.
	ID string
	// Queue is the immutable queue name scoping the source intent and batch IDs.
	Queue string
	// Source is the immutable logical message identity within Queue.
	Source DLQReconciliationSource
	// BatchIDs retains every captured target ID; successive snapshots never drop IDs.
	BatchIDs []string
	// Completed is monotone: a completed obligation never becomes pending again.
	Completed bool
	// Version starts at one and increases on each successful conditional replacement.
	Version int32
}
