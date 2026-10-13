// Copyright (c) 2026 Uber Technologies, Inc.
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

// TargetLabel is one build target label and the 64-bit ID it hashes to. The
// whole record is immutable: (Queue, ID) is its identity and there is no update
// path. Two labels can share an ID on a hash collision; the first one recorded
// stays.
type TargetLabel struct {
	// Queue is the queue the label was seen in. Unique together with ID.
	Queue string

	// ID is the 64-bit hash of Label.
	ID uint64

	// Label is the target label exactly as the build graph reported it.
	Label string

	// CreatedAt is the Unix milliseconds timestamp when the record was created.
	CreatedAt int64
}
