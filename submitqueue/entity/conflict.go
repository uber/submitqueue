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

// ConflictType classifies why two batches are considered to conflict.
// New values may be added as more sophisticated analyzers are introduced.
type ConflictType string

const (
	// ConflictTypeUnknown is the unreachable zero value, set by default when
	// the structure is initialized. It should never be seen in the system.
	ConflictTypeUnknown ConflictType = ""
	// ConflictTypeConservative means the analyzer treated the batches as
	// conflicting because it could not prove otherwise, without identifying a
	// specific reason. Used by conservative analyzers that serialize
	// everything by default.
	ConflictTypeConservative ConflictType = "conservative"
	// ConflictTypeTargetOverlap means the two batches modify one or more of
	// the same build targets and may therefore interfere with each other.
	ConflictTypeTargetOverlap ConflictType = "target_overlap"
)

// Conflict reports a single conflict between an analyzed batch and one of the
// in-flight batches.
type Conflict struct {
	// BatchID is the ID of the in-flight batch that conflicts with the
	// analyzed batch.
	BatchID string
	// Type classifies the conflict. A single (analyzed, in-flight) pair may
	// be reported with multiple Conflict entries when different conflict
	// types apply.
	Type ConflictType
}
