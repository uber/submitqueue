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

import (
	"fmt"

	"github.com/uber/submitqueue/platform/base/id"
)

// RequestIssueSummary is a read model of an observation and its retained repair association.
// Observation membership survives recovery; repair association does not imply issue recovery.
type RequestIssueSummary struct {
	// RequestID is the canonical decimal request identifier within the bound queue.
	RequestID string
	// BuildID identifies the build where the failure was observed.
	BuildID string
	// TaskID identifies the validation task and must be nonempty.
	TaskID string
	// UnitID identifies the validation unit; empty means a task-wide failure.
	UnitID string
	// FixID identifies the retained repair association; empty means none is recorded.
	FixID string
	// Version is the positive optimistic-lock version of the projection.
	Version int32
}

// Validate checks the observation identity and projection version.
func (s RequestIssueSummary) Validate() error {
	if err := id.Validate(s.RequestID); err != nil {
		return fmt.Errorf("invalid request issue request ID: %w", err)
	}
	if s.BuildID == "" || s.TaskID == "" {
		return fmt.Errorf("request issue requires build and task IDs")
	}
	if s.Version <= 0 {
		return fmt.Errorf("request issue projection version must be positive")
	}
	return nil
}
