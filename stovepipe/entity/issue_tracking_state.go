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

// IssueTrackingPhase describes whether one source build's issue updates are underway or complete.
type IssueTrackingPhase string

const (
	// IssueTrackingPhaseUnknown is the unset phase.
	IssueTrackingPhaseUnknown IssueTrackingPhase = ""
	// IssueTrackingPhaseRefreshing means reported build issues are being opened or kept open.
	IssueTrackingPhaseRefreshing IssueTrackingPhase = "refreshing"
	// IssueTrackingPhaseRefreshed means a refresh pass completed; later refresh notifications remain eligible.
	IssueTrackingPhaseRefreshed IssueTrackingPhase = "refreshed"
	// IssueTrackingPhaseFinalizing means existing issues are being reconciled after all reported issues were applied.
	IssueTrackingPhaseFinalizing IssueTrackingPhase = "finalizing"
	// IssueTrackingPhaseFinalized means final reconciliation completed; later refresh notifications are ignored.
	IssueTrackingPhaseFinalized IssueTrackingPhase = "finalized"
)

// IssueTrackingState is the queue-scoped source-order boundary and resumable reconciliation state.
type IssueTrackingState struct {
	// RequestID is the newest accepted source request, compared by ingest order.
	RequestID string `json:"request_id"`
	// BuildID identifies the source build whose pass is underway or most recently completed.
	BuildID string `json:"build_id"`
	// NotificationID identifies the message that owns this reconciliation pass.
	NotificationID string `json:"notification_id"`
	// CursorTaskID identifies the current issue's task; active passes revisit it on continuation.
	// Empty with an empty CursorUnitID starts the phase's range.
	CursorTaskID string `json:"cursor_task_id"`
	// CursorUnitID identifies the unit at the cursor; empty is task-wide.
	CursorUnitID string `json:"cursor_unit_id"`
	// Finalize identifies a terminal reconciliation pass rather than an ordinary refresh.
	Finalize bool `json:"finalize"`
	// HasTaskWideIssues records whether this pass has encountered a task-wide issue.
	HasTaskWideIssues bool `json:"has_task_wide_issues"`
	// Phase distinguishes running passes from completed refresh or final passes.
	Phase IssueTrackingPhase `json:"phase"`
	// Version is the positive optimistic-lock version of this state.
	Version int32 `json:"version"`
	// HasNewIssues records whether this pass encountered an issue first opened or reopened by its source build.
	HasNewIssues bool `json:"has_new_issues"`
}

// Validate verifies the identity, phase, and version of a persisted state.
func (s IssueTrackingState) Validate() error {
	if err := id.Validate(s.RequestID); err != nil {
		return fmt.Errorf("invalid issue tracking request ID: %w", err)
	}
	if s.BuildID == "" {
		return fmt.Errorf("issue tracking build ID must not be empty")
	}
	if s.NotificationID == "" {
		return fmt.Errorf("issue tracking notification ID must not be empty")
	}
	if s.CursorTaskID == "" && s.CursorUnitID != "" {
		return fmt.Errorf("issue tracking cursor unit requires a task")
	}
	if s.Version <= 0 {
		return fmt.Errorf("issue tracking version must be positive")
	}
	switch s.Phase {
	case IssueTrackingPhaseRefreshing:
	case IssueTrackingPhaseRefreshed:
		if s.Finalize {
			return fmt.Errorf("a terminal pass cannot finish as an ordinary refresh")
		}
	case IssueTrackingPhaseFinalizing:
		if !s.Finalize {
			return fmt.Errorf("finalizing requires terminal intent")
		}
		if s.HasTaskWideIssues {
			return fmt.Errorf("task-wide failures prevent closing missing issues")
		}
	case IssueTrackingPhaseFinalized:
		if !s.Finalize {
			return fmt.Errorf("finalized requires terminal intent")
		}
	default:
		return fmt.Errorf("invalid issue tracking phase %q", s.Phase)
	}
	return nil
}
