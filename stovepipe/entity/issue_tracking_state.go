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

// IssueTrackingPhase describes whether one source build's issue updates are pending or complete.
type IssueTrackingPhase string

const (
	// IssueTrackingPhaseUnknown is the unset phase.
	IssueTrackingPhaseUnknown IssueTrackingPhase = ""
	// IssueTrackingPhaseRefreshing means positive observations are being applied.
	IssueTrackingPhaseRefreshing IssueTrackingPhase = "refreshing"
	// IssueTrackingPhaseRefreshed means a refresh pass completed; later observations remain eligible.
	IssueTrackingPhaseRefreshed IssueTrackingPhase = "refreshed"
	// IssueTrackingPhaseFinalizing means final opening and resolution updates are pending.
	IssueTrackingPhaseFinalizing IssueTrackingPhase = "finalizing"
	// IssueTrackingPhaseFinalized means final reconciliation completed; later observations are ignored.
	IssueTrackingPhaseFinalized IssueTrackingPhase = "finalized"
)

// IssueTrackingState is the queue-scoped source-order boundary and resumable reconciliation state.
type IssueTrackingState struct {
	// RequestID is the newest accepted source request, compared by ingest order.
	RequestID string `json:"request_id"`
	// BuildID identifies the source build whose pass is pending or most recently completed.
	BuildID string `json:"build_id"`
	// Phase distinguishes pending work from completed observation or final passes.
	Phase IssueTrackingPhase `json:"phase"`
	// Version is the positive optimistic-lock version of this state.
	Version int32 `json:"version"`
}

// IsPending reports whether the phase represents unfinished reconciliation.
func (s IssueTrackingState) IsPending() bool {
	return s.Phase == IssueTrackingPhaseRefreshing || s.Phase == IssueTrackingPhaseFinalizing
}

// Validate verifies the identity, phase, and version of a persisted state.
func (s IssueTrackingState) Validate() error {
	if err := id.Validate(s.RequestID); err != nil {
		return fmt.Errorf("invalid issue tracking request ID: %w", err)
	}
	if s.BuildID == "" {
		return fmt.Errorf("issue tracking build ID must not be empty")
	}
	if s.Version <= 0 {
		return fmt.Errorf("issue tracking version must be positive")
	}
	switch s.Phase {
	case IssueTrackingPhaseRefreshing, IssueTrackingPhaseRefreshed, IssueTrackingPhaseFinalizing, IssueTrackingPhaseFinalized:
		return nil
	default:
		return fmt.Errorf("invalid issue tracking phase %q", s.Phase)
	}
}
