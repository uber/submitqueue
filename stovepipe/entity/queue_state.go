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

import "fmt"

// QueueEnablement is applied enablement, independent of execution controls and verdicts.
type QueueEnablement string

const (
	// QueueEnablementUnknown is unset and never an explicit disablement.
	QueueEnablementUnknown QueueEnablement = ""
	// QueueEnablementEnabled identifies enabled queue coverage.
	QueueEnablementEnabled QueueEnablement = "enabled"
	// QueueEnablementDisabled identifies disabled queue coverage.
	QueueEnablementDisabled QueueEnablement = "disabled"
)

// QueueState is one applied enablement occurrence, independent of storage row versions.
type QueueState struct {
	// Queue is the stable repo-and-ref namespace, at most 255 bytes.
	Queue string
	// State is enabled or disabled.
	State QueueEnablement
	// EffectiveFromCommitURI is the inclusive immutable boundary, at most 255 bytes; empty only at version 1.
	EffectiveFromCommitURI string
	// ChangedAtMs is the application timestamp in Unix milliseconds.
	ChangedAtMs int64
	// Version is the positive queue-scoped state version.
	Version int64
}

// Validate verifies the invariants of a persisted state snapshot.
func (s QueueState) Validate() error {
	if s.Queue == "" || len(s.Queue) > 255 || s.Version <= 0 || s.ChangedAtMs <= 0 {
		return fmt.Errorf("invalid queue state identity, version or timestamp")
	}
	if s.State != QueueEnablementEnabled && s.State != QueueEnablementDisabled {
		return fmt.Errorf("invalid queue enablement %q", s.State)
	}
	if s.Version == 1 {
		if s.State != QueueEnablementDisabled || s.EffectiveFromCommitURI != "" {
			return fmt.Errorf("initial queue state must be disabled without a boundary")
		}
	} else if s.EffectiveFromCommitURI == "" || len(s.EffectiveFromCommitURI) > 255 {
		return fmt.Errorf("invalid queue state commit boundary")
	}
	return nil
}
