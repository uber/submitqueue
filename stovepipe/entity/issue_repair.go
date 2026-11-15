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

// IssueRepair is one executor's repair submission state for an issue opening.
// Empty FixID and SkipReason identify pending submission, not backend acceptance.
type IssueRepair struct {
	// Issue identifies the issue opening within a queue.
	Issue OpenIssue
	// ExecutorID is the stable queue-local executor identity.
	ExecutorID string
	// FixID is the backend request or run ID for accepted work, empty while pending or skipped.
	FixID string
	// SkipReason is the terminal skip reason, empty while pending or accepted.
	SkipReason string
	// Metadata contains executor-owned string values; nil and empty maps are equivalent.
	Metadata map[string]string
	// Version is the positive optimistic-lock version.
	Version int32
}
