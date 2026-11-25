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

// NearestGreenCommitRequest identifies a commit and project in a queue.
type NearestGreenCommitRequest struct {
	// Queue names the ordered repository ref.
	Queue string
	// DesiredCommitURI is the inclusive lower bound, even if never validated.
	DesiredCommitURI string
	// Project is the stable project identifier used by validation facts.
	Project string
}

// NearestGreenCommitResult identifies the first matching validated head, if any.
type NearestGreenCommitResult struct {
	// CommitURI is the first matching head; empty when there is no match.
	CommitURI string
	// RequestID is the validation request for CommitURI; empty when there is no match.
	RequestID string
}
