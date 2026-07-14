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

package messagequeue

import "github.com/uber/submitqueue/platform/consumer"

// TopicKey is the typed identifier used to look up a queue backend, topic name,
// and subscription config in a consumer.TopicRegistry. The constants below are
// the wire topic names a client uses to publish to / consume from Runway's merge
// queues; they are the same strings each message lists in its topics option.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyMergeConflictCheck carries dry-run merge-conflict check requests.
	// A client publishes a MergeRequest here; Runway attempts the merge without
	// committing and reports only whether it was mergeable.
	TopicKeyMergeConflictCheck TopicKey = "merge-conflict-check"
	// TopicKeyMergeConflictCheckSignal carries merge-conflict check results.
	// Runway publishes a MergeResult here (with no produced revisions); the
	// requesting client consumes it.
	TopicKeyMergeConflictCheckSignal TopicKey = "merge-conflict-check-signal"
	// TopicKeyMerge carries committing merge requests. A client publishes a
	// MergeRequest here; Runway applies the steps, commits the result, and
	// reports the revisions it produced.
	TopicKeyMerge TopicKey = "runway-merge"
	// TopicKeyMergeSignal carries committing merge results. Runway publishes a
	// MergeResult here (with the produced revisions populated); the requesting
	// client consumes it.
	TopicKeyMergeSignal TopicKey = "merge-signal"
)
