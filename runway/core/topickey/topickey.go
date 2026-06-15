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

// Package topickey defines the Runway-owned queue identifiers. Runway owns the
// merge queues — a dry-run merge-conflict check pair and a committing merge
// pair, both carrying the shared entity.MergeRequest/MergeResult contract.
// Other services (e.g. SubmitQueue) import these keys to publish onto / consume
// from them.
package topickey

import "github.com/uber/submitqueue/core/consumer"

// TopicKey is the shared pipeline stage identifier type.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyMergeConflictCheck is the runway-owned queue that carries dry-run
	// merge-conflict check requests. A client publishes a full
	// entity.MergeRequest here; runway consumes it, attempts the merge without
	// committing, and reports only whether it was mergeable.
	TopicKeyMergeConflictCheck TopicKey = "merge-conflict-checker"
	// TopicKeyMergeConflictCheckSignal is the runway-owned queue that carries
	// merge-conflict check results. Runway publishes a full entity.MergeResult
	// here (with no produced revisions); the requesting client consumes it.
	TopicKeyMergeConflictCheckSignal TopicKey = "merge-conflict-checker-signal"
	// TopicKeyMerge is the runway-owned queue that carries committing merge
	// requests. A client publishes a full entity.MergeRequest here; runway
	// consumes it, applies the steps, commits the result, and reports the
	// revisions it produced.
	TopicKeyMerge TopicKey = "merger"
	// TopicKeyMergeSignal is the runway-owned queue that carries committing
	// merge results. Runway publishes a full entity.MergeResult here (with the
	// produced revisions populated); the requesting client consumes it.
	TopicKeyMergeSignal TopicKey = "merger-signal"
)
