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

// Package topickey re-exports SubmitQueue pipeline stage identifiers from
// submitqueue/core/messagequeue, the package that owns the topic-key constants
// and the payloads bound to them.
package topickey

import "github.com/uber/submitqueue/submitqueue/core/messagequeue"

// TopicKey is the shared pipeline stage identifier type.
type TopicKey = messagequeue.TopicKey

const (
	// TopicKeyStart carries new land requests from the gateway to start.
	TopicKeyStart = messagequeue.TopicKeyStart
	// TopicKeyCancel carries cancellation requests from the gateway to cancel.
	TopicKeyCancel = messagequeue.TopicKeyCancel
	// TopicKeyValidate carries request ids from start to validate.
	TopicKeyValidate = messagequeue.TopicKeyValidate
	// TopicKeyBatch carries request ids from landconflictsignal to batch.
	TopicKeyBatch = messagequeue.TopicKeyBatch
	// TopicKeyDependencyAnalysis carries newly created batch ids for conflict analysis.
	TopicKeyDependencyAnalysis = messagequeue.TopicKeyDependencyAnalysis
	// TopicKeySpeculate carries batch ids for speculation.
	TopicKeySpeculate = messagequeue.TopicKeySpeculate
	// TopicKeyBuild carries batch ids whose speculated heads should be built.
	TopicKeyBuild = messagequeue.TopicKeyBuild
	// TopicKeyBuildSignal carries build ids to poll.
	TopicKeyBuildSignal = messagequeue.TopicKeyBuildSignal
	// TopicKeyLand carries batch ids to the internal land stage before Runway.
	TopicKeyLand = messagequeue.TopicKeyLand
	// TopicKeyConclude carries batch ids for terminal request reconciliation.
	TopicKeyConclude = messagequeue.TopicKeyConclude
	// TopicKeyLog carries per-request log entries from the orchestrator to the gateway.
	TopicKeyLog = messagequeue.TopicKeyLog
	// MetadataKeyFailureReason is the conclude message metadata attribute for a failed batch's reason.
	MetadataKeyFailureReason = messagequeue.MetadataKeyFailureReason
)