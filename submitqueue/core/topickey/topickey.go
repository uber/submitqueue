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

// Package topickey defines SubmitQueue pipeline stage identifiers.
package topickey

import "github.com/uber/submitqueue/platform/consumer"

// TopicKey is the shared pipeline stage identifier type.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyStart is the pipeline stage where new requests arrive from the gateway.
	TopicKeyStart TopicKey = "start"
	// TopicKeyCancel is the pipeline stage where cancellation requests arrive from the gateway.
	TopicKeyCancel TopicKey = "cancel"
	// TopicKeyValidate is the pipeline stage where requests are published for validation.
	TopicKeyValidate TopicKey = "validate"
	// TopicKeyBatch is the pipeline stage where validated requests are published for batching.
	TopicKeyBatch TopicKey = "batch"
	// TopicKeyScore is the pipeline stage where batches are published for scoring.
	TopicKeyScore TopicKey = "score"
	// TopicKeySpeculate is the pipeline stage where scored batches are published for speculation.
	TopicKeySpeculate TopicKey = "speculate"
	// TopicKeyPrioritize is the queue-wide reconcile stage that rations the
	// build budget across every in-flight batch of a queue. Each message
	// carries a QueueID; the consumer loads every Speculating batch's tree,
	// runs the queue's Prioritizer over the candidate paths, applies the
	// resulting decisions — promoting paths into the build budget, or
	// cancelling in-flight paths a preemptive policy evicts — and republishes
	// to TopicKeyBuild for any path cleared to run.
	TopicKeyPrioritize TopicKey = "prioritize"
	// TopicKeyBuild is the pipeline stage where speculated batches are published for builds.
	TopicKeyBuild TopicKey = "build"
	// TopicKeyBuildSignal is the polling stage for triggered builds. Each
	// message carries a Build; the consumer calls BuildRunner.Status,
	// persists the latest status, publishes the batch ID to TopicKeySpeculate
	// so the state machine re-evaluates, and re-publishes itself via
	// PublishAfter when the build has not yet reached a terminal state.
	TopicKeyBuildSignal TopicKey = "buildsignal"
	// TopicKeyMerge is the pipeline stage where speculated batches are published for merging.
	TopicKeyMerge TopicKey = "merge"
	// TopicKeyConclude is the pipeline stage where merged requests are published for conclusion.
	TopicKeyConclude TopicKey = "conclude"
	// TopicKeyLog is the pipeline stage where per-request logs are written.
	TopicKeyLog TopicKey = "log"
)
