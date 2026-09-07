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
// the logical topic keys for SubmitQueue's internal pipeline stages; they are
// the same strings each message lists in its topic_keys option.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyStart carries new land requests from the gateway to start.
	TopicKeyStart TopicKey = "start"
	// TopicKeyCancel carries cancellation requests from the gateway to cancel.
	TopicKeyCancel TopicKey = "cancel"
	// TopicKeyValidate carries request ids from start to validate.
	TopicKeyValidate TopicKey = "validate"
	// TopicKeyBatch carries request ids from landconflictsignal to batch.
	TopicKeyBatch TopicKey = "batch"
	// TopicKeyDependencyAnalysis carries newly created batch ids for conflict
	// analysis. Messages must be partitioned by queue: analysis reads the
	// queue's dependency-eligible batches, so two batches of one queue analyzed
	// concurrently would each miss the other.
	TopicKeyDependencyAnalysis TopicKey = "dependency-analysis"
	// TopicKeySpeculate carries batch ids for speculation.
	TopicKeySpeculate TopicKey = "speculate"
	// TopicKeyBuild carries batch ids whose speculated heads should be built.
	TopicKeyBuild TopicKey = "build"
	// TopicKeyBuildSignal carries build ids to poll. The consumer calls
	// BuildRunner.Status, persists the latest status, publishes the batch id to
	// TopicKeySpeculate so the state machine re-evaluates, and holds the
	// delivery for the next poll when the build has not yet reached a terminal
	// state.
	TopicKeyBuildSignal TopicKey = "buildsignal"
	// TopicKeyLand carries batch ids to the internal land stage before Runway.
	TopicKeyLand TopicKey = "submitqueue-land"
	// TopicKeyConclude carries batch ids for terminal request reconciliation.
	TopicKeyConclude TopicKey = "conclude"
	// TopicKeyLog carries per-request log entries from the orchestrator to the gateway.
	TopicKeyLog TopicKey = "log"
)

// MetadataKeyFailureReason is the conclude message's metadata attribute carrying
// a failed batch's human-readable reason. Set by the failure sites (land and
// speculate) on the conclude publish and read by conclude to stamp the request's
// terminal log; absent on the landed and cancelled paths.
const MetadataKeyFailureReason = "failure_reason"
