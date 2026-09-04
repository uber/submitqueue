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
// the wire topic names a client uses to publish to / consume from Runway's land
// queues; they are the same strings each message lists in its topics option.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyLandConflictCheck carries dry-run land-conflict check requests.
	// A client publishes a LandRequest here; Runway attempts the land without
	// committing and reports only whether it was landable.
	TopicKeyLandConflictCheck TopicKey = "land-conflict-check"
	// TopicKeyLandConflictCheckSignal carries land-conflict check results.
	// Runway publishes a LandResult here (with no produced revisions); the
	// requesting client consumes it.
	TopicKeyLandConflictCheckSignal TopicKey = "land-conflict-check-signal"
	// TopicKeyLand carries committing land requests. A client publishes a
	// LandRequest here; Runway applies the steps, commits the result, and
	// reports the revisions it produced.
	TopicKeyLand TopicKey = "runway-land"
	// TopicKeyLandSignal carries committing land results. Runway publishes a
	// LandResult here (with the produced revisions populated); the requesting
	// client consumes it.
	TopicKeyLandSignal TopicKey = "land-signal"
)
