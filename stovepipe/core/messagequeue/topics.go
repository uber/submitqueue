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
// the logical topic keys for Stovepipe's internal pipeline stages; they are the
// same strings each message lists in its topic_keys option.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyProcess carries newly accepted requests from ingest to the process
	// stage. ingest publishes a ProcessRequest (the request id) here; the process
	// controller consumes it, reloads the Request, and decides the build strategy.
	TopicKeyProcess TopicKey = "process"

	// TopicKeyBuild carries admitted requests from process to build. process
	// publishes a BuildRequest (the request id) after it persists the strategy
	// and baseline; build reloads the Request from storage.
	TopicKeyBuild TopicKey = "build"
)
