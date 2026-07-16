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

	// TopicKeyBuild carries requests whose build scope has been decided from
	// process/analyze to the build stage. The producer publishes a BuildRequest
	// (the request id) here; the build controller consumes it, reloads the
	// Request, and triggers the build. Partitioned by request id.
	TopicKeyBuild TopicKey = "build"

	// TopicKeyBuildSignal carries builds to poll from build to the buildsignal
	// stage, and from buildsignal back to itself between polls. Producers
	// publish a BuildSignal (the build id) here; the buildsignal controller
	// consumes it, polls the build runner, and records terminal status.
	// Partitioned by build id, so each build's poll loop is an independent
	// partition.
	TopicKeyBuildSignal TopicKey = "buildsignal"

	// TopicKeyRecord carries a build's terminal status from buildsignal to the
	// record stage. The buildsignal controller publishes a Record (the build
	// id) here once, and only once, a build reaches a terminal status;
	// non-terminal polls never publish here. Partitioned by request id.
	TopicKeyRecord TopicKey = "record"
)
