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

package buildkite

// Config holds the per-queue settings for a Buildkite BuildRunner.
type Config struct {
	// QueueName is the SQ queue this runner serves. Passed as SQ_QUEUE in
	// the build environment so the pipeline script can select queue-specific
	// test targets.
	QueueName string
}
