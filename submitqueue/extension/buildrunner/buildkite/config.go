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

import "time"

// Config holds the per-queue settings for a Buildkite BuildRunner.
type Config struct {
	// QueueName is the SQ queue this runner serves. Passed as SQ_QUEUE in
	// the build environment so the pipeline script can select queue-specific
	// test targets.
	QueueName string

	// TriggerQueueSize is the buffer capacity of the async trigger channel.
	// Trigger returns an error when the channel is full (the build controller
	// will nack and retry). Defaults to 256.
	TriggerQueueSize int

	// CancelQueueSize is the buffer capacity of the async cancel channel.
	// Defaults to 256.
	CancelQueueSize int

	// SubmitTimeout is the per-call HTTP timeout used by the background worker
	// when contacting the Buildkite API. Defaults to 30s.
	SubmitTimeout time.Duration

	// MaxSubmitAttempts is the number of times the background worker tries a
	// Buildkite create/cancel call before giving up, to ride out transient
	// failures. Defaults to 5.
	MaxSubmitAttempts int

	// SubmitBackoff is the base delay between background-worker retries; the
	// delay grows linearly with the attempt number. Defaults to 1s.
	SubmitBackoff time.Duration
}
