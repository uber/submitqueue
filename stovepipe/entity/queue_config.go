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

package entity

// QueueConfig holds deployment configuration for a Stovepipe validation queue.
// Mutable runtime state (latest head, in-flight count) lives on the Queue row;
// knobs such as max_concurrent are resolved here at gate-check time. Immutable
// after load.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	// Referenced by Request.Queue.
	Name string `json:"name" yaml:"name"`
	// MaxConcurrent is the cap on concurrent in-flight validations for the queue.
	MaxConcurrent int32 `json:"max_concurrent" yaml:"max_concurrent"`
	// GateWaitDelayMs is the PublishAfter delay when the latest head waits for a slot.
	GateWaitDelayMs int64 `json:"gate_wait_delay_ms" yaml:"gate_wait_delay_ms"`
}
