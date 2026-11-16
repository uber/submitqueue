// Copyright (c) 2026 Uber Technologies, Inc.
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

package queueconfig

import "fmt"

// Process contains configuration for the process stage.
type Process struct {
	// MaxConcurrent is the positive concurrent validation limit.
	MaxConcurrent int32 `json:"max_concurrent" yaml:"max_concurrent"`

	// GateWaitDelayMs is the positive wait for admission, in milliseconds.
	GateWaitDelayMs int64 `json:"gate_wait_delay_ms" yaml:"gate_wait_delay_ms"`
}

// Validate checks the process stage's configuration constraints.
func (c Process) Validate() error {
	if c.MaxConcurrent <= 0 {
		return fmt.Errorf("MaxConcurrent must be positive, got %d", c.MaxConcurrent)
	}

	if c.GateWaitDelayMs <= 0 {
		return fmt.Errorf("GateWaitDelayMs must be positive, got %d", c.GateWaitDelayMs)
	}

	return nil
}
