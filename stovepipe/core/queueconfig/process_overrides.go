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

import "time"

// ProcessOverrides contains optional configuration for the process stage.
// Nil fields inherit their previous value; explicit values are retained for validation.
type ProcessOverrides struct {
	// MaxConcurrent overrides the concurrent validation limit.
	MaxConcurrent *int32 `yaml:"maxConcurrent"`

	// GateWaitDelay overrides the wait for admission.
	GateWaitDelay *time.Duration `yaml:"gateWaitDelay"`
}

// Apply copies base and replaces explicitly supplied settings.
// Durations are truncated to milliseconds. Validate the final resolved configuration.
func (o ProcessOverrides) Apply(base Process) Process {
	if o.MaxConcurrent != nil {
		base.MaxConcurrent = *o.MaxConcurrent
	}

	if o.GateWaitDelay != nil {
		base.GateWaitDelayMs = o.GateWaitDelay.Milliseconds()
	}

	return base
}

func defaultProcess() Process {
	return Process{
		MaxConcurrent:   1,
		GateWaitDelayMs: (5 * time.Second).Milliseconds(),
	}
}
