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

// BuildAnalysis contains configuration for the build-analysis stage.
type BuildAnalysis struct {
	// PollDelayMs is the positive delay before the next build-analysis poll, in milliseconds.
	PollDelayMs int64 `json:"poll_delay_ms" yaml:"poll_delay_ms"`
}

// Validate checks the build-analysis stage's configuration constraints.
func (c BuildAnalysis) Validate() error {
	if c.PollDelayMs <= 0 {
		return fmt.Errorf("PollDelayMs must be positive, got %d", c.PollDelayMs)
	}

	return nil
}
