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

// QueueConfig is an immutable, resolved configuration for a validation queue.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	Name string `json:"name" yaml:"name"`

	// Stages contains configuration grouped by pipeline stage.
	Stages Stages `json:"stages" yaml:"stages"`
}

// Validate checks queue identity and stage configuration constraints.
func (c QueueConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("queue name must not be empty")
	}

	if err := c.Stages.Validate(); err != nil {
		return fmt.Errorf("queue %q: %w", c.Name, err)
	}

	return nil
}
