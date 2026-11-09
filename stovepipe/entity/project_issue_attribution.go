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

package entity

import "fmt"

// ProjectIssueAttribution is one immutable issue/project pair within an attribution result.
type ProjectIssueAttribution struct {
	// TaskID identifies the validation task and must be nonempty.
	TaskID string
	// UnitID identifies the failed unit within the task and must be nonempty.
	UnitID string
	// Project is an affected project name and must be nonempty.
	Project string
}

// Validate verifies the identity of a unit-level attribution.
func (a ProjectIssueAttribution) Validate() error {
	if a.TaskID == "" {
		return fmt.Errorf("project issue attribution task ID must not be empty")
	}
	if a.UnitID == "" {
		return fmt.Errorf("project issue attribution unit ID must not be empty")
	}
	if a.Project == "" {
		return fmt.Errorf("project issue attribution project must not be empty")
	}
	return nil
}
