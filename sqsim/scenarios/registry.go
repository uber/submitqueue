// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package scenarios contains the concrete SQSim scenario catalog.
package scenarios

import (
	"fmt"
	"sort"

	"github.com/uber/submitqueue/sqsim"
)

// Builder constructs and validates one scenario.
type Builder func() (sqsim.Scenario, error)

var registry = map[string]Builder{
	"build-failure":                 BuildFailure,
	"build-status-recovery":         BuildStatusRecovery,
	"build-trigger-recovery":        BuildTriggerRecovery,
	"happy-path":                    HappyPath,
	"load-1000":                     Load1000,
	"merge-conflict":                MergeConflict,
	"merge-conflict-check-recovery": MergeConflictCheckRecovery,
	"merge-response-lost":           MergeResponseLost,
	"mixed-concurrent":              MixedConcurrent,
}

// Names returns registered scenario names in lexical order.
func Names() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Build constructs the registered scenario with the given name.
func Build(name string) (sqsim.Scenario, error) {
	builder, ok := registry[name]
	if !ok {
		return sqsim.Scenario{}, fmt.Errorf("unknown scenario %q", name)
	}
	scenario, err := builder()
	if err != nil {
		return sqsim.Scenario{}, fmt.Errorf("build scenario %q: %w", name, err)
	}
	return scenario, nil
}

// ValidateAll builds every registered scenario.
func ValidateAll() error {
	for _, name := range Names() {
		if _, err := Build(name); err != nil {
			return err
		}
	}
	return nil
}
