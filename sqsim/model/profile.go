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

package model

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/uber/submitqueue/sqsim"
	"github.com/uber/submitqueue/sqsim/entity"
)

// Profile is the immutable scenario input shared with SQSim adapters.
type Profile struct {
	// Name is the public scenario registry name.
	Name string `json:"name"`
	// Scenario is the workload and modeled external behavior.
	Scenario entity.Scenario `json:"scenario"`
}

// Compile validates a named scenario and returns its runtime profile.
func Compile(name string, scenario sqsim.Scenario) (Profile, error) {
	if err := validateName("scenario", name); err != nil {
		return Profile{}, err
	}
	if err := sqsim.Validate(scenario); err != nil {
		return Profile{}, fmt.Errorf("validate scenario: %w", err)
	}
	return cloneProfile(Profile{Name: name, Scenario: scenario}), nil
}

// Write writes a profile as JSON to path.
func Write(path string, profile Profile) error {
	if path == "" {
		return fmt.Errorf("profile path is required")
	}
	if err := validateProfile(profile); err != nil {
		return err
	}
	data, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal profile: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write profile: %w", err)
	}
	return nil
}

// Load reads and validates a profile from path.
func Load(path string) (Profile, error) {
	if path == "" {
		return Profile{}, fmt.Errorf("profile path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, fmt.Errorf("read profile: %w", err)
	}
	var profile Profile
	if err := json.Unmarshal(data, &profile); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	if err := validateProfile(profile); err != nil {
		return Profile{}, err
	}
	return cloneProfile(profile), nil
}

func validateProfile(profile Profile) error {
	if err := validateName("scenario", profile.Name); err != nil {
		return err
	}
	if err := sqsim.Validate(profile.Scenario); err != nil {
		return fmt.Errorf("validate scenario: %w", err)
	}
	return nil
}

func cloneProfile(profile Profile) Profile {
	lands := make([]entity.Land, len(profile.Scenario.Lands))
	for i, land := range profile.Scenario.Lands {
		lands[i] = cloneLand(land)
	}
	profile.Scenario.Lands = lands
	return profile
}

func cloneLand(land entity.Land) entity.Land {
	triggers := make([]entity.Invocation[entity.BuildTriggerOutcome], len(land.Behavior.BuildRunner.Triggers))
	for i, trigger := range land.Behavior.BuildRunner.Triggers {
		trigger.Outcome.Build.Timeline = append([]entity.BuildStatusPoint(nil), trigger.Outcome.Build.Timeline...)
		trigger.Outcome.Build.StatusFaults = append([]entity.FaultOnCall(nil), trigger.Outcome.Build.StatusFaults...)
		triggers[i] = trigger
	}
	land.Behavior.BuildRunner.Triggers = triggers
	land.Behavior.MergeConflictCheck.Invocations = append(
		[]entity.Invocation[entity.MergeConflictCheckOutcome](nil),
		land.Behavior.MergeConflictCheck.Invocations...,
	)
	land.Behavior.Merge.Invocations = append(
		[]entity.Invocation[entity.MergeOutcome](nil),
		land.Behavior.Merge.Invocations...,
	)
	return land
}
