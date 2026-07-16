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

// Package model interprets immutable SQSim profiles for external-system adapters.
package model

import (
	"fmt"

	"github.com/uber/submitqueue/sqsim/entity"
)

// Runtime owns invocation cursors for one adapter process.
type Runtime struct {
	profile Profile
	lands   map[string]*landRuntime
	clock   Clock
}

type landRuntime struct {
	land                entity.Land
	buildTriggers       *Sequence[entity.Invocation[entity.BuildTriggerOutcome]]
	mergeConflictChecks *Sequence[entity.Invocation[entity.MergeConflictCheckOutcome]]
	merges              *Sequence[entity.Invocation[entity.MergeOutcome]]
}

// NewRuntime validates a profile and initializes its invocation cursors.
func NewRuntime(profile Profile, clock Clock) (*Runtime, error) {
	if err := validateProfile(profile); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, fmt.Errorf("clock is required")
	}
	runtime := &Runtime{
		profile: cloneProfile(profile),
		lands:   make(map[string]*landRuntime, len(profile.Scenario.Lands)),
		clock:   clock,
	}
	for _, land := range runtime.profile.Scenario.Lands {
		runtime.lands[land.Name] = &landRuntime{
			land:                cloneLand(land),
			buildTriggers:       NewSequence(land.Behavior.BuildRunner.Triggers),
			mergeConflictChecks: NewSequence(land.Behavior.MergeConflictCheck.Invocations),
			merges:              NewSequence(land.Behavior.Merge.Invocations),
		}
	}
	return runtime, nil
}

// Clock returns the runtime clock.
func (r *Runtime) Clock() Clock {
	return r.clock
}

// Resolve returns the immutable Land selected by a synthetic change URI.
func (r *Runtime) Resolve(changeURI string) (entity.Land, error) {
	land, err := r.resolve(changeURI)
	if err != nil {
		return entity.Land{}, err
	}
	return cloneLand(land.land), nil
}

// NextBuildTrigger consumes the next Build Runner Trigger invocation.
func (r *Runtime) NextBuildTrigger(changeURI string) (entity.Invocation[entity.BuildTriggerOutcome], error) {
	land, err := r.resolve(changeURI)
	if err != nil {
		return entity.Invocation[entity.BuildTriggerOutcome]{}, err
	}
	return land.buildTriggers.Next()
}

// NextMergeConflictCheck consumes the next merge-conflict-check invocation.
func (r *Runtime) NextMergeConflictCheck(changeURI string) (entity.Invocation[entity.MergeConflictCheckOutcome], error) {
	land, err := r.resolve(changeURI)
	if err != nil {
		return entity.Invocation[entity.MergeConflictCheckOutcome]{}, err
	}
	return land.mergeConflictChecks.Next()
}

// NextMerge consumes the next committing Merge invocation.
func (r *Runtime) NextMerge(changeURI string) (entity.Invocation[entity.MergeOutcome], error) {
	land, err := r.resolve(changeURI)
	if err != nil {
		return entity.Invocation[entity.MergeOutcome]{}, err
	}
	return land.merges.Next()
}

func (r *Runtime) resolve(changeURI string) (*landRuntime, error) {
	ref, err := ParseChangeURI(changeURI)
	if err != nil {
		return nil, err
	}
	if ref.Scenario != r.profile.Name {
		return nil, fmt.Errorf("change URI scenario %q does not match profile %q", ref.Scenario, r.profile.Name)
	}
	land, ok := r.lands[ref.Land]
	if !ok {
		return nil, fmt.Errorf("land %q is not present in scenario %q", ref.Land, ref.Scenario)
	}
	return land, nil
}
