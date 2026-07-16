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

package sqsim

import (
	"fmt"
	"regexp"

	"github.com/uber/submitqueue/sqsim/entity"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Validate checks a complete immutable Scenario.
func Validate(scenario Scenario) error {
	if scenario.TimeoutMs <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if len(scenario.Lands) == 0 {
		return fmt.Errorf("at least one land is required")
	}

	names := make(map[string]struct{}, len(scenario.Lands))
	for i, land := range scenario.Lands {
		if err := validateLand(land, scenario.TimeoutMs); err != nil {
			return fmt.Errorf("land %d: %w", i, err)
		}
		if _, ok := names[land.Name]; ok {
			return fmt.Errorf("land %d: duplicate name %q", i, land.Name)
		}
		names[land.Name] = struct{}{}
	}
	return nil
}

func validateLand(land entity.Land, timeoutMs int64) error {
	if !namePattern.MatchString(land.Name) {
		return fmt.Errorf("name %q must be one URI-safe path segment", land.Name)
	}
	if land.Queue == "" {
		return fmt.Errorf("queue is required")
	}
	if land.SubmitAfterMs < 0 {
		return fmt.Errorf("submit delay must be non-negative")
	}
	if land.SubmitAfterMs >= timeoutMs {
		return fmt.Errorf("submit delay must be less than scenario timeout")
	}
	if err := validateExpectedStatus(land.Expectation.Status); err != nil {
		return err
	}
	return validateBehavior(land.Behavior)
}

func validateExpectedStatus(status entity.ExpectedRequestStatus) error {
	switch status {
	case entity.RequestLanded, entity.RequestError, entity.RequestCancelled:
		return nil
	default:
		return fmt.Errorf("expected terminal request status is required")
	}
}

func validateBehavior(behavior entity.Behavior) error {
	if err := validateBuildRunner(behavior.BuildRunner); err != nil {
		return fmt.Errorf("build runner: %w", err)
	}
	if err := validateMergeConflictCheck(behavior.MergeConflictCheck); err != nil {
		return fmt.Errorf("merge conflict check: %w", err)
	}
	if err := validateMerge(behavior.Merge); err != nil {
		return fmt.Errorf("merge: %w", err)
	}
	return nil
}

func validateBuildRunner(behavior entity.BuildRunnerBehavior) error {
	if len(behavior.Triggers) == 0 {
		return fmt.Errorf("at least one trigger invocation is required")
	}
	for i, trigger := range behavior.Triggers {
		if trigger.DelayMs < 0 {
			return fmt.Errorf("trigger %d: delay must be non-negative", i)
		}
		if err := validateInvocationFault(trigger.Fault, true); err != nil {
			return fmt.Errorf("trigger %d: %w", i, err)
		}
		if trigger.Fault.Phase == entity.FaultBeforeSideEffect {
			continue
		}
		if err := validateBuildExecution(trigger.Outcome.Build); err != nil {
			return fmt.Errorf("trigger %d: %w", i, err)
		}
	}
	return nil
}

func validateBuildExecution(build entity.BuildExecution) error {
	if len(build.Timeline) == 0 {
		return fmt.Errorf("build timeline is required")
	}
	if build.Timeline[0].AfterMs != 0 {
		return fmt.Errorf("build timeline must begin at zero")
	}
	previous := int64(-1)
	for i, point := range build.Timeline {
		if point.AfterMs < 0 || point.AfterMs <= previous {
			return fmt.Errorf("timeline point %d must have a strictly increasing non-negative offset", i)
		}
		if point.Status == "" {
			return fmt.Errorf("timeline point %d status is required", i)
		}
		previous = point.AfterMs
	}
	if !build.Timeline[len(build.Timeline)-1].Status.IsTerminal() {
		return fmt.Errorf("build timeline must end in a terminal status")
	}

	calls := make(map[int]struct{}, len(build.StatusFaults))
	for i, fault := range build.StatusFaults {
		if fault.Call <= 0 {
			return fmt.Errorf("status fault %d call must be positive", i)
		}
		if _, ok := calls[fault.Call]; ok {
			return fmt.Errorf("status fault %d duplicates call %d", i, fault.Call)
		}
		calls[fault.Call] = struct{}{}
		if err := validateInvocationFault(fault.Fault, false); err != nil {
			return fmt.Errorf("status fault %d: %w", i, err)
		}
	}
	return nil
}

func validateMergeConflictCheck(behavior entity.MergeConflictCheckBehavior) error {
	if len(behavior.Invocations) == 0 {
		return fmt.Errorf("at least one invocation is required")
	}
	for i, invocation := range behavior.Invocations {
		if invocation.DelayMs < 0 {
			return fmt.Errorf("invocation %d: delay must be non-negative", i)
		}
		if err := validateInvocationFault(invocation.Fault, false); err != nil {
			return fmt.Errorf("invocation %d: %w", i, err)
		}
		if invocation.Fault.Phase == entity.FaultAfterSideEffect {
			return fmt.Errorf("invocation %d: after-side-effect faults are not valid for a dry-run check", i)
		}
		if invocation.Fault.Kind == entity.FaultNone {
			switch invocation.Outcome {
			case entity.Mergeable, entity.MergeConflict:
			default:
				return fmt.Errorf("invocation %d outcome is required", i)
			}
		}
	}
	return nil
}

func validateMerge(behavior entity.MergeBehavior) error {
	if len(behavior.Invocations) == 0 {
		return fmt.Errorf("at least one invocation is required")
	}
	for i, invocation := range behavior.Invocations {
		if invocation.DelayMs < 0 {
			return fmt.Errorf("invocation %d: delay must be non-negative", i)
		}
		if err := validateInvocationFault(invocation.Fault, true); err != nil {
			return fmt.Errorf("invocation %d: %w", i, err)
		}
		if invocation.Fault.Phase != entity.FaultBeforeSideEffect && invocation.Outcome.Result != entity.MergeSucceeded {
			return fmt.Errorf("invocation %d outcome is required", i)
		}
	}
	return nil
}

func validateInvocationFault(fault entity.Fault, allowAfterSideEffect bool) error {
	if fault.Kind == entity.FaultNone {
		if fault.Phase != "" {
			return fmt.Errorf("fault phase requires a fault kind")
		}
		return nil
	}
	switch fault.Kind {
	case entity.FaultRetryable, entity.FaultNonRetryable:
	default:
		return fmt.Errorf("unknown fault kind %q", fault.Kind)
	}
	switch fault.Phase {
	case entity.FaultBeforeSideEffect:
		return nil
	case entity.FaultAfterSideEffect:
		if allowAfterSideEffect {
			return nil
		}
		return fmt.Errorf("after-side-effect fault is not supported")
	default:
		return fmt.Errorf("fault phase is required")
	}
}
