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
	"fmt"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/sqsim/entity"
)

// FaultError is an external-system failure selected by a scenario.
type FaultError struct {
	// Kind identifies retryability.
	Kind entity.FaultKind
	// Phase identifies whether the modeled side effect happened.
	Phase entity.FaultPhase
}

// Error describes the modeled failure.
func (e *FaultError) Error() string {
	return fmt.Sprintf("sqsim modeled %s fault %s", e.Kind, e.Phase)
}

// ErrorForFault returns the typed error selected by fault.
func ErrorForFault(fault entity.Fault) error {
	if fault.Kind == entity.FaultNone {
		return nil
	}
	return &FaultError{Kind: fault.Kind, Phase: fault.Phase}
}

// Classifier classifies modeled external-system errors.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

func (classifier) Classify(err error) errs.Verdict {
	fault, ok := err.(*FaultError)
	if !ok {
		return errs.Unknown
	}
	switch fault.Kind {
	case entity.FaultRetryable:
		return errs.InfraDependencyRetryable
	case entity.FaultNonRetryable:
		return errs.InfraDependency
	default:
		return errs.Unknown
	}
}
