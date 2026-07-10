// Copyright (c) 2025 Uber Technologies, Inc.
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

// Package fake provides a validator.Validator that always passes and a
// validator.Factory that returns it. Intended for examples, tests, and default
// wiring when no custom validation is needed.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

type fakeValidator struct{}

// New returns a validator.Validator that always passes (returns nil).
func New() validator.Validator {
	return fakeValidator{}
}

func (fakeValidator) Validate(context.Context, entity.Request) error {
	return nil
}

type fakeFactory struct{}

// NewFactory returns a validator.Factory that always returns a passing validator.
func NewFactory() validator.Factory {
	return fakeFactory{}
}

func (fakeFactory) For(validator.Config) (validator.Validator, error) {
	return fakeValidator{}, nil
}
