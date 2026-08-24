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

package composite

import (
	"context"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

// compositeValidator runs all validators and groups their errors.
type compositeValidator struct {
	// cfg is the per-queue identity this validator was built for.
	cfg validator.Config
	// validators is the set of validators to run.
	validators []validator.Validator
}

// New creates a Validator bound to the queue named in cfg that evaluates all
// child validators and groups their errors.
func New(cfg validator.Config, validators []validator.Validator) validator.Validator {
	return &compositeValidator{cfg: cfg, validators: validators}
}

func (c *compositeValidator) Validate(ctx context.Context, request entity.Request) error {
	var failures []error
	for _, v := range c.validators {
		if err := v.Validate(ctx, request); err != nil {
			failures = append(failures, err)
		}
	}
	// Group, not errors.Join: the children ran independently, so each failure
	// must be classified on its own rather than the first one speaking for all.
	return errs.Group(failures...)
}
