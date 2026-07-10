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
	"errors"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

// compositeValidator runs all validators and joins their errors.
type compositeValidator struct {
	// validators is the set of validators to run.
	validators []validator.Validator
}

// New creates a Validator that evaluates all child validators and joins their errors.
func New(validators []validator.Validator) validator.Validator {
	return &compositeValidator{validators: validators}
}

func (c *compositeValidator) Validate(ctx context.Context, request entity.Request) error {
	var errs []error
	for _, v := range c.validators {
		if err := v.Validate(ctx, request); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
