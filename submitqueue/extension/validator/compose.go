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

package validator

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Compose returns a Validator that runs all provided validators and joins their errors.
func Compose(validators []Validator) Validator {
	return compositeValidator(validators)
}

type compositeValidator []Validator

func (c compositeValidator) Validate(ctx context.Context, request entity.Request, changes []entity.ChangeInfo) error {
	var errs []error
	for _, v := range c {
		if err := v.Validate(ctx, request, changes); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
