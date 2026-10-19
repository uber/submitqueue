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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
	validatormock "github.com/uber/submitqueue/submitqueue/extension/validator/mock"
	"go.uber.org/mock/gomock"
)

func TestNew(t *testing.T) {
	errA := errors.New("validation A failed")
	errB := errors.New("validation B failed")

	tests := []struct {
		name     string
		setup    func(v1, v2 *validatormock.MockValidator) []validator.Validator
		wantErrs []error
	}{
		{
			name:  "no validators returns no error",
			setup: func(v1, v2 *validatormock.MockValidator) []validator.Validator { return nil },
		},
		{
			name: "single passing validator",
			setup: func(v1, v2 *validatormock.MockValidator) []validator.Validator {
				v1.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(nil)
				return []validator.Validator{v1}
			},
		},
		{
			name: "single failing validator",
			setup: func(v1, v2 *validatormock.MockValidator) []validator.Validator {
				v1.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(errA)
				return []validator.Validator{v1}
			},
			wantErrs: []error{errA},
		},
		{
			name: "all pass",
			setup: func(v1, v2 *validatormock.MockValidator) []validator.Validator {
				v1.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(nil)
				v2.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(nil)
				return []validator.Validator{v1, v2}
			},
		},
		{
			name: "some fail",
			setup: func(v1, v2 *validatormock.MockValidator) []validator.Validator {
				v1.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(nil)
				v2.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(errA)
				return []validator.Validator{v1, v2}
			},
			wantErrs: []error{errA},
		},
		{
			name: "all fail",
			setup: func(v1, v2 *validatormock.MockValidator) []validator.Validator {
				v1.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(errA)
				v2.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(errB)
				return []validator.Validator{v1, v2}
			},
			wantErrs: []error{errA, errB},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			v1 := validatormock.NewMockValidator(ctrl)
			v2 := validatormock.NewMockValidator(ctrl)
			validators := tt.setup(v1, v2)

			v := New(validator.Config{QueueName: "test-queue"}, validators)

			err := v.Validate(context.Background(), entity.Request{})
			if len(tt.wantErrs) == 0 {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tt.wantErrs {
				assert.ErrorIs(t, err, want)
			}
		})
	}
}

func TestNew_RejectsWhenAnyChildRejects(t *testing.T) {
	rejection := fmt.Errorf("%w: missing approval", validator.ErrRejected)
	failure := errors.New("could not reach the policy service")

	tests := []struct {
		name         string
		children     []error
		wantRejected bool
	}{
		{name: "one child rejects", children: []error{nil, rejection}, wantRejected: true},
		{name: "a rejection beside a failure still rejects", children: []error{failure, rejection}, wantRejected: true},
		{name: "failures alone do not reject", children: []error{failure, nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var children []validator.Validator
			for _, err := range tt.children {
				child := validatormock.NewMockValidator(ctrl)
				child.EXPECT().Validate(gomock.Any(), gomock.Any()).Return(err)
				children = append(children, child)
			}

			err := New(validator.Config{QueueName: "test-queue"}, children).Validate(context.Background(), entity.Request{})

			require.Error(t, err)
			assert.Equal(t, tt.wantRejected, errors.Is(err, validator.ErrRejected))
		})
	}
}
