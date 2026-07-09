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

package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ validator.Validator = New()
}

func TestNew_AlwaysPasses(t *testing.T) {
	v := New()
	assert.NoError(t, v.Validate(context.Background(), entity.Request{}))
}

func TestNewFactory_ImplementsInterface(t *testing.T) {
	var _ validator.Factory = NewFactory()
}

func TestNewFactory_ReturnsPassingValidator(t *testing.T) {
	f := NewFactory()
	v, err := f.For(validator.Config{QueueName: "test-queue"})
	assert.NoError(t, err)
	assert.NoError(t, v.Validate(context.Background(), entity.Request{}))
}
