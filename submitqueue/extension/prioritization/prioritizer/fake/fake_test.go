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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestPrioritize_DefaultAdmitsNothing(t *testing.T) {
	got, err := New().Prioritize(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestPrioritize_ReturnsSeededAdmitted(t *testing.T) {
	want := []entity.Build{{ID: "a"}, {ID: "b"}}
	got, err := New().SetAdmitted(want...).Prioritize(context.Background())
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestPrioritize_FailWith(t *testing.T) {
	sentinel := errors.New("boom")
	_, err := New().FailWith(sentinel).Prioritize(context.Background())
	require.ErrorIs(t, err, sentinel)
}
