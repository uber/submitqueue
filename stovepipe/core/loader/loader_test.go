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

package loader

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type widget struct {
	Name string
}

func TestByID(t *testing.T) {
	t.Run("success returns the loaded value unwrapped", func(t *testing.T) {
		get := func(_ context.Context, id string) (widget, error) {
			return widget{Name: id}, nil
		}

		got, err := ByID(context.Background(), "abc", get, "TestController", "widget")

		require.NoError(t, err)
		assert.Equal(t, widget{Name: "abc"}, got)
	})

	t.Run("failure returns the zero value and a wrapped error", func(t *testing.T) {
		sentinel := errors.New("boom")
		get := func(_ context.Context, _ string) (widget, error) {
			return widget{}, sentinel
		}

		got, err := ByID(context.Background(), "abc", get, "TestController", "widget")

		require.Error(t, err)
		assert.True(t, errors.Is(err, sentinel))
		assert.Equal(t, widget{}, got)
	})
}
