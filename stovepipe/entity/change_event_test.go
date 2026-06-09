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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testURI = "git://uber/monorepo/main/abcdef0123456789abcdef0123456789abcdef01"

func TestChangeEvent_Validate(t *testing.T) {
	t.Run("valid URI", func(t *testing.T) {
		event := ChangeEvent{URI: testURI}
		require.NoError(t, event.Validate())
	})

	t.Run("rejects empty URI", func(t *testing.T) {
		require.Error(t, ChangeEvent{}.Validate())
	})

	t.Run("rejects non-git URI", func(t *testing.T) {
		event := ChangeEvent{URI: "github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"}
		require.Error(t, event.Validate())
	})
}

func TestChangeEventFromBytes(t *testing.T) {
	original := ChangeEvent{URI: testURI}
	data, err := original.ToBytes()
	require.NoError(t, err)

	got, err := ChangeEventFromBytes(data)
	require.NoError(t, err)
	assert.Equal(t, original.URI, got.URI)
}
