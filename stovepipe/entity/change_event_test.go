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

const testURI = "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/abcdef0123456789abcdef0123456789abcdef01"

func TestChangeEvent_Validate(t *testing.T) {
	t.Run("valid scheme-qualified URI", func(t *testing.T) {
		require.NoError(t, ChangeEvent{URI: testURI}.Validate())
	})

	t.Run("rejects empty URI", func(t *testing.T) {
		require.Error(t, ChangeEvent{}.Validate())
	})

	t.Run("rejects URI without scheme", func(t *testing.T) {
		require.Error(t, ChangeEvent{URI: "not-a-uri"}.Validate())
	})

	// Validate is VCS-agnostic: a non-git but scheme-qualified URI passes here;
	// rejecting an unsupported VCS is the resolver/wiring layer's job.
	t.Run("accepts non-git scheme", func(t *testing.T) {
		require.NoError(t, ChangeEvent{URI: "hg://example.com/repo/rev"}.Validate())
	})
}

func TestChangeEvent_Scheme(t *testing.T) {
	assert.Equal(t, "git", ChangeEvent{URI: testURI}.Scheme())
	assert.Equal(t, "", ChangeEvent{URI: "not-a-uri"}.Scheme())
}

func TestChangeEventFromBytes(t *testing.T) {
	original := ChangeEvent{URI: testURI}
	data, err := original.ToBytes()
	require.NoError(t, err)

	got, err := ChangeEventFromBytes(data)
	require.NoError(t, err)
	assert.Equal(t, original.URI, got.URI)
}
