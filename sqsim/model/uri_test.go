// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeURIParseRoundTrip(t *testing.T) {
	uri, err := ChangeURI("happy-path", "l1")
	require.NoError(t, err)
	assert.Equal(t, "sqsim://local/happy-path/l1", uri)

	ref, err := ParseChangeURI(uri)
	require.NoError(t, err)
	assert.Equal(t, Reference{Scenario: "happy-path", Land: "l1"}, ref)
}

func TestParseChangeURIRejectsUnexpectedShape(t *testing.T) {
	tests := []string{
		"https://local/happy-path/l1",
		"sqsim://other/happy-path/l1",
		"sqsim://local/happy-path",
		"sqsim://local/happy-path/l1?attempt=2",
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			_, err := ParseChangeURI(raw)
			require.Error(t, err)
		})
	}
}
