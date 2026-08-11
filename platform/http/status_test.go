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

package http

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStatusError(t *testing.T) {
	tests := []struct {
		name string
		code int
		body []byte
		want string
	}{
		{
			name: "with body",
			code: 502,
			body: []byte("proxy forward failed: relay-connection-failed"),
			want: "unexpected status 502: proxy forward failed: relay-connection-failed",
		},
		{name: "nil body", code: 500, want: "unexpected status 500"},
		{name: "empty body", code: 404, body: []byte{}, want: "unexpected status 404"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewStatusError(tt.code, tt.body)
			require.NotNil(t, err)
			assert.Equal(t, tt.code, err.StatusCode)
			assert.Equal(t, tt.want, err.Error())
		})
	}
}

func TestStatusError_SurvivesWrapping(t *testing.T) {
	// Callers wrap with the operation that failed. The code has to stay
	// reachable through the chain, otherwise the classifier cannot read it.
	inner := NewStatusError(503, []byte("unavailable"))
	wrapped := fmt.Errorf("get build org/pipeline/builds/1: %w", inner)

	var se *StatusError
	require.True(t, errors.As(wrapped, &se))
	assert.Equal(t, 503, se.StatusCode)
	assert.Equal(t, "get build org/pipeline/builds/1: unexpected status 503: unavailable", wrapped.Error())
}
