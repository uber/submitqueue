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

package mergechecker

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity"
)

// stubChecker is a test stub that returns a fixed result.
type stubChecker struct {
	result Result
	err    error
}

func (s *stubChecker) Check(_ context.Context, _ string, _ entity.Change) (Result, error) {
	return s.result, s.err
}

func TestMultiChecker_RoutesToCorrectChecker(t *testing.T) {
	githubChecker := &stubChecker{result: Result{Mergeable: true}}
	gheChecker := &stubChecker{result: Result{Mergeable: false}}

	mc := NewMultiChecker(map[string]MergeChecker{
		"github": githubChecker,
		"ghe":    gheChecker,
	})

	// Route to github checker
	result, err := mc.Check(context.Background(), "test-queue", entity.Change{URIs: []string{"github://uber/repo/1/abc123"}})
	require.NoError(t, err)
	assert.True(t, result.Mergeable)

	// Route to ghe checker
	result, err = mc.Check(context.Background(), "test-queue", entity.Change{URIs: []string{"ghe://uber/repo/1/abc123"}})
	require.NoError(t, err)
	assert.False(t, result.Mergeable)
}

func TestMultiChecker_UnknownScheme(t *testing.T) {
	mc := NewMultiChecker(map[string]MergeChecker{
		"github": &stubChecker{result: Result{Mergeable: true}},
	})

	_, err := mc.Check(context.Background(), "test-queue", entity.Change{URIs: []string{"unknown://uber/repo/1/abc123"}})
	require.Error(t, err)
}

func TestMultiChecker_PropagatesError(t *testing.T) {
	mc := NewMultiChecker(map[string]MergeChecker{
		"github": &stubChecker{err: fmt.Errorf("api failure")},
	})

	_, err := mc.Check(context.Background(), "test-queue", entity.Change{URIs: []string{"github://uber/repo/1/abc123"}})
	require.Error(t, err)
}

func TestMultiChecker_EmptyURIs(t *testing.T) {
	mc := NewMultiChecker(map[string]MergeChecker{
		"github": &stubChecker{result: Result{Mergeable: true}},
	})

	_, err := mc.Check(context.Background(), "test-queue", entity.Change{URIs: []string{}})
	require.Error(t, err)
}
