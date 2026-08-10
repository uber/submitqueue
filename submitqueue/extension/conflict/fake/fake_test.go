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
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/none"
)

// testCfg is the per-queue identity used by every case in this file.
var testCfg = conflict.Config{QueueName: "test-queue"}

func TestNew_ImplementsInterface(t *testing.T) {
	var _ conflict.Analyzer = New(testCfg, none.New(testCfg), nil)
}

func TestAnalyze_DelegatesWhenNoFailOn(t *testing.T) {
	// Delegate to "all": one conflict per in-flight batch. nil failOn -> passthrough.
	a := New(testCfg, all.New(testCfg), nil)
	got, err := a.Analyze(context.Background(),
		entity.Batch{ID: "q/batch/1"},
		[]entity.Batch{{ID: "q/batch/2"}, {ID: "q/batch/3"}})
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestAnalyze_DelegatesWhenFailOnFalse(t *testing.T) {
	a := New(testCfg, none.New(testCfg), func(entity.Batch, []entity.Batch) bool { return false })
	got, err := a.Analyze(context.Background(), entity.Batch{ID: "q/batch/1"}, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestAnalyze_FailAlways(t *testing.T) {
	a := New(testCfg, none.New(testCfg), FailAlways)
	_, err := a.Analyze(context.Background(), entity.Batch{ID: "q/batch/1"}, nil)
	require.Error(t, err)
}

func TestAnalyze_FailOnPredicate(t *testing.T) {
	// Inject an error only for a specific batch ID.
	a := New(testCfg, none.New(testCfg), func(b entity.Batch, _ []entity.Batch) bool {
		return b.ID == "q/batch/bad"
	})

	_, err := a.Analyze(context.Background(), entity.Batch{ID: "q/batch/bad"}, nil)
	require.Error(t, err)

	got, err := a.Analyze(context.Background(), entity.Batch{ID: "q/batch/ok"}, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
