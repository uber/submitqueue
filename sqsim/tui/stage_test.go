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

package tui

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/sqsim/runner"
)

func TestStageCellsUseHistory(t *testing.T) {
	cells := stageCells(runner.Request{
		Status: "building",
		History: []runner.HistoryEvent{
			{Status: "validating"},
			{Status: "validated"},
			{Status: "batched"},
			{Status: "scored"},
			{Status: "speculating"},
			{Status: "building"},
		},
	}, "/")
	require.Len(t, cells, 6)
	assert.Equal(t, []string{"ok", "ok", "ok", "ok", "/", "."}, cells[:])
}
