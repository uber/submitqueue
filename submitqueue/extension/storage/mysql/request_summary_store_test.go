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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestMergeSummary_GuardsWinner(t *testing.T) {
	existing := rowFromLog(entity.RequestLog{
		RequestID:   "q/1",
		Queue:       "q",
		ChangeURIs:  []string{"change"},
		TimestampMs: 200,
		Status:      entity.RequestStatusBuilding,
		Metadata:    map[string]string{},
	})

	older := mergeSummary(existing, entity.RequestLog{
		RequestID:   "q/1",
		Queue:       "q",
		TimestampMs: 100,
		Status:      entity.RequestStatusAccepted,
		Metadata:    map[string]string{"ignored": "true"},
	})
	assert.Equal(t, entity.RequestStatusBuilding, older.summary.Status)
	assert.Equal(t, int64(100), older.summary.StartedAtMs)
	assert.Equal(t, int64(200), older.summary.UpdatedAtMs)

	terminal := mergeSummary(older, entity.RequestLog{
		RequestID:      "q/1",
		Queue:          "q",
		TimestampMs:    150,
		Status:         entity.RequestStatusLanded,
		RequestVersion: 1,
		LastError:      "",
		Metadata:       map[string]string{"terminal": "true"},
	})
	assert.Equal(t, entity.RequestStatusLanded, terminal.summary.Status)
	assert.Equal(t, int32(1), terminal.requestVersion)
	assert.True(t, terminal.winnerTerminalVersion)
	assert.True(t, terminal.summary.Terminal)
	assert.Equal(t, int64(150), terminal.summary.CompletedAtMs)

	laterNonTerminal := mergeSummary(terminal, entity.RequestLog{
		RequestID:   "q/1",
		Queue:       "q",
		TimestampMs: 300,
		Status:      entity.RequestStatusBuilding,
		Metadata:    map[string]string{"ignored": "true"},
	})
	assert.Equal(t, entity.RequestStatusLanded, laterNonTerminal.summary.Status)
	assert.Equal(t, int64(150), laterNonTerminal.summary.UpdatedAtMs)

	higherTerminal := mergeSummary(laterNonTerminal, entity.RequestLog{
		RequestID:      "q/1",
		Queue:          "q",
		TimestampMs:    250,
		Status:         entity.RequestStatusError,
		RequestVersion: 2,
		LastError:      "boom",
		Metadata:       map[string]string{"winner": "true"},
	})
	assert.Equal(t, entity.RequestStatusError, higherTerminal.summary.Status)
	assert.Equal(t, int32(2), higherTerminal.requestVersion)
	assert.Equal(t, int64(250), higherTerminal.summary.UpdatedAtMs)
	assert.Equal(t, "boom", higherTerminal.summary.LastError)
	assert.Equal(t, map[string]string{"winner": "true"}, higherTerminal.summary.Metadata)
	assert.Equal(t, []string{"change"}, higherTerminal.summary.ChangeURIs)
}
