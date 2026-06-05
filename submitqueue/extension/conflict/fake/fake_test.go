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

func TestNew_ImplementsInterface(t *testing.T) {
	var _ conflict.Analyzer = New(none.New())
}

func batch(id string, uris ...string) entity.BatchChanges {
	changes := make([]entity.ChangeInfo, 0, len(uris))
	for _, u := range uris {
		changes = append(changes, entity.ChangeInfo{URI: u})
	}
	return entity.BatchChanges{BatchID: id, Queue: "q", Changes: changes}
}

func TestAnalyze_DelegatesWhenUnmarked(t *testing.T) {
	// Delegate to "all": one conflict per in-flight batch.
	a := New(all.New())
	got, err := a.Analyze(context.Background(),
		batch("q/batch/1", "github://o/r/pull/1/a"),
		[]entity.BatchChanges{batch("q/batch/2"), batch("q/batch/3")})
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestAnalyze_ErrorMarker(t *testing.T) {
	a := New(all.New())
	_, err := a.Analyze(context.Background(),
		batch("q/batch/1", "github://o/r/pull/1/a?sq-fake=conflict-error"),
		[]entity.BatchChanges{batch("q/batch/2")})
	require.Error(t, err)
}

func TestAnalyze_MarkerOnSecondURI(t *testing.T) {
	a := New(none.New())
	_, err := a.Analyze(context.Background(),
		batch("q/batch/1", "github://o/r/pull/1/a", "github://o/r/pull/2/b?sq-fake=conflict-error"),
		nil)
	require.Error(t, err)
}
