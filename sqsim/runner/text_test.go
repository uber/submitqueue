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

package runner

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTextObserverPrintsEachTransitionOnce(t *testing.T) {
	var output bytes.Buffer
	observer := NewTextObserver(&output)
	snapshot := Snapshot{
		StartedAt: time.Unix(0, 0),
		Now:       time.Unix(1, 0),
		Requests:  []Request{{Name: "l1", SQID: "sqsim/1", Status: "building"}},
	}
	observer.Observe(snapshot)
	observer.Observe(snapshot)
	assert.Contains(t, output.String(), "building")
	assert.Equal(t, 1, bytes.Count(output.Bytes(), []byte("building")))
}
