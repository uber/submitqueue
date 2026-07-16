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
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequenceConsumesValuesOnceAcrossConcurrentCallers(t *testing.T) {
	sequence := NewSequence([]int{1, 2, 3})
	results := make(chan int, 3)
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := sequence.Next()
			require.NoError(t, err)
			results <- value
		}()
	}
	wg.Wait()
	close(results)

	var got []int
	for result := range results {
		got = append(got, result)
	}
	sort.Ints(got)
	assert.Equal(t, []int{1, 2, 3}, got)
	assert.Equal(t, 3, sequence.Consumed())

	_, err := sequence.Next()
	require.Error(t, err)
}
