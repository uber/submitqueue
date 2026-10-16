// Copyright (c) 2026 Uber Technologies, Inc.
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

package tango

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cacheTestSignature(queue, batchID string, targets int) TargetSignature {
	signature := TargetSignature{Queue: queue, BatchID: batchID, BaseRevision: "rev"}
	for i := 0; i < targets; i++ {
		signature.Targets = append(signature.Targets, SignatureTarget{ID: uint64(i), Distance: int32(i % 3)})
	}
	return signature
}

func TestSignatureCache_GetPut(t *testing.T) {
	stored := cacheTestSignature("q", "1", 4)

	tests := []struct {
		name      string
		put       []TargetSignature
		queue     string
		batchID   string
		wantFound bool
	}{
		{name: "miss on empty cache", queue: "q", batchID: "1"},
		{name: "hit after put", put: []TargetSignature{stored}, queue: "q", batchID: "1", wantFound: true},
		{name: "same batch ID in another queue is a miss", put: []TargetSignature{stored}, queue: "other", batchID: "1"},
		{name: "other batch ID is a miss", put: []TargetSignature{stored}, queue: "q", batchID: "2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newSignatureCache(1 << 20)
			for _, signature := range tt.put {
				cache.put(signature)
			}

			got, found := cache.get(tt.queue, tt.batchID)

			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				assert.Equal(t, stored, got)
			}
		})
	}
}

func TestSignatureCache_Eviction(t *testing.T) {
	a := cacheTestSignature("q", "a", 10)
	b := cacheTestSignature("q", "b", 10)
	c := cacheTestSignature("q", "c", 10)
	budgetForTwo := signatureCacheBytes(a) + signatureCacheBytes(b)

	tests := []struct {
		name      string
		run       func(cache *signatureCache)
		wantFound map[string]bool
	}{
		{
			name: "least recently put is evicted first",
			run: func(cache *signatureCache) {
				cache.put(a)
				cache.put(b)
				cache.put(c)
			},
			wantFound: map[string]bool{"a": false, "b": true, "c": true},
		},
		{
			name: "a read refreshes recency",
			run: func(cache *signatureCache) {
				cache.put(a)
				cache.put(b)
				cache.get("q", "a")
				cache.put(c)
			},
			wantFound: map[string]bool{"a": true, "b": false, "c": true},
		},
		{
			name: "putting an existing key does not evict a neighbour",
			run: func(cache *signatureCache) {
				cache.put(a)
				cache.put(b)
				for i := 0; i < 100; i++ {
					cache.put(a)
				}
			},
			wantFound: map[string]bool{"a": true, "b": true, "c": false},
		},
		{
			name: "a signature larger than the budget is not cached",
			run: func(cache *signatureCache) {
				cache.put(a)
				cache.put(cacheTestSignature("q", "c", 1000))
			},
			wantFound: map[string]bool{"a": true, "b": false, "c": false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newSignatureCache(budgetForTwo)

			tt.run(cache)

			for batchID, want := range tt.wantFound {
				_, found := cache.get("q", batchID)
				assert.Equal(t, want, found, "batch %s", batchID)
			}
		})
	}
}

func TestSignatureCache_ZeroBudgetCachesNothing(t *testing.T) {
	cache := newSignatureCache(0)

	cache.put(cacheTestSignature("q", "1", 0))

	_, found := cache.get("q", "1")
	assert.False(t, found)
}

func TestSignatureCache_ConcurrentUse(t *testing.T) {
	const workers = 8
	const perWorker = 200
	sample := cacheTestSignature("q", "0", 5)
	cache := newSignatureCache(signatureCacheBytes(sample) * 4)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				batchID := strconv.Itoa((w + i) % 10)
				cache.put(cacheTestSignature("q", batchID, 5))
				cache.get("q", batchID)
			}
		}(w)
	}
	wg.Wait()

	retained := 0
	for i := 0; i < 10; i++ {
		if _, found := cache.get("q", strconv.Itoa(i)); found {
			retained++
		}
	}
	require.LessOrEqual(t, retained, 4)
}
