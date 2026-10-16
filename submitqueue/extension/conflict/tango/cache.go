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
	"container/list"
	"sync"
)

// The budget counts memory: a SignatureTarget is 16 bytes (uint64 plus int32,
// padded) and the backing array is charged by capacity, and the fixed overhead
// covers the entry, key and map bookkeeping.
const (
	signatureTargetBytes   = 16
	signatureEntryOverhead = 256
)

type signatureCacheKey struct {
	queue   string
	batchID string
}

type signatureCacheEntry struct {
	key       signatureCacheKey
	signature TargetSignature
	bytes     int64
}

// signatureCache is a byte-bounded LRU of target signatures keyed by queue and
// batch ID, safe for concurrent use. Signatures are immutable, so a cached
// value is shared with callers: neither side may modify Targets.
type signatureCache struct {
	mu        sync.Mutex
	maxBytes  int64
	usedBytes int64
	byRecency *list.List
	entries   map[signatureCacheKey]*list.Element
}

func newSignatureCache(maxBytes int64) *signatureCache {
	return &signatureCache{
		maxBytes:  maxBytes,
		byRecency: list.New(),
		entries:   make(map[signatureCacheKey]*list.Element),
	}
}

func signatureCacheBytes(signature TargetSignature) int64 {
	return signatureEntryOverhead +
		int64(len(signature.Queue)+len(signature.BatchID)+len(signature.BaseRevision)) +
		int64(cap(signature.Targets))*signatureTargetBytes
}

// get returns the cached signature and marks it most recently used.
func (c *signatureCache) get(queue, batchID string) (TargetSignature, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.entries[signatureCacheKey{queue: queue, batchID: batchID}]
	if !ok {
		return TargetSignature{}, false
	}
	c.byRecency.MoveToFront(element)
	return element.Value.(*signatureCacheEntry).signature, true
}

// put caches signature, evicting least recently used entries to stay within the
// budget. A signature larger than the whole budget is not cached.
func (c *signatureCache) put(signature TargetSignature) {
	size := signatureCacheBytes(signature)
	if size > c.maxBytes {
		return
	}
	key := signatureCacheKey{queue: signature.Queue, batchID: signature.BatchID}

	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.entries[key]; ok {
		c.removeLocked(element)
	}
	c.entries[key] = c.byRecency.PushFront(&signatureCacheEntry{key: key, signature: signature, bytes: size})
	c.usedBytes += size

	for c.usedBytes > c.maxBytes {
		c.removeLocked(c.byRecency.Back())
	}
}

func (c *signatureCache) removeLocked(element *list.Element) {
	entry := c.byRecency.Remove(element).(*signatureCacheEntry)
	delete(c.entries, entry.key)
	c.usedBytes -= entry.bytes
}
