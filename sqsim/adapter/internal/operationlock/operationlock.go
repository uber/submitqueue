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

// Package operationlock serializes duplicate calls for one production operation.
package operationlock

import "sync"

// Locker supplies a stable mutex for each operation key.
type Locker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// New returns an empty keyed locker.
func New() *Locker {
	return &Locker{locks: make(map[string]*sync.Mutex)}
}

// Lock locks key and returns its unlock function.
func (l *Locker) Lock(key string) func() {
	l.mu.Lock()
	lock, ok := l.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		l.locks[key] = lock
	}
	l.mu.Unlock()

	lock.Lock()
	return lock.Unlock
}
