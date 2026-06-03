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

package storage

// staticFactory is the default Factory. It returns the same Storage for every
// queue, ignoring the name. It exists so call sites can route through the
// Factory contract today (a no-op) and adopt true per-queue backends later
// without further consumer changes.
type staticFactory struct {
	storage Storage
}

// NewStaticFactory returns a Factory that serves the given Storage for every
// queue name.
func NewStaticFactory(s Storage) Factory {
	return staticFactory{storage: s}
}

// For returns the configured Storage for any queue name.
func (f staticFactory) For(string) (Storage, error) {
	return f.storage, nil
}
