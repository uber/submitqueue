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

// Package page defines a generic, cursor-paginated result envelope shared across
// domains. Producers return one bounded Page at a time; callers walk further by
// passing the page's NextCursor back to the producing call until it is empty.
package page

// Page is one bounded slice of a larger sequence, plus an opaque cursor for
// fetching the next page. The element type T is the domain value being paged
// (e.g. a commit URI string, or an entity).
type Page[T any] struct {
	// Items are the elements in this page, in the producer's defined order.
	Items []T
	// NextCursor is an opaque token for fetching the next page, passed back to
	// the producing call. It is empty when this page is the last one. Its
	// encoding is defined and interpreted solely by the producer.
	NextCursor string
}
