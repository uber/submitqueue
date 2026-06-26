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

// Package fake provides an in-memory sourcecontrol.SourceControl seeded with a
// single queue's linear ref history, ordered newest-first. It is intended for
// examples and tests only, never production. Ancestry is decided by position in
// the seeded slice: an earlier commit (larger index) is an ancestor of a later
// one (smaller index).
package fake

import (
	"context"

	"github.com/uber/submitqueue/platform/base/page"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
)

// sourceControlFake serves a single queue's linear history. history[0] is the
// latest commit; higher indices are progressively older ancestors.
type sourceControlFake struct {
	history []string
}

// New returns a sourcecontrol.SourceControl backed by the given ref history,
// ordered newest-first (history[0] is the latest commit). The slice is copied so
// later mutation by the caller does not affect the fake.
func New(history []string) sourcecontrol.SourceControl {
	cp := make([]string, len(history))
	copy(cp, history)
	return sourceControlFake{history: cp}
}

// Latest returns the newest commit URI, or ErrNotFound when the history is empty.
func (s sourceControlFake) Latest(_ context.Context) (string, error) {
	if len(s.history) == 0 {
		return "", sourcecontrol.ErrNotFound
	}
	return s.history[0], nil
}

// IsAncestor reports whether ancestor is an ancestor of descendant. Both URIs
// must be on the ref; an unknown URI yields ErrNotFound. Since the history is
// newest-first, ancestor is an ancestor of descendant when its index is greater
// than or equal to descendant's (older-or-equal commit).
func (s sourceControlFake) IsAncestor(_ context.Context, ancestor, descendant string) (bool, error) {
	ai := s.indexOf(ancestor)
	di := s.indexOf(descendant)
	if ai < 0 || di < 0 {
		return false, sourcecontrol.ErrNotFound
	}
	return ai >= di, nil
}

// History returns one page of commit URIs, newest first. The cursor is the URI
// of the first commit of the page to return; an empty cursor starts at the latest
// commit. A limit of zero or less returns the rest of the history from the cursor
// in a single page. The returned NextCursor is the URI of the next, older commit,
// or empty when the page reaches the end of the history.
func (s sourceControlFake) History(_ context.Context, cursor string, limit int) (page.Page[string], error) {
	start := 0
	if cursor != "" {
		start = s.indexOf(cursor)
		if start < 0 {
			return page.Page[string]{}, sourcecontrol.ErrNotFound
		}
	}

	end := len(s.history)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	uris := make([]string, end-start)
	copy(uris, s.history[start:end])

	next := ""
	if end < len(s.history) {
		next = s.history[end]
	}
	return page.Page[string]{Items: uris, NextCursor: next}, nil
}

// indexOf returns the index of uri in the history, or -1 if absent.
func (s sourceControlFake) indexOf(uri string) int {
	for i, u := range s.history {
		if u == uri {
			return i
		}
	}
	return -1
}
