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

package errs

import "strings"

// Group reports several failures that happened together as one error, for a
// step that must run every handler rather than stop at the first failure.
//
// nil arguments are dropped and Group returns nil when all of them are nil.
// errors.Is and errors.As reach every error passed in.
func Group(errs ...error) error {
	members := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			members = append(members, err)
		}
	}
	if len(members) == 0 {
		return nil
	}
	return &groupedError{members: members}
}

// groupedError is the error Group returns.
type groupedError struct {
	// members are the failures reported together, in the order given. Never
	// empty, and never contains a nil.
	members []error
}

// Error joins the member messages on one line, because these land in
// structured logs where a multi-line message becomes several records.
func (e *groupedError) Error() string {
	msgs := make([]string, 0, len(e.members))
	for _, m := range e.members {
		msgs = append(msgs, m.Error())
	}
	return strings.Join(msgs, "; ")
}

// Unwrap returns the grouped failures for errors.Is/As compatibility.
func (e *groupedError) Unwrap() []error {
	return e.members
}
