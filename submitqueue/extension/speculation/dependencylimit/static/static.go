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

// Package static provides a dependencylimit.DependencyLimit that always
// returns a fixed, construction-time value.
package static

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/extension/speculation/dependencylimit"
)

// staticLimit is a dependencylimit.DependencyLimit that always returns a
// fixed value.
type staticLimit struct {
	// limit is the fixed maximum active-dependency count returned by every
	// call to Limit.
	limit int
}

// New returns a dependencylimit.DependencyLimit whose Limit always returns
// limit.
func New(limit int) dependencylimit.DependencyLimit {
	return staticLimit{limit: limit}
}

// Limit returns the fixed value given to New. It never errors.
func (l staticLimit) Limit(_ context.Context) (int, error) {
	return l.limit, nil
}
