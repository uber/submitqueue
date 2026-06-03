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

package mergechecker

import (
	"context"
	"fmt"
	"strings"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// multiChecker dispatches mergeability checks to scheme-specific checkers
// based on the URI scheme of the first change URI. Each scheme
// (e.g., "github", "ghe", "ghes") maps to a checker configured for that host.
type multiChecker struct {
	// checkers maps URI scheme values to their corresponding MergeChecker.
	checkers map[string]MergeChecker
}

// NewMultiChecker creates a MergeChecker that routes mergeability checks
// to scheme-specific checkers. The map keys correspond to URI schemes
// (e.g., "github", "ghe") extracted from the first change URI.
func NewMultiChecker(checkers map[string]MergeChecker) MergeChecker {
	return &multiChecker{checkers: checkers}
}

// Check dispatches the mergeability check to the checker registered for
// the change URI scheme.
func (m *multiChecker) Check(ctx context.Context, queue string, change entity.Change) (Result, error) {
	if len(change.URIs) == 0 {
		return Result{}, fmt.Errorf("no change URIs provided")
	}

	scheme, _, ok := strings.Cut(change.URIs[0], "://")
	if !ok || scheme == "" {
		return Result{}, fmt.Errorf("invalid change URI %q: missing scheme", change.URIs[0])
	}

	checker, ok := m.checkers[scheme]
	if !ok {
		return Result{}, fmt.Errorf("no mergeability checker configured for scheme %q", scheme)
	}
	return checker.Check(ctx, queue, change)
}
