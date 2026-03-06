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

//go:generate mockgen -source=mergechecker.go -destination=mock/mergechecker_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// MergeChecker predicts whether a set of changes can merge cleanly.
type MergeChecker interface {
	// Check is a fail-fast mergeability check that optimistically assesses
	// whether the changes can be merged. A positive result does not
	// guarantee that the changes will apply cleanly at merge time.
	Check(ctx context.Context, queue string, change entity.Change) (Result, error)
}

// Result holds the outcome of a mergeability check.
type Result struct {
	// Mergeable is true if the request's changes are expected to merge cleanly.
	Mergeable bool
	// Reason is a human-readable explanation when Mergeable is false.
	// Empty when Mergeable is true.
	Reason string
}
