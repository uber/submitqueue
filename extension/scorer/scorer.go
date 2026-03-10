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

package scorer

//go:generate mockgen -source=scorer.go -destination=mock/scorer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// Scorer computes a success probability score for a change based on its characteristics.
type Scorer interface {
	// Score returns a probability between 0.0 and 1.0 indicating the likelihood
	// of a successful land for the given change.
	Score(ctx context.Context, change entity.Change) (float64, error)
}
