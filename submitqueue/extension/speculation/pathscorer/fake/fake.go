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

// Package fake provides a programmable pathscorer.Scorer for tests and
// examples. By default Score echoes each input path's current score back,
// keyed by its ID; Returns overrides that with canned path scores, and
// FailWith injects an error on every call. It is intended for examples and
// tests only, never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/pathscorer"
)

// Scorer is a programmable pathscorer.Scorer.
type Scorer struct {
	scores    []entity.PathScore
	hasScores bool
	err       error
}

// New returns a fake Scorer that echoes each input path's current score back.
// Override the returned scores with Returns.
func New() *Scorer {
	return &Scorer{}
}

// Returns makes every Score call return scores instead of echoing its input.
func (s *Scorer) Returns(scores []entity.PathScore) *Scorer {
	s.scores = scores
	s.hasScores = true
	return s
}

// FailWith makes every Score call return err.
func (s *Scorer) FailWith(err error) *Scorer {
	s.err = err
	return s
}

// Score returns the canned scores if set with Returns, otherwise one PathScore
// per input path echoing the path's current score, unchanged.
func (s *Scorer) Score(_ context.Context, tree entity.SpeculationTree) ([]entity.PathScore, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.hasScores {
		return s.scores, nil
	}
	scores := make([]entity.PathScore, 0, len(tree.Paths))
	for _, p := range tree.Paths {
		scores = append(scores, entity.PathScore{PathID: p.ID, Score: p.Score})
	}
	return scores, nil
}

// ensure the fake satisfies the interface.
var _ pathscorer.Scorer = (*Scorer)(nil)
