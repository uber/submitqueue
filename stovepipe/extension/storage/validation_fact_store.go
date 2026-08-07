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

//go:generate mockgen -source=validation_fact_store.go -destination=mock/validation_fact_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// ValidationFactStore holds the immutable record of how broken a scope was at a commit,
// keyed by (queue, URI, project). Facts are create-only: there is no Update, because the
// first fact written for an identity is the permanent answer. A caller that needs to know
// whether it won the race reads ErrAlreadyExists from Create and then loads the winner.
type ValidationFactStore interface {
	// Create writes one immutable fact for the bound queue. Returns ErrAlreadyExists if
	// the queue already holds a fact for the (uri, project) pair, leaving the stored fact
	// untouched — first writer wins.
	Create(ctx context.Context, fact entity.ValidationFact) error

	// Get returns the bound queue's fact for uri and project. Returns ErrNotFound if no
	// fact exists, which means the scope was never validated rather than validated green.
	Get(ctx context.Context, uri, project string) (entity.ValidationFact, error)
}
