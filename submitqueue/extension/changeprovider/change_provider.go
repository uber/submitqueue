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

package changeprovider

//go:generate mockgen -source=change_provider.go -destination=mock/change_provider_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// ChangeProvider fetches change metadata from external systems
// Each implementation is configured for a specific provider (GitHub, GitLab, Phabricator).
//
// The change value types it produces — entity.ChangeInfo, entity.ChangeDetails,
// entity.Author, entity.ChangedFile — live in the entity package so the same typed
// facts can be persisted (entity.ChangeRecord) and scored without re-declaration.
type ChangeProvider interface {
	// Get retrieves change information for the provided request.
	// It is handed the request identity and reads request.Change itself.
	// For a Change with multiple URIs (e.g., stacked PRs), returns one ChangeInfo per URI.
	// Returns a slice of ChangeInfo, one for each change in the stack.
	Get(ctx context.Context, request entity.Request) ([]entity.ChangeInfo, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this ChangeProvider serves.
	QueueName string
}

// Factory builds the ChangeProvider for a queue. Implementations are provided
// by integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the ChangeProvider for the given queue.
	For(cfg Config) (ChangeProvider, error)
}
