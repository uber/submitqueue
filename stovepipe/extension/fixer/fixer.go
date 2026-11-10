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

// Package fixer defines repair submission for open issues.
package fixer

//go:generate mockgen -source=fixer.go -destination=mock/fixer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RepairExecutor chooses how to group repair work. Implementations must be concurrency-safe.
type RepairExecutor interface {
	// Submit returns per-item backend acceptance or terminal skips, not repair completion.
	// Valid submissions returned alongside errors are persisted; omitted items remain pending.
	// Retries may include subsets or additional items; implementations deduplicate by opening.
	// Pending rows exist before Submit. Executors manage metadata through injected storage,
	// preserving submission fields and reloading on version conflicts.
	Submit(context.Context, []entity.OpenIssue) ([]Submission, error)
}

// Submission associates one input item with accepted repair work or a deliberate skip.
// Acceptance does not indicate repair completion or success.
type Submission struct {
	// Issue identifies the input opening, including its first-seen build.
	Issue entity.OpenIssue
	// FixID is the backend request or run ID for accepted work; multiple items may share it.
	FixID string
	// SkipReason is nonempty for a terminal skip. Exactly one of FixID and SkipReason is required.
	SkipReason string
}

// Registration binds a stable queue-local identity to an executor.
type Registration struct {
	// ID is durable across configuration changes and must not be reused for another backend.
	ID string
	// Executor implements the registered repair policy.
	Executor RepairExecutor
}

// Config identifies a validation queue.
type Config struct {
	// QueueName identifies the validation queue.
	QueueName string
}

// Factory resolves all registered executors; an empty slice disables dispatch.
type Factory interface {
	For(Config) ([]Registration, error)
}
