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

// Package issueattribution defines project attribution for validation issues,
// independently of project status resolution.
package issueattribution

//go:generate mockgen -source=issueattribution.go -destination=mock/issueattribution_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// IssueProjects associates one issue opening with projects within an assessed scope.
type IssueProjects struct {
	// Issue identifies the requested condition and its opening.
	Issue entity.OpenIssue
	// Projects contains the distinct, nonempty project IDs affected by this issue.
	// Every project must belong to the result's AssessedProjects.
	Projects []string
}

// Result describes attribution within an explicitly assessed project scope.
type Result struct {
	// AssessedProjects contains the distinct, nonempty project IDs assessed for every
	// requested issue; projects outside this scope have not been reassessed.
	// An empty scope means no attribution was performed.
	AssessedProjects []string
	// Issues contains exactly one result per requested opening when the scope is
	// nonempty, including an empty Projects list for an issue affecting none of it.
	// Issues must be empty when AssessedProjects is empty.
	Issues []IssueProjects
}

// Resolver attributes a batch of issue openings using one build's validation scope.
// Implementations resolve build metadata and project scope through injected dependencies.
// Calls must be concurrency-safe and must not mutate inputs or consume evidence.
// Return plain dependency errors; results returned with an error must not be used.
type Resolver interface {
	Resolve(ctx context.Context, buildID entity.BuildID, issues []entity.OpenIssue) (Result, error)
}

// Config identifies the queue whose attribution resolver is requested.
type Config struct {
	// QueueName identifies the validation queue.
	QueueName string
}

// Factory resolves queue-bound attribution resolvers; routing belongs to service wiring.
type Factory interface {
	For(cfg Config) (Resolver, error)
}
