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

// Package projectissues defines project attribution for individual build issues.
package projectissues

//go:generate mockgen -source=projectissues.go -destination=mock/projectissues_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Attribution identifies the projects affected by one issue within a validation's scope.
type Attribution struct {
	// TaskID identifies the issue's validation task and must be nonempty.
	TaskID string
	// UnitID identifies the failed unit within the task and must be nonempty.
	UnitID string
	// Projects contains distinct, nonempty project names, using the same identity as project status.
	// An empty list means the issue affects no projects within this validation's scope.
	Projects []string
}

// Attributer attributes confirmed build issues within the request's validation scope.
// Return an error for incomplete attribution, including any task-wide failure.
// Returning no results is valid; discard any results returned with an error.
// Results must be stable across retries for the same request.
// Calls must be concurrency-safe and leave inputs unchanged.
type Attributer interface {
	Attribute(ctx context.Context, request entity.Request) ([]Attribution, error)
}

// AttributerConfig identifies the queue whose attributer is requested.
type AttributerConfig struct {
	// QueueName identifies the validation queue.
	QueueName string
}

// AttributerFactory provides an attributer for one queue.
type AttributerFactory interface {
	For(cfg AttributerConfig) (Attributer, error)
}
