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

// Package projectstatus defines the optional integration that resolves project
// status results from a completed validation.
package projectstatus

//go:generate go run go.uber.org/mock/mockgen -source=projectstatus.go -destination=mock/projectstatus_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Result is one project status result. The record stage supplies the request
// identity and recording timestamp when it persists this result.
type Result struct {
	// Project identifies the project to which this result applies.
	Project string
	// Degree describes how broken the project is, on the closed interval [0, 1].
	Degree float64
}

// Resolver resolves named project status results from one terminal validation.
// terminalBuildID identifies the build that established the request's terminal
// state, or is empty when no build established it. Implementations may use any
// repository-specific analysis they need to obtain those results.
// Returning no results is valid.
type Resolver interface {
	Resolve(ctx context.Context, request entity.Request, terminalBuildID string) ([]Result, error)
}

// ResolverConfig carries the queue identity handed to a ResolverFactory.
type ResolverConfig struct {
	// QueueName identifies the queue served by the resolver.
	QueueName string
}

// ResolverFactory constructs a Resolver for one queue.
type ResolverFactory interface {
	For(cfg ResolverConfig) (Resolver, error)
}
