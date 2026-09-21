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

// Package projectresult defines the optional integration that attributes a
// completed validation to named projects.
package projectresult

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Result is one project-scoped validation outcome. The record stage supplies
// the request identity and recording timestamp when it persists this result.
type Result struct {
	// Project identifies the project to which this result applies.
	Project string
	// Degree describes how broken the project is, on the closed interval [0, 1].
	Degree float64
}

// Resolver attributes one terminal validation request to named project
// outcomes. Implementations may use any repository-specific analysis they need
// to obtain those outcomes. Returning no results is valid.
type Resolver interface {
	Resolve(ctx context.Context, request entity.Request) ([]Result, error)
}

// Config carries the queue identity handed to a Factory.
type Config struct {
	// QueueName identifies the queue served by the resolver.
	QueueName string
}

// Factory constructs a Resolver for one queue.
type Factory interface {
	For(cfg Config) (Resolver, error)
}

// NoopFactory returns a resolver that records no project outcomes. It is the
// default for deployments that do not configure project attribution.
type NoopFactory struct{}

// For returns the no-op resolver for a queue.
func (NoopFactory) For(Config) (Resolver, error) {
	return noopResolver{}, nil
}

type noopResolver struct{}

func (noopResolver) Resolve(context.Context, entity.Request) ([]Result, error) {
	return nil, nil
}
