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

// Package noop provides a projectstatus.ResolverFactory that returns no
// project status results. Use it when a deployment has not configured project
// status resolution.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
)

// Verify interface compliance at compile time.
var _ projectstatus.ResolverFactory = ResolverFactory{}

// ResolverFactory returns a resolver with no project status results.
type ResolverFactory struct{}

// New returns a no-op project-status ResolverFactory.
func New() ResolverFactory {
	return ResolverFactory{}
}

// For returns the no-op resolver for a queue.
func (ResolverFactory) For(projectstatus.ResolverConfig) (projectstatus.Resolver, error) {
	return resolver{}, nil
}

type resolver struct{}

func (resolver) Resolve(context.Context, entity.Request, string) ([]projectstatus.Result, error) {
	return nil, nil
}
