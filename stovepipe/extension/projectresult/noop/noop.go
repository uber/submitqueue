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

// Package noop provides a projectresult.Factory that records no project
// outcomes. Use it when a deployment has not configured project attribution.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectresult"
)

// Verify interface compliance at compile time.
var _ projectresult.Factory = Factory{}

// Factory returns a resolver that records no project outcomes.
type Factory struct{}

// New returns a no-op project-result Factory.
func New() Factory {
	return Factory{}
}

// For returns the no-op resolver for a queue.
func (Factory) For(projectresult.Config) (projectresult.Resolver, error) {
	return resolver{}, nil
}

type resolver struct{}

func (resolver) Resolve(context.Context, entity.Request, string) ([]projectresult.Result, error) {
	return nil, nil
}
