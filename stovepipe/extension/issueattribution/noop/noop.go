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

// Package noop provides issue attribution without assessing any projects.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/issueattribution"
)

// Factory returns an attribution resolver with no assessed scope for every queue.
type Factory struct{}

// New returns a no-op attribution factory.
func New() Factory { return Factory{} }

func (Factory) For(issueattribution.Config) (issueattribution.Resolver, error) {
	return resolver{}, nil
}

type resolver struct{}

func (resolver) Resolve(context.Context, entity.BuildID, []entity.OpenIssue) (issueattribution.Result, error) {
	return issueattribution.Result{}, nil
}

var _ issueattribution.Factory = Factory{}
var _ issueattribution.Resolver = resolver{}
