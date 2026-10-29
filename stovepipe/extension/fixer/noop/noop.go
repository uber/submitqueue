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

// Package noop provides a repair executor that accepts submissions without launching work.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
)

type executor struct{}

// New returns a repair executor that accepts submissions without launching work.
func New() fixer.RepairExecutor {
	return executor{}
}

func (executor) Submit(_ context.Context, issue entity.OpenIssue) (string, error) {
	return "noop/" + issue.FirstSeenBuildID, nil
}
