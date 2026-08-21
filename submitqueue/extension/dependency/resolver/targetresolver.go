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

// Package resolver defines the TargetResolver interface for resolving the set
// of build targets a batch affects. The interface is deliberately free of
// Tango wire types so that each deployment can provide its own adapter against
// whatever proto import path its monorepo uses — the analyzer sees only batch
// identity in and targets out.
package resolver

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Target is a build target a batch affects.
type Target struct {
	// Name identifies the target (e.g. "//service/foo:lib").
	Name string
	// Attributes carries backend-specific metadata the analyzer does not
	// interpret today. Future consumers (e.g. conflict relaxation) can read
	// keys like "distance" or "rule_type" without an interface change.
	Attributes map[string]string
}

// TargetResolver resolves the set of build targets a batch affects. The
// production implementation translates the batch's changes into a Tango
// GetChangedTargets call; tests supply a fake.
type TargetResolver interface {
	ChangedTargets(ctx context.Context, batch entity.Batch) ([]Target, error)
}
