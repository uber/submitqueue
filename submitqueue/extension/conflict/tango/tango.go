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

// Package tango provides a conflict.Analyzer that reports a conflict between
// two batches when their changed build targets overlap. The targets a batch
// affects are resolved through an injected TargetResolver, whose production
// implementation calls the Tango service. The interface is deliberately free
// of Tango wire types so that each deployment can provide its own adapter
// against whatever proto import path its monorepo uses.
package tango

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
)

// Target is a build target a batch affects, carrying the fields Tango reports
// for each changed target. The analyzer reads only Name for overlap today;
// ChangeType and Distance are available for future consumers such as conflict
// relaxation.
type Target struct {
	// Name identifies the target (e.g. "//service/foo:lib").
	Name string
	// ChangeType classifies how the target changed: "new", "deleted", or "changed".
	ChangeType string
	// Distance from the nearest directly changed source file in the reverse
	// dependency graph. 0 = directly changed, 1+ = transitive.
	Distance int
}

// TargetResolver resolves the set of build targets a batch affects. The
// production implementation translates the batch's changes into a Tango
// GetChangedTargets call; tests supply a fake.
type TargetResolver interface {
	ChangedTargets(ctx context.Context, batch entity.Batch) ([]Target, error)
}

// New returns a conflict.Analyzer that flags an in-flight batch as conflicting
// when its changed build targets overlap with the candidate batch's, bound to
// the queue named in cfg.
func New(cfg conflict.Config, targets TargetResolver) conflict.Analyzer {
	return &analyzer{cfg: cfg, targets: targets}
}

type analyzer struct {
	cfg     conflict.Config
	targets TargetResolver
	// TODO: cache resolved target sets per batch ID so in-flight batches
	// compared against successive arrivals pay only one resolution each. Consider
	// a TTL for high-traffic queues where trunk moves fast, and a max-size cap.
}

// Analyze returns one ConflictTypeTargetOverlap Conflict per in-flight batch
// whose changed build targets overlap with batch, preserving the in-flight
// order. A batch that affects no targets conflicts with nothing.
func (a *analyzer) Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]entity.Conflict, error) {
	if len(inFlight) == 0 {
		return nil, nil
	}

	// TODO: when TargetResolver fails, fall back to a queue-configured
	// analyzer (all or none) instead of propagating the error. The queue config
	// decides whether a Tango outage over-serializes (all) or maximizes
	// parallelism (none).
	candidate, err := a.resolve(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve targets for batch %s: %w", batch.ID, err)
	}
	if len(candidate) == 0 {
		return nil, nil
	}

	var conflicts []entity.Conflict
	for _, other := range inFlight {
		keys, err := a.resolve(ctx, other)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve targets for batch %s: %w", other.ID, err)
		}
		if intersects(candidate, keys) {
			conflicts = append(conflicts, entity.Conflict{
				BatchID: other.ID,
				Type:    entity.ConflictTypeTargetOverlap,
			})
		}
	}
	return conflicts, nil
}

// resolve returns the set of target names the batch affects.
func (a *analyzer) resolve(ctx context.Context, batch entity.Batch) (map[string]struct{}, error) {
	targets, err := a.targets.ChangedTargets(ctx, batch)
	if err != nil {
		return nil, err
	}

	keys := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		keys[t.Name] = struct{}{}
	}
	return keys, nil
}

// intersects reports whether the two sets share any element.
func intersects(a, b map[string]struct{}) bool {
	if len(b) < len(a) {
		a, b = b, a
	}
	for k := range a {
		if _, ok := b[k]; ok {
			return true
		}
	}
	return false
}
