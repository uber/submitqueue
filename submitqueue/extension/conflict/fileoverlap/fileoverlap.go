// Copyright (c) 2025 Uber Technologies, Inc.
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

// Package fileoverlap provides a conflict.Analyzer that reports a conflict
// between two batches when they change one or more of the same files. It is the
// first analyzer to use the capability the extension contract unblocks: it takes
// only batch identity and resolves each batch's changed files itself through an
// injected changeset resolver, rather than depending on the controller to
// pre-compute them. A shared file is the concrete notion of target overlap, so
// it reports conflict.ConflictTypeTargetOverlap.
package fileoverlap

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
)

// analyzer reports a conflict between batches that change a common file. The
// files a batch changes are resolved from each batch's change details.
type analyzer struct {
	resolver changeset.Resolver
}

// New returns a conflict.Analyzer that flags an in-flight batch as conflicting
// when it changes a file the candidate batch also changes. The resolver
// resolves each batch's changed files.
func New(resolver changeset.Resolver) conflict.Analyzer {
	return analyzer{resolver: resolver}
}

// Analyze returns one ConflictTypeTargetOverlap Conflict per in-flight batch
// that shares a changed file with batch, preserving the in-flight order. A batch
// that changes no files conflicts with nothing.
func (a analyzer) Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]conflict.Conflict, error) {
	if len(inFlight) == 0 {
		return nil, nil
	}

	candidate, err := a.files(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve files for batch %s: %w", batch.ID, err)
	}
	if len(candidate) == 0 {
		return nil, nil
	}

	var conflicts []conflict.Conflict
	for _, other := range inFlight {
		files, err := a.files(ctx, other)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve files for batch %s: %w", other.ID, err)
		}
		if intersects(candidate, files) {
			conflicts = append(conflicts, conflict.Conflict{
				BatchID: other.ID,
				Type:    conflict.ConflictTypeTargetOverlap,
			})
		}
	}
	return conflicts, nil
}

// files resolves the set of file paths the batch changes.
func (a analyzer) files(ctx context.Context, batch entity.Batch) (map[string]struct{}, error) {
	changes, err := a.resolver.DetailedForBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	files := make(map[string]struct{})
	for _, change := range changes.Changes {
		for _, file := range change.Details.ChangedFiles {
			files[file.Path] = struct{}{}
		}
	}
	return files, nil
}

// intersects reports whether the two sets share any element.
func intersects(a, b map[string]struct{}) bool {
	// Iterate the smaller set for fewer lookups.
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
