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

// Package pathoverlap provides a conflict.Analyzer that reports a conflict
// between two batches when the paths they change share a key. The key is a
// projection of the path chosen at construction: ByFile compares whole paths,
// so only batches touching the same file conflict; ByDirectory compares parent
// directories, so batches touching sibling files conflict too. It is the first
// analyzer to use the capability the extension contract unblocks: it takes only
// batch identity and resolves each batch's changed files itself through an
// injected changeset resolver, rather than depending on the controller to
// pre-compute them. A shared path key is the concrete notion of target overlap,
// so it reports entity.ConflictTypeTargetOverlap.
package pathoverlap

import (
	"context"
	"fmt"
	"path"

	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
)

// PathKey projects a changed file path onto the key overlap is measured on. Two
// batches conflict when any of their changed paths yield the same key, so a
// coarser projection serializes more batches.
type PathKey func(path string) string

// ByFile keys on the whole path: batches conflict only when they change the
// same file.
func ByFile(p string) string {
	return path.Clean(p)
}

// ByDirectory keys on the path's immediate parent directory: batches conflict
// when they change any files in the same directory, whether or not the files
// themselves are the same. Overlap by directory is strictly coarser than
// overlap by file — every file overlap is also a directory overlap. Paths at
// the repository root share the key ".".
func ByDirectory(p string) string {
	return path.Dir(p)
}

// analyzer reports a conflict between batches whose changed paths share a key.
// The paths a batch changes are resolved from each batch's change details.
type analyzer struct {
	// cfg is the per-queue identity this analyzer was built for.
	cfg      conflict.Config
	resolver changeset.Resolver
	// key projects each changed path onto the key overlap is measured on.
	key PathKey
}

// New returns a conflict.Analyzer that flags an in-flight batch as conflicting
// when it changes a path whose key matches one the candidate batch changes,
// bound to the queue named in cfg. The resolver resolves each batch's changed
// files, and key selects the granularity of overlap.
// Panics if key is nil.
func New(cfg conflict.Config, resolver changeset.Resolver, key PathKey) conflict.Analyzer {
	if key == nil {
		panic("pathoverlap.New: key must not be nil")
	}
	return analyzer{cfg: cfg, resolver: resolver, key: key}
}

// Analyze returns one ConflictTypeTargetOverlap Conflict per in-flight batch
// that shares a path key with batch, preserving the in-flight order. A batch
// that changes no files conflicts with nothing.
func (a analyzer) Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]entity.Conflict, error) {
	if len(inFlight) == 0 {
		return nil, nil
	}

	candidate, err := a.keys(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve files for batch %s: %w", batch.ID, err)
	}
	if len(candidate) == 0 {
		return nil, nil
	}

	var conflicts []entity.Conflict
	for _, other := range inFlight {
		keys, err := a.keys(ctx, other)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve files for batch %s: %w", other.ID, err)
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

// keys resolves the set of path keys the batch changes.
func (a analyzer) keys(ctx context.Context, batch entity.Batch) (map[string]struct{}, error) {
	changes, err := a.resolver.DetailedForBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for _, change := range changes.Changes {
		for _, file := range change.Details.ChangedFiles {
			keys[a.key(file.Path)] = struct{}{}
		}
	}
	return keys, nil
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
