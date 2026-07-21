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

// Package loader holds the "load one entity by id from a store" pattern
// shared by Stovepipe's queue controllers (build, process, buildsignal).
package loader

import (
	"context"
	"fmt"
)

// ByID loads one entity by id via get, returning it unwrapped on success. On
// failure it wraps the error as "<controllerName> failed to load <entityName>
// <id>: <cause>" so every stovepipe controller reports load failures in the
// same shape.
//
// get is typically a store's Get method value passed directly (e.g.
// c.store.GetRequestStore().Get), which fixes T through inference so callers
// never spell out the type parameter.
//
// Not-found is never special-cased into a retryable error here: per the
// storage read-after-write-consistency contract (see
// stovepipe/extension/storage/README.md), a Get miss on a row that a
// causally-prior write should already have produced is a storage
// implementation defect, not a lag condition worth retrying through. It
// surfaces as a plain error, non-retryable by platform/errs's default.
func ByID[T any](ctx context.Context, id string, get func(context.Context, string) (T, error), controllerName, entityName string) (T, error) {
	got, err := get(ctx, id)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("%s failed to load %s %s: %w", controllerName, entityName, id, err)
	}
	return got, nil
}
