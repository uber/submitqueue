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

package buildrunner

import (
	"context"

	"github.com/uber/submitqueue/entity/change"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// ResolveBatches resolves each batch's changes through the resolver and
// concatenates them in order. It is shared by BuildRunner implementations that
// need a flat change list (e.g. the base, assembled from several dependency
// batches) so the per-batch resolution loop is not duplicated per backend.
func ResolveBatches(ctx context.Context, resolver changeset.Resolver, batches []entity.Batch) ([]change.Change, error) {
	var changes []change.Change
	for _, b := range batches {
		cs, err := resolver.ChangesForBatch(ctx, b)
		if err != nil {
			return nil, err
		}
		changes = append(changes, cs...)
	}
	return changes, nil
}
