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

// Package extension holds Stovepipe-specific extension implementations.
package extension

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// ChangeIngester subscribes to change events from a VCS source
// and dispatches them for processing. The source and VCS are
// implementation details left to the injected backend.
type ChangeIngester interface {
	Start(ctx context.Context) error
}

// ChangeHandler processes a single change received from the ingester.
type ChangeHandler interface {
	IngestChange(ctx context.Context, info entity.ChangeInfo) error
}
