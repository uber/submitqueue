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

package baserevision

//go:generate mockgen -source=baserevision.go -destination=mock/baserevision_mock.go -package=mock

import "context"

// Resolver returns the commit a queue's new batches are analyzed against.
type Resolver interface {
	// Resolve returns the full commit SHA of the tip of the queue's target
	// branch as of the call. An error means the tip could not be determined,
	// and a caller must not fall back to an earlier value. Implementations
	// must be safe for concurrent use.
	Resolve(ctx context.Context, queue string) (string, error)
}
