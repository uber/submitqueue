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

package tango

//go:generate mockgen -source=request.go -destination=mock_request.go -package=tango

import (
	"context"

	"github.com/uber/submitqueue/platform/base/change"
)

// ChangedTargetsRequest asks which targets change when ChangeURIs are applied
// on top of a base revision.
type ChangedTargetsRequest struct {
	// BaseRevision is the full commit SHA the changes are applied on top of.
	BaseRevision string

	// ChangeURIs lists every change to apply, in batch order.
	ChangeURIs []string
}

// ChangedTargetsClient returns the targets a request affects. An adapter owns
// the Tango client and its stream, collects the response, and resolves each
// target's label; any failure to produce a complete answer is an error.
type ChangedTargetsClient interface {
	// ChangedTargets returns one entry per affected target, in no particular
	// order. An empty result is valid and means nothing is affected.
	ChangedTargets(ctx context.Context, request ChangedTargetsRequest) ([]LabelDistance, error)
}

func changedTargetsRequestFor(baseRevision string, changes []change.Change) ChangedTargetsRequest {
	var uris []string
	for _, c := range changes {
		uris = append(uris, c.URIs...)
	}
	return ChangedTargetsRequest{BaseRevision: baseRevision, ChangeURIs: uris}
}
