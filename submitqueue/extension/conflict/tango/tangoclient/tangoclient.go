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

// Package tangoclient implements tango.ChangedTargetsClient over the Tango
// service's YARPC client. It is the only production package that imports
// Tango's generated types.
package tangoclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/uber/tango/tangopb"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
)

// Config holds what Tango needs that a changed-targets request does not carry.
type Config struct {
	// Remote identifies the repository to Tango, in the form Tango is
	// configured with.
	Remote string

	// Strategy is how Tango computes the target graph of the base revision.
	Strategy tangopb.ComputationStrategy
}

type client struct {
	tangoClient tangopb.TangoYARPCClient
	config      Config
}

// New returns a ChangedTargetsClient that asks the Tango service behind
// tangoClient. It serves one repository, the one named by config.Remote.
func New(tangoClient tangopb.TangoYARPCClient, config Config) tango.ChangedTargetsClient {
	return &client{tangoClient: tangoClient, config: config}
}

type changedTarget struct {
	localID  int32
	distance int32
}

func (c *client) ChangedTargets(ctx context.Context, request tango.ChangedTargetsRequest) ([]tango.LabelDistance, error) {
	stream, err := c.tangoClient.GetChangedTargets(ctx, &tangopb.GetChangedTargetsRequest{
		FirstRevision:  c.description(request.BaseRevision, nil),
		SecondRevision: c.description(request.BaseRevision, request.ChangeURIs),
		// max_distance defaults to 0, which would keep only the seeds.
		OutputConfig: &tangopb.OutputConfig{MaxDistance: -1},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to request changed targets: %w", err)
	}
	defer func() { _ = stream.CloseSend() }()

	// Only each target's local ID and distance are kept, so a response with
	// millions of targets is not buffered whole.
	var changed []changedTarget
	var labels map[int32]string
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive changed targets: %w", err)
		}
		for _, target := range response.GetChangedTargets().GetChangedTargets() {
			// A deleted target exists only in the first revision.
			node := target.GetNewTarget()
			if target.GetChangeType() == tangopb.CHANGE_TYPE_DELETED {
				node = target.GetOldTarget()
			}
			if node == nil {
				return nil, fmt.Errorf("changed target of type %s has no target", target.GetChangeType())
			}
			changed = append(changed, changedTarget{localID: node.GetId(), distance: target.GetDistance()})
		}
		if metadata := response.GetMetadata(); metadata != nil {
			labels = metadata.GetTargetIdMapping()
		}
	}

	targets := make([]tango.LabelDistance, 0, len(changed))
	for _, target := range changed {
		label, ok := labels[target.localID]
		if !ok {
			return nil, fmt.Errorf("no label for changed target %d", target.localID)
		}
		targets = append(targets, tango.LabelDistance{Label: label, Distance: target.distance})
	}
	return targets, nil
}

func (c *client) description(baseRevision string, changeURIs []string) *tangopb.BuildDescription {
	requests := make([]*tangopb.Request, len(changeURIs))
	for i, uri := range changeURIs {
		requests[i] = &tangopb.Request{Url: uri}
	}
	return &tangopb.BuildDescription{
		Remote:   c.config.Remote,
		BaseSha:  baseRevision,
		Requests: requests,
		Strategy: c.config.Strategy,
	}
}
