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

package client

import (
	"context"
	"fmt"
	"strings"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

// Land puts a change on a queue and returns the request id tracking it.
//
// The URIs are one change, in caller order: several of them are a stack landing
// as a single request, not several requests.
func (c *Client) Land(
	ctx context.Context,
	queue string,
	uris []string,
	strategy mergestrategypb.Strategy,
) (string, error) {
	if queue == "" {
		return "", fmt.Errorf("queue must not be empty")
	}
	if len(uris) == 0 {
		return "", fmt.Errorf("at least one change URI is required")
	}

	resp, err := c.gw.Land(ctx, &pb.LandRequest{
		Queue:    queue,
		Change:   &changepb.Change{Uris: uris},
		Strategy: strategy,
	})
	if err != nil {
		return "", fmt.Errorf("land %s failed: %w", strings.Join(uris, ","), err)
	}
	return resp.GetSqid(), nil
}

// ParseStrategy maps a strategy name to its wire value. An empty name selects
// the queue's configured default.
func ParseStrategy(name string) (mergestrategypb.Strategy, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "DEFAULT":
		return mergestrategypb.Strategy_DEFAULT, nil
	case "REBASE":
		return mergestrategypb.Strategy_REBASE, nil
	case "SQUASH_REBASE":
		return mergestrategypb.Strategy_SQUASH_REBASE, nil
	case "MERGE":
		return mergestrategypb.Strategy_MERGE, nil
	case "PROMOTE":
		return mergestrategypb.Strategy_PROMOTE, nil
	default:
		return mergestrategypb.Strategy_DEFAULT, fmt.Errorf("unknown strategy %q", name)
	}
}
