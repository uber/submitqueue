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

// Package mapper translates gateway wire (proto) types to and from the domain
// entities the controllers operate on. Each RPC gets its own file (land.go,
// status.go, cancel.go, …); translation lives here so controllers stay
// proto-free.
package mapper

import (
	"errors"
	"fmt"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// errUnknownStrategy is returned when a proto Strategy enum has no known
// mergestrategy.MergeStrategy mapping.
var errUnknownStrategy = errors.New("unknown land strategy in proto message")

// ProtoToLandRequest maps the wire LandRequest to the entity.LandRequest the controller operates on.
// The ID is left empty; the controller assigns it.
func ProtoToLandRequest(req *pb.LandRequest) (entity.LandRequest, error) {
	strategy, err := resolveMergeStrategy(req.GetStrategy())
	if err != nil {
		return entity.LandRequest{}, fmt.Errorf("failed to map land strategy: %w", err)
	}
	return entity.LandRequest{
		Queue:        req.GetQueue(),
		Change:       change.Change{URIs: req.GetChange().GetUris()},
		LandStrategy: strategy,
	}, nil
}

// resolveMergeStrategy maps a proto Strategy enum to the shared mergestrategy.MergeStrategy.
func resolveMergeStrategy(s mergestrategypb.Strategy) (mergestrategy.MergeStrategy, error) {
	switch s {
	case mergestrategypb.Strategy_DEFAULT:
		// TODO: resolve default strategy based on queue configuration
		return mergestrategy.MergeStrategyRebase, nil
	case mergestrategypb.Strategy_REBASE:
		return mergestrategy.MergeStrategyRebase, nil
	case mergestrategypb.Strategy_SQUASH_REBASE:
		return mergestrategy.MergeStrategySquashRebase, nil
	case mergestrategypb.Strategy_MERGE:
		return mergestrategy.MergeStrategyMerge, nil
	default:
		return mergestrategy.MergeStrategyUnknown, fmt.Errorf("%w: %v", errUnknownStrategy, s)
	}
}
