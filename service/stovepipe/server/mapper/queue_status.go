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

package mapper

import (
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func ProtoToGetQueueStatusRequest(req *pb.GetQueueStatusRequest) entity.GetQueueStatusRequest {
	return entity.GetQueueStatusRequest{Queue: req.GetQueue(), ChangeURI: req.GetChangeUri()}
}

func GetQueueStatusResultToProto(result entity.GetQueueStatusResult) *pb.GetQueueStatusResponse {
	return &pb.GetQueueStatusResponse{
		Queue: result.Queue, Enablement: queueEnablementStatusToProto(result.Enablement),
	}
}

func queueEnablementStatusToProto(applied entity.QueueState) *pb.QueueEnablementStatus {
	state := pb.QueueEnablement_QUEUE_ENABLEMENT_UNKNOWN
	switch applied.State {
	case entity.QueueEnablementEnabled:
		state = pb.QueueEnablement_QUEUE_ENABLEMENT_ENABLED
	case entity.QueueEnablementDisabled:
		state = pb.QueueEnablement_QUEUE_ENABLEMENT_DISABLED
	}
	return &pb.QueueEnablementStatus{Version: applied.Version, State: state, EffectiveFromCommitUri: applied.EffectiveFromCommitURI, ChangedAtMs: applied.ChangedAtMs}
}
