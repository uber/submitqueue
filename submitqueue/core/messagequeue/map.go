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

package messagequeue

import (
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// StartFromLandRequest copies a gateway-owned land request onto the start payload.
func StartFromLandRequest(r entity.LandRequest) *Start {
	return &Start{
		Id:           r.ID,
		Queue:        r.Queue,
		Change:       &changepb.Change{Uris: append([]string{}, r.Change.URIs...)},
		LandStrategy: landStrategyToProto(r.LandStrategy),
	}
}

// LandRequestFromStart copies a start payload onto the gateway-owned land request.
func LandRequestFromStart(m *Start) entity.LandRequest {
	var uris []string
	if m.GetChange() != nil {
		uris = append([]string{}, m.GetChange().GetUris()...)
	}
	return entity.LandRequest{
		ID:           m.GetId(),
		Queue:        m.GetQueue(),
		Change:       change.Change{URIs: uris},
		LandStrategy: landStrategyFromProto(m.GetLandStrategy()),
	}
}

// CancelFromEntity copies a cancellation onto the cancel payload.
func CancelFromEntity(r entity.CancelRequest) *Cancel {
	return &Cancel{Id: r.ID, Queue: r.Queue, Reason: r.Reason}
}

// CancelToEntity copies a cancel payload onto the domain cancellation.
func CancelToEntity(m *Cancel) entity.CancelRequest {
	return entity.CancelRequest{ID: m.GetId(), Queue: m.GetQueue(), Reason: m.GetReason()}
}

// LogFromEntity copies a request-log entry onto the log payload.
func LogFromEntity(r entity.RequestLog) *Log {
	return &Log{
		RequestId:      r.RequestID,
		Queue:          r.Queue,
		TimestampMs:    r.TimestampMs,
		Type:           string(r.Type),
		Status:         string(r.Status),
		Event:          string(r.Event),
		RequestVersion: r.RequestVersion,
		LastError:      r.LastError,
		Metadata:       r.Metadata,
	}
}

// LogToEntity copies a log payload onto a request-log entry. An empty type is
// treated as a status entry, matching entries written before the type field
// existed. A nil metadata map becomes an empty map.
func LogToEntity(m *Log) entity.RequestLog {
	meta := m.GetMetadata()
	if meta == nil {
		meta = make(map[string]string)
	}
	typ := entity.RequestLogType(m.GetType())
	if typ == "" {
		typ = entity.RequestLogTypeStatus
	}
	return entity.RequestLog{
		RequestID:      m.GetRequestId(),
		Queue:          m.GetQueue(),
		TimestampMs:    m.GetTimestampMs(),
		Type:           typ,
		Status:         entity.RequestStatus(m.GetStatus()),
		Event:          entity.RequestEvent(m.GetEvent()),
		RequestVersion: m.GetRequestVersion(),
		LastError:      m.GetLastError(),
		Metadata:       meta,
	}
}

func landStrategyToProto(s mergestrategy.MergeStrategy) strategypb.Strategy {
	switch s {
	case mergestrategy.MergeStrategyRebase:
		return strategypb.Strategy_REBASE
	case mergestrategy.MergeStrategySquashRebase:
		return strategypb.Strategy_SQUASH_REBASE
	case mergestrategy.MergeStrategyMerge:
		return strategypb.Strategy_MERGE
	case mergestrategy.MergeStrategyPromote:
		return strategypb.Strategy_PROMOTE
	default:
		return strategypb.Strategy_DEFAULT
	}
}

func landStrategyFromProto(s strategypb.Strategy) mergestrategy.MergeStrategy {
	switch s {
	case strategypb.Strategy_REBASE:
		return mergestrategy.MergeStrategyRebase
	case strategypb.Strategy_SQUASH_REBASE:
		return mergestrategy.MergeStrategySquashRebase
	case strategypb.Strategy_MERGE:
		return mergestrategy.MergeStrategyMerge
	case strategypb.Strategy_PROMOTE:
		return mergestrategy.MergeStrategyPromote
	default:
		return mergestrategy.MergeStrategyUnknown
	}
}
