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
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/uber/submitqueue/submitqueue/entity"
)

type idPayload interface {
	proto.Message
	GetId() string
	GetQueue() string
}

func idOnlyMessage(key TopicKey, id, queue string) (idPayload, error) {
	switch key {
	case TopicKeyValidate:
		return &Validate{Id: id, Queue: queue}, nil
	case TopicKeyBatch:
		return &Batch{Id: id, Queue: queue}, nil
	case TopicKeyDependencyAnalysis:
		return &DependencyAnalysis{Id: id, Queue: queue}, nil
	case TopicKeySpeculate:
		return &Speculate{Id: id, Queue: queue}, nil
	case TopicKeyBuild:
		return &Build{Id: id, Queue: queue}, nil
	case TopicKeyBuildSignal:
		return &BuildSignal{Id: id, Queue: queue}, nil
	case TopicKeyLand:
		return &Merge{Id: id, Queue: queue}, nil
	case TopicKeyConclude:
		return &Conclude{Id: id, Queue: queue}, nil
	default:
		return nil, fmt.Errorf("topic %q does not carry an id-only payload", key)
	}
}

// MarshalID serializes the id-only payload bound to key. Start, cancel, and log
// are not id-only; callers marshal those messages directly.
func MarshalID(key TopicKey, id, queue string) ([]byte, error) {
	m, err := idOnlyMessage(key, id, queue)
	if err != nil {
		return nil, err
	}
	return Marshal(m)
}

// UnmarshalID reads id and queue from the id-only payload bound to key.
// Consumers pass the topic they subscribe to so a field added to that message
// is decoded rather than discarded as unknown on a different type.
func UnmarshalID(key TopicKey, b []byte) (id, queue string, err error) {
	m, err := idOnlyMessage(key, "", "")
	if err != nil {
		return "", "", err
	}
	if err := Unmarshal(b, m); err != nil {
		return "", "", err
	}
	return m.GetId(), m.GetQueue(), nil
}

// UnmarshalLandRequest reads a start payload into the gateway-owned land request.
func UnmarshalLandRequest(b []byte) (entity.LandRequest, error) {
	m := &Start{}
	if err := Unmarshal(b, m); err != nil {
		return entity.LandRequest{}, err
	}
	return LandRequestFromStart(m), nil
}

// UnmarshalCancelRequest reads a cancel payload into the domain cancellation.
func UnmarshalCancelRequest(b []byte) (entity.CancelRequest, error) {
	m := &Cancel{}
	if err := Unmarshal(b, m); err != nil {
		return entity.CancelRequest{}, err
	}
	return CancelToEntity(m), nil
}

// UnmarshalRequestLog reads a log payload into a request-log entry.
func UnmarshalRequestLog(b []byte) (entity.RequestLog, error) {
	m := &Log{}
	if err := Unmarshal(b, m); err != nil {
		return entity.RequestLog{}, err
	}
	return LogToEntity(m), nil
}

// UnmarshalRequestID reads a request-scoped id-only payload (validate or batch).
func UnmarshalRequestID(key TopicKey, b []byte) (entity.RequestID, error) {
	switch key {
	case TopicKeyValidate, TopicKeyBatch:
	default:
		return entity.RequestID{}, fmt.Errorf("topic %q does not carry a request-id payload", key)
	}
	id, queue, err := UnmarshalID(key, b)
	if err != nil {
		return entity.RequestID{}, err
	}
	return entity.RequestID{ID: id, Queue: queue}, nil
}

// UnmarshalBatchID reads a batch-scoped id-only payload.
func UnmarshalBatchID(key TopicKey, b []byte) (entity.BatchID, error) {
	switch key {
	case TopicKeyDependencyAnalysis, TopicKeySpeculate, TopicKeyBuild, TopicKeyLand, TopicKeyConclude:
	default:
		return entity.BatchID{}, fmt.Errorf("topic %q does not carry a batch-id payload", key)
	}
	id, queue, err := UnmarshalID(key, b)
	if err != nil {
		return entity.BatchID{}, err
	}
	return entity.BatchID{ID: id, Queue: queue}, nil
}

// UnmarshalBuildID reads a buildsignal payload.
func UnmarshalBuildID(key TopicKey, b []byte) (entity.BuildID, error) {
	if key != TopicKeyBuildSignal {
		return entity.BuildID{}, fmt.Errorf("topic %q does not carry a build-id payload", key)
	}
	id, queue, err := UnmarshalID(key, b)
	if err != nil {
		return entity.BuildID{}, err
	}
	return entity.BuildID{ID: id, Queue: queue}, nil
}
