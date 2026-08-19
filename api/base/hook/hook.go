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

// Package hook holds the hook event contract: the wire payload every domain
// publishes to its own hook topic for fire-and-forget lifecycle side effects.
package hook

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/uber/submitqueue/api/base/hook/protopb"
	basemqpb "github.com/uber/submitqueue/api/base/messagequeue/protopb"
)

// HookEvent aliases the generated binding so callers reference the contract
// through this package rather than protopb.
type HookEvent = protopb.HookEvent

// UseProtoNames keeps JSON field names snake_case, matching the declared
// contract rather than protojson's default lowerCamelCase.
var marshalOpts = protojson.MarshalOptions{UseProtoNames: true}

// DiscardUnknown makes an additive contract change backward-compatible: a field
// this consumer does not know yet is ignored rather than rejected.
var unmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// Marshal serializes a contract message to protojson bytes for the queue payload.
func Marshal(m proto.Message) ([]byte, error) {
	return marshalOpts.Marshal(m)
}

// Unmarshal deserializes protojson bytes into the contract message m.
func Unmarshal[T proto.Message](b []byte, m T) error {
	return unmarshalOpts.Unmarshal(b, m)
}

// TopicKeys returns the logical topic keys bound to a message via the
// topic_keys proto option, or nil if it declares none. These are not wire topic
// names; a caller maps each key to its backend's topic.
func TopicKeys(m proto.Message) []string {
	opts := m.ProtoReflect().Descriptor().Options()
	if opts == nil {
		return nil
	}
	keys, ok := proto.GetExtension(opts, basemqpb.E_TopicKeys).([]string)
	if !ok {
		return nil
	}
	return keys
}
