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

// Package messagequeue holds Stovepipe's internal message-queue contract: the
// wire payloads for the pipeline queues Stovepipe owns, defined by the proto
// files in proto/ and generated into protopb/. The proto is the language-neutral
// authority; the generated Go types in protopb are the binding for Go callers.
//
// It is internal — used only within the Stovepipe domain — so it lives under
// stovepipe/core rather than api/. The message types are generated into protopb;
// this package adds only generic protojson glue (Marshal/Unmarshal) and the
// topic-key reflection lookup (TopicKeys), so there is no per-message
// serialization code. Payloads are serialized as protobuf JSON, not binary, so
// the MySQL-backed queue keeps storing self-describing JSON. The topic key that
// carries each payload is declared on the message itself via the topic_keys
// proto option (see api/base/messagequeue).
package messagequeue

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	basemqpb "github.com/uber/submitqueue/api/base/messagequeue/protopb"
	"github.com/uber/submitqueue/stovepipe/core/messagequeue/protopb"
)

// Wire payload types. These alias the generated protobuf bindings so callers
// reference the contract through this curated package rather than protopb.
type (
	// ProcessRequest is the payload ingest publishes to the process stage: the
	// minted request id to validate.
	ProcessRequest = protopb.ProcessRequest

	// BuildRequest is the payload process publishes to the build stage: the
	// admitted request id whose persisted strategy and baseline build reloads.
	BuildRequest = protopb.BuildRequest
)

// marshalOpts keeps the JSON field names identical to the proto field names
// (snake_case), so the wire shape matches the declared contract rather than
// protojson's default lowerCamelCase. Zero-valued fields are omitted.
var marshalOpts = protojson.MarshalOptions{UseProtoNames: true}

// unmarshalOpts tolerates unknown fields so an additive contract change (a new
// field a producer sends but this consumer does not yet know) is ignored rather
// than rejected.
var unmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// Marshal serializes any contract message to protojson bytes for the queue
// payload, keeping the proto field names (snake_case) on the wire.
func Marshal(m proto.Message) ([]byte, error) {
	return marshalOpts.Marshal(m)
}

// Unmarshal deserializes protojson bytes into the contract message m, tolerating
// unknown fields so an additive contract change is ignored rather than rejected.
func Unmarshal[T proto.Message](b []byte, m T) error {
	return unmarshalOpts.Unmarshal(b, m)
}

// TopicKeys returns the stable logical topic keys bound to a message via the
// topic_keys proto option — not concrete wire names; a caller maps each key to
// its backend's topic name. Returns nil for a message that declares no keys.
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
