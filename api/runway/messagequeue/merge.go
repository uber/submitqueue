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

// Package messagequeue holds Runway's external message-queue contract: the wire
// payloads for the merge queues Runway owns, defined by the proto files in
// proto/ and generated into protopb/. The proto is the language-neutral
// authority; the generated Go types in protopb are the binding for Go callers.
//
// The message types are generated into protopb; this package adds only generic
// protojson glue (Marshal/Unmarshal) and the topic-key reflection lookup
// (TopicKeys), so there is no per-message serialization code. Payloads are
// serialized as protobuf JSON, not binary, so the MySQL-backed queue keeps
// storing self-describing JSON. The topic key that carries each payload is
// declared on the message itself via the topic_keys proto option (see
// api/base/messagequeue).
//
// One contract serves two queue pairs because a merge-conflict check is a dry
// run of a merge: Runway applies the same ordered steps onto the same target
// branch, and the only difference is whether it commits the result and reports
// the revisions it produced. The topic key a request arrives on encodes that choice
// — the merge-conflict-check pair for a dry run, the merge pair for a
// committing merge — so MergeRequest and MergeResult are identical on both.
package messagequeue

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	basemqpb "github.com/uber/submitqueue/api/base/messagequeue/protopb"
	"github.com/uber/submitqueue/api/runway/messagequeue/protopb"
)

// Wire payload types. These alias the generated protobuf bindings so callers
// reference the contract through this curated package rather than protopb.
type (
	// MergeRequest is the payload a client publishes to one of Runway's merge
	// queues: the merge-conflict-check topic for a dry-run check, the merge
	// topic for a committing merge.
	MergeRequest = protopb.MergeRequest
	// MergeStep is one step of an ordered merge: a single set of change(s)
	// applied with a strategy.
	MergeStep = protopb.MergeStep
	// MergeResult is the payload Runway publishes to the corresponding signal
	// queue once a request completes.
	MergeResult = protopb.MergeResult
	// StepResult reports what happened to a single MergeStep.
	StepResult = protopb.StepResult
	// StepOutput is a single revision a merge step produced on the merge target.
	StepOutput = protopb.StepOutput
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
