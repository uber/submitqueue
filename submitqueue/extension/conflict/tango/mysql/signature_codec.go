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

package mysql

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
)

// idBytes is the width of an encoded target ID.
const idBytes = 8

// encodeSignatureTargets packs each target as its ID (8 big-endian bytes) and a
// signed varint distance, in the order given. The result is never nil, because a
// nil slice would be written as NULL into a NOT NULL column.
func encodeSignatureTargets(targets []tango.SignatureTarget) []byte {
	encoded := make([]byte, 0, len(targets)*(idBytes+1))
	for _, target := range targets {
		encoded = binary.BigEndian.AppendUint64(encoded, target.ID)
		encoded = binary.AppendVarint(encoded, int64(target.Distance))
	}
	return encoded
}

// decodeSignatureTargets reverses encodeSignatureTargets. It returns an error on
// truncated input or a distance outside the int32 range.
func decodeSignatureTargets(encoded []byte) ([]tango.SignatureTarget, error) {
	var targets []tango.SignatureTarget
	for offset := 0; offset < len(encoded); {
		if len(encoded)-offset < idBytes {
			return nil, fmt.Errorf("truncated target ID at offset %d of %d bytes", offset, len(encoded))
		}
		id := binary.BigEndian.Uint64(encoded[offset:])
		offset += idBytes

		distance, read := binary.Varint(encoded[offset:])
		if read <= 0 {
			return nil, fmt.Errorf("invalid distance varint at offset %d of %d bytes", offset, len(encoded))
		}
		offset += read
		if distance < math.MinInt32 || distance > math.MaxInt32 {
			return nil, fmt.Errorf("distance %d for target ID %d is outside the int32 range", distance, id)
		}

		targets = append(targets, tango.SignatureTarget{ID: id, Distance: int32(distance)})
	}
	return targets, nil
}
