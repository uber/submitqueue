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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
)

func TestSignatureTargetsCodec_RoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		targets []tango.SignatureTarget
	}{
		{
			name:    "empty",
			targets: nil,
		},
		{
			name:    "single target",
			targets: []tango.SignatureTarget{{ID: 1, Distance: 0}},
		},
		{
			name: "several targets in ascending ID order",
			targets: []tango.SignatureTarget{
				{ID: 0x735cc8dafbb93f02, Distance: 0},
				{ID: 0x90afc9166e08f639, Distance: 1},
				{ID: 0xfcf7cdab318e139e, Distance: 2},
			},
		},
		{
			name: "extreme IDs and distances",
			targets: []tango.SignatureTarget{
				{ID: 0, Distance: math.MaxInt32},
				{ID: math.MaxUint64, Distance: 63},
				{ID: math.MaxUint64 - 1, Distance: 64},
			},
		},
		{
			name:    "negative distance",
			targets: []tango.SignatureTarget{{ID: 7, Distance: -1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeSignatureTargets(encodeSignatureTargets(tt.targets))
			require.NoError(t, err)
			assert.Equal(t, tt.targets, got)
		})
	}
}

func TestEncodeSignatureTargets_EmptyIsNotNil(t *testing.T) {
	assert.NotNil(t, encodeSignatureTargets(nil))
	assert.Empty(t, encodeSignatureTargets(nil))
}

func TestEncodeSignatureTargets_Layout(t *testing.T) {
	got := encodeSignatureTargets([]tango.SignatureTarget{{ID: 0x0102030405060708, Distance: 2}})

	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x04}, got)
}

func TestDecodeSignatureTargets_Invalid(t *testing.T) {
	idOnly := binary.BigEndian.AppendUint64(nil, 1)

	tests := []struct {
		name    string
		encoded []byte
	}{
		{
			name:    "truncated ID",
			encoded: []byte{0x01, 0x02, 0x03},
		},
		{
			name:    "ID without a distance",
			encoded: idOnly,
		},
		{
			name:    "unterminated distance varint",
			encoded: append(append([]byte{}, idOnly...), 0x80),
		},
		{
			name:    "distance outside int32",
			encoded: binary.AppendVarint(append([]byte{}, idOnly...), math.MaxInt32+1),
		},
		{
			name:    "valid target followed by a truncated one",
			encoded: append(binary.AppendVarint(append([]byte{}, idOnly...), 0), 0x01, 0x02),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeSignatureTargets(tt.encoded)
			assert.Error(t, err)
		})
	}
}
