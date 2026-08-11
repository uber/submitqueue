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

package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   Failure
		want Failure
	}{
		{
			name: "subjects and detail",
			in: New("speculator failed", Subject{Type: "queue", ID: "test-queue"}).
				withDetail(map[string]any{"stage": "ask"}),
			want: Failure{
				Subjects: []Subject{{Type: "queue", ID: "test-queue"}},
				Detail:   map[string]any{"stage": "ask"},
			},
		},
		{
			name: "several subjects keep their order",
			in:   New("two at fault", Subject{Type: "batch", ID: "q/batch/2"}, Subject{Type: "batch", ID: "q/batch/1"}),
			want: Failure{Subjects: []Subject{{Type: "batch", ID: "q/batch/2"}, {Type: "batch", ID: "q/batch/1"}}},
		},
		{
			name: "nested detail survives",
			in:   Failure{Detail: map[string]any{"path": map[string]any{"id": "abc"}}},
			want: Failure{Detail: map[string]any{"path": map[string]any{"id": "abc"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Encode(tt.in)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			got, err := Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The message is carried outside the blob so that whatever stores it keeps a
// legible column, and so decoding never has to tell an encoded failure apart
// from a message that happens to look like one.
func TestEncodeOmitsMessage(t *testing.T) {
	encoded, err := Encode(New("boom", Subject{Type: "batch", ID: "q/batch/1"}))
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "boom")

	got, err := Decode(encoded)
	require.NoError(t, err)
	assert.Empty(t, got.Message)
	assert.Equal(t, []Subject{{Type: "batch", ID: "q/batch/1"}}, got.Subjects)
}

// Nothing structured means nothing to store, which is what lets a caller treat
// an absent blob as "unattributed" without a sentinel.
func TestEncodeNothingStructured(t *testing.T) {
	encoded, err := Encode(New("just a message"))
	require.NoError(t, err)
	assert.Nil(t, encoded)
}

func TestDecodeEmpty(t *testing.T) {
	got, err := Decode(nil)
	require.NoError(t, err)
	assert.Equal(t, Failure{}, got)
}

func TestDecodeMalformed(t *testing.T) {
	_, err := Decode([]byte("not json"))
	assert.Error(t, err)
}

// Detail goes through encoding/json, so every number returns as a float64
// whatever its Go type going in. Pinned because a caller that stores an int64
// and reads it back expecting one would otherwise find out at runtime.
func TestDetailNumbersDecodeAsFloat64(t *testing.T) {
	encoded, err := Encode(Failure{Detail: map[string]any{"attempt": int64(3)}})
	require.NoError(t, err)

	got, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, float64(3), got.Detail["attempt"])
}

func TestIDsOfType(t *testing.T) {
	f := New("mixed",
		Subject{Type: "batch", ID: "q/batch/1"},
		Subject{Type: "queue", ID: "q"},
		Subject{Type: "batch", ID: "q/batch/2"},
	)

	assert.Equal(t, []string{"q/batch/1", "q/batch/2"}, f.IDsOfType("batch"))
	assert.Equal(t, []string{"q"}, f.IDsOfType("queue"))
	assert.Empty(t, f.IDsOfType("request"))
	assert.Empty(t, Failure{}.IDsOfType("batch"))
}

// withDetail keeps the table above readable; New covers message and subjects,
// which is what most callers set.
func (f Failure) withDetail(detail map[string]any) Failure {
	f.Detail = detail
	return f
}
