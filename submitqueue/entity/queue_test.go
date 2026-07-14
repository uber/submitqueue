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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueID_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		queueID QueueID
	}{
		{
			name:    "simple queue name",
			queueID: QueueID{Name: "queueA"},
		},
		{
			name:    "another queue name",
			queueID: QueueID{Name: "queueB"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.queueID.ToBytes()
			require.NoError(t, err)

			deserialized, err := QueueIDFromBytes(data)
			require.NoError(t, err)

			assert.Equal(t, tt.queueID, deserialized)
		})
	}
}

func TestQueueIDFromBytes_InvalidJSON(t *testing.T) {
	_, err := QueueIDFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}

func TestQueueIDFromBytes_EmptyJSON(t *testing.T) {
	queueID, err := QueueIDFromBytes([]byte(`{}`))
	require.NoError(t, err)

	assert.Empty(t, queueID.Name)
}

func TestQueueIDFromBytes_EmptyBytes(t *testing.T) {
	_, err := QueueIDFromBytes([]byte{})
	assert.Error(t, err)
}

func TestQueueIDFromBytes_NilBytes(t *testing.T) {
	_, err := QueueIDFromBytes(nil)
	assert.Error(t, err)
}
