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
	"github.com/uber/submitqueue/platform/base/change"
)

const (
	testURI   = "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/abcdef0123456789abcdef0123456789abcdef01"
	testSPID  = "stovepipe-monorepo/1"
	testQueue = "stovepipe-monorepo"
)

func validIngestRequest() IngestRequest {
	return IngestRequest{
		ID:     testSPID,
		Queue:  testQueue,
		Change: change.Change{URIs: []string{testURI}},
	}
}

func TestIngestRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     IngestRequest
		wantErr bool
	}{
		{name: "valid", req: validIngestRequest()},
		{name: "missing id", req: IngestRequest{Queue: testQueue, Change: change.Change{URIs: []string{testURI}}}, wantErr: true},
		{name: "missing queue", req: IngestRequest{ID: testSPID, Change: change.Change{URIs: []string{testURI}}}, wantErr: true},
		{name: "no uris", req: IngestRequest{ID: testSPID, Queue: testQueue}, wantErr: true},
		{name: "empty uri", req: IngestRequest{ID: testSPID, Queue: testQueue, Change: change.Change{URIs: []string{""}}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestIngestRequestFromBytes(t *testing.T) {
	t.Run("round-trips a valid request", func(t *testing.T) {
		original := validIngestRequest()
		data, err := original.ToBytes()
		require.NoError(t, err)

		got, err := IngestRequestFromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, original, got)
	})

	t.Run("rejects invalid json", func(t *testing.T) {
		_, err := IngestRequestFromBytes([]byte(`{"invalid": json"}`))
		require.Error(t, err)
	})

	t.Run("rejects a payload that fails validation", func(t *testing.T) {
		_, err := IngestRequestFromBytes([]byte(`{"id":"x","queue":"q","change":{"uris":[]}}`))
		require.Error(t, err)
	})
}
