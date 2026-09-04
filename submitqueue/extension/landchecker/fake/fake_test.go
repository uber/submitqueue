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

package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/landchecker"
)

// testCfg is the per-queue identity used by every case in this file.
var testCfg = landchecker.Config{QueueName: "test-queue"}

func TestNew_ImplementsInterface(t *testing.T) {
	var _ landchecker.LandChecker = New(testCfg)
}

func TestChecker_Check(t *testing.T) {
	tests := []struct {
		name         string
		uris         []string
		wantLandable bool
		wantErr      bool
	}{
		{
			name:         "no marker is landable",
			uris:         []string{"github://github.example.com/owner/repo/pull/1/abc"},
			wantLandable: true,
		},
		{
			name:         "no URIs is landable",
			uris:         nil,
			wantLandable: true,
		},
		{
			name: "unlandable marker",
			uris: []string{"github://github.example.com/owner/repo/pull/1/abc?sq-fake=unlandable"},
		},
		{
			name:    "error marker",
			uris:    []string{"github://github.example.com/owner/repo/pull/1/abc?sq-fake=landcheck-error"},
			wantErr: true,
		},
		{
			name: "marker on second uri",
			uris: []string{
				"github://github.example.com/owner/repo/pull/1/abc",
				"github://github.example.com/owner/repo/pull/2/def?sq-fake=unlandable",
			},
		},
	}

	c := New(testCfg)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := c.Check(context.Background(), entity.Request{Change: change.Change{URIs: tt.uris}})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantLandable, res.Landable)
		})
	}
}
