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
	"github.com/uber/submitqueue/submitqueue/extension/mergechecker"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ mergechecker.MergeChecker = New()
}

func TestChecker_Check(t *testing.T) {
	tests := []struct {
		name          string
		uris          []string
		wantMergeable bool
		wantErr       bool
	}{
		{
			name:          "no marker is mergeable",
			uris:          []string{"github://owner/repo/pull/1/abc"},
			wantMergeable: true,
		},
		{
			name:          "no URIs is mergeable",
			uris:          nil,
			wantMergeable: true,
		},
		{
			name: "unmergeable marker",
			uris: []string{"github://owner/repo/pull/1/abc?sq-fake=unmergeable"},
		},
		{
			name:    "error marker",
			uris:    []string{"github://owner/repo/pull/1/abc?sq-fake=mergecheck-error"},
			wantErr: true,
		},
		{
			name: "marker on second uri",
			uris: []string{
				"github://owner/repo/pull/1/abc",
				"github://owner/repo/pull/2/def?sq-fake=unmergeable",
			},
		},
	}

	c := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := c.Check(context.Background(), entity.Request{Change: change.Change{URIs: tt.uris}})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMergeable, res.Mergeable)
		})
	}
}
