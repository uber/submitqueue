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
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// testCfg is the per-queue identity used by every case in this file.
var testCfg = changeprovider.Config{QueueName: "test-queue"}

func TestNew_ImplementsInterface(t *testing.T) {
	var _ changeprovider.ChangeProvider = New(testCfg)
}

func TestProvider_Get_OnePerURI(t *testing.T) {
	tests := []struct {
		name string
		uris []string
	}{
		{name: "nil URIs", uris: nil},
		{name: "single URI", uris: []string{"github://github.example.com/owner/repo/pull/1/abc"}},
		{
			name: "multiple URIs (stack)",
			uris: []string{
				"github://github.example.com/owner/repo/pull/1/abc",
				"github://github.example.com/owner/repo/pull/2/def",
			},
		},
	}

	p := New(testCfg)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{URIs: tt.uris}})
			require.NoError(t, err)
			require.Len(t, infos, len(tt.uris))
			for i, uri := range tt.uris {
				assert.Equal(t, uri, infos[i].URI)
			}
		})
	}
}

func TestProvider_Get_ErrorMarker(t *testing.T) {
	p := New(testCfg)
	_, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"github://github.example.com/owner/repo/pull/1/abc?sq-fake=provider-error"},
	}})
	require.Error(t, err)
}

func TestProvider_Get_SyntheticFilesAreStable(t *testing.T) {
	ctx := context.Background()
	request := entity.Request{Change: change.Change{URIs: []string{
		"git://demo.example.com/demo/refs%2Fheads%2Fa/abc",
		"git://demo.example.com/demo/refs%2Fheads%2Fb/def",
	}}}
	first, err := New(testCfg).Get(ctx, request)
	require.NoError(t, err)
	for _, info := range first {
		require.NotEmpty(t, info.Details.ChangedFiles)
		for _, file := range info.Details.ChangedFiles {
			assert.NotEmpty(t, file.Path)
			assert.Positive(t, file.LinesAdded)
		}
	}
	for range 2 {
		again, err := New(testCfg).Get(ctx, request)
		require.NoError(t, err)
		assert.Equal(t, first, again)
	}
	request.Change.URIs[0], request.Change.URIs[1] = request.Change.URIs[1], request.Change.URIs[0]
	reordered, err := New(testCfg).Get(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, []entity.ChangeInfo{first[1], first[0]}, reordered)
}

func TestProvider_Get_ProducesOverlappingAndIndependentDirectories(t *testing.T) {
	var uris []string
	for i := range 64 {
		uris = append(uris, fmt.Sprintf("git://demo.example.com/demo/ref/%040d", i))
	}
	infos, err := New(testCfg).Get(context.Background(), entity.Request{Change: change.Change{URIs: uris}})
	require.NoError(t, err)
	directories := make(map[string]int)
	for _, info := range infos {
		require.NotEmpty(t, info.Details.ChangedFiles)
		directories[path.Dir(info.Details.ChangedFiles[0].Path)]++
	}
	assert.Greater(t, len(directories), 1, "some changes must be independent")
	assert.Less(t, len(directories), len(uris), "some changes must overlap")
}
