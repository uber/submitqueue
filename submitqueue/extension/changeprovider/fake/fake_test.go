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

// Without this the path-keyed conflict analyzers see a change that touches
// nothing, and a batch that touches nothing conflicts with nothing — so a queue
// configured to serialize on overlap silently runs everything in parallel.
func TestProvider_Get_ReportsFilesFromTheURI(t *testing.T) {
	p := New(testCfg)

	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"git://git.example.com/sandbox/refs%2Fheads%2Fa/abc?sq-files=demo/alpha/one.txt,demo/alpha/two.txt"},
	}})
	require.NoError(t, err)
	require.Len(t, infos, 1)

	paths := make([]string, 0, len(infos[0].Details.ChangedFiles))
	for _, f := range infos[0].Details.ChangedFiles {
		paths = append(paths, f.Path)
	}
	assert.Equal(t, []string{"demo/alpha/one.txt", "demo/alpha/two.txt"}, paths)
}

func TestProvider_Get_ReportsNoFilesWithoutTheMarker(t *testing.T) {
	p := New(testCfg)

	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"git://git.example.com/sandbox/refs%2Fheads%2Fa/abc"},
	}})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Empty(t, infos[0].Details.ChangedFiles)
}
