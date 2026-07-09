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

func TestNew_ImplementsInterface(t *testing.T) {
	var _ changeprovider.ChangeProvider = New()
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

	p := New()
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
	p := New()
	_, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"github://github.example.com/owner/repo/pull/1/abc?sq-fake=provider-error"},
	}})
	require.Error(t, err)
}
