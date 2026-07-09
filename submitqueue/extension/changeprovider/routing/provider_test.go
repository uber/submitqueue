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

package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/entity"
	changeprovmock "github.com/uber/submitqueue/submitqueue/extension/changeprovider/mock"
	"go.uber.org/mock/gomock"
)

const (
	githubURI1 = "github://github.example.com/uber/repo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	githubURI2 = "github://github.example.com/uber/repo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	phabURI1   = "phab://phab.example.com/D123/456"
	phabURI2   = "phab://phab.example.com/D789/101"
	gitURI1    = "git://remote.example.com/uber/repo/refs%2Fheads%2Fmain/cccccccccccccccccccccccccccccccccccccccc"
)

func newRequest(uris ...string) entity.Request {
	return entity.Request{
		ID:    "test-queue/1",
		Queue: "test-queue",
		Change: change.Change{
			URIs: uris,
		},
	}
}

func changeInfoFor(uri string) entity.ChangeInfo {
	return entity.ChangeInfo{URI: uri}
}

func TestNewProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	ghProvider := changeprovmock.NewMockChangeProvider(ctrl)

	tests := []struct {
		name       string
		params     Params
		wantErrMsg string
	}{
		{
			name:   "github only",
			params: Params{GitHub: ghProvider},
		},
		{
			name:       "all nil returns error",
			params:     Params{},
			wantErrMsg: "at least one change provider must be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProvider(tt.params)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, got)
		})
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		name       string
		uris       []string
		setup      func(gh, phab *changeprovmock.MockChangeProvider)
		params     func(gh, phab *changeprovmock.MockChangeProvider) Params
		want       []entity.ChangeInfo
		wantErrMsg string
	}{
		{
			name: "single provider all URIs",
			uris: []string{githubURI1, githubURI2},
			setup: func(gh, _ *changeprovmock.MockChangeProvider) {
				gh.EXPECT().Get(gomock.Any(), gomock.Any()).Return(
					[]entity.ChangeInfo{changeInfoFor(githubURI1), changeInfoFor(githubURI2)}, nil,
				)
			},
			params: func(gh, _ *changeprovmock.MockChangeProvider) Params {
				return Params{GitHub: gh}
			},
			want: []entity.ChangeInfo{changeInfoFor(githubURI1), changeInfoFor(githubURI2)},
		},
		{
			name: "mixed URIs routed to different providers and results reassembled in order",
			uris: []string{githubURI1, phabURI1, githubURI2, phabURI2},
			setup: func(gh, phab *changeprovmock.MockChangeProvider) {
				gh.EXPECT().Get(gomock.Any(), newRequest(githubURI1, githubURI2)).Return(
					[]entity.ChangeInfo{changeInfoFor(githubURI1), changeInfoFor(githubURI2)}, nil,
				)
				phab.EXPECT().Get(gomock.Any(), newRequest(phabURI1, phabURI2)).Return(
					[]entity.ChangeInfo{changeInfoFor(phabURI1), changeInfoFor(phabURI2)}, nil,
				)
			},
			params: func(gh, phab *changeprovmock.MockChangeProvider) Params {
				return Params{GitHub: gh, Phabricator: phab}
			},
			want: []entity.ChangeInfo{
				changeInfoFor(githubURI1),
				changeInfoFor(phabURI1),
				changeInfoFor(githubURI2),
				changeInfoFor(phabURI2),
			},
		},
		{
			name: "downstream provider error is propagated",
			uris: []string{githubURI1},
			setup: func(gh, _ *changeprovmock.MockChangeProvider) {
				gh.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("github API error"))
			},
			params: func(gh, _ *changeprovmock.MockChangeProvider) Params {
				return Params{GitHub: gh}
			},
			wantErrMsg: "github API error",
		},
		{
			name: "provider returns wrong result count",
			uris: []string{githubURI1, githubURI2},
			setup: func(gh, _ *changeprovmock.MockChangeProvider) {
				gh.EXPECT().Get(gomock.Any(), gomock.Any()).Return(
					[]entity.ChangeInfo{changeInfoFor(githubURI1)}, nil,
				)
			},
			params: func(gh, _ *changeprovmock.MockChangeProvider) Params {
				return Params{GitHub: gh}
			},
			wantErrMsg: "provider returned 1 results for 2 URIs",
		},
		{
			name:       "unrecognized URI fails before calling any provider",
			uris:       []string{"bogus://nope"},
			setup:      func(_, _ *changeprovmock.MockChangeProvider) {},
			params:     func(gh, _ *changeprovmock.MockChangeProvider) Params { return Params{GitHub: gh} },
			wantErrMsg: "does not match any known change type",
		},
		{
			name:  "empty URIs returns empty results",
			uris:  nil,
			setup: func(_, _ *changeprovmock.MockChangeProvider) {},
			params: func(gh, _ *changeprovmock.MockChangeProvider) Params {
				return Params{GitHub: gh}
			},
			want: []entity.ChangeInfo{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ghMock := changeprovmock.NewMockChangeProvider(ctrl)
			phabMock := changeprovmock.NewMockChangeProvider(ctrl)

			tt.setup(ghMock, phabMock)
			p, err := NewProvider(tt.params(ghMock, phabMock))
			require.NoError(t, err)

			got, err := p.Get(context.Background(), newRequest(tt.uris...))
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGroupChangesByProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	ghProvider := changeprovmock.NewMockChangeProvider(ctrl)
	phabProvider := changeprovmock.NewMockChangeProvider(ctrl)
	gitProvider := changeprovmock.NewMockChangeProvider(ctrl)

	tests := []struct {
		name       string
		params     Params
		uris       []string
		wantGroups map[changeprovider.ChangeProvider][]matchedURI
		wantErrMsg string
	}{
		{
			name:   "all github URIs grouped together",
			params: Params{GitHub: ghProvider},
			uris:   []string{githubURI1, githubURI2},
			wantGroups: map[changeprovider.ChangeProvider][]matchedURI{
				ghProvider: {
					{index: 0, uri: githubURI1},
					{index: 1, uri: githubURI2},
				},
			},
		},
		{
			name:   "mixed github and phabricator",
			params: Params{GitHub: ghProvider, Phabricator: phabProvider},
			uris:   []string{githubURI1, phabURI1, githubURI2},
			wantGroups: map[changeprovider.ChangeProvider][]matchedURI{
				ghProvider: {
					{index: 0, uri: githubURI1},
					{index: 2, uri: githubURI2},
				},
				phabProvider: {
					{index: 1, uri: phabURI1},
				},
			},
		},
		{
			name:   "all three change types",
			params: Params{GitHub: ghProvider, Phabricator: phabProvider, Git: gitProvider},
			uris:   []string{phabURI1, gitURI1, githubURI1},
			wantGroups: map[changeprovider.ChangeProvider][]matchedURI{
				phabProvider: {
					{index: 0, uri: phabURI1},
				},
				gitProvider: {
					{index: 1, uri: gitURI1},
				},
				ghProvider: {
					{index: 2, uri: githubURI1},
				},
			},
		},
		{
			name:       "match error is propagated",
			params:     Params{GitHub: ghProvider},
			uris:       []string{githubURI1, "bogus://unknown/uri"},
			wantErrMsg: "does not match any known change type",
		},
		{
			name:       "empty URI list returns empty groups",
			params:     Params{GitHub: ghProvider},
			uris:       nil,
			wantGroups: map[changeprovider.ChangeProvider][]matchedURI{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &provider{
				github:      tt.params.GitHub,
				phabricator: tt.params.Phabricator,
				git:         tt.params.Git,
			}

			got, err := p.groupChangesByProvider(tt.uris)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantGroups, got)
		})
	}
}

func TestMatchURIToChangeProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	ghProvider := changeprovmock.NewMockChangeProvider(ctrl)
	phabProvider := changeprovmock.NewMockChangeProvider(ctrl)
	gitProvider := changeprovmock.NewMockChangeProvider(ctrl)

	tests := []struct {
		name         string
		params       Params
		uri          string
		wantProvider changeprovider.ChangeProvider
		wantErrMsg   string
	}{
		{
			name:         "github URI matches github provider",
			params:       Params{GitHub: ghProvider, Phabricator: phabProvider, Git: gitProvider},
			uri:          githubURI1,
			wantProvider: ghProvider,
		},
		{
			name:         "phabricator URI matches phabricator provider",
			params:       Params{GitHub: ghProvider, Phabricator: phabProvider, Git: gitProvider},
			uri:          phabURI1,
			wantProvider: phabProvider,
		},
		{
			name:         "git URI matches git provider",
			params:       Params{GitHub: ghProvider, Phabricator: phabProvider, Git: gitProvider},
			uri:          gitURI1,
			wantProvider: gitProvider,
		},
		{
			name:       "unrecognized URI returns error",
			params:     Params{GitHub: ghProvider},
			uri:        "bogus://unknown/uri",
			wantErrMsg: "does not match any known change type",
		},
		{
			name:       "github URI with nil github provider returns error",
			params:     Params{Phabricator: phabProvider},
			uri:        githubURI1,
			wantErrMsg: "no GitHub provider is configured",
		},
		{
			name:       "phabricator URI with nil phabricator provider returns error",
			params:     Params{GitHub: ghProvider},
			uri:        phabURI1,
			wantErrMsg: "no Phabricator provider is configured",
		},
		{
			name:       "git URI with nil git provider returns error",
			params:     Params{GitHub: ghProvider},
			uri:        gitURI1,
			wantErrMsg: "no git provider is configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &provider{
				github:      tt.params.GitHub,
				phabricator: tt.params.Phabricator,
				git:         tt.params.Git,
			}

			got, err := p.matchURIToChangeProvider(tt.uri)
			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantProvider, got)
		})
	}
}
