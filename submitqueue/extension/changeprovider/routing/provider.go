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

// Package routing provides a ChangeProvider that dispatches URIs to
// downstream change providers based on the URI's change type.
package routing

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/platform/base/change/git"
	"github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/platform/base/change/phabricator"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// Params holds the optional downstream providers, keyed by change type.
// At least one must be non-nil.
type Params struct {
	// GitHub handles URIs that parse as GitHub change IDs.
	GitHub changeprovider.ChangeProvider
	// Phabricator handles URIs that parse as Phabricator change IDs.
	Phabricator changeprovider.ChangeProvider
	// Git handles URIs that parse as git change IDs.
	Git changeprovider.ChangeProvider
}

// matchedURI pairs a URI with its original position in the input slice.
type matchedURI struct {
	// index is the position of this URI in the original request.Change.URIs slice.
	index int
	// uri is the raw URI string.
	uri string
}

type provider struct {
	github      changeprovider.ChangeProvider
	phabricator changeprovider.ChangeProvider
	git         changeprovider.ChangeProvider
}

// NewProvider creates a ChangeProvider that routes URIs to the appropriate
// downstream provider based on the URI's change type.
// Returns an error if all providers are nil.
func NewProvider(params Params) (changeprovider.ChangeProvider, error) {
	if params.GitHub == nil && params.Phabricator == nil && params.Git == nil {
		return nil, fmt.Errorf("at least one change provider must be configured")
	}
	return &provider{
		github:      params.GitHub,
		phabricator: params.Phabricator,
		git:         params.Git,
	}, nil
}

// Get classifies each URI in the request by change type, groups them by provider,
// calls each provider once with its subset of changes, and reassembles results in the original order.
func (p *provider) Get(ctx context.Context, request entity.Request) ([]entity.ChangeInfo, error) {
	changesByProvider, err := p.groupChangesByProvider(request.Change.URIs)
	if err != nil {
		return nil, err
	}

	results := make([]entity.ChangeInfo, len(request.Change.URIs))
	for changeProvider, changes := range changesByProvider {
		uris := make([]string, 0, len(changes))
		for _, c := range changes {
			uris = append(uris, c.uri)
		}

		// Subrequest for each provider containing only its subset of changes.
		subRequest := request
		subRequest.Change.URIs = uris

		infos, getErr := changeProvider.Get(ctx, subRequest)
		if getErr != nil {
			return nil, getErr
		}

		if len(infos) != len(changes) {
			return nil, fmt.Errorf("provider returned %d results for %d URIs", len(infos), len(changes))
		}

		// Put the changes back in their original positions in the results.
		for i, changeInfo := range infos {
			results[changes[i].index] = changeInfo
		}
	}

	return results, nil
}

// groupChangesByProvider classifies each URI by trying ParseChangeID functions and groups them by the matched provider.
// Returns an error if a URI matches no known type or matches a type whose provider was not configured.
func (p *provider) groupChangesByProvider(uris []string) (map[changeprovider.ChangeProvider][]matchedURI, error) {
	grouped := make(map[changeprovider.ChangeProvider][]matchedURI)
	for i, uri := range uris {
		matchedProvider, err := p.matchURIToChangeProvider(uri)
		if err != nil {
			return nil, err
		}
		grouped[matchedProvider] = append(grouped[matchedProvider], matchedURI{
			index: i,
			uri:   uri,
		})
	}

	return grouped, nil
}

// matchURIToChangeProvider returns the provider for the given URI by trying each ParseChangeID function.
// Returns an error if no parser matches, or if a parser matches, but the corresponding provider was not configured.
func (p *provider) matchURIToChangeProvider(uri string) (changeprovider.ChangeProvider, error) {
	if _, err := github.ParseChangeID(uri); err == nil {
		if p.github == nil {
			return nil, fmt.Errorf("URI %q is a GitHub change but no GitHub provider is configured", uri)
		}
		return p.github, nil
	} else if _, err := phabricator.ParseChangeID(uri); err == nil {
		if p.phabricator == nil {
			return nil, fmt.Errorf("URI %q is a Phabricator change but no Phabricator provider is configured", uri)
		}
		return p.phabricator, nil
	} else if _, err := git.ParseChangeID(uri); err == nil {
		if p.git == nil {
			return nil, fmt.Errorf("URI %q is a git change but no git provider is configured", uri)
		}
		return p.git, nil
	}

	return nil, fmt.Errorf("URI %q does not match any known change type", uri)
}
