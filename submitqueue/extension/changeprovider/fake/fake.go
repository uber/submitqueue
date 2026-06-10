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

// Package fake provides a changeprovider.ChangeProvider whose outcome is driven
// by the input change. With no marker it returns one empty ChangeInfo per URI,
// behaving as a best-case stub for wiring and baselines. A failure can be
// injected end-to-end (e.g. from an e2e land request) by embedding a marker
// token in a change URI of the form "sq-fake=<token>":
//
//	sq-fake=provider-error -> non-nil error
//
// This lets a single running stack exercise negative paths purely by varying
// request payloads. It is intended for examples and tests only, never
// production.
package fake

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/core/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// Recognized marker tokens. See the package doc for the convention.
const tokenError = "provider-error"

// provider is a changeprovider.ChangeProvider that returns empty change info
// unless a marker token in a change URI requests a failure.
type provider struct{}

// New returns a changeprovider.ChangeProvider that defaults to returning one
// empty ChangeInfo per URI and honors marker tokens embedded in change URIs.
func New() changeprovider.ChangeProvider {
	return provider{}
}

// Get returns one ChangeInfo per URI in the request's change, unless a recognized
// marker token requests a failure. The "one ChangeInfo per URI" contract is preserved.
func (provider) Get(_ context.Context, request entity.Request) ([]entity.ChangeInfo, error) {
	change := request.Change
	if fakemarker.Token(change.URIs) == tokenError {
		return nil, fmt.Errorf("fake: marked provider error")
	}

	infos := make([]entity.ChangeInfo, 0, len(change.URIs))
	for _, uri := range change.URIs {
		infos = append(infos, entity.ChangeInfo{URI: uri})
	}
	return infos, nil
}
