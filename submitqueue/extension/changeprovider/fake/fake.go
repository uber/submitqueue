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

// Package fake provides a changeprovider.ChangeProvider with synthetic file
// metadata. File paths are pseudo-random but stable for each URI, so retries
// and separate processes resolve the same change without shared state.
// A change-URI marker "sq-fake=provider-error" injects a provider failure.
// This provider is intended for examples and tests only, never production.
package fake

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/uber/submitqueue/platform/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// Recognized marker tokens. See the package doc for the convention.
const tokenError = "provider-error"

type provider struct {
	// cfg is the per-queue identity this provider was built for.
	cfg changeprovider.Config
}

// New returns a fake provider bound to the queue named in cfg.
func New(cfg changeprovider.Config) changeprovider.ChangeProvider {
	return provider{cfg: cfg}
}

// Get returns one synthetic ChangeInfo per URI, in input order, unless a
// recognized marker requests a failure.
func (provider) Get(_ context.Context, request entity.Request) ([]entity.ChangeInfo, error) {
	change := request.Change
	if fakemarker.Token(change.URIs) == tokenError {
		return nil, fmt.Errorf("fake: marked provider error")
	}

	infos := make([]entity.ChangeInfo, 0, len(change.URIs))
	for _, uri := range change.URIs {
		info := entity.ChangeInfo{URI: uri}
		// A small directory pool makes distinct changes overlap in the demo.
		sum := sha256.Sum256([]byte(uri))
		for i := range 1 + int(sum[1]%4) {
			info.Details.ChangedFiles = append(info.Details.ChangedFiles, entity.ChangedFile{
				Path:       fmt.Sprintf("demo/%02d/%x-%d.txt", sum[0]%8, sum[2:10], i),
				LinesAdded: 1,
			})
		}
		infos = append(infos, info)
	}
	return infos, nil
}
