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

package gitrepo

import "context"

// Auth prepares a local repository to authenticate to its remote.
//
// This package never decides what a credential is, where it comes from, or how
// long it lives. An integrator wires an implementation in — reading an
// environment variable, calling a secrets manager, minting a short-lived token —
// and only that implementation changes when the answer does.
//
// Apply runs immediately before every fetch rather than once at provisioning,
// so an implementation backed by an expiring credential can refresh it. It must
// therefore be cheap and idempotent.
//
// A nil Auth means the remote needs none, which covers a local path and an SSH
// remote served by the host's own SSH configuration and agent.
type Auth interface {
	// Apply configures repoPath so that git commands run against remoteURL from
	// inside it can authenticate.
	Apply(ctx context.Context, repoPath, remoteURL string) error
}
