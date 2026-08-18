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

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gitprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/git"
)

// credentialFile holds the git configuration fragment carrying a token. It is
// written inside the repository and included from its config, so the token
// never appears in a remote URL — which git echoes back into error messages,
// and from there into logs and dead-letter payloads.
const credentialFile = "submitqueue-changeprovider-credentials.config"

// tokenAuth is the default gitprovider.Auth: a credential read from an
// environment variable and presented to git as an HTTP header.
//
// It is deliberately here rather than in the extension. The extension takes an
// Auth and calls it; what a credential is and where it comes from is a
// deployment's business, so a deployment that mints short-lived tokens or
// reads a secrets manager supplies its own implementation instead of this one
// and changes nothing else.
type tokenAuth struct {
	tokenEnv  string
	tokenUser string
}

// Apply writes the credential fragment, or removes it when the remote needs
// none, so a repository that stops using a token stops carrying one.
//
// Called before every fetch rather than once, which is what lets a short-lived
// credential be refreshed — reading the variable again each time is the whole
// mechanism for that.
func (a tokenAuth) Apply(ctx context.Context, repoPath, remoteURL string) error {
	path := filepath.Join(repoPath, credentialFile)

	if !isHTTPRemote(remoteURL) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("could not remove stale credential %q: %w", path, err)
		}
		return nil
	}

	token, ok := os.LookupEnv(a.tokenEnv)
	if !ok || token == "" {
		return fmt.Errorf("environment variable %q named by tokenEnv is not set", a.tokenEnv)
	}

	basic := base64.StdEncoding.EncodeToString([]byte(a.tokenUser + ":" + token))
	fragment := fmt.Sprintf("[http %q]\n\textraheader = Authorization: Basic %s\n", remoteURL, basic)
	if err := os.WriteFile(path, []byte(fragment), 0o600); err != nil {
		return fmt.Errorf("could not write credential %q: %w", path, err)
	}

	// include.path resolves relative to the config file holding it, so the bare
	// filename lands beside it. A bare repository's config is at its root.
	return gitprovider.SetConfig(ctx, repoPath, "include.path", credentialFile)
}

func isHTTPRemote(remoteURL string) bool {
	return strings.HasPrefix(remoteURL, "http://") || strings.HasPrefix(remoteURL, "https://")
}

// newChangeRepo builds the local copy a git change provider reads through, and
// provisions it.
//
// Provisioning happens here, at wiring time, rather than on first use: a change
// provider is resolved once per message on the validate path, so a clone
// started there would sit inside a retry loop and report an unreachable remote
// as a queue processing failure instead of a service that is misconfigured.
func newChangeRepo(ctx context.Context, cfg gitProviderConfig) (*gitprovider.Repo, error) {
	var auth gitprovider.Auth
	if cfg.TokenEnv != "" {
		auth = tokenAuth{tokenEnv: cfg.TokenEnv, tokenUser: cfg.TokenUser}
	}

	repo, err := gitprovider.NewRepo(gitprovider.RepoConfig{
		Path:      cfg.RepoPath,
		RemoteURL: cfg.RemoteURL,
		Remote:    cfg.Remote,
		Target:    cfg.Target,
		Auth:      auth,
	})
	if err != nil {
		return nil, err
	}
	if err := repo.Provision(ctx); err != nil {
		return nil, fmt.Errorf("could not provision change repository %q: %w", cfg.RepoPath, err)
	}
	return repo, nil
}
