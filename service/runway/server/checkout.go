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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	gitexec "github.com/uber/submitqueue/platform/git/exec"
	gitlander "github.com/uber/submitqueue/runway/extension/lander/git"
)

// credentialFile is the name, inside the checkout's .git directory, of the
// config fragment holding the remote's credential. It is included from the
// repository config rather than written into it, so the secret lives in one
// file this service owns and can chmod, and `git config --list` on the
// repository does not print it back out.
const credentialFile = "submitqueue-credentials.config"

// provisionCheckout makes the working tree the git lander requires: a
// repository at CheckoutPath with the configured remote, its credential in
// place, and the target branch checked out.
//
// The lander deliberately does none of this — it declares that the checkout
// must already exist, so that it never has to decide whether a surprising
// working tree is one it should repair or one it should refuse. Creating it is
// the wiring layer's job, and doing it here keeps the decision out of the land
// path.
//
// It is idempotent: an existing checkout has its remote URL and credential
// brought back in line rather than being recreated, so a restart against a
// persisted volume costs nothing and a rotated token takes effect.
func provisionCheckout(ctx context.Context, logger *zap.SugaredLogger, runtime gitlander.GitRuntime, cfg landerConfig) error {
	if err := os.MkdirAll(cfg.CheckoutPath, 0o755); err != nil {
		return fmt.Errorf("create checkout dir %q: %w", cfg.CheckoutPath, err)
	}

	fresh, err := initRepository(ctx, runtime, cfg)
	if err != nil {
		return err
	}

	// Written before the first fetch, because a private remote cannot be read
	// without it.
	if err := writeCredential(cfg); err != nil {
		return err
	}
	if err := configureRemote(ctx, runtime, cfg); err != nil {
		return err
	}

	if _, err := runGit(ctx, runtime, cfg.CheckoutPath, "fetch", cfg.Remote); err != nil {
		return fmt.Errorf("fetch %s from %s: %w", cfg.Remote, cfg.RemoteURL, err)
	}
	remoteRef := cfg.Remote + "/" + cfg.Target
	if _, err := runGit(ctx, runtime, cfg.CheckoutPath, "checkout", "-B", cfg.Target, remoteRef); err != nil {
		return fmt.Errorf("check out %s: %w", remoteRef, err)
	}

	// The lander points HOME here. Its own `git clean -fdx` may remove it
	// again, which is harmless — nothing is stored there, and the scrubbed
	// environment means git reads no configuration from it either way.
	if err := os.MkdirAll(filepath.Join(cfg.CheckoutPath, ".submitqueue-git-home", "xdg"), 0o700); err != nil {
		return fmt.Errorf("create git home in %q: %w", cfg.CheckoutPath, err)
	}

	logger.Infow("provisioned land checkout",
		"checkout", cfg.CheckoutPath,
		"remote", cfg.Remote,
		"remote_url", cfg.RemoteURL,
		"target", cfg.Target,
		"cloned", fresh,
		"credential", cfg.needsHTTPCredential(),
	)
	return nil
}

// initRepository creates the repository if the path does not already hold one,
// reporting whether it had to. An existing repository is left in place: it may
// carry fetched objects worth keeping, and re-creating it would discard them.
func initRepository(ctx context.Context, runtime gitlander.GitRuntime, cfg landerConfig) (bool, error) {
	if info, err := os.Stat(filepath.Join(cfg.CheckoutPath, ".git")); err == nil && info.IsDir() {
		return false, nil
	}
	if _, err := runGit(ctx, runtime, cfg.CheckoutPath, "init", "-b", cfg.Target); err != nil {
		return false, fmt.Errorf("init repository at %q: %w", cfg.CheckoutPath, err)
	}
	return true, nil
}

// configureRemote points the configured remote name at the configured URL,
// adding it when absent and correcting it when it has drifted.
func configureRemote(ctx context.Context, runtime gitlander.GitRuntime, cfg landerConfig) error {
	out, err := runGit(ctx, runtime, cfg.CheckoutPath, "remote")
	if err != nil {
		return fmt.Errorf("list remotes in %q: %w", cfg.CheckoutPath, err)
	}
	for _, name := range strings.Fields(string(out)) {
		if name != cfg.Remote {
			continue
		}
		if _, err := runGit(ctx, runtime, cfg.CheckoutPath, "remote", "set-url", cfg.Remote, cfg.RemoteURL); err != nil {
			return fmt.Errorf("set url of remote %q: %w", cfg.Remote, err)
		}
		return nil
	}
	if _, err := runGit(ctx, runtime, cfg.CheckoutPath, "remote", "add", cfg.Remote, cfg.RemoteURL); err != nil {
		return fmt.Errorf("add remote %q: %w", cfg.Remote, err)
	}
	return nil
}

// writeCredential stores the remote's token as an HTTP Authorization header in
// a config fragment the repository includes.
//
// The token deliberately never enters the remote URL. The lander folds git's
// stderr into the errors it returns, so a URL-embedded credential would be
// reprinted into logs and dead-letter payloads by any failed fetch. It is kept
// out of the command line for the same reason — a header written by this
// process into a file it owns is visible to neither.
func writeCredential(cfg landerConfig) error {
	path := filepath.Join(cfg.CheckoutPath, ".git", credentialFile)

	if !cfg.needsHTTPCredential() {
		// A local path or SSH remote authenticates by other means. Remove any
		// fragment a previous configuration left, so a target that stops using
		// a token stops carrying one.
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale credential %q: %w", path, err)
		}
		return nil
	}

	token, ok := cfg.token()
	if !ok {
		return fmt.Errorf("environment variable %q named by tokenEnv is not set", cfg.TokenEnv)
	}
	if token == "" {
		return fmt.Errorf("environment variable %q named by tokenEnv is empty", cfg.TokenEnv)
	}

	basic := base64.StdEncoding.EncodeToString([]byte(cfg.TokenUser + ":" + token))
	fragment := fmt.Sprintf("[http %q]\n\textraheader = Authorization: Basic %s\n", cfg.RemoteURL, basic)
	if err := os.WriteFile(path, []byte(fragment), 0o600); err != nil {
		return fmt.Errorf("write credential %q: %w", path, err)
	}

	// include.path resolves relative to the config file holding it, which is
	// .git/config — so the bare filename lands beside it.
	if err := setLocalConfig(cfg.CheckoutPath, "include.path", credentialFile); err != nil {
		return err
	}
	return nil
}

// setLocalConfig sets a repository-local config key, replacing any previous
// value for it. It edits .git/config through git itself rather than by hand so
// the file stays git's to format.
func setLocalConfig(checkoutPath, key, value string) error {
	// Uses the ambient git rather than the pinned runtime: this touches only
	// the repository's own config file, never the remote, and the pinned
	// runtime exists to make *land results* reproducible.
	cmd := exec.Command("git", "config", "--local", "--replace-all", key, value)
	cmd.Dir = checkoutPath
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git config --local %s: %w: %s", key, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// runGit invokes the pinned git in dir with an environment scrubbed of ambient
// configuration but retaining what is needed to reach a remote. It composes that
// environment through gitexec.Env, the same source the lander uses, so
// provisioning and landing authenticate — and behave — identically.
func runGit(ctx context.Context, runtime gitlander.GitRuntime, dir string, args ...string) ([]byte, error) {
	full := append([]string{
		"--exec-path=" + runtime.ExecPath,
		"-c", "init.templateDir=" + runtime.TemplateDir,
	}, args...)

	cmd := exec.CommandContext(ctx, runtime.Executable, full...)
	cmd.Dir = dir
	cmd.Env = gitexec.Env(gitexec.EnvOptions{
		Transport:   true,
		Passthrough: runtime.PassthroughEnv,
		Literal: []string{
			"HOME=" + filepath.Join(dir, ".submitqueue-git-home"),
			"GIT_EXEC_PATH=" + runtime.ExecPath,
			"GIT_TEMPLATE_DIR=" + runtime.TemplateDir,
			"LC_ALL=C",
			"LANG=C",
		},
	})

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
