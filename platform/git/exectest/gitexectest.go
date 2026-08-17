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

// Package gitexectest resolves the Bazel-pinned git a test target supplies.
//
// Tests that shell out to git run against the same pinned build the services
// use, rather than whatever the host happens to have, so a developer's git
// version cannot change what a test proves. A target opts in by depending on
// the binary and naming it in the environment:
//
//	go_test(
//	    data = ["@git"],
//	    env = {"SUBMITQUEUE_TEST_GIT": "$(location @git//:git)"},
//	)
//
// The indirection through an environment variable is Bazel's: rules_go expands
// $(location) to an execroot-relative path, which for an external output has to
// be re-rooted under the runfiles tree the test actually runs from.
package gitexectest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// GitEnv is the variable a test target sets to the pinned git binary.
const GitEnv = "SUBMITQUEUE_TEST_GIT"

// Git returns an absolute path to the pinned git binary, failing the test if
// the target did not supply one.
func Git(t *testing.T) string {
	t.Helper()
	return Runfile(t, GitEnv)
}

// Runfile resolves the $(location)-expanded path held by the named environment
// variable, failing the test if it is unset or does not resolve.
//
// A path that is already usable is returned as-is, which is what happens under
// `bazel run`, where the process already starts inside the runfiles tree.
func Runfile(t *testing.T, name string) string {
	t.Helper()

	path := os.Getenv(name)
	require.NotEmpty(t, path, "%s must be set by the test target", name)

	if absolute, err := filepath.Abs(path); err == nil {
		if _, err := os.Stat(absolute); err == nil {
			return absolute
		}
	}

	// Both spellings occur: the path is execroot-relative for a target in this
	// repository, and carries a leading segment for one reached through another.
	slashed := filepath.ToSlash(path)
	external := ""
	if strings.HasPrefix(slashed, "external/") {
		external = strings.TrimPrefix(slashed, "external/")
	} else if i := strings.Index(slashed, "/external/"); i >= 0 {
		external = slashed[i+len("/external/"):]
	}
	require.NotEmpty(t, external, "%s=%q is not a runfile", name, path)

	root := os.Getenv("TEST_SRCDIR")
	require.NotEmpty(t, root, "TEST_SRCDIR must be set when %s is a runfile", name)

	candidate := filepath.Join(root, filepath.FromSlash(external))
	_, err := os.Stat(candidate)
	require.NoError(t, err, "%s=%q does not resolve under TEST_SRCDIR", name, path)
	return candidate
}
