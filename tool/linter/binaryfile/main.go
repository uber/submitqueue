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

// Command binaryfile checks that no binary file is tracked in the repository.
//
// The build is Bazel-driven and every artifact it produces lands in an ignored
// directory — bazel-bin/, bin/, or .docker-bin/ — so a tracked binary is always
// a mistake. The usual cause is an ad-hoc `go build ./service/...` run from the
// repo root: it names its executable after the package directory and writes it
// to the working directory, where a broad `git add` sweeps it up.
//
// Checking rather than ignoring is deliberate. A .gitignore entry would stop
// the file being committed but would also stop git mentioning it at all, so the
// mistake becomes invisible and the stray artifact simply accumulates. A check
// that fails names the file and says what to do instead.
//
// Detection follows git's own heuristic: a file is binary if a NUL byte appears
// in its leading bytes.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// sniffLen is how many leading bytes are examined for a NUL. It matches the
// window git uses for the same decision, which is large enough to cover any
// text header an executable format might begin with.
const sniffLen = 8000

// allowed lists tracked paths that are legitimately binary, relative to the
// repository root. It is empty because nothing in the repository is: the tree
// is source, schemas, and generated Go. An entry belongs here only when a
// binary genuinely has to be versioned — a test fixture that cannot be built,
// or an image a document renders — never to silence a stray build artifact.
var allowed = map[string]bool{}

// violation is one tracked file that is binary.
type violation struct {
	path string
	size int64
}

func main() {
	flag.Parse()

	root, err := findRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	files, err := trackedFiles(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	var violations []violation
	var checked int
	for _, path := range files {
		if allowed[path] {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, path))
		// A tracked path that is missing or is a symlink has no contents of its
		// own to judge; skip it rather than failing the whole run.
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		checked++

		binary, err := isBinaryFile(filepath.Join(root, path))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		if binary {
			violations = append(violations, violation{path: path, size: info.Size()})
		}
	}

	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "%d binary file(s) are tracked:\n\n", len(violations))
		for _, v := range violations {
			fmt.Fprintf(os.Stderr, "  %s (%d bytes)\n", v.path, v.size)
		}
		fmt.Fprintf(os.Stderr, "\nThe build is Bazel-driven and writes to bazel-bin/, bin/, and\n")
		fmt.Fprintf(os.Stderr, ".docker-bin/, all of which are ignored, so a tracked binary is a\n")
		fmt.Fprintf(os.Stderr, "mistake. If this is a stray `go build` output, delete it and use\n")
		fmt.Fprintf(os.Stderr, "Bazel instead: `make build`, or `make run-client-submitqueue-gateway`\n")
		fmt.Fprintf(os.Stderr, "to run the client without producing a binary at all.\n")
		os.Exit(1)
	}

	fmt.Printf("All %d tracked files are text.\n", checked)
}

// isBinaryFile reports whether the file at path is binary, reading no more than
// the leading sniffLen bytes.
func isBinaryFile(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, sniffLen)
	n, err := file.Read(buf)
	if err != nil && n == 0 {
		// A read that returns nothing, including io.EOF on an empty file, leaves
		// an empty window, which isBinary correctly reports as text.
		return isBinary(nil), nil
	}
	return isBinary(buf[:n]), nil
}

// isBinary reports whether a leading window of a file's contents looks binary,
// which is true exactly when it contains a NUL byte. An empty window is text:
// an empty file has nothing to make it binary.
func isBinary(window []byte) bool {
	return bytes.IndexByte(window, 0) >= 0
}

// trackedFiles returns every path git tracks, relative to root.
//
// The check is about what is committed rather than what is present, so the file
// list comes from the index; a stray artifact that is untracked is a local
// matter and not this linter's business.
func trackedFiles(root string) ([]string, error) {
	cmd := exec.Command("git", "-C", root, "ls-files", "-z")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list tracked files: %w", err)
	}

	var files []string
	for _, path := range strings.Split(string(out), "\x00") {
		if path != "" {
			files = append(files, path)
		}
	}
	return files, nil
}

func findRepoRoot() (string, error) {
	// Bazel `run` executes from the runfiles tree; BUILD_WORKSPACE_DIRECTORY
	// points back at the source tree.
	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		return dir, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find repository root (no go.mod found)")
		}
		dir = parent
	}
}
