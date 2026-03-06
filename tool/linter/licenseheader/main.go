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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const header = `// Copyright (c) 2025 Uber Technologies, Inc.
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
// limitations under the License.`

func main() {
	fix := flag.Bool("fix", false, "add missing license headers in-place")
	check := flag.Bool("check", false, "check for missing license headers (default mode)")
	flag.Parse()

	// Default to check mode.
	if !*fix && !*check {
		*check = true
	}

	root, err := findRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	files, err := findSourceFiles(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error finding files: %v\n", err)
		os.Exit(1)
	}

	var missing []string
	for _, f := range files {
		ok, err := hasLicenseHeader(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading %s: %v\n", f, err)
			os.Exit(1)
		}
		if !ok {
			missing = append(missing, f)
		}
	}

	if len(missing) == 0 {
		fmt.Println("All files have license headers.")
		return
	}

	if *fix {
		for _, f := range missing {
			if err := addLicenseHeader(f); err != nil {
				fmt.Fprintf(os.Stderr, "error fixing %s: %v\n", f, err)
				os.Exit(1)
			}
			rel, _ := filepath.Rel(root, f)
			fmt.Printf("fixed: %s\n", rel)
		}
		fmt.Printf("\nAdded license headers to %d files.\n", len(missing))
		return
	}

	// Check mode.
	fmt.Printf("Found %d files missing license headers:\n", len(missing))
	for _, f := range missing {
		rel, _ := filepath.Rel(root, f)
		fmt.Printf("  %s\n", rel)
	}
	os.Exit(1)
}

// findRepoRoot walks up from the working directory to find the repository root
// by looking for a go.mod file.
func findRepoRoot() (string, error) {
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

// findSourceFiles returns all .go and .proto files that should have license headers.
func findSourceFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			name := info.Name()
			// Skip hidden directories, pkg/, and bazel output directories.
			if strings.HasPrefix(name, ".") || name == "pkg" || strings.HasPrefix(name, "bazel-") {
				return filepath.SkipDir
			}
			return nil
		}

		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".proto" {
			return nil
		}

		// Skip generated protobuf files.
		if isGeneratedFile(path) {
			return nil
		}

		files = append(files, path)
		return nil
	})
	return files, err
}

// isGeneratedFile returns true for protobuf-generated files.
func isGeneratedFile(path string) bool {
	return strings.HasSuffix(path, ".pb.go") ||
		strings.HasSuffix(path, "_grpc.pb.go") ||
		strings.HasSuffix(path, ".pb.yarpc.go")
}

// hasLicenseHeader checks if a file starts with the expected license header.
func hasLicenseHeader(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	content := string(data)

	// If the file has a //go:build line, the header comes after it.
	if strings.HasPrefix(content, "//go:build ") {
		idx := strings.Index(content, "\n")
		if idx >= 0 {
			content = strings.TrimLeft(content[idx+1:], "\n")
		}
	}

	return strings.HasPrefix(content, header), nil
}

// addLicenseHeader prepends the license header to a file.
// If the file starts with a //go:build directive, the header is placed after it.
func addLicenseHeader(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	content := string(data)

	var result string
	if strings.HasPrefix(content, "//go:build ") {
		idx := strings.Index(content, "\n")
		if idx >= 0 {
			buildLine := content[:idx+1]
			rest := content[idx+1:]
			result = buildLine + "\n" + header + "\n\n" + strings.TrimLeft(rest, "\n")
		} else {
			result = content + "\n\n" + header + "\n"
		}
	} else {
		result = header + "\n\n" + content
	}

	return os.WriteFile(path, []byte(result), 0644)
}
