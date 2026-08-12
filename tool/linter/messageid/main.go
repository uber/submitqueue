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

// Command messageid checks that queue messages are only constructed where the
// message-ID convention is enforced.
//
// A message ID is a deduplication key, matched against every message the
// backend still holds for the same topic and partition key — consumed ones
// included. A publish whose ID collides is reported as a success and stores
// nothing, so an ID chosen carelessly does not fail loudly; it drops an event.
// The safe choice is not something a reviewer can see locally either, because
// it depends on what every other producer on that topic uses.
//
// Whether a given ID expression is well chosen cannot be decided by reading it,
// so this does not try. It enforces the structural rule that makes the question
// answerable in one place: platform/base/messagequeue.NewMessage may only be
// called from platform/publish, which composes IDs from an entity and a cause,
// and from the queue backends, which rebuild messages they are handed back.
// Every producer then goes through the helper.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// messageQueuePkg is the import path whose NewMessage is restricted.
const messageQueuePkg = "github.com/uber/submitqueue/platform/base/messagequeue"

// allowedRoots are the directories permitted to construct messages directly.
//
// platform/publish owns the convention, so it is where the one remaining call
// belongs. The backends under platform/extension/messagequeue rebuild a Message
// from rows they are handed back on the read path, which is not a publish and
// chooses no ID.
var allowedRoots = []string{
	"platform/publish",
	"platform/extension/messagequeue",
}

// skipDirs are directory names never worth walking.
var skipDirs = map[string]bool{
	".git":     true,
	"bazel-in": true,
}

// violation is one direct construction of a queue message outside the
// allowed roots.
type violation struct {
	file string
	line int
	fn   string
}

func main() {
	flag.Parse()

	root, err := findRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	var violations []violation
	var checked int

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if skipDirs[name] || strings.HasPrefix(name, "bazel-") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// Tests construct messages to feed a controller, which is the consuming
		// side: nothing is published and no ID is chosen against a live topic.
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)
		if allowed(rel) {
			return nil
		}
		checked++

		found, checkErr := check(rel, path)
		if checkErr != nil {
			return checkErr
		}
		violations = append(violations, found...)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "%d direct queue-message construction(s) outside platform/publish:\n\n", len(violations))
		for _, v := range violations {
			fmt.Fprintf(os.Stderr, "  %s:%d: %s\n", v.file, v.line, v.fn)
		}
		fmt.Fprintf(os.Stderr, "\nA message ID is a deduplication key: a publish that reuses one is reported\n")
		fmt.Fprintf(os.Stderr, "as a success and stores nothing, silently dropping the event. Publish through\n")
		fmt.Fprintf(os.Stderr, "platform/publish and build the ID with publish.IntentID, naming the entity the\n")
		fmt.Fprintf(os.Stderr, "message is about and the cause this particular message exists for.\n")
		os.Exit(1)
	}

	fmt.Printf("All %d files construct queue messages only through platform/publish.\n", checked)
}

// allowed reports whether a repo-relative path may construct messages directly.
func allowed(rel string) bool {
	for _, prefix := range allowedRoots {
		if rel == prefix || strings.HasPrefix(rel, prefix+"/") {
			return true
		}
	}
	return false
}

// check parses one file and reports every call constructing a queue message.
//
// Parsing rather than grepping, so a NewMessage of some unrelated package, or
// the name in a comment or string, is not mistaken for one of these.
func check(rel, path string) ([]violation, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", rel, err)
	}

	// Names the file binds to the message-queue package. Usually one alias,
	// but a file may import it more than once.
	locals := map[string]bool{}
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) != messageQueuePkg {
			continue
		}
		switch {
		case imp.Name == nil:
			locals["messagequeue"] = true
		case imp.Name.Name == "." || imp.Name.Name == "_":
			// A dot import would make the call unqualified and a blank one
			// cannot call anything; neither appears in this repo.
		default:
			locals[imp.Name.Name] = true
		}
	}
	if len(locals) == 0 {
		return nil, nil
	}

	var violations []violation
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !strings.HasPrefix(sel.Sel.Name, "NewMessage") {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || !locals[pkg.Name] {
			return true
		}
		violations = append(violations, violation{
			file: rel,
			line: fset.Position(call.Pos()).Line,
			fn:   pkg.Name + "." + sel.Sel.Name,
		})
		return true
	})
	return violations, nil
}

// findRepoRoot walks up from the working directory to the module root.
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
