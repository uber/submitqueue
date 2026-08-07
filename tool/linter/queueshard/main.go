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

// Command queueshard checks that every domain table is shardable by queue: its
// primary key must lead with the queue column, and no secondary index may span
// queues.
//
// A table is shardable by queue when one queue's rows are unreachable through
// another queue's binding. That holds exactly when the queue is the leading
// primary-key column, because every read is then a primary-key-prefix scan
// within one queue. A secondary index that does not itself lead with the queue
// reintroduces a cross-queue access path, so those are rejected too.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// queueColumns are the column names that identify the owning queue. Most tables
// call it "queue"; a table whose rows *are* queues (stovepipe's queue table)
// names it "name" because the row's identity is the queue itself.
var queueColumns = map[string]bool{
	"queue": true,
	"name":  true,
}

// schemaRoots are the directories scanned for table definitions.
//
// platform/extension/messagequeue is deliberately absent: it is a message-queue
// backend keyed by (consumer_group, topic, partition_key), not a domain table
// set, and sharding it is tracked separately.
var schemaRoots = []string{
	"submitqueue/extension/storage/mysql/schema",
	"stovepipe/extension/storage/mysql/schema",
	"platform/extension/counter/mysql/schema",
}

var (
	createTableRe = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`?" + `(\w+)` + "`?" + `\s*\((.*)\)\s*ENGINE`)
	primaryKeyRe  = regexp.MustCompile(`(?i)PRIMARY\s+KEY\s*\(([^)]*)\)`)
	indexRe       = regexp.MustCompile(`(?im)^\s*(?:UNIQUE\s+)?(?:KEY|INDEX)\s+` + "`?" + `(\w+)` + "`?" + `\s*\(([^)]*)\)`)
)

// violation is one table that is not shardable by queue.
type violation struct {
	file    string
	table   string
	problem string
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
	for _, schemaRoot := range schemaRoots {
		files, err := filepath.Glob(filepath.Join(root, schemaRoot, "*.sql"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error globbing %s: %v\n", schemaRoot, err)
			os.Exit(1)
		}
		if len(files) == 0 {
			fmt.Fprintf(os.Stderr, "error: no .sql files under %s\n", schemaRoot)
			os.Exit(1)
		}
		for _, file := range files {
			content, err := os.ReadFile(file)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error reading %s: %v\n", file, err)
				os.Exit(1)
			}
			rel, relErr := filepath.Rel(root, file)
			if relErr != nil {
				rel = file
			}
			found, tableViolations := check(rel, string(content))
			checked += found
			violations = append(violations, tableViolations...)
		}
	}

	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "%d table(s) are not shardable by queue:\n\n", len(violations))
		for _, v := range violations {
			fmt.Fprintf(os.Stderr, "  %s: table %q %s\n", v.file, v.table, v.problem)
		}
		fmt.Fprintf(os.Stderr, "\nEvery table's primary key must lead with the queue column, and no\n")
		fmt.Fprintf(os.Stderr, "secondary index may span queues, so that one queue's rows are\n")
		fmt.Fprintf(os.Stderr, "unreachable through another queue's binding.\n")
		os.Exit(1)
	}

	fmt.Printf("All %d tables are shardable by queue.\n", checked)
}

// check returns the number of tables found in content and any violations.
func check(file, content string) (int, []violation) {
	var violations []violation
	matches := createTableRe.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		table, body := match[1], match[2]

		pk := primaryKeyRe.FindStringSubmatch(body)
		if pk == nil {
			violations = append(violations, violation{file, table, "has no PRIMARY KEY"})
			continue
		}
		columns := splitColumns(pk[1])
		if len(columns) == 0 {
			violations = append(violations, violation{file, table, "has an empty PRIMARY KEY"})
			continue
		}
		if !queueColumns[columns[0]] {
			violations = append(violations, violation{
				file, table,
				fmt.Sprintf("leads its PRIMARY KEY with %q, not the queue column", columns[0]),
			})
		}

		for _, idx := range indexRe.FindAllStringSubmatch(body, -1) {
			idxColumns := splitColumns(idx[2])
			if len(idxColumns) == 0 || !queueColumns[idxColumns[0]] {
				lead := "(empty)"
				if len(idxColumns) > 0 {
					lead = idxColumns[0]
				}
				violations = append(violations, violation{
					file, table,
					fmt.Sprintf("has index %q leading with %q, which spans queues", idx[1], lead),
				})
			}
		}
	}
	return len(matches), violations
}

// splitColumns parses a comma-separated column list, stripping backticks,
// whitespace, and any length prefix such as `col(20)`.
func splitColumns(list string) []string {
	var columns []string
	for _, raw := range strings.Split(list, ",") {
		col := strings.TrimSpace(raw)
		col = strings.Trim(col, "`")
		if idx := strings.IndexByte(col, '('); idx >= 0 {
			col = col[:idx]
		}
		col = strings.TrimSpace(col)
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
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
