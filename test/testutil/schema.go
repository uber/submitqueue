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

package testutil

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// Runfile resolves a workspace-relative path (e.g.
// "service/submitqueue/docker-compose.yml") to an absolute path inside the
// Bazel runfiles tree. Every resolved path must be declared as a `data`
// dependency of the test target — that is what keeps Docker-based tests
// hermetic. Outside Bazel (plain `go test` from the repo root) the path is
// returned unchanged.
func Runfile(relativePath string) string {
	if dir := os.Getenv("TEST_SRCDIR"); dir != "" {
		return filepath.Join(dir, os.Getenv("TEST_WORKSPACE"), relativePath)
	}
	return relativePath
}

// SchemaDir returns the path to a schema directory.
// It checks for both Bazel runfiles and direct go test paths.
// relativePath should be like "submitqueue/extension/storage/mysql/schema" or "platform/extension/messagequeue/mysql/schema"
func SchemaDir(relativePath string) string {
	return Runfile(relativePath)
}

// ApplySchema reads every .sql file under the schema directory, including
// subdirectories, and executes them on the database. A schema may group its
// tables into per-owner subpackages (see
// submitqueue/extension/storage/mysql/schema), so passing the root applies the
// whole schema regardless of how it is subdivided.
func ApplySchema(t *testing.T, log *TestLogger, db *sql.DB, schemaDirectory string) {
	t.Helper()

	var files []string
	err := filepath.WalkDir(schemaDirectory, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".sql") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err, "failed to walk schema files")
	require.NotEmpty(t, files, "no .sql schema files found under %s", schemaDirectory)

	// Sort files to ensure deterministic schema application order.
	sort.Strings(files)

	for _, f := range files {
		name, relErr := filepath.Rel(schemaDirectory, f)
		require.NoError(t, relErr, "failed to relativize schema file %s", f)
		log.Logf("Applying schema: %s", name)

		content, err := os.ReadFile(f)
		require.NoError(t, err, "failed to read schema file %s", name)

		_, err = db.ExecContext(context.Background(), string(content))
		require.NoError(t, err, "failed to execute schema file %s", name)

		log.Logf("Schema applied: %s", name)
	}
}
