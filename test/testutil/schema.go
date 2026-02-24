package testutil

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// SchemaDir returns the path to a schema directory.
// It checks for both Bazel runfiles and direct go test paths.
// relativePath should be like "extension/storage/mysql/schema" or "extension/queue/mysql/schema"
func SchemaDir(relativePath string) string {
	// Bazel runfiles path
	if dir := os.Getenv("TEST_SRCDIR"); dir != "" {
		return filepath.Join(dir, os.Getenv("TEST_WORKSPACE"), relativePath)
	}
	// Direct go test path (run from repo root)
	return relativePath
}

// ApplySchema reads all .sql files from the schema directory and executes them on the database.
func ApplySchema(t *testing.T, log *TestLogger, db *sql.DB, schemaDirectory string) {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(schemaDirectory, "*.sql"))
	require.NoError(t, err, "failed to glob schema files")
	require.NotEmpty(t, files, "no .sql schema files found in %s", schemaDirectory)

	// Sort files to ensure deterministic schema application order.
	sort.Strings(files)

	for _, f := range files {
		name := filepath.Base(f)
		log.Logf("Applying schema: %s", name)

		content, err := os.ReadFile(f)
		require.NoError(t, err, "failed to read schema file %s", name)

		_, err = db.ExecContext(context.Background(), string(content))
		require.NoError(t, err, "failed to execute schema file %s", name)

		log.Logf("Schema applied: %s", name)
	}
}
