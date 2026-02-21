package testutil

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/network"
)

// TestLogger is a simple test-aware logger that records elapsed time between logs.
type TestLogger struct {
	t    *testing.T // The testing object to report logs to.
	last time.Time  // Timestamp of the last log, for elapsed calculation.
}

// NewTestLogger creates a TestLogger for the current test.
func NewTestLogger(t *testing.T) *TestLogger {
	t.Helper()
	return &TestLogger{t: t}
}

// Logf prints a formatted log message with timestamp and elapsed time since last log.
func (l *TestLogger) Logf(format string, args ...any) {
	l.t.Helper()
	now := time.Now()
	delta := ""
	if !l.last.IsZero() {
		delta = " +" + now.Sub(l.last).Truncate(time.Millisecond).String()
	}
	l.last = now
	l.t.Logf("[%s%s] "+format, append([]any{now.Format(time.RFC3339Nano), delta}, args...)...)
}

// SchemaDir returns the path to a schema directory.
// It checks for both Bazel runfiles and direct go test paths.
// relativePath should be like "extensions/storage/mysql/schema" or "extensions/queue/sql/schema"
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

// SetupMySQL starts a MySQL container on the given Docker network, applies the schema,
// and returns the container, db connection, and DSN for use in tests.
// The caller is responsible for cleanup (closing db, terminating container).
// schemaPath is the relative path to the schema directory (e.g., "extensions/storage/mysql/schema").
func SetupMySQL(t *testing.T, log *TestLogger, nw *testcontainers.DockerNetwork, schemaPath string) (*mysql.MySQLContainer, *sql.DB, string) {
	t.Helper()

	ctx := context.Background()

	log.Logf("Starting MySQL container")
	mysqlContainer, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithDatabase("submitqueue"),
		mysql.WithUsername("root"),
		mysql.WithPassword("root"),
		network.WithNetwork([]string{"mysql"}, nw),
	)
	require.NoError(t, err, "failed to start MySQL container")
	log.Logf("MySQL container started")

	dsn, err := mysqlContainer.ConnectionString(ctx, "parseTime=true")
	require.NoError(t, err, "failed to get MySQL connection string")
	log.Logf("MySQL DSN obtained: %s", dsn)

	log.Logf("Opening MySQL connection")
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open MySQL connection")
	log.Logf("MySQL connection opened")

	dir := SchemaDir(schemaPath)
	ApplySchema(t, log, db, dir)

	return mysqlContainer, db, dsn
}
