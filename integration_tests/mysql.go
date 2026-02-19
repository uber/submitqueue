package integration_tests

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

// testLogger is a simple test-aware logger that records elapsed time between logs.
type testLogger struct {
	t    *testing.T // The testing object to report logs to.
	last time.Time  // Timestamp of the last log, for elapsed calculation.
}

// newTestLogger creates a testLogger for the current test.
func newTestLogger(t *testing.T) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

// logf prints a formatted log message with timestamp and elapsed time since last log.
func (l *testLogger) logf(format string, args ...any) {
	l.t.Helper()
	now := time.Now()
	delta := ""
	if !l.last.IsZero() {
		delta = " +" + now.Sub(l.last).Truncate(time.Millisecond).String()
	}
	l.last = now
	l.t.Logf("[%s%s] "+format, append([]any{now.Format(time.RFC3339Nano), delta}, args...)...)
}

// schemaDir returns the path to the MySQL schema directory.
// It checks for both Bazel runfiles and direct go test paths.
func schemaDir() string {
	// Bazel runfiles path
	if dir := os.Getenv("TEST_SRCDIR"); dir != "" {
		return filepath.Join(dir, os.Getenv("TEST_WORKSPACE"), "extensions/storage/mysql/schema")
	}
	// Direct go test path (run from repo root)
	return "extensions/storage/mysql/schema"
}

// applySchema reads all .sql files from the schema directory and executes them on the database.
func applySchema(t *testing.T, log *testLogger, db *sql.DB) {
	t.Helper()

	dir := schemaDir()
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	require.NoError(t, err, "failed to glob schema files")
	require.NotEmpty(t, files, "no .sql schema files found in %s", dir)

	// Sort files to ensure deterministic schema application order.
	sort.Strings(files)

	for _, f := range files {
		name := filepath.Base(f)
		log.logf("Applying schema: %s", name)

		content, err := os.ReadFile(f)
		require.NoError(t, err, "failed to read schema file %s", name)

		_, err = db.ExecContext(context.Background(), string(content))
		require.NoError(t, err, "failed to execute schema file %s", name)

		log.logf("Schema applied: %s", name)
	}
}

// setupMySQL starts a MySQL container on the given Docker network, applies the schema,
// and registers cleanup. The container is reachable by other containers on the network at "mysql:3306".
func setupMySQL(t *testing.T, log *testLogger, nw *testcontainers.DockerNetwork) {
	t.Helper()

	ctx := context.Background()

	log.logf("Starting MySQL container")
	mysqlContainer, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithDatabase("submitqueue"),
		mysql.WithUsername("root"),
		mysql.WithPassword("root"),
		network.WithNetwork([]string{"mysql"}, nw),
	)
	require.NoError(t, err, "failed to start MySQL container")
	log.logf("MySQL container started")
	t.Cleanup(func() {
		log.logf("Terminating MySQL container")
		require.NoError(t, mysqlContainer.Terminate(ctx), "failed to terminate MySQL container")
		log.logf("MySQL container terminated")
	})

	dsn, err := mysqlContainer.ConnectionString(ctx, "parseTime=true")
	require.NoError(t, err, "failed to get MySQL connection string")
	log.logf("MySQL DSN obtained: %s", dsn)

	log.logf("Opening MySQL connection")
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open MySQL connection")
	log.logf("MySQL connection opened")
	defer db.Close()

	applySchema(t, log, db)
}
