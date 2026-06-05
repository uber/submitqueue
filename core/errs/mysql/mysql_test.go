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

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/core/errs"
)

func TestClassifier_MySQLErrorNumbers(t *testing.T) {
	tests := []struct {
		name   string
		number uint16
		want   errs.Verdict
	}{
		// Constraint violations -> Infra. The classifier never returns User;
		// controllers wrap with NewUserError explicitly when the violation
		// reflects bad user input.
		{"duplicate entry (1062)", 1062, errs.Infra},
		{"FK insert (1452)", 1452, errs.Infra},
		{"FK delete (1451)", 1451, errs.Infra},
		{"check constraint (3819)", 3819, errs.Infra},

		// Transient server / lock / connection -> InfraRetryable.
		{"deadlock (1213)", 1213, errs.InfraRetryable},
		{"lock wait timeout (1205)", 1205, errs.InfraRetryable},
		{"too many connections (1040)", 1040, errs.InfraRetryable},
		{"server shutdown (1053)", 1053, errs.InfraRetryable},
		{"read-only mode (1290)", 1290, errs.InfraRetryable},
		{"server gone (2006)", 2006, errs.InfraRetryable},
		{"server lost (2013)", 2013, errs.InfraRetryable},

		// Programmer / schema bugs -> Infra.
		{"unknown column (1054)", 1054, errs.Infra},
		{"syntax error (1064)", 1064, errs.Infra},
		{"no such table (1146)", 1146, errs.Infra},
		{"truncated value (1366)", 1366, errs.Infra},

		// Unknown numbers -> Unknown so the chain walker keeps looking.
		{"unrecognized number (9999)", 9999, errs.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &gomysql.MySQLError{Number: tt.number, Message: "test"}
			assert.Equal(t, tt.want, Classifier.Classify(err))
		})
	}
}

func TestClassifier_ConnectionLifecycle(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want errs.Verdict
	}{
		{"driver bad conn", driver.ErrBadConn, errs.InfraRetryable},
		{"sql conn done", sql.ErrConnDone, errs.InfraRetryable},
		{"sql tx done", sql.ErrTxDone, errs.Infra},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_NetErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "net.OpError",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: errors.New("connection refused"),
			},
		},
		{
			name: "net.DNSError",
			err:  &net.DNSError{Err: "no such host", Name: "mysql"},
		},
		{
			name: "timeoutError",
			err:  netTimeoutErr{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, errs.InfraRetryable, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_Unknown(t *testing.T) {
	// A plain, unrecognised error must yield Unknown so the surrounding
	// classifier-processor walker can move on to the next node in the chain
	// rather than locking in a verdict.
	assert.Equal(t, errs.Unknown, Classifier.Classify(errors.New("anything")))
	assert.Equal(t, errs.Unknown, Classifier.Classify(nil))
}

func TestClassifier_AppliedViaProcessor(t *testing.T) {
	// End-to-end behavior: the consumer's call site runs the configured
	// errs.ErrorProcessor (typically errs.NewClassifierProcessor(
	// mysqlerrs.Classifier)) and then inspects errs.IsUserError /
	// IsRetryable / IsDependencyError. These tests pin that contract — given
	// an err from a controller, the returned chain answers the right question.
	p := errs.NewClassifierProcessor(Classifier)

	t.Run("mysql connection error surfaces as retryable infra", func(t *testing.T) {
		// Simulates the queue or storage layer returning a wrapped net.OpError
		// for a failed connection to MySQL.
		netErr := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")}
		wrapped := fmt.Errorf("publish: %w", netErr)

		out := p.Process(wrapped)
		assert.True(t, errs.IsRetryable(out))
		assert.False(t, errs.IsUserError(out))
	})

	t.Run("mysql deadlock surfaces as retryable infra", func(t *testing.T) {
		dl := &gomysql.MySQLError{Number: 1213, Message: "deadlock"}
		wrapped := fmt.Errorf("update: %w", dl)

		out := p.Process(wrapped)
		assert.True(t, errs.IsRetryable(out))
	})

	t.Run("mysql schema error is non-retryable infra", func(t *testing.T) {
		se := &gomysql.MySQLError{Number: 1054, Message: "unknown column"}
		wrapped := fmt.Errorf("select: %w", se)

		// Verdict Infra means "non-retryable infra" — the default for an
		// unwrapped chain — so the processor leaves the chain alone.
		out := p.Process(wrapped)
		assert.False(t, errs.IsRetryable(out))
		assert.False(t, errs.IsUserError(out))
		assert.Same(t, wrapped, out, "Infra verdict should not add a wrap")
	})

	t.Run("mysql duplicate key is non-retryable infra", func(t *testing.T) {
		// The classifier never returns User — a controller that wants to surface
		// a duplicate-key as a user error must wrap with errs.NewUserError
		// explicitly (see the controller-override test below).
		dup := &gomysql.MySQLError{Number: 1062, Message: "duplicate"}
		out := p.Process(dup)
		assert.False(t, errs.IsRetryable(out))
		assert.False(t, errs.IsUserError(out))
		assert.Same(t, dup, out, "Infra verdict should not add a wrap")
	})

	t.Run("controller User wrap turns duplicate key into a user error", func(t *testing.T) {
		dup := &gomysql.MySQLError{Number: 1062, Message: "duplicate"}
		err := errs.NewUserError(fmt.Errorf("create: %w", dup))

		out := p.Process(err)
		assert.Same(t, err, out)
		assert.True(t, errs.IsUserError(out))
		assert.False(t, errs.IsRetryable(out))
	})

	t.Run("controller-level User wrap beats deeper mysql classification", func(t *testing.T) {
		// Even though the underlying cause is a transient mysql deadlock, the
		// outermost frame is an explicit NewUserError. The processor sees a
		// framework wrap already in the chain and returns the chain untouched.
		deadlock := &gomysql.MySQLError{Number: 1213, Message: "deadlock"}
		err := errs.NewUserError(fmt.Errorf("conflict: %w", deadlock))

		out := p.Process(err)
		assert.Same(t, err, out)
		assert.True(t, errs.IsUserError(out))
		assert.False(t, errs.IsRetryable(out))
	})

	t.Run("controller-level Retryable wrap beats deeper mysql schema verdict", func(t *testing.T) {
		// Inverse: a schema error would normally be non-retryable infra, but the
		// controller chose to mark it retryable anyway. The outer framework wrap
		// is already in the chain, so the processor is a no-op.
		schemaErr := &gomysql.MySQLError{Number: 1054, Message: "unknown column"}
		err := errs.NewRetryableError(fmt.Errorf("query: %w", schemaErr))

		out := p.Process(err)
		assert.Same(t, err, out)
		assert.True(t, errs.IsRetryable(out))
	})

	t.Run("unwrapped non-mysql error stays unclassified", func(t *testing.T) {
		raw := errors.New("something else")
		out := p.Process(raw)
		assert.Same(t, raw, out)
		assert.False(t, errs.IsRetryable(out))
		assert.False(t, errs.IsUserError(out))
	})
}

// netTimeoutErr satisfies net.Error so we can verify the net.Error branch of
// Classifier.Classify without having to provoke a real network timeout.
type netTimeoutErr struct{}

func (netTimeoutErr) Error() string   { return "i/o timeout" }
func (netTimeoutErr) Timeout() bool   { return true }
func (netTimeoutErr) Temporary() bool { return true }

// Sanity check: net.Error is the interface we rely on.
var _ net.Error = netTimeoutErr{}
