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

// Package mysql provides an errs.Classifier for errors originating from the
// go-sql-driver/mysql driver and the standard database/sql + net packages
// commonly seen when talking to a MySQL backend.
//
// The classifier inspects a single error node at a time, as required by the
// errs.Classifier contract. It returns errs.Unknown for nodes it does not
// recognise so the surrounding errs.Classify chain walk can continue to
// deeper nodes.
package mysql

import (
	"database/sql"
	"database/sql/driver"
	"net"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/uber/submitqueue/core/errs"
)

// Classifier implements errs.Classifier for MySQL-related errors. It recognises:
//
//   - *gomysql.MySQLError values, dispatching on the server-reported error
//     number to one of InfraRetryable (transient server / lock / connection
//     issues) or Infra (everything else the driver reports — schema bugs,
//     constraint violations, programmer errors).
//   - driver.ErrBadConn and sql.ErrConnDone — pooled connection failures that
//     are safe to retry on a fresh connection.
//   - sql.ErrTxDone — a programming error, non-retryable.
//   - net.Error values (including *net.OpError, *net.DNSError) — transient
//     network failures while talking to the server, retryable.
//
// Anything else returns errs.Unknown so the surrounding errs.Classify walker
// can keep looking down the unwrap chain.
//
// The classifier never returns errs.User. Constraint violations and similar
// codes that a caller might want to surface as user errors must be wrapped
// explicitly with errs.NewUserError at the controller — only the controller
// knows whether a duplicate key, FK violation, etc. reflects bad input from
// the user or an internal invariant being broken. The framework-wrap check
// in errs.Classify short-circuits before this classifier runs, so an
// explicit controller wrap always wins.
//
// The classifier is stateless; this package-level singleton is the canonical
// handle. Pass it into errs.Classify (typically as a vararg to consumer.New).
var Classifier errs.Classifier = classifier{}

type classifier struct{}

// Classify dispatches a single error node to one of the recognisers above.
// See the Classifier var docs for the full list.
func (classifier) Classify(err error) errs.Verdict {
	// MySQL server-reported errors. We do the type assertion directly on the
	// current node (not errors.As) so an outer framework wrap (e.g. an explicit
	// NewUserError from the controller) keeps winning — errs.Classify owns the
	// chain walk and stops at any *userError / *infraError it sees first.
	if me, ok := err.(*gomysql.MySQLError); ok {
		return classifyMySQLNumber(me.Number)
	}

	// Pooled-connection lifecycle errors from database/sql.
	switch err {
	case driver.ErrBadConn, sql.ErrConnDone:
		return errs.InfraRetryable
	case sql.ErrTxDone:
		// Using a transaction after Commit/Rollback is a programmer bug, not
		// a transient failure — retrying will not help.
		return errs.Infra
	}

	// Any network-layer failure while talking to the server is transient by
	// nature: connection reset, timeout, DNS hiccup, etc. The net.Error
	// interface covers *net.OpError, *net.DNSError, and other concrete types.
	if _, ok := err.(net.Error); ok {
		return errs.InfraRetryable
	}

	return errs.Unknown
}

// classifyMySQLNumber maps a server-reported MySQL error number to a Verdict.
// Codes not listed here return Unknown so the chain walker can continue.
//
// This classifier never returns User. Constraint violations (duplicate key,
// FK violation, check constraint) are mapped to Infra because only the
// calling controller knows whether the violation reflects bad user input or
// an internal invariant being broken — controllers must wrap with
// errs.NewUserError explicitly when the former applies.
//
// References:
//   - https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
//   - https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html
func classifyMySQLNumber(number uint16) errs.Verdict {
	switch number {
	// --- Transient: server load / locking / connection-level failures. ---
	case
		1040, // ER_CON_COUNT_ERROR — too many connections
		1042, // ER_BAD_HOST_ERROR — can't get hostname (DNS / lookup)
		1043, // ER_HANDSHAKE_ERROR
		1053, // ER_SERVER_SHUTDOWN
		1077, // ER_NORMAL_SHUTDOWN
		1078, // ER_GOT_SIGNAL
		1079, // ER_SHUTDOWN_COMPLETE
		1080, // ER_FORCING_CLOSE
		1129, // ER_HOST_IS_BLOCKED
		1130, // ER_HOST_NOT_PRIVILEGED — usually a transient ACL replication lag
		1158, // ER_NET_READ_ERROR_FROM_PIPE
		1159, // ER_NET_READ_INTERRUPTED
		1160, // ER_NET_ERROR_ON_WRITE
		1161, // ER_NET_WRITE_INTERRUPTED
		1205, // ER_LOCK_WAIT_TIMEOUT
		1213, // ER_LOCK_DEADLOCK
		1290, // ER_OPTION_PREVENTS_STATEMENT — read-only mode (failover in progress)
		1317, // ER_QUERY_INTERRUPTED
		1836, // ER_READ_ONLY_MODE
		2002, // CR_CONNECTION_ERROR — can't connect via socket
		2003, // CR_CONN_HOST_ERROR — can't connect via TCP
		2004, // CR_IPSOCK_ERROR — can't create TCP/IP socket
		2005, // CR_UNKNOWN_HOST
		2006, // CR_SERVER_GONE_ERROR
		2013, // CR_SERVER_LOST
		2055: // CR_SERVER_LOST_EXTENDED
		return errs.InfraRetryable

	// --- Non-retryable: schema bugs, constraint violations, programmer errors.
	// Controllers wrap with errs.NewUserError when a constraint violation
	// reflects bad user input; otherwise these stay infra. ---
	case
		1022, // ER_DUP_KEY (older duplicate-key code)
		1044, // ER_DBACCESS_DENIED_ERROR — auth/grants misconfiguration
		1045, // ER_ACCESS_DENIED_ERROR
		1049, // ER_BAD_DB_ERROR — unknown database
		1054, // ER_BAD_FIELD_ERROR — unknown column
		1062, // ER_DUP_ENTRY — duplicate value for a unique key
		1064, // ER_PARSE_ERROR — SQL syntax
		1146, // ER_NO_SUCH_TABLE
		1149, // ER_SYNTAX_ERROR
		1169, // ER_DUP_UNIQUE
		1216, // ER_NO_REFERENCED_ROW — FK constraint fails (insert/update child)
		1217, // ER_ROW_IS_REFERENCED — FK constraint fails (delete parent)
		1364, // ER_NO_DEFAULT_FOR_FIELD
		1366, // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD — encoding / type mismatch
		1406, // ER_DATA_TOO_LONG
		1451, // ER_ROW_IS_REFERENCED_2
		1452, // ER_NO_REFERENCED_ROW_2
		1557, // ER_FOREIGN_DUPLICATE_KEY
		3819: // ER_CHECK_CONSTRAINT_VIOLATED
		return errs.Infra
	}

	// Unrecognised number — defer to the surrounding chain walk and/or default
	// non-retryable infra treatment in the caller.
	return errs.Unknown
}
