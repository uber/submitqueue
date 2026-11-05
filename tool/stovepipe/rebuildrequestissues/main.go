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

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
)

func main() {
	queue := flag.String("queue", "", "configured queue containing the request")
	requestID := flag.String("request-id", "", "canonical decimal request ID to repair")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := rebuildRequestIssues(ctx, os.Getenv("STOVEPIPE_MYSQL_DSN"), *queue, *requestID); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Request issue projection repaired.")
}

func rebuildRequestIssues(ctx context.Context, dsn, queue, requestID string) error {
	if dsn == "" {
		return fmt.Errorf("STOVEPIPE_MYSQL_DSN must be set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open projection storage: %w", err)
	}
	defer db.Close()
	backend, err := mysql.NewStorage(db, tally.NoopScope)
	if err != nil {
		return err
	}
	stores, err := backend.For(queue)
	if err != nil {
		return err
	}
	return requestlog.RebuildRequestIssues(ctx, stores, queue, requestID)
}
