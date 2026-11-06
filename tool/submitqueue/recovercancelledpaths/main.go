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

// Command recovercancelledpaths nudges explicitly selected queues through the
// ordinary speculation consumer without changing any stored outcome.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/uber-go/tally"
	resourceid "github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/consumer"
	queuemysql "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/publish"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	corebatch "github.com/uber/submitqueue/submitqueue/orchestrator/core/batch"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mysql"
	"go.uber.org/zap"
)

const recoveryCause = "cancelled-path-recovery"

type options struct {
	queues     []string
	recoveryID string
	apply      bool
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	var dryRun bool
	flags := flag.NewFlagSet("recovercancelledpaths", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Func("queue", "queue to inspect (required, repeatable)", func(queue string) error {
		if queue == "" || strings.TrimSpace(queue) != queue {
			return fmt.Errorf("queue must be non-empty without surrounding whitespace")
		}
		for _, existing := range opts.queues {
			if existing == queue {
				return fmt.Errorf("duplicate queue %q", queue)
			}
		}
		opts.queues = append(opts.queues, queue)
		return nil
	})
	flags.StringVar(&opts.recoveryID, "recovery-id", "", "stable operator invocation identity (required with --apply)")
	flags.BoolVar(&opts.apply, "apply", false, "publish recovery nudges")
	flags.BoolVar(&dryRun, "dry-run", false, "inspect only (the default)")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if len(opts.queues) == 0 || flags.NArg() != 0 {
		return options{}, fmt.Errorf("explicit --queue selections and no positional arguments are required")
	}
	if opts.apply && dryRun {
		return options{}, fmt.Errorf("--apply conflicts with --dry-run")
	}
	if opts.apply && strings.TrimSpace(opts.recoveryID) == "" {
		return options{}, fmt.Errorf("--recovery-id is required with --apply")
	}
	return opts, nil
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	opts, err := parseOptions(args, output)
	if err != nil {
		return err
	}
	appDB, err := openDatabase("MYSQL_DSN")
	if err != nil {
		return err
	}
	defer appDB.Close()
	backend, err := mysqlstorage.NewStorage(appDB, tally.NoopScope)
	if err != nil {
		return err
	}

	var registry consumer.TopicRegistry
	if opts.apply {
		queueDB, err := openDatabase("QUEUE_MYSQL_DSN")
		if err != nil {
			return err
		}
		defer queueDB.Close()
		queue, err := queuemysql.NewQueue(queuemysql.Params{DB: queueDB, Logger: zap.NewNop(), MetricsScope: tally.NoopScope})
		if err != nil {
			return err
		}
		defer queue.Close()
		registry, err = consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: sqmq.TopicKeySpeculate, Name: "speculate", Queue: queue}})
		if err != nil {
			return err
		}
	}
	return publishCancelledPathRecovery(ctx, opts, backend.For, registry, output)
}

func openDatabase(env string) (*sql.DB, error) {
	dsn := os.Getenv(env)
	if dsn == "" {
		return nil, fmt.Errorf("%s environment variable is required", env)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid %s configuration", env)
	}
	return db, nil
}

func publishCancelledPathRecovery(ctx context.Context, opts options, storeFor func(string) (orchstorage.Storage, error), registry consumer.TopicRegistry, output io.Writer) error {
	var failures []error
	for _, queue := range opts.queues {
		if err := recoverQueue(ctx, opts, queue, storeFor, registry, output); err != nil {
			failures = append(failures, fmt.Errorf("queue %q: %w", queue, err))
		}
	}
	return errors.Join(failures...)
}

func recoverQueue(ctx context.Context, opts options, queue string, storeFor func(string) (orchstorage.Storage, error), registry consumer.TopicRegistry, output io.Writer) error {
	store, err := storeFor(queue)
	if err != nil {
		return err
	}
	if store == nil {
		return fmt.Errorf("storage is required")
	}
	headID, err := affectedHead(ctx, store)
	if err != nil {
		return err
	}
	if headID == "" {
		_, err := fmt.Fprintf(output, "queue=%q unaffected\n", queue)
		return err
	}
	if _, err := fmt.Fprintf(output, "queue=%q affected-head=%s\n", queue, headID); err != nil {
		return err
	}
	if !opts.apply {
		return nil
	}
	payload, err := sqmq.MarshalID(sqmq.TopicKeySpeculate, headID, queue)
	if err != nil {
		return err
	}
	// This nudge depends only on stored facts; the consumer owns all decisions.
	if err := publish.Message(ctx, registry, sqmq.TopicKeySpeculate, publish.MessageParams{
		Tenant: queue, PartitionKey: queue, Payload: payload,
		ID: publish.IntentID(headID, recoveryCause, opts.recoveryID),
	}); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "queue=%q queued-head=%s recovery-id=%s\n", queue, headID, opts.recoveryID)
	return err
}

func affectedHead(ctx context.Context, store orchstorage.Storage) (string, error) {
	batches, err := corebatch.ListByStates(ctx, store, []entity.BatchState{entity.BatchStateSpeculating})
	if err != nil {
		return "", err
	}
	var selected string
	for _, batch := range batches {
		set, err := store.GetSpeculationPathSetStore().Get(ctx, batch.ID)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !hasCancelledPath(set) {
			continue
		}
		if err := resourceid.Validate(batch.ID); err != nil {
			return "", err
		}
		if selected == "" {
			selected = batch.ID
			continue
		}
		order, err := resourceid.Compare(batch.ID, selected)
		if err != nil {
			return "", err
		}
		if order < 0 {
			selected = batch.ID
		}
	}
	return selected, nil
}

func hasCancelledPath(set entity.SpeculationPathSet) bool {
	for _, path := range set.Paths {
		if path.Status == entity.SpeculationPathStatusCancelled {
			return true
		}
	}
	return false
}
