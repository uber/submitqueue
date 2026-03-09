# Queue MySQL Integration Tests

Integration tests for the SQL-backed queue (publisher, subscriber, partitioning, rebalance, DLQ, crash recovery).

## Infrastructure

Tests run against a real MySQL 8.0 instance via Docker Compose (`docker-compose.yml`). The `testutil.ComposeStack` helper manages the container lifecycle:

1. Starts MySQL on a random ephemeral port
2. Waits for the health check to pass
3. Connects and applies schemas from `extension/queue/mysql/schema/`
4. Tears down on test completion

All tests share a single MySQL instance within the suite (`SetupSuite` / `TearDownSuite`). Each test uses unique topic names to avoid cross-test interference.

## Running

```bash
make integration-test                     # all integration tests
bazel test //test/integration/extension/queue/mysql:mysql_test --test_output=streamed
```

Requires Docker.

## Event-driven waiting

Tests use **zero `time.Sleep` calls**. Instead, they use the subscriber's `OnSignal` hook — a single channel that emits typed `HookSignal` values after internal lifecycle events complete:

| Signal | Meaning |
|--------|---------|
| `SignalDeliveryCheck` | A partition was checked for deliverable messages (including watermark advancement) |
| `SignalPartitionUpdate` | Partition ownership was evaluated (discovery, rebalance, lease renewal) |

Signal names describe behavioral concerns, not implementation details, so they remain stable across internal refactors.

### Test helpers

| Helper | What it does |
|--------|--------------|
| `receiveWithTimeout` | Blocks on the delivery channel with a 10s safety-net timeout |
| `waitForSignal` | Drains stale signals, then blocks until a signal of the requested type arrives |
| `assertNoDelivery` | Waits for N signals of a given type, then asserts the delivery channel is empty |
| `waitForCondition` | Waits for signals until a condition function returns true (used for rebalance convergence) |

### Why not use defaults for `testSubConfig`?

`testSubConfig` overrides visibility timeout (2s), lease duration (3s), and lease renewal interval (1s). The production defaults are 60s, 30s, and 10s respectively. These control real DB timeouts that the subscriber must wait for — even with event-driven hooks, a message stays invisible until the DB timeout expires. Short values keep crash recovery tests under 5s instead of 90s.

## Test categories

- **Publish/subscribe basics** — ordering, metadata, partitioning, late subscribers, idempotency
- **Visibility and retry** — timeout expiry, `ExtendVisibilityTimeout`, nack with delay
- **Crash recovery** — worker crash with in-flight messages, reject + crash, retry-limit + crash
- **Consumer groups** — independent state, multiple workers in a group, load balancing
- **Rebalance** — even distribution, subscriber leave, odd partitions, excess subscribers
- **Watermark** — contiguous advancement with out-of-order acks
- **Admin CLI** — topic stats, consumer lag, leases, offsets, delete/purge, reset
