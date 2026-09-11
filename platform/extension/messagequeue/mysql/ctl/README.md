# Queue Admin CLI

Admin CLI for inspecting, managing, and troubleshooting the MySQL-backed message queue. Operates directly on the queue database tables (`queue_messages`, `queue_offsets`, `queue_partition_leases`).

## Setup

Start the local stack and find the MySQL queue port:

```bash
make local-submitqueue-start
make local-submitqueue-ps   # note the MySQL Queue port
```

Set the DSN once for the session:

```bash
export QUEUE_MYSQL_DSN="root:root@tcp(localhost:<PORT>)/submitqueue"
```

Or pass `--dsn` on every command.

## Running

Via Make (uses Bazel):

```bash
make run-queue-admin ARGS="list-topics --tenant monorepo/main"
make run-queue-admin ARGS="topic-stats --tenant monorepo/main --topic land_queue"
```

Via Bazel directly:

```bash
bazel run //platform/extension/messagequeue/mysql/ctl -- list-topics --tenant monorepo/main
bazel run //platform/extension/messagequeue/mysql/ctl -- topic-stats --tenant monorepo/main --topic land_queue
```

## Commands

Commands operate on one tenant unless they explicitly accept `--all-tenants`. The `list-topics`, `list-offsets`, `list-leases`, and `stale-leases` commands require exactly one of `--tenant` or `--all-tenants`; fleet-wide queries are never implicit.

### Inspect Topics

```bash
# List one tenant's topics with message counts
queue-admin list-topics --tenant monorepo/main

# Explicitly list topics across every tenant
queue-admin list-topics --all-tenants

# Detailed stats for a topic (total messages, DLQ count, partitions, consumer groups)
queue-admin topic-stats --tenant monorepo/main --topic land_queue
```

### Inspect Messages

```bash
# List messages (default limit 50)
queue-admin list-messages --tenant monorepo/main --topic land_queue

# Filter by partition, custom limit
queue-admin list-messages --tenant monorepo/main --topic land_queue --partition uber/cadence --limit 10

# Full message details including payload and metadata
queue-admin inspect-message --tenant monorepo/main --topic land_queue --partition uber/cadence --message-id msg-123
```

### Manage Messages

Destructive commands prompt for confirmation by default. Use `--no-interactive` to skip prompts (for scripting).

```bash
# Delete a single message
queue-admin delete-message --tenant monorepo/main --topic land_queue --partition uber/cadence --message-id msg-123

# Purge all messages from a topic
queue-admin purge-topic --tenant monorepo/main --topic land_queue

# Skip confirmation prompt (for scripting)
queue-admin purge-topic --tenant monorepo/main --topic land_queue --no-interactive
```

### Dead Letter Queue (DLQ)

DLQ messages live in the same `queue_messages` table under `topic + "_dlq"` (default suffix).

```bash
# List DLQ messages
queue-admin list-dlq --tenant monorepo/main --topic land_queue

# Inspect a DLQ message (use the DLQ topic name)
queue-admin inspect-message --tenant monorepo/main --topic land_queue_dlq --partition uber/cadence --message-id msg-456

# Move a DLQ message back to the original topic
queue-admin requeue-dlq --tenant monorepo/main --topic land_queue --partition uber/cadence --message-id msg-456

# Purge all DLQ messages
queue-admin purge-dlq --tenant monorepo/main --topic land_queue

# Custom DLQ suffix (if not using default "_dlq")
queue-admin list-dlq --tenant monorepo/main --topic land_queue --dlq-suffix _dead
```

### Consumer Lag

```bash
# Per-partition lag for all consumer groups on a topic
queue-admin consumer-lag --tenant monorepo/main --topic land_queue
```

Output shows `ACKED` (last processed offset), `LATEST` (newest message offset), and `LAG` (unprocessed count) per partition per consumer group.

### Consumer Offsets

```bash
# List one tenant's consumer group offsets
queue-admin list-offsets --tenant monorepo/main

# Explicitly list offsets across every tenant
queue-admin list-offsets --all-tenants

# Filter by consumer group
queue-admin list-offsets --tenant monorepo/main --consumer-group orchestrator

# Reset offset to 0 (reprocess all messages)
queue-admin reset-offset --tenant monorepo/main --consumer-group orchestrator --topic land_queue --partition uber/cadence

# Reset to a specific offset
queue-admin reset-offset --tenant monorepo/main --consumer-group orchestrator --topic land_queue --partition uber/cadence --offset 42
```

### Partition Leases

```bash
# List one tenant's active partition leases
queue-admin list-leases --tenant monorepo/main

# Explicitly list active leases across every tenant
queue-admin list-leases --all-tenants

# Find stale leases (not renewed within threshold, likely dead workers)
queue-admin stale-leases --tenant monorepo/main                    # default 60s threshold
queue-admin stale-leases --tenant monorepo/main --threshold 30000  # 30s threshold
queue-admin stale-leases --all-tenants                             # explicit fleet-wide query

# Force-release a stuck lease
queue-admin release-lease --tenant monorepo/main --consumer-group orchestrator --topic land_queue --partition uber/cadence
```

### JSON Output

Add `--json` to any read command for machine-readable output:

```bash
queue-admin list-topics --tenant monorepo/main --json
queue-admin consumer-lag --tenant monorepo/main --topic land_queue --json
queue-admin list-messages --tenant monorepo/main --topic land_queue --json | jq '.[] | .ID'
```
