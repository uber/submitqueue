-- MESSAGES TABLE (Immutable Log)
-- Single table for all topics. Partition key determines distribution across workers.
-- Messages are append-only; per-consumer-group delivery tracking is in queue_delivery_state.
-- Example: topic="merge_queue", partition_key="uber/cadence"

CREATE TABLE IF NOT EXISTS queue_messages (
    -- Auto-incrementing global offset for ordering
    offset BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

    -- Topic identifies the queue type
    topic VARCHAR(255) NOT NULL,

    -- Partition key for distributing work across workers
    -- Example: repo ID, user ID, tenant ID
    partition_key VARCHAR(255) NOT NULL,

    -- Message identification
    id VARCHAR(255) NOT NULL,

    -- Message data
    payload BLOB NOT NULL,
    metadata JSON,

    -- Timestamps (epoch milliseconds)
    created_at BIGINT UNSIGNED NOT NULL,
    published_at BIGINT UNSIGNED NOT NULL,

    -- DLQ-specific fields (0/"" for normal messages, populated for DLQ messages)
    failed_at BIGINT UNSIGNED NOT NULL,
    -- failure_count stores how many times the message failed on the ORIGINAL topic before moving to DLQ
    failure_count INT UNSIGNED NOT NULL,
    last_error TEXT NOT NULL,
    original_topic VARCHAR(255) NOT NULL,

    -- visible_after defers delivery: subscribers skip rows where visible_after > now.
    -- 0 (the default) means immediately visible. Set by Publisher.PublishAfter
    -- to schedule a fresh message for delivery at a future time without
    -- consuming a delivery_state retry slot (used e.g. by the orchestrator's
    -- buildstatus polling consumer to space out Status calls).
    -- Placed last so incremental UQL migrations can ADD COLUMN without reordering.
    visible_after BIGINT UNSIGNED NOT NULL DEFAULT 0,

    -- Supports: SELECT ... WHERE topic=? AND partition_key=? AND offset > ? ORDER BY offset
    -- Used by subscribers to poll for messages within their assigned partition
    INDEX idx_topic_partition_offset (topic, partition_key, offset),

    -- Supports: INSERT ... ON DUPLICATE KEY to enforce idempotent publishes
    -- Also enables efficient lookups for message updates/deletes by ID
    UNIQUE KEY idx_topic_partition_id (topic, partition_key, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
