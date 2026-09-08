-- MESSAGES TABLE (Immutable Log)
-- Single table for all topics. tenant is the Vitess vindex; partition_key orders work within a tenant.
-- Messages are append-only; per-consumer-group delivery tracking is in queue_delivery_state.
-- Example: tenant="monorepo/main", topic="merge_queue", partition_key="uber/cadence"

CREATE TABLE IF NOT EXISTS queue_messages (
    -- tenant is the shard isolation identity (SubmitQueue maps queueName here at wiring)
    tenant VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Topic identifies the pipeline stage
    topic VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Partition key for distributing work across workers within a tenant
    partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    -- Auto-incrementing offset for ordering within (tenant, topic, partition_key)
    offset BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- Message identification
    id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    -- Message data
    payload BLOB NOT NULL,
    metadata JSON,

    -- Timestamps (epoch milliseconds)
    created_at BIGINT UNSIGNED NOT NULL,
    published_at BIGINT UNSIGNED NOT NULL,

    -- DLQ-specific fields (0/"" for normal messages, populated for DLQ messages)
    failed_at BIGINT UNSIGNED NOT NULL,
    failure_count INT UNSIGNED NOT NULL,
    last_error TEXT NOT NULL,
    original_topic VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    failure_detail JSON,

    PRIMARY KEY (tenant, topic, partition_key, offset),

    -- Supports: INSERT ... ON DUPLICATE KEY to enforce idempotent publishes
    UNIQUE KEY idx_tenant_topic_partition_id (tenant, topic, partition_key, id),

    -- InnoDB requires AUTO_INCREMENT column to be leftmost on some index
    KEY idx_offset (offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
