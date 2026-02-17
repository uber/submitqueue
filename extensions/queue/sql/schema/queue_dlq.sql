-- DEAD LETTER QUEUE TABLE
-- Failed messages that exhausted retry attempts.

CREATE TABLE IF NOT EXISTS queue_dlq (
    -- Auto-incrementing global offset for ordering/acking in DLQ
    offset BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

    -- Original topic and partition
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) NOT NULL,

    -- Message identification (for deduplication)
    id VARCHAR(255) NOT NULL,

    -- Message data
    payload BLOB NOT NULL,
    metadata JSON,

    -- Original timestamps (epoch milliseconds)
    created_at BIGINT UNSIGNED NOT NULL,
    published_at BIGINT UNSIGNED NOT NULL,

    -- DLQ-specific fields
    failed_at BIGINT UNSIGNED NOT NULL,
    failure_count INT UNSIGNED NOT NULL,
    last_error TEXT,

    -- Supports: SELECT ... WHERE topic=? AND partition_key=? AND failed_at>=? ORDER BY failed_at
    -- Used for fetching recently failed messages for a specific topic/partition, e.g., for retrying or monitoring
    INDEX idx_topic_partition_failed (topic, partition_key, failed_at),

    -- Supports: SELECT ... WHERE topic=? AND failed_at>=? ORDER BY failed_at
    -- Used for fetching recently failed messages across all partitions of a topic
    INDEX idx_failed_at (topic, failed_at),

    -- Unique constraint to prevent duplicate entries for the same message in the DLQ
    -- Supports: INSERT ... ON DUPLICATE KEY to enforce idempotent DLQ operations
    -- Also enables efficient lookups for retrying or inspecting specific failed messages
    UNIQUE KEY idx_topic_partition_id (topic, partition_key, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
