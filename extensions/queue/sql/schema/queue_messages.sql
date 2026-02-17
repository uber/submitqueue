-- MESSAGES TABLE
-- Single table for all topics. Partition key determines distribution across workers.
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

    -- Retry tracking (persistent across workers)
    retry_count INT UNSIGNED NOT NULL,

    -- Visibility timeout (epoch milliseconds)
    -- Messages invisible until this timestamp expires
    invisible_until BIGINT UNSIGNED NOT NULL,

    -- Timestamps (epoch milliseconds)
    created_at BIGINT UNSIGNED NOT NULL,
    published_at BIGINT UNSIGNED NOT NULL,

    -- Supports: SELECT ... WHERE topic=? AND partition_key=? AND invisible_until<=? ORDER BY offset
    -- Used by subscribers to poll for ready-to-process messages within their assigned partition
    INDEX idx_topic_partition_visible_offset (topic, partition_key, invisible_until, offset),

    -- Supports: INSERT ... ON DUPLICATE KEY to enforce idempotent publishes
    -- Also enables efficient lookups for message updates/deletes by ID
    UNIQUE KEY idx_topic_partition_id (topic, partition_key, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
