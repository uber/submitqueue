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

    -- DLQ-specific fields (0/"" for normal messages, populated for DLQ messages)
    failed_at BIGINT UNSIGNED NOT NULL,
    -- failure_count stores how many times the message failed on the ORIGINAL topic before moving to DLQ
    -- This is different from retry_count, which tracks retries on the CURRENT topic and gets reset to 0 on DLQ move
    -- We need both because: retry_count must reset for DLQ processing, but we still need to know original failure count
    failure_count INT UNSIGNED NOT NULL,
    last_error TEXT NOT NULL,
    original_topic VARCHAR(255) NOT NULL,

    -- Hot-path index for the subscriber poll loop (FetchByOffset).
    -- Query: SELECT ... WHERE topic=? AND partition_key=? AND offset>? AND invisible_until<=? ORDER BY offset LIMIT ?
    -- Also used by DiscoverPartitions: SELECT DISTINCT partition_key WHERE topic=? (leading prefix).
    INDEX idx_topic_partition_visible_offset (topic, partition_key, invisible_until, offset),

    -- Enforces idempotent publishes — prevents duplicate message IDs within a partition.
    -- Used by AckMessage: DELETE ... WHERE topic=? AND partition_key=? AND id=?
    -- Used by FetchByOffset visibility update: UPDATE ... WHERE topic=? AND partition_key=? AND id IN (...)
    -- Used by Delete: DELETE ... WHERE topic=? AND partition_key=? AND id=?
    -- Used by MoveToDLQ: SELECT/DELETE ... WHERE topic=? AND partition_key=? AND id=?
    -- Used by SetVisibilityTimeout: UPDATE ... WHERE topic=? AND partition_key=? AND id=?
    UNIQUE KEY idx_topic_partition_id (topic, partition_key, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
