-- CONSUMER OFFSETS TABLE
-- Tracks consumption progress per consumer group + topic + partition.
-- Each partition has independent offset tracking for crash recovery.

CREATE TABLE IF NOT EXISTS queue_offsets (
    -- Consumer group consuming the topic
    consumer_group VARCHAR(255) NOT NULL,

    -- Topic being consumed
    topic VARCHAR(255) NOT NULL,

    -- Partition being consumed
    partition_key VARCHAR(255) NOT NULL,

    -- Last offset that was successfully acked for this partition
    offset_acked BIGINT UNSIGNED NOT NULL,

    -- Last update timestamp (epoch milliseconds)
    updated_at BIGINT UNSIGNED NOT NULL,

    -- Primary key ensures each consumer group has one offset per topic/partition
    -- Supports: INSERT ... ON DUPLICATE KEY UPDATE for idempotent offset updates
    -- Also enables efficient lookups: SELECT ... WHERE consumer_group=? AND topic=? AND partition_key=?
    PRIMARY KEY (consumer_group, topic, partition_key),

    -- Supports: SELECT ... WHERE consumer_group=?
    -- Used for querying all offsets for a specific consumer group (e.g., for monitoring or rebalancing)
    INDEX idx_consumer_group (consumer_group),

    -- Supports: SELECT ... WHERE topic=?
    -- Used for querying all consumer groups consuming a specific topic
    INDEX idx_topic (topic)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
