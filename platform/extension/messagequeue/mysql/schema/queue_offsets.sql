-- CONSUMER OFFSETS TABLE
-- Tracks consumption progress per consumer group + tenant + topic + partition.
-- Each partition has independent offset tracking for crash recovery.
-- tenant/topic/consumer_group: VARCHAR(255) ascii/ascii_bin (255 bytes, byte-wise compare).
-- partition_key: VARCHAR(255) utf8mb4/utf8mb4_bin (255 Unicode chars). See queue_messages.sql.

CREATE TABLE IF NOT EXISTS queue_offsets (
    -- tenant is the shard isolation identity
    tenant VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Topic being consumed
    topic VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Partition being consumed
    partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    -- Consumer group consuming the topic
    consumer_group VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Last offset that was successfully acked for this partition
    offset_acked BIGINT UNSIGNED NOT NULL,

    -- Last update timestamp (epoch milliseconds)
    updated_at BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY (tenant, topic, partition_key, consumer_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
