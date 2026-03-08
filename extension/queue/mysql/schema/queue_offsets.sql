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

    -- Ensures each consumer group has one offset per topic/partition.
    -- Used by Initialize: INSERT IGNORE ... (exact PK match for duplicate check)
    -- Used by GetAckedOffset: SELECT ... WHERE consumer_group=? AND topic=? AND partition_key=?
    -- Used by UpdateAckedOffset: UPDATE ... WHERE consumer_group=? AND topic=? AND partition_key=?
    -- Used by AckMessage: INSERT ... ON DUPLICATE KEY UPDATE (exact PK match)
    -- Used by admin ListOffsets: SELECT ... WHERE consumer_group=? (PK leading prefix)
    -- Note: idx_consumer_group was removed — consumer_group is the leading PK column,
    --   so MySQL already uses the PK for WHERE consumer_group=? queries.
    PRIMARY KEY (consumer_group, topic, partition_key),

    -- Used by admin queries: SELECT COUNT(DISTINCT consumer_group) WHERE topic=?
    -- topic is the second column of the PK, so WHERE topic=? alone cannot use the PK.
    -- This index enables admin/monitoring queries to find all consumer groups for a topic.
    INDEX idx_topic (topic)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
