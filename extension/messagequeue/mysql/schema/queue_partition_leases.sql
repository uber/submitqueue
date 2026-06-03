-- PARTITION LEASES TABLE
-- Tracks which worker has leased which partition for exclusive processing.
-- Workers must renew leases to maintain ownership; stale leases can be stolen.
--
-- The primary key (consumer_group, topic, partition_key) serves as the main
-- lookup index. Queries by leased_by always include consumer_group and topic,
-- so the primary key's left-prefix is sufficient.

CREATE TABLE IF NOT EXISTS queue_partition_leases (
    -- Consumer group (e.g., "orchestrator")
    consumer_group VARCHAR(255) NOT NULL,

    -- Topic being consumed
    topic VARCHAR(255) NOT NULL,

    -- Partition that is leased
    partition_key VARCHAR(255) NOT NULL,

    -- Worker that owns the lease (e.g., "worker-1")
    leased_by VARCHAR(255) NOT NULL,

    -- When lease was acquired (epoch milliseconds)
    leased_at BIGINT UNSIGNED NOT NULL,

    -- Last lease renewal timestamp (epoch milliseconds)
    -- Used to detect stale leases
    lease_renewed_at BIGINT UNSIGNED NOT NULL,

    -- Primary key ensures each partition can only be leased by one worker per consumer group.
    -- Supports: INSERT ... ON DUPLICATE KEY UPDATE for lease acquisition and renewal.
    -- Also enables efficient lookups: SELECT ... WHERE consumer_group=? AND topic=? AND partition_key=?
    -- Left-prefix covers: SELECT ... WHERE consumer_group=? AND topic=? AND leased_by=?
    PRIMARY KEY (consumer_group, topic, partition_key),

    -- Supports: SELECT ... WHERE lease_renewed_at<?
    -- Used for finding stale leases that can be stolen by other workers
    INDEX idx_lease_renewed (lease_renewed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
