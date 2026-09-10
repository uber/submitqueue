-- PARTITION LEASES TABLE
-- Tracks which worker has leased which partition for exclusive processing.
-- Workers must renew leases to maintain ownership; stale leases can be stolen.
-- tenant/topic/consumer_group/leased_by: VARCHAR(255) ascii/ascii_bin (255 bytes, byte-wise compare).
-- partition_key: VARCHAR(255) utf8mb4/utf8mb4_bin (255 Unicode chars). See queue_messages.sql.

CREATE TABLE IF NOT EXISTS queue_partition_leases (
    -- tenant is the shard isolation identity
    tenant VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Consumer group (e.g., "orchestrator")
    consumer_group VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Topic being consumed
    topic VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Partition that is leased
    partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    -- Worker that owns the lease (e.g., "worker-1")
    leased_by VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- When lease was acquired (epoch milliseconds)
    leased_at BIGINT UNSIGNED NOT NULL,

    -- Last lease renewal timestamp (epoch milliseconds)
    lease_renewed_at BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY (tenant, consumer_group, topic, partition_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
