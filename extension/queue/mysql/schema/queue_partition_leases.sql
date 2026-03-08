-- PARTITION LEASES TABLE
-- Tracks which worker has leased which partition for exclusive processing.
-- Workers must renew leases to maintain ownership; stale leases can be stolen.

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

    -- Ensures each partition can only be leased by one worker per consumer group.
    -- Used by TryAcquireLease: INSERT ... ON DUPLICATE KEY UPDATE (exact PK match)
    -- Used by RenewLease: UPDATE ... WHERE consumer_group=? AND topic=? AND partition_key=? AND leased_by=?
    -- Used by ReleaseLease: DELETE ... WHERE consumer_group=? AND topic=? AND partition_key=? AND leased_by=?
    -- Used by GetLeasedPartitions: SELECT ... WHERE consumer_group=? AND topic=? AND leased_by=?
    --   (PK prefix consumer_group, topic narrows the scan; leased_by filtered on remaining rows.
    --    Partition count per topic per consumer group is small, so no separate index needed.)
    -- Note: idx_leased_by was removed — no query uses WHERE leased_by=? alone.
    PRIMARY KEY (consumer_group, topic, partition_key),

    -- Used by admin StaleLeases: SELECT ... WHERE lease_renewed_at < ?
    -- Enables efficient detection of stale leases that can be stolen by other workers.
    INDEX idx_lease_renewed (lease_renewed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
