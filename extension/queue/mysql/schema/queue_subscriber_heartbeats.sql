-- SUBSCRIBER HEARTBEATS TABLE
-- Tracks active subscribers for fair partition leasing.
-- Each subscriber registers itself with periodic heartbeat renewal.
-- Stale entries (no renewal within lease duration) are considered dead.

CREATE TABLE IF NOT EXISTS queue_subscriber_heartbeats (
    -- consumer_group identifies the consumer group this subscriber belongs to
    consumer_group VARCHAR(255) NOT NULL,
    -- topic is the topic this subscriber is consuming from
    topic VARCHAR(255) NOT NULL,
    -- subscriber_name uniquely identifies this subscriber within the consumer group
    subscriber_name VARCHAR(255) NOT NULL,
    -- heartbeat_at is the Unix timestamp in milliseconds of the last heartbeat
    heartbeat_at BIGINT UNSIGNED NOT NULL,
    -- deregistered_at is the Unix timestamp in milliseconds when the subscriber was deregistered.
    -- 0 means active, >0 means deregistered at that time.
    deregistered_at BIGINT UNSIGNED NOT NULL,

    -- Ensures each subscriber has exactly one heartbeat entry per consumer group + topic.
    -- Used by Heartbeat: INSERT ... ON DUPLICATE KEY UPDATE (exact PK match for upsert)
    -- Used by Deregister: UPDATE ... WHERE consumer_group=? AND topic=? AND subscriber_name=? (exact PK match)
    -- Used by ActiveSubscribers: SELECT ... WHERE consumer_group=? AND topic=? AND heartbeat_at>=? AND deregistered_at=0
    --   (PK prefix consumer_group, topic narrows the scan; heartbeat_at and deregistered_at filtered
    --    on remaining rows. Subscriber count per topic is small — typically a handful of workers —
    --    so no secondary index needed.)
    PRIMARY KEY (consumer_group, topic, subscriber_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
