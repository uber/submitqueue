-- SUBSCRIBER HEARTBEATS TABLE
-- Tracks active subscribers for fair partition leasing.
-- Each subscriber registers itself with periodic heartbeat renewal.
-- Stale entries (no renewal within lease duration) are considered dead.
-- Soft-deleted via deregistered_at during graceful shutdown.

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

    PRIMARY KEY (consumer_group, topic, subscriber_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
