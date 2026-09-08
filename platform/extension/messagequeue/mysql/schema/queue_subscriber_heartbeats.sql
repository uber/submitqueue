-- SUBSCRIBER HEARTBEATS TABLE
-- Tracks active subscribers for fair partition leasing per tenant.
-- Each subscriber registers itself with periodic heartbeat renewal.

CREATE TABLE IF NOT EXISTS queue_subscriber_heartbeats (
    -- tenant is the shard isolation identity
    tenant VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- consumer_group identifies the consumer group this subscriber belongs to
    consumer_group VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- topic is the topic this subscriber is consuming from
    topic VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- subscriber_name uniquely identifies this subscriber within the consumer group
    subscriber_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- heartbeat_at is the Unix timestamp in milliseconds of the last heartbeat
    heartbeat_at BIGINT UNSIGNED NOT NULL,

    -- deregistered_at is the Unix timestamp in milliseconds when the subscriber was deregistered.
    -- 0 means active, >0 means deregistered at that time.
    deregistered_at BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY (tenant, consumer_group, topic, subscriber_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
