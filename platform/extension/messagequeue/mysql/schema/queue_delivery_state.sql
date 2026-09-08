-- DELIVERY STATE TABLE
-- Per-consumer-group delivery tracking for messages in the immutable log.
-- Tracks visibility, ack state, and retry count independently per consumer group.
-- tenant/topic/consumer_group: VARCHAR(255) ascii/ascii_bin (255 bytes, byte-wise compare).
-- partition_key: VARCHAR(255) utf8mb4/utf8mb4_bin (255 Unicode chars). See queue_messages.sql.
--
-- State encoding:
--   acked = TRUE                          → processed, never redeliver
--   acked = FALSE, invisible_until > now  → in-flight, nack delay, or postpone delay
--   acked = FALSE, invisible_until <= now → ready for (re-)delivery

CREATE TABLE IF NOT EXISTS queue_delivery_state (
    -- tenant is the shard isolation identity
    tenant VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Consumer group this delivery state belongs to
    consumer_group VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Topic of the message
    topic VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

    -- Partition key of the message
    partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    -- Offset of the message in the immutable log
    message_offset BIGINT UNSIGNED NOT NULL,

    -- Whether this consumer group has successfully processed this message
    acked BOOLEAN NOT NULL DEFAULT FALSE,

    -- Visibility timeout (epoch milliseconds)
    invisible_until BIGINT UNSIGNED NOT NULL DEFAULT 0,

    -- Number of times this message has been redelivered to this consumer group
    retry_count INT UNSIGNED NOT NULL DEFAULT 0,

    -- Whether the last delivery was postponed (deliberate wait, not a failure).
    postponed BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (tenant, consumer_group, topic, partition_key, message_offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
