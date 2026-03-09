-- DELIVERY STATE TABLE
-- Per-consumer-group delivery tracking for messages in the immutable log.
-- Tracks visibility, ack state, and retry count independently per consumer group.
--
-- State encoding:
--   acked = TRUE                          → processed, never redeliver
--   acked = FALSE, invisible_until > now  → in-flight or nack delay
--   acked = FALSE, invisible_until <= now → ready for (re-)delivery

CREATE TABLE IF NOT EXISTS queue_delivery_state (
    -- Consumer group this delivery state belongs to
    consumer_group VARCHAR(255) NOT NULL,

    -- Topic of the message
    topic VARCHAR(255) NOT NULL,

    -- Partition key of the message
    partition_key VARCHAR(255) NOT NULL,

    -- Offset of the message in the immutable log
    message_offset BIGINT UNSIGNED NOT NULL,

    -- Whether this consumer group has successfully processed this message
    acked BOOLEAN NOT NULL DEFAULT FALSE,

    -- Visibility timeout (epoch milliseconds)
    -- Only meaningful when acked = FALSE.
    -- Future timestamp = in-flight or nack delay, 0/past = ready for delivery.
    invisible_until BIGINT UNSIGNED NOT NULL DEFAULT 0,

    -- Number of times this message has been redelivered to this consumer group
    retry_count INT UNSIGNED NOT NULL DEFAULT 0,

    PRIMARY KEY (consumer_group, topic, partition_key, message_offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
