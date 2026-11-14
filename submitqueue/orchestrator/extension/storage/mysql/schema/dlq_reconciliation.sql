-- VARBINARY keeps queue/topic byte-exact, including trailing spaces.
-- utf8mb4_0900_bin is NO PAD: opaque partition/message IDs remain distinct.
-- Maximum primary-key payload: 255 + 255 + 4*255 + 4*255 = 2550 bytes.
CREATE TABLE IF NOT EXISTS dlq_reconciliation (
    queue VARBINARY(255) NOT NULL,
    source_topic VARBINARY(255) NOT NULL,
    source_partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    source_message_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    id VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    batch_ids JSON NOT NULL,
    completed BOOLEAN NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, source_topic, source_partition_key, source_message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
