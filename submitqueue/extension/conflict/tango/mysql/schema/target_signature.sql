-- targets is the binary encoding of the signature's sorted (id, distance)
-- entries. LONGBLOB because the largest signatures run to tens of MB. The row
-- is written once and never updated.
CREATE TABLE IF NOT EXISTS target_signature (
    queue VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    base_revision VARCHAR(255) NOT NULL,
    targets LONGBLOB NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (queue, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
