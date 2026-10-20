-- Repairs retain explicit lifecycle fields, typed issue opening membership, and optional metadata.
CREATE TABLE IF NOT EXISTS fix (
    queue         VARBINARY(1020) NOT NULL,
    id            VARBINARY(1020) NOT NULL,
    build_id      TEXT            NOT NULL,
    state         VARCHAR(64)     NOT NULL,
    issues        JSON            NOT NULL,
    execution_ref TEXT            NOT NULL,
    metadata      JSON            NOT NULL,
    created_at    BIGINT          NOT NULL,
    updated_at    BIGINT          NOT NULL,
    version       INT             NOT NULL,
    PRIMARY KEY (queue, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
