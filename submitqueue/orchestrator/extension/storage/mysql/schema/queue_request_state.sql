-- Membership records filing each in-queue request under its current state.
-- The (queue, state, request_id) PK *is* the lookup: "requests of queue Q in state S"
-- is a PK-prefix scan, so no secondary index is needed. Rows are written in the same
-- transaction as the request row they mirror and removed when the request reaches a
-- terminal state, so the table only ever holds in-queue requests.
CREATE TABLE IF NOT EXISTS queue_request_state (
    queue         VARCHAR(255) NOT NULL,
    state         VARCHAR(64)  NOT NULL,
    request_id    VARCHAR(255) NOT NULL,
    entered_at_ms BIGINT       NOT NULL,
    PRIMARY KEY (queue, state, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
