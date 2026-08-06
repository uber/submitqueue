-- Membership records filing each in-queue batch under a lifecycle state bucket.
-- The (queue, state, batch_id) PK *is* the lookup: "batches of queue Q in state S"
-- is a PK-prefix scan, so no secondary index is needed. Rows are advisory — the
-- authoritative state is batch.state — and a batch's rows are removed when it
-- exits the queue, so the table only ever holds in-queue batches.
CREATE TABLE IF NOT EXISTS queue_batch_state (
    queue    VARCHAR(255) NOT NULL,
    state    VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (queue, state, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
