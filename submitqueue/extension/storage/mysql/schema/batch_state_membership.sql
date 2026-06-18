-- batch_state_membership is the app-maintained lookup for
-- queue,state -> batch_ids. Batch rows are authoritative: callers resolve IDs
-- through the batch table and filter by the current persisted state.
CREATE TABLE IF NOT EXISTS batch_state_membership (
    queue    VARCHAR(255) NOT NULL,
    state    VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (queue, state, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
