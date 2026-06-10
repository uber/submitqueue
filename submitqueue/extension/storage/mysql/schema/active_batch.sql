-- active_batch is the membership index for "active (non-terminal) batches in a
-- queue", keeping the set bounded by the live speculation window rather than batch
-- history. queue leads the PK so listing is a PK-prefix scan (shardable by queue;
-- portable to a key-value store with queue = partition key, batch_id = sort key).
-- Membership is best-effort: it is added on Create (before the batch row) and
-- removed on read by ListActive once a batch is terminal. A reconcile job reclaims
-- rows left dangling by a failed or crashed create. See schema/README.md.
CREATE TABLE IF NOT EXISTS active_batch (
    queue    VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (queue, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
