-- counter holds one monotonic sequence per (owner domain, queue, resource kind).
-- queue leads the primary key so deployments can shard counters by queue.
CREATE TABLE IF NOT EXISTS counter (
    owner_domain VARCHAR(255) NOT NULL,
    queue VARCHAR(255) NOT NULL,
    resource_kind VARCHAR(255) NOT NULL,
    value BIGINT NOT NULL,
    PRIMARY KEY (queue, owner_domain, resource_kind)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
