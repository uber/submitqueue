-- counter holds one monotonic sequence per (queue, domain). queue leads the PK so the
-- table is shardable by queue; domain names the sequence within it (e.g. "request", "batch").
CREATE TABLE IF NOT EXISTS counter (
    queue VARCHAR(255) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    value BIGINT NOT NULL,
    PRIMARY KEY (queue, domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
