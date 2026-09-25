-- change_uri_request_mapping is the reverse index from a change URI to the requests that
-- claimed it, serving bounded newest-first Status lookup by change URI. queue leads the PK so
-- the table is shardable by queue: a change URI landed into several queues is looked up one
-- queue at a time, and each queue's mappings are unreachable through another queue's binding.
CREATE TABLE IF NOT EXISTS change_uri_request_mapping (
    queue VARCHAR(255) NOT NULL,
    change_uri VARCHAR(255) NOT NULL,
    received_at_ms BIGINT NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (queue, change_uri, received_at_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
