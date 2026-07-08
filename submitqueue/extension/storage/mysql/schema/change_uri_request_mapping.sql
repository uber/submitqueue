CREATE TABLE IF NOT EXISTS change_uri_request_mapping (
    change_uri VARCHAR(255) NOT NULL,
    received_at_ms BIGINT NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (change_uri, received_at_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
