-- Queue-local identities pending terminal observation, ordered by the primary key.
CREATE TABLE IF NOT EXISTS active_request (
    queue VARBINARY(255) NOT NULL,
    request_id VARBINARY(255) NOT NULL,
    PRIMARY KEY (queue, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
