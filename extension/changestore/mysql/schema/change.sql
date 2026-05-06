CREATE TABLE IF NOT EXISTS `change` (
    uri         VARCHAR(255) NOT NULL,
    request_id  VARCHAR(255) NOT NULL,
    queue       VARCHAR(255) NOT NULL,
    metadata    JSON,
    created_at  BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL,
    PRIMARY KEY (uri, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
