-- queue holds per-queue coordination state for the validation pipeline: the last-green
-- bookmark, in-flight gate count, and latest-request id pointer.
CREATE TABLE IF NOT EXISTS queue (
    name               VARCHAR(255) NOT NULL,
    last_green_uri     VARCHAR(255) NOT NULL DEFAULT '',
    in_flight_count    INT          NOT NULL DEFAULT 0,
    latest_request_id  VARCHAR(255) NOT NULL DEFAULT '',
    version            INT          NOT NULL,
    PRIMARY KEY (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
