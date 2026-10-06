-- Immutable lookup keys for acceptance-time listing; lifecycle state stays in request_summary.
-- VARBINARY preserves bytewise request-ID ordering, including case and trailing spaces.
-- 1020 bytes accommodates the existing VARCHAR(255) utf8mb4 request identifiers.
CREATE TABLE IF NOT EXISTS request_acceptance (
    queue          VARCHAR(255)   NOT NULL,
    accepted_at_ms BIGINT         NOT NULL,
    request_id     VARBINARY(1020) NOT NULL,
    PRIMARY KEY (queue, accepted_at_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
