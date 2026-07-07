-- request_uri is the reverse index from a validated commit to the request that owns it. The
-- composite PK (queue, uri) enforces exactly one request per (queue, commit URI) — the RFC's
-- dedup guarantee — and a duplicate insert surfaces as the "already being validated" signal.
-- queue leads the PK so the row is addressed by its (queue, uri) key (NoSQL partition/sort style;
-- no secondary indexes). version carries the optimistic-locking version for record consistency
-- with the other stores; the mapping is currently insert-once, so it is written as 1.
CREATE TABLE IF NOT EXISTS request_uri (
    queue      VARCHAR(255) NOT NULL,
    uri        VARCHAR(255) NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    version    INT          NOT NULL,
    PRIMARY KEY (queue, uri)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
