-- validation_fact holds the immutable record of how broken one scope was at one commit.
-- The composite PK (queue, uri, project) is the fact's identity and makes the write
-- create-only: a second insert for the same identity raises a duplicate-key error rather
-- than overwriting the first fact. queue leads the PK so the table is shardable by queue.
-- project is empty for a whole-repository fact and names a narrower scope otherwise; it is
-- part of the key rather than a nullable attribute so both kinds of fact coexist per commit.
CREATE TABLE IF NOT EXISTS validation_fact (
    queue       VARCHAR(255) NOT NULL,
    uri         VARCHAR(255) NOT NULL,
    project     VARCHAR(255) NOT NULL DEFAULT '',
    degree      DOUBLE       NOT NULL,
    request_id  VARCHAR(255) NOT NULL,
    created_at  BIGINT       NOT NULL,
    PRIMARY KEY (queue, uri, project)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
