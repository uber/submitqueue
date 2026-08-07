-- speculation_path_set holds one head batch's chosen speculation paths, versioned as a whole.
-- queue leads the PK so the table is shardable by queue; head is the head batch's ID, stored
-- opaquely and never parsed.
CREATE TABLE IF NOT EXISTS speculation_path_set (
    queue VARCHAR(255) NOT NULL,
    head VARCHAR(255) NOT NULL,
    paths JSON NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, head)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
