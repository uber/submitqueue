-- Rows are written once and never updated. Two labels can share an id on a
-- hash collision; the first one written stays.
CREATE TABLE IF NOT EXISTS target_dictionary (
    queue VARCHAR(255) NOT NULL,
    id BIGINT UNSIGNED NOT NULL,
    label TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (queue, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
