-- SETUP --
CREATE TABLE e2e_pg_partitioned_range (
    id INTEGER NOT NULL,
    created_at DATE NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE e2e_pg_partitioned_range_2024 PARTITION OF e2e_pg_partitioned_range
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE e2e_pg_partitioned_range_2025 PARTITION OF e2e_pg_partitioned_range
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

ALTER TABLE e2e_pg_partitioned_range_2024 REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_partitioned_range_2025 REPLICA IDENTITY FULL;

-- DATA --
INSERT INTO e2e_pg_partitioned_range (id, created_at, name) VALUES
    (1, '2024-03-15', 'alice'),
    (2, '2024-07-20', 'bob'),
    (3, '2025-02-10', 'charlie'),
    (4, '2025-06-25', 'diana');
