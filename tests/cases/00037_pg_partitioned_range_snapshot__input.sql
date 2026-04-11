-- SETUP --
CREATE TABLE e2e_pg_part_range_snap (
    id INTEGER NOT NULL,
    created_at DATE NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE e2e_pg_part_range_snap_2024 PARTITION OF e2e_pg_part_range_snap
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE e2e_pg_part_range_snap_2025 PARTITION OF e2e_pg_part_range_snap
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

ALTER TABLE e2e_pg_part_range_snap_2024 REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_range_snap_2025 REPLICA IDENTITY FULL;

-- Pre-existing rows: inserted BEFORE pg2iceberg starts.
-- These should be captured by initial snapshot.
INSERT INTO e2e_pg_part_range_snap (id, created_at, name) VALUES
    (1, '2024-03-15', 'alice'),
    (2, '2024-09-20', 'bob'),
    (3, '2025-02-10', 'charlie');

-- DATA --
-- Rows inserted AFTER pg2iceberg starts (captured via WAL).
INSERT INTO e2e_pg_part_range_snap (id, created_at, name) VALUES
    (4, '2024-11-05', 'diana'),
    (5, '2025-06-25', 'eve');
