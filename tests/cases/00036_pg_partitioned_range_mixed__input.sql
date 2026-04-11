-- SETUP --
CREATE TABLE e2e_pg_part_range_mixed (
    id INTEGER NOT NULL,
    created_at DATE NOT NULL,
    name TEXT NOT NULL,
    amount INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE e2e_pg_part_range_mixed_2024 PARTITION OF e2e_pg_part_range_mixed
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE e2e_pg_part_range_mixed_2025 PARTITION OF e2e_pg_part_range_mixed
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

ALTER TABLE e2e_pg_part_range_mixed_2024 REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_range_mixed_2025 REPLICA IDENTITY FULL;

-- DATA --
INSERT INTO e2e_pg_part_range_mixed (id, created_at, name, amount) VALUES
    (1, '2024-06-01', 'alice', 100),
    (2, '2024-09-15', 'bob', 200),
    (3, '2025-03-10', 'charlie', 300),
    (4, '2025-08-20', 'diana', 400);
UPDATE e2e_pg_part_range_mixed SET amount = 150 WHERE id = 1 AND created_at = '2024-06-01';
DELETE FROM e2e_pg_part_range_mixed WHERE id = 2 AND created_at = '2024-09-15';
INSERT INTO e2e_pg_part_range_mixed (id, created_at, name, amount) VALUES
    (5, '2025-11-05', 'eve', 500);
