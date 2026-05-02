-- Verify that a `CREATE TABLE ... PARTITION OF <parent>` issued after
-- pg2iceberg has started replicating is picked up automatically — i.e.
-- that `publish_via_partition_root = true` on the publication makes
-- newly attached children inherit the parent's publication membership
-- without any operator action. Mirrors how partitioned tables are
-- typically managed in production: a partition manager creates next
-- year's range as the calendar rolls.

-- SETUP --
CREATE TABLE e2e_pg_part_add (
    id INTEGER NOT NULL,
    created_at DATE NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE e2e_pg_part_add_2024 PARTITION OF e2e_pg_part_add
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE e2e_pg_part_add_2025 PARTITION OF e2e_pg_part_add
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

ALTER TABLE e2e_pg_part_add_2024 REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_add_2025 REPLICA IDENTITY FULL;

-- DATA --
INSERT INTO e2e_pg_part_add (id, created_at, name) VALUES
    (1, '2024-03-15', 'alice'),
    (2, '2025-06-25', 'bob');

-- SLEEP 5 --
-- DATA --
-- New partition attached AFTER pg2iceberg started; with
-- publish_via_partition_root the child inherits the parent's
-- publication membership transparently.
CREATE TABLE e2e_pg_part_add_2026 PARTITION OF e2e_pg_part_add
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE e2e_pg_part_add_2026 REPLICA IDENTITY FULL;

-- SLEEP 3 --
-- DATA --
INSERT INTO e2e_pg_part_add (id, created_at, name) VALUES
    (3, '2026-04-10', 'charlie'),
    (4, '2026-08-22', 'diana');
