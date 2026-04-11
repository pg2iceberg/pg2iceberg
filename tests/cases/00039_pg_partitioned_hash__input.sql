-- SETUP --
CREATE TABLE e2e_pg_part_hash (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    score INTEGER NOT NULL
) PARTITION BY HASH (id);

CREATE TABLE e2e_pg_part_hash_0 PARTITION OF e2e_pg_part_hash
    FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE e2e_pg_part_hash_1 PARTITION OF e2e_pg_part_hash
    FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE e2e_pg_part_hash_2 PARTITION OF e2e_pg_part_hash
    FOR VALUES WITH (MODULUS 3, REMAINDER 2);

ALTER TABLE e2e_pg_part_hash_0 REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_hash_1 REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_hash_2 REPLICA IDENTITY FULL;

-- DATA --
INSERT INTO e2e_pg_part_hash (id, name, score) VALUES
    (1, 'alice', 85),
    (2, 'bob', 92),
    (3, 'charlie', 78),
    (4, 'diana', 95);
