-- SETUP --
CREATE TABLE e2e_pg_part_list (
    id SERIAL,
    region TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE e2e_pg_part_list_us PARTITION OF e2e_pg_part_list
    FOR VALUES IN ('us');
CREATE TABLE e2e_pg_part_list_eu PARTITION OF e2e_pg_part_list
    FOR VALUES IN ('eu');
CREATE TABLE e2e_pg_part_list_ap PARTITION OF e2e_pg_part_list
    FOR VALUES IN ('ap');

ALTER TABLE e2e_pg_part_list_us REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_list_eu REPLICA IDENTITY FULL;
ALTER TABLE e2e_pg_part_list_ap REPLICA IDENTITY FULL;

-- DATA --
INSERT INTO e2e_pg_part_list (id, region, name) VALUES
    (1, 'us', 'alice'),
    (2, 'eu', 'bob'),
    (3, 'ap', 'charlie'),
    (4, 'us', 'diana');
