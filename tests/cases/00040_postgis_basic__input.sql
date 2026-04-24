-- SETUP --
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE e2e_postgis_basic (
    id   SERIAL PRIMARY KEY,
    name TEXT,
    pt   geometry(Point, 4326),
    loc  geography(Point, 4326)
);
ALTER TABLE e2e_postgis_basic REPLICA IDENTITY FULL;
-- DATA --
INSERT INTO e2e_postgis_basic (id, name, pt, loc) VALUES
(1, 'Origin',
    ST_SetSRID(ST_MakePoint(1.5, 2.5), 4326),
    ST_SetSRID(ST_MakePoint(1.5, 2.5), 4326)::geography),
(2, 'NYC',
    ST_SetSRID(ST_MakePoint(-73.9857, 40.7484), 4326),
    ST_SetSRID(ST_MakePoint(-73.9857, 40.7484), 4326)::geography),
(3, 'NullGeom', NULL, NULL);
