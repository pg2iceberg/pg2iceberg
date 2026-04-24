---
icon: lucide/arrow-right-left
---

# Type Mapping

pg2iceberg maps PostgreSQL column types to Iceberg types automatically during schema discovery. Aliases (e.g. `integer`, `serial`) are normalized to their canonical form before mapping.

## Type map

| PostgreSQL type | Iceberg type | Notes |
|----------------|--------------|-------|
| `smallint` | `int` | |
| `integer`, `serial`, `oid` | `int` | |
| `bigint`, `bigserial` | `long` | |
| `real` | `float` | |
| `double precision` | `double` | |
| `numeric(p,s)` where p ≤ 38 | `decimal(p,s)` | Precision preserved exactly |
| `numeric(p,s)` where p > 38 | — | **Pipeline refuses to start** |
| `numeric` (unconstrained) | `decimal(38,18)` | Warning logged; overflow will error |
| `boolean` | `boolean` | |
| `text`, `varchar`, `char`, `name` | `string` | |
| `bytea` | `binary` | |
| `date` | `date` | |
| `time`, `timetz` | `time` | Microsecond precision |
| `timestamp` | `timestamp` | Microsecond precision |
| `timestamptz` | `timestamptz` | Microsecond precision |
| `uuid` | `uuid` | |
| `json`, `jsonb` | `string` | Stored as text |
| `geometry`, `geography` | `binary` | PostGIS EWKB bytes; SRID recorded in the column `doc` field (see [PostGIS types](#postgis-types)) |
| Other (`inet`, `interval`, `xml`, …) | `string` | Stored as text representation |

## Decimal precision limit

Iceberg supports a maximum decimal precision of 38. If a PostgreSQL table has a `numeric(p,s)` column with `p > 38`, pg2iceberg will refuse to start and will also reject schema evolution that introduces such a column. This is intentional — silently truncating precision would cause data corruption.

Unconstrained `numeric` columns (no precision specified) are mapped to `decimal(38,18)`. A warning is logged at startup. Values that exceed 38 digits of precision will cause a runtime error.

!!! tip
    If you have unconstrained `numeric` columns, add an explicit precision constraint in PostgreSQL (`ALTER TABLE ... ALTER COLUMN ... TYPE numeric(p,s)`) before starting replication to avoid the `decimal(38,18)` fallback.

## PostGIS types

`geometry` and `geography` columns are replicated as PostGIS EWKB bytes into an Iceberg `binary` column. The column's `doc` field carries `postgis:geometry;srid=<N>` (or `postgis:geography;srid=<N>`), which lets downstream readers recognise the column as spatial and a future upgrade to Iceberg v3 native `geometry`/`geography` types be a metadata-only migration.

- **SRID** is looked up from the PostGIS `geometry_columns` / `geography_columns` catalog views at schema discovery. If the extension is not installed, the columns fall back to the standard `binary` mapping with no SRID annotation.
- **`geography`** is validated to have SRID `4326` (PostGIS's only supported geodetic CRS, matching Iceberg v3 geography). Other SRIDs are rejected at startup.
- **Query engines** can read the column with:
    - DuckDB (spatial ext.): `ST_GeomFromWKB(geom)` — accepts PostGIS EWKB.
    - Apache Sedona / Spark: `ST_GeomFromWKB(geom)`.
    - Trino, Athena, Snowflake: no native Iceberg v3 spatial support yet; the column reads as `VARBINARY` and can be parsed with engine-specific WKB/EWKB functions.
- **Schema evolution**: adding a `geometry`/`geography` column mid-stream is recognised on pipeline restart. PostGIS OID resolution during live WAL replication is not yet wired — the column will be treated as `text` until the pipeline is restarted and schema is re-discovered.
