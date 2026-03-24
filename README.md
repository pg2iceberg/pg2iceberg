# pg2iceberg

pg2iceberg replicates data from Postgres directly to Iceberg.

```mermaid
graph LR
  App[Application] <-->|Read/Write| PG

  subgraph pg2iceberg
      PG[Postgres] -->|Replicate| ICE[Iceberg]
  end

  ICE --> OLAP[Snowflake<br />ClickHouse<br />etc.]
```

## Quickstart

```sh
cd example
docker compose up -d --wait
```

Then go to http://localhost:8123 and run:

```sql
select * from rideshare.`rideshare.rides`
```

You should see new rows added over time.

## FAQ

### Will it support other sources and sinks in the future?

No. As its name suggests, it's specifically designed to replicate data from Postgres to Iceberg.
