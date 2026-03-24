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

## FAQ

### Will it support other sources and sinks in the future?

No. As its name suggests, it's specifically designed to replicate data from Postgres to Iceberg.
