# pg2iceberg

pg2iceberg replicates data from Postgres directly to Iceberg.

```mermaid
graph LR
  App[Application] <-->|Read/Write| PG

  subgraph pg2iceberg
      PG[Postgres] -->|Replicate| ICE[Iceberg]
  end

  OLAP[Snowflake<br />ClickHouse<br />etc.] -- Query --> ICE
```

## Quickstart

```sh
cd example/single
docker compose up -d --wait
```

Then go to http://localhost:8123/play and run:

```sql
select * from rideshare.`rideshare.rides`
```

You should see new rows added over time.

## Single vs Multitenant mode

pg2iceberg has two modes of operation:

### Single mode

Runs one pipeline from a config file. This is the simplest way to replicate a single Postgres database into Iceberg.

```sh
docker run -v ./config.yaml:/etc/pg2iceberg/config.yaml \
  pg2iceberg --config /etc/pg2iceberg/config.yaml
```

See [`example/single`](example/single) for a full working example.

### Multitenant (server) mode

Runs an HTTP API server that manages multiple pipelines. Pipelines are created, listed, and deleted via REST API.

```sh
docker run -p 8080:8080 pg2iceberg \
  --server \
  --listen=:8080 \
  --store-url="postgresql://postgres:postgres@mydb:5432/pg2iceberg?sslmode=disable"
```

The web UI is a separate container that proxies API requests to the server:

```sh
docker run -p 3000:80 pg2iceberg-ui
```

Then open http://localhost:3000 to manage pipelines.

See [`example/multitenant`](example/multitenant) for a full working example with multiple Postgres sources.

## FAQ

### Will it support other sources and sinks in the future?

No. As its name suggests, it's specifically designed to replicate data from Postgres to Iceberg.
