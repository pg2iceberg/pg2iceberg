# Kubernetes / Helm Examples

Example values files for deploying pg2iceberg with Helm.

## Install

```sh
helm install pg2iceberg oci://ghcr.io/pg2iceberg/charts/pg2iceberg -f <values-file>
```

## Values files

| File | Description |
|------|-------------|
| `values-combined.yaml` | Single pod, WAL writer + materializer together |
| `values-horizontal.yaml` | One writer + N materializer pods |
| `values-aws.yaml` | RDS + S3 + Glue with IRSA, existingSecret |
| `values-multi-db-users.yaml` | Multi-database setup (users DB) |
| `values-multi-db-orders.yaml` | Multi-database setup (orders DB) |

## Local testing

Prerequisites:

```sh
brew install kind kubectl helm
```

Run:

```sh
cd example/kubernetes
./test-local.sh
```

This will:

1. Start Postgres, MinIO, and Iceberg REST catalog via `example/single` docker-compose
2. Create a [kind](https://kind.sigs.k8s.io/) cluster
3. Build the pg2iceberg image and load it into kind
4. Install the Helm chart pointing at the docker-compose services

Once running:

```sh
# Check pod status
kubectl --context kind-pg2iceberg-test get pods

# View logs
kubectl --context kind-pg2iceberg-test logs deploy/pg2iceberg

# Health check
kubectl --context kind-pg2iceberg-test port-forward deploy/pg2iceberg 9090:9090
curl http://localhost:9090/healthz
curl http://localhost:9090/ready

# Prometheus metrics
curl http://localhost:9090/metrics
```

Tear down:

```sh
./test-local.sh clean
```

## Multiple databases

Each Postgres database needs its own Helm release with a unique `slotName` and `publicationName`:

```sh
helm install pg2iceberg-users oci://ghcr.io/pg2iceberg/charts/pg2iceberg -f values-multi-db-users.yaml
helm install pg2iceberg-orders oci://ghcr.io/pg2iceberg/charts/pg2iceberg -f values-multi-db-orders.yaml
```

## Scaling materializers

In horizontal mode, scale materializer workers with:

```sh
kubectl scale deployment pg2iceberg-materializer --replicas=4
```

Workers claim tables via heartbeat locks and rebalance automatically.
