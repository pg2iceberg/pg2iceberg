#!/usr/bin/env bash
set -euo pipefail

# Test the Helm chart locally using kind + existing docker-compose infra.
#
# Prerequisites: docker, kind, kubectl, helm
#   brew install kind kubectl helm
#
# Usage:
#   cd example/kubernetes
#   ./test-local.sh        # setup and install
#   ./test-local.sh clean  # tear down everything

CLUSTER_NAME="pg2iceberg-test"
RELEASE_NAME="pg2iceberg"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ "${1:-}" = "clean" ]; then
    echo "==> Cleaning up..."
    kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
    (cd "$SCRIPT_DIR/../single" && docker compose down -v 2>/dev/null) || true
    echo "Done."
    exit 0
fi

# 1. Start infrastructure via docker-compose.
echo "==> Starting infrastructure (Postgres, MinIO, Iceberg REST)..."
(cd "$SCRIPT_DIR/../single" && docker compose up -d --wait postgres minio create-bucket iceberg-postgres iceberg-rest)

# 2. Create kind cluster with host access.
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "==> kind cluster '$CLUSTER_NAME' already exists"
else
    echo "==> Creating kind cluster..."
    cat <<EOF | kind create cluster --name "$CLUSTER_NAME" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30090
        hostPort: 30090
        protocol: TCP
EOF
fi

# 3. Get the host IP accessible from inside kind containers.
HOST_IP=$(docker inspect "${CLUSTER_NAME}-control-plane" \
    --format '{{ .NetworkSettings.Networks.kind.Gateway }}')
echo "==> Host gateway: $HOST_IP"

# 4. Build and load image into kind.
echo "==> Building pg2iceberg image..."
docker build -t pg2iceberg:local "$REPO_ROOT"
echo "==> Loading image into kind..."
kind load docker-image pg2iceberg:local --name "$CLUSTER_NAME"

# 5. Patch values to use the actual host IP (replace host-gateway placeholder).
TMPVALUES=$(mktemp)
sed "s/host-gateway/${HOST_IP}/g" "$SCRIPT_DIR/values-local.yaml" > "$TMPVALUES"

# 6. Install (or upgrade) the Helm chart.
echo "==> Installing Helm chart..."
helm upgrade --install "$RELEASE_NAME" "$REPO_ROOT/charts/pg2iceberg" \
    -f "$TMPVALUES" \
    --kube-context "kind-${CLUSTER_NAME}" \
    --wait --timeout 60s

rm -f "$TMPVALUES"

echo ""
echo "==> Done! Check status:"
echo "    kubectl --context kind-${CLUSTER_NAME} get pods"
echo "    kubectl --context kind-${CLUSTER_NAME} logs deploy/${RELEASE_NAME}"
echo ""
echo "    Healthz:  kubectl --context kind-${CLUSTER_NAME} port-forward deploy/${RELEASE_NAME} 9090:9090"
echo "              curl http://localhost:9090/healthz"
echo ""
echo "    Cleanup:  ./test-local.sh clean"
