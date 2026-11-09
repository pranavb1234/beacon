#!/usr/bin/env bash
# scripts/demo.sh - Demonstrates 3-node Beacon cluster request routing with Docker Compose.
#
# 1. Starts 3 brokers via docker compose with healthchecks.
# 2. Identifies and prints which broker owns topic "orders" per the consistent hash ring.
# 3. Starts a subscriber connected to broker1 (host port 50051) for topic "orders".
# 4. Publishes 20 messages through broker3 (host port 50053).
# 5. Shows cross-node routing delivery in action.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/docker-compose.yml"

cd "${ROOT_DIR}"

echo "============================================================"
echo " Beacon 3-Node Cluster Demo"
echo "============================================================"

# Ensure local binaries are built for publisher/subscriber
echo "==> Building local publisher and subscriber CLI tools..."
go build -o "${ROOT_DIR}/bin/publisher" "${ROOT_DIR}/cmd/publisher"
go build -o "${ROOT_DIR}/bin/subscriber" "${ROOT_DIR}/cmd/subscriber"
go build -o "${ROOT_DIR}/bin/broker" "${ROOT_DIR}/cmd/broker"

# 1. Start cluster with Docker Compose
echo ""
echo "==> Step 1: Starting cluster with Docker Compose..."
docker compose -f "${COMPOSE_FILE}" up -d --build

echo "==> Waiting for brokers to become healthy..."
MAX_WAIT=30
WAITED=0
until [ "${WAITED}" -ge "${MAX_WAIT}" ]; do
    UNHEALTHY=$(docker compose -f "${COMPOSE_FILE}" ps --format json 2>/dev/null | grep -i "starting\|unhealthy" || true)
    if [ -z "${UNHEALTHY}" ]; then
        break
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done

# Check status
docker compose -f "${COMPOSE_FILE}" ps

# 4. Print which broker owns "orders"
echo ""
echo "==> Step 4 (Admin check): Querying topic ownership for 'orders'..."
BEACON_NODE_ID="broker1" \
BEACON_LISTEN_ADDR=":9001" \
BEACON_PEERS="broker2=broker2:9001,broker3=broker3:9001" \
"${ROOT_DIR}/bin/broker" -topic-owner orders

# 2. Start subscriber on broker1 (port 50051) in background
echo ""
echo "==> Step 2: Starting subscriber on broker1 (127.0.0.1:50051) for topic 'orders'..."
"${ROOT_DIR}/bin/subscriber" -addr 127.0.0.1:50051 -topic orders -id demo-subscriber &
SUB_PID=$!

# Ensure subscriber is killed on exit
cleanup() {
    echo ""
    echo "==> Stopping background subscriber (PID: ${SUB_PID})..."
    kill -INT "${SUB_PID}" 2>/dev/null || true
    wait "${SUB_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# Allow subscriber to connect and establish stream
sleep 2

# 3. Publish 20 messages through broker3 (port 50053)
echo ""
echo "==> Step 3: Publishing 20 messages through broker3 (127.0.0.1:50053)..."
"${ROOT_DIR}/bin/publisher" \
    -addr 127.0.0.1:50053 \
    -topic orders \
    -count 20 \
    -rate 10 \
    -payload "package-item"

# Allow in-flight messages to be delivered and printed by subscriber
sleep 2

echo ""
echo "============================================================"
echo " Demo completed successfully!"
echo "============================================================"
echo "Useful commands:"
echo "  View broker cluster logs:"
echo "    docker compose -f deploy/docker-compose.yml logs -f"
echo ""
echo "  Stop a specific broker (e.g. broker2):"
echo "    docker compose -f deploy/docker-compose.yml stop broker2"
echo ""
echo "  Tear down the cluster:"
echo "    docker compose -f deploy/docker-compose.yml down"
echo "============================================================"
