#!/usr/bin/env bash
set -euo pipefail

GROUP_ID="${GROUP_ID:-order-audit-consumer}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/compose.yaml}"

echo "=== All Consumer Groups ==="
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  kafka-consumer-groups --bootstrap-server "${BOOTSTRAP_SERVERS}" --list

echo ""
echo "=== Consumer Group: ${GROUP_ID} ==="
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  kafka-consumer-groups --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --describe --group "${GROUP_ID}" || echo "(Group ${GROUP_ID} not found or has no active members)"
