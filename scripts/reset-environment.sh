#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker/compose.yaml}"
DESTROY_VOLUMES="${DESTROY_VOLUMES:-false}"

for arg in "$@"; do
  if [ "$arg" = "--destroy" ]; then
    DESTROY_VOLUMES=true
  fi
done

if [ "$DESTROY_VOLUMES" = "true" ]; then
  echo "WARNING: This will destroy all Kafka data!"
  read -r -p "Are you sure? (yes/no): " confirm
  if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
  fi
  echo "Stopping containers and removing volumes..."
  docker compose -f "${COMPOSE_FILE}" down --volumes --remove-orphans
  echo "Environment destroyed."
else
  echo "Stopping containers (data preserved)..."
  docker compose -f "${COMPOSE_FILE}" down
  echo "Environment stopped."
fi
