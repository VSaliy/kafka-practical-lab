#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
TIMEOUT="${TIMEOUT:-60}"
echo "Waiting for Kafka at ${BOOTSTRAP_SERVERS} (timeout: ${TIMEOUT}s)..."
end=$((SECONDS + TIMEOUT))
while [ $SECONDS -lt $end ]; do
  if docker compose -f docker/compose.yaml exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "Kafka is ready."
    exit 0
  fi
  echo "Not ready yet, retrying in 2s..."
  sleep 2
done
echo "ERROR: Kafka did not become ready within ${TIMEOUT}s"
exit 1
