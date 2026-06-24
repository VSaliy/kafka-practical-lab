#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/compose.yaml}"

echo "Creating topics on ${BOOTSTRAP_SERVERS}..."

create_topic() {
  local name="$1"
  local partitions="$2"
  local replication="$3"
  local config="${4:-}"

  cmd="kafka-topics --bootstrap-server ${BOOTSTRAP_SERVERS} --create --if-not-exists --topic ${name} --partitions ${partitions} --replication-factor ${replication}"
  if [ -n "$config" ]; then
    cmd="$cmd --config $config"
  fi

  docker compose -f "${COMPOSE_FILE}" exec -T kafka bash -c "$cmd"
  echo "  Topic: ${name}"
}

create_topic "orders.created.v1" 3 1 "retention.ms=604800000"
create_topic "inventory.commands.v1" 3 1 "retention.ms=604800000"
create_topic "inventory.events.v1" 3 1 "retention.ms=604800000"
create_topic "payments.commands.v1" 3 1 "retention.ms=604800000"
create_topic "payments.events.v1" 3 1 "retention.ms=604800000"
create_topic "orders.completed.v1" 3 1 "retention.ms=604800000"
create_topic "orders.failed.v1" 3 1 "retention.ms=604800000"
create_topic "orders.created.v1-dlt" 1 1 "retention.ms=2592000000"
create_topic "customer-order-statistics.v1" 3 1 "cleanup.policy=compact"
create_topic "fraud.alerts.v1" 3 1 "retention.ms=604800000"

echo "All topics created successfully."
