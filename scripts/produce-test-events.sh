#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${TOPIC:-orders.created.v1}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/compose.yaml}"

echo "Producing test events to ${TOPIC}..."

events=(
  'order-001:{"eventId":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","orderId":"order-001","customerId":"customer-001","amount":99.99,"currency":"USD","createdAt":"2024-01-01T10:00:00Z"}'
  'order-002:{"eventId":"b2c3d4e5-f6a7-8901-bcde-f01234567891","orderId":"order-002","customerId":"customer-002","amount":149.50,"currency":"EUR","createdAt":"2024-01-01T10:01:00Z"}'
  'order-003:{"eventId":"c3d4e5f6-a7b8-9012-cdef-012345678902","orderId":"order-003","customerId":"customer-001","amount":25.00,"currency":"GBP","createdAt":"2024-01-01T10:02:00Z"}'
  'order-004:{"eventId":"d4e5f6a7-b8c9-0123-defa-123456789013","orderId":"order-004","customerId":"customer-003","amount":500.00,"currency":"USD","createdAt":"2024-01-01T10:03:00Z"}'
  'order-005:{"eventId":"e5f6a7b8-c9d0-1234-efab-234567890124","orderId":"order-005","customerId":"customer-002","amount":75.25,"currency":"CAD","createdAt":"2024-01-01T10:04:00Z"}'
)

for event in "${events[@]}"; do
  key="${event%%:*}"
  value="${event#*:}"
  echo "${key}:${value}" | docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    kafka-console-producer --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --topic "${TOPIC}" \
    --property "parse.key=true" \
    --property "key.separator=:"
  echo "  Produced: ${key}"
done

echo "Done producing test events."
