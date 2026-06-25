# Topic Catalog

All topics provisioned by the `plain-java/admin-client` module.

## Topics

| Topic | Key | Producer | Consumer Group(s) | Partitions | Retention | Cleanup Policy | Schema | Purpose |
|---|---|---|---|---|---|---|---|---|
| `orders.created.v1` | `orderId` | Order Service | inventory-svc, payment-svc, audit-consumer | 3 | 7 days | delete | `OrderCreated` (JSON → Avro) | New order placed by a customer |
| `inventory.commands.v1` | `orderId` | Order Service / Streams | inventory-svc | 3 | 7 days | delete | `ReserveInventory` (planned) | Instruction to reserve stock |
| `inventory.events.v1` | `orderId` | Inventory Service | streams, order-svc | 3 | 7 days | delete | `InventoryEvent` (planned) | Inventory reserved or rejected |
| `payments.commands.v1` | `orderId` | Order Service / Streams | payment-svc | 3 | 7 days | delete | `ChargePayment` (planned) | Instruction to charge a payment |
| `payments.events.v1` | `orderId` | Payment Service | streams, order-svc | 3 | 7 days | delete | `PaymentEvent` (planned) | Payment succeeded or failed |
| `orders.completed.v1` | `orderId` | Kafka Streams | notification-svc | 3 | 7 days | delete | `OrderCompleted` (planned) | Order fulfilled end-to-end |
| `orders.failed.v1` | `orderId` | Kafka Streams | notification-svc | 3 | 7 days | delete | `OrderFailed` (planned) | Order could not be fulfilled |
| `orders.created.v1-dlt` | `orderId` | Consumer framework | ops-team | 1 | 30 days | delete | same as source | Dead-letter topic for poison messages |
| `customer-order-statistics.v1` | `customerId` | Kafka Streams | analytics | 3 | forever | compact | `CustomerOrderStats` (planned) | Compacted per-customer order totals |
| `fraud.alerts.v1` | `orderId` | Kafka Streams | fraud-ops | 3 | 7 days | delete | `FraudAlert` (planned) | Suspicious orders flagged by detector |

## Configuration Details

### Business topics (`orders.created.v1`, `inventory.*`, `payments.*`, `orders.completed.v1`, `orders.failed.v1`, `fraud.alerts.v1`)
- **Partitions:** 3
- **Replication factor:** configured at provisioning time; default 1 for single-broker dev, use 3 for the three-broker cluster
- **Retention:** 604800000 ms (7 days)
- **Cleanup policy:** `delete`

### Dead-letter topic (`orders.created.v1-dlt`)
- **Partitions:** 1
- **Replication factor:** configured at provisioning time; default 1 for single-broker dev, use 3 for the three-broker cluster
- **Retention:** 2592000000 ms (30 days)
- **Cleanup policy:** `delete`
- **Purpose:** Long retention to allow investigation and replay of failed messages

### Statistics topic (`customer-order-statistics.v1`)
- **Partitions:** 3
- **Replication factor:** configured at provisioning time; default 1 for single-broker dev, use 3 for the three-broker cluster
- **Retention:** -1 (infinite, controlled by compaction)
- **Cleanup policy:** `compact`
- **Purpose:** Maintains latest per-customer statistics; compaction preserves the last value per key

## Partition Key Conventions

| Key | Rationale |
|---|---|
| `orderId` | Guarantees all events for one order go to the same partition — preserving causal ordering throughout the order lifecycle |
| `customerId` | Ensures all statistics for a customer land on the same partition — required for correct compaction semantics |

## Provisioning

Topics are provisioned by the `plain-java/admin-client` module using the Kafka AdminClient API.

```bash
# Provision all topics (idempotent)
make topics

# Or directly
./mvnw -pl plain-java/admin-client exec:java \
  -Dexec.mainClass=com.example.kafkalab.admin.AdminClientMain

# Or using the script
bash scripts/create-topics.sh
```

The provisioner:
- Creates missing topics
- Leaves compatible existing topics unchanged
- Logs warnings for incompatible settings (partition count or config mismatches)
- Never deletes or recreates topics
- Exits non-zero on unrecoverable errors
