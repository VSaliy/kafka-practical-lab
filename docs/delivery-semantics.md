# Delivery Semantics

This document explains the delivery guarantee models available in Apache Kafka and how they apply to this lab.

## Overview

Kafka's delivery semantics describe what happens when a producer sends a message and a consumer reads it. Understanding these guarantees is essential for building reliable event-driven systems.

| Semantic | Producer side | Consumer side | Use case |
|---|---|---|---|
| At-most-once | Fire and forget | Auto-commit before processing | Metrics, logs where loss is acceptable |
| At-least-once | Acks + retries | Commit after processing | Most business events |
| Exactly-once (Kafka-internal) | Idempotent + transactions | Read-process-write in same transaction | Kafka-to-Kafka pipelines |

---

## At-Most-Once

A message may be **lost** but will never be delivered more than once.

**Producer side:**
- `acks=0` — fire-and-forget, no acknowledgement
- Or `acks=1` with no retries

**Consumer side:**
- Commit offsets **before** processing
- If the consumer crashes after committing but before finishing processing, the message is lost

```java
// Dangerous: offset committed before processing
consumer.commitSync();
processRecord(record); // crash here → message lost
```

**When to use:** Metrics collection, non-critical telemetry, real-time dashboards where a small amount of data loss is acceptable.

---

## At-Least-Once

A message will be delivered **one or more times** — it may be processed more than once.

**Producer side:**
- `acks=all` — wait for all in-sync replicas to acknowledge
- Retries enabled (`retries=Integer.MAX_VALUE`)
- `enable.idempotence=true` to prevent duplicates *within a single producer session*

**Consumer side:**
- Commit offsets **after** successful processing
- If the consumer crashes after processing but before committing, the message will be redelivered on restart

```java
// At-least-once: process first, then commit
processRecord(record);  // crash here → redelivered on restart
consumer.commitSync(Map.of(
    new TopicPartition(record.topic(), record.partition()),
    new OffsetAndMetadata(record.offset() + 1)
));
```

**When to use:** The default choice for most business events when downstream processing is idempotent.

---

## Exactly-Once Within Kafka

Messages are processed **exactly once** end-to-end — but only within Kafka itself.

This is achieved by combining:
1. **Idempotent producer:** `enable.idempotence=true` — the broker deduplicates retries within a producer epoch
2. **Transactions:** `transactional.id` on the producer — atomically publish to multiple topics/partitions
3. **Transactional consumer:** `isolation.level=read_committed` — only reads messages from committed transactions

```java
// Exactly-once Kafka Streams or transactional producer/consumer pattern
producer.initTransactions();
try {
    producer.beginTransaction();
    producer.send(new ProducerRecord<>(outputTopic, key, value));
    producer.sendOffsetsToTransaction(offsets, groupMetadata);
    producer.commitTransaction();
} catch (ProducerFencedException e) {
    producer.close();
} catch (KafkaException e) {
    producer.abortTransaction();
}
```

**Important:** This guarantees exactly-once *within Kafka*. External side effects (database writes, HTTP calls, emails) are **not** covered — see [Duplicate Processing](#duplicate-processing-and-idempotent-consumers) below.

---

## Duplicate Processing and Idempotent Consumers

When using at-least-once delivery, your downstream processing must be designed to handle duplicates safely.

### Strategies for idempotent consumers

**1. Natural idempotency**
The operation produces the same result when repeated.
```java
// Safe: setting a value is idempotent
database.upsert("order_status", orderId, "COMPLETED");
```

**2. Deduplication with a seen-IDs store**
Track the `eventId` and skip already-processed events.
```java
if (seenEvents.contains(event.eventId())) {
    log.info("Skipping duplicate event {}", event.eventId());
    return;
}
// process...
seenEvents.add(event.eventId()); // persist atomically with the effect
```

**3. Conditional update**
Only apply the change if the current state allows it.
```java
// Only mark as shipped if currently in PAID state
database.updateWhere("orders", "status = 'PAID'", orderId, "status", "SHIPPED");
```

All `OrderCreated` events in this lab include a `UUID eventId` field specifically to support deduplication.

---

## Producer Idempotence

`enable.idempotence=true` (default since Kafka 3.0) gives the producer sequence numbers and a producer ID (PID). The broker uses these to detect and discard retried duplicates **within the same producer session**.

Requirements imposed by idempotence:
- `acks=all`
- `max.in.flight.requests.per.connection <= 5`
- `retries > 0`

The `OrderProducerFactory` in this lab configures all of these correctly.

**Caveat:** A new producer instance gets a new PID, so idempotence does not deduplicate across application restarts. Use transactions + a stable `transactional.id` if you need that guarantee.

---

## Offset Commit Timing

Offset commit timing is the primary lever that controls at-most-once vs. at-least-once for consumers.

| Commit timing | Semantic | Risk |
|---|---|---|
| Auto-commit (background) | Approximately at-most-once | Messages inflight when commit fires may be lost on crash |
| Manual commit before processing | At-most-once | Message lost if crash before processing |
| Manual commit after processing | At-least-once | Message reprocessed if crash before commit |
| Transactional (sendOffsetsToTransaction) | Exactly-once (Kafka-internal) | Complexity; only works for Kafka→Kafka |

**This lab always uses manual commit after processing** (`enable.auto.commit=false`).

```java
// From OrderEventConsumer — partition-level commit
Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
offsets.put(
    new TopicPartition(record.topic(), record.partition()),
    new OffsetAndMetadata(record.offset() + 1)
);
consumer.commitSync(offsets);
```

Committing per-partition (rather than calling `commitSync()` with no arguments) is safer: it commits only the offsets you have actually processed.

---

## Partition-Scoped Ordering

Kafka only guarantees ordering **within a single partition**.

- All events for a given `orderId` are produced with `orderId` as the key
- Kafka's default partitioner hashes the key and routes all records with the same key to the same partition
- This guarantees that `OrderCreated → InventoryReserved → PaymentCharged → OrderCompleted` events for one order are consumed in sequence

**Do not rely on ordering across partitions or topics.**

---

## Lab Exercises Related to Delivery Semantics

| Module | Semantic demonstrated |
|---|---|
| `plain-java/producer` | At-least-once with idempotent producer |
| `plain-java/consumer` | Manual offset commit (at-least-once) |
| `plain-java/transactional-processor` *(planned)* | Exactly-once Kafka-internal |
| `streams/*` *(planned)* | Exactly-once via Kafka Streams processing guarantee |

See the [Learning Roadmap](../README.md#learning-roadmap) for the full exercise sequence.
