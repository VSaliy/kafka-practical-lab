# Transactional Processor

## Purpose

Demonstrate Kafka transactions and exactly-once processing for a Kafka-to-Kafka order workflow.

The processor consumes `orders.created.v1`, applies a deterministic order rule, produces a terminal result, and commits the consumed offsets in the same Kafka transaction:

- orders below `500.00` go to `orders.completed.v1`
- orders at or above `500.00` go to `orders.failed.v1`

This gives exactly-once behavior inside Kafka: downstream consumers using `isolation.level=read_committed` see output records only after the transaction commits, and the input offsets are committed atomically with those output records.

## Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant Producer as Order Producer
    participant Input as orders.created.v1
    participant Processor as Transactional Processor
    participant TxProducer as Kafka Transactional Producer
    participant Completed as orders.completed.v1
    participant Failed as orders.failed.v1
    participant Offsets as Consumer Group Offsets
    participant Downstream as read_committed Consumers

    Producer->>Input: Publish OrderCreated
    Processor->>Input: Poll records
    Processor->>TxProducer: initTransactions()
    Processor->>TxProducer: beginTransaction()

    alt amount < 500.00
        Processor->>TxProducer: send(OrderCompleted)
        TxProducer-->>Completed: Buffered in transaction
    else amount >= 500.00
        Processor->>TxProducer: send(OrderFailed)
        TxProducer-->>Failed: Buffered in transaction
    end

    Processor->>TxProducer: sendOffsetsToTransaction(offsets, groupMetadata)
    TxProducer-->>Offsets: Buffered offset commit
    Processor->>TxProducer: commitTransaction()
    TxProducer-->>Completed: Make completed records visible
    TxProducer-->>Failed: Make failed records visible
    TxProducer-->>Offsets: Commit consumed offsets
    Downstream->>Completed: Read committed results
    Downstream->>Failed: Read committed failures

    rect rgb(229, 231, 235)
        Note over Processor,TxProducer: On unexpected failure: abortTransaction()
        TxProducer-->>Completed: Discard buffered completed records
        TxProducer-->>Failed: Discard buffered failed records
        TxProducer-->>Offsets: Do not commit offsets, input is retried
    end
```

## Run

Start the local stack and provision topics:

```bash
make up
bash scripts/wait-for-kafka.sh
make topics
make produce
```

Run the processor:

```bash
make transactional
```

Or run it directly:

```bash
./mvnw -pl plain-java/transactional-processor exec:java \
  -Dexec.mainClass=com.example.kafkalab.transactional.TransactionalProcessorMain
```

Custom arguments:

```bash
./mvnw -pl plain-java/transactional-processor exec:java \
  -Dexec.mainClass=com.example.kafkalab.transactional.TransactionalProcessorMain \
  '-Dexec.args=--bootstrap-servers localhost:9092 --input-topic orders.created.v1 --completed-topic orders.completed.v1 --failed-topic orders.failed.v1 --group-id order-transactional-processor --transactional-id order-transactional-processor'
```

## Important Limits

Kafka transactions cover Kafka input offsets and Kafka output records. They do not make external side effects exactly once. Database writes, HTTP calls, and emails still need their own idempotency or transactional boundary.
