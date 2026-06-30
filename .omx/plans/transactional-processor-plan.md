# Transactional Processor Plan

## Requirements Summary

Implement `plain-java/transactional-processor`, currently a placeholder Maven module (`plain-java/transactional-processor/pom.xml`) with a README TODO (`plain-java/transactional-processor/README.md`).

The processor demonstrates Kafka-internal exactly-once processing:

- consume from `orders.created.v1`
- transform each order into a terminal result
- produce to `orders.completed.v1` for successful orders
- produce to `orders.failed.v1` for rejected/failed orders
- commit consumed offsets in the same producer transaction

This should follow the delivery-semantics contract already documented in `docs/delivery-semantics.md`: use `transactional.id`, `beginTransaction()`, `sendOffsetsToTransaction(...)`, and `commitTransaction()` for Kafka-to-Kafka exactly-once behavior.

## Acceptance Criteria

1. `plain-java/transactional-processor` contains Java source code and no longer builds an empty JAR.
2. The module can be run with defaults against the local Kafka stack:
   - input topic: `orders.created.v1`
   - completed output topic: `orders.completed.v1`
   - failed output topic: `orders.failed.v1`
   - consumer group: `order-transactional-processor`
   - transactional id prefix: `order-transactional-processor`
3. The consumer uses `enable.auto.commit=false` and `isolation.level=read_committed`.
4. The producer uses `enable.idempotence=true`, `acks=all`, and a stable `transactional.id`.
5. For each poll batch, output records and consumed offsets are committed in one transaction.
6. If processing or producing fails, the transaction is aborted and offsets are not committed.
7. Unit tests cover:
   - routing completed orders to `orders.completed.v1`
   - routing rejected orders to `orders.failed.v1`
   - transaction commit sends offsets to transaction
   - transaction abort occurs on processing/producer failure
8. Documentation explains that exactly-once applies only inside Kafka, not to external side effects.

## Implementation Steps

1. Update `plain-java/transactional-processor/pom.xml`.
   - Add dependencies on `common`, `kafka-clients`, `slf4j-api`, runtime `logback-classic`, JUnit, and AssertJ.
   - Configure `maven-jar-plugin` main class as `com.example.kafkalab.transactional.TransactionalProcessorMain`.
   - Add `exec-maven-plugin` for local execution.

2. Add settings and factories.
   - `TransactionalProcessorSettings`: bootstrap servers, input topic, completed topic, failed topic, group id, transactional id.
   - `TransactionalProcessorFactory`: builds producer and consumer properties.
   - Producer config must include transactional/idempotent settings.
   - Consumer config must disable auto-commit and use `read_committed`.

3. Add transformation model.
   - Reuse `OrderCreated` and `JsonSerde` from `plain-java/common`.
   - Add simple result event records in this module, for example:
     - `OrderCompleted`
     - `OrderFailed`
   - Keep the rule deterministic for tests, such as amount greater than a threshold routes to failed, otherwise completed.

4. Implement transactional loop.
   - Subscribe to `orders.created.v1`.
   - Poll records.
   - For non-empty batches:
     - `producer.beginTransaction()`
     - process each record
     - send result records to the correct output topic
     - compute offsets per partition
     - call `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())`
     - `producer.commitTransaction()`
   - On `KafkaException` or processing failure:
     - `producer.abortTransaction()`
     - do not commit offsets
   - Use graceful shutdown with `consumer.wakeup()`.

5. Add CLI entry point.
   - `TransactionalProcessorMain` parses:
     - `--bootstrap-servers`
     - `--input-topic`
     - `--completed-topic`
     - `--failed-topic`
     - `--group-id`
     - `--transactional-id`
   - Defaults should match the topic catalog.

6. Add tests.
   - Unit-test property builders.
   - Unit-test transformation/routing logic.
   - Unit-test transaction orchestration with lightweight fakes or mocks around producer/consumer adapters.
   - Avoid requiring Docker for unit tests.

7. Update docs.
   - Replace the README TODO with run instructions and failure-mode notes.
   - Add Makefile target `transactional` or document direct Maven invocation.
   - Update root README checklist from planned/TODO to implemented once complete.

## Risks and Mitigations

- Risk: KafkaProducer and KafkaConsumer are hard to unit test directly.
  - Mitigation: introduce small package-private adapter interfaces for transaction operations and polling, while factories still create real Kafka clients.

- Risk: unstable `transactional.id` can fence instances or break restart behavior.
  - Mitigation: default to one stable transactional id for local single-instance lab use and document that multiple instances need unique IDs per instance.

- Risk: poison messages can cause endless abort/retry.
  - Mitigation: route deterministic business rejections to `orders.failed.v1`; reserve aborts for unexpected technical failures. Document that production systems need retry budgets and DLT handling.

- Risk: exactly-once expectations are overstated.
  - Mitigation: explicitly state in README that this only covers Kafka input offset plus Kafka output records, not databases, HTTP calls, or emails.

## Verification Steps

1. Run unit tests:

   ```powershell
   .\mvnw.cmd -pl plain-java/transactional-processor -am test
   ```

2. Run full reactor without integration tests:

   ```powershell
   .\mvnw.cmd clean verify -DskipITs
   ```

3. Manual smoke test:

   ```bash
   make up
   bash scripts/wait-for-kafka.sh
   make topics
   make produce
   ./mvnw -pl plain-java/transactional-processor exec:java \
     -Dexec.mainClass=com.example.kafkalab.transactional.TransactionalProcessorMain
   ```

4. Inspect output topics:

   ```bash
   docker compose -f docker/compose.yaml exec -T kafka \
     kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic orders.completed.v1 --from-beginning --max-messages 5
   ```

## Stop Condition

The module is considered complete when the transactional processor has runnable source code, tests prove transaction commit/abort behavior, the full Maven reactor passes, and README instructions allow a local user to run the flow end to end.
