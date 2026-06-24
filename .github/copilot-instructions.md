# Copilot Instructions for kafka-practical-lab

## Project Overview

An educational, production-quality Java 21 project for learning Apache Kafka through progressive practical exercises. The platform models an event-driven order-processing system.

## Language and Style

- Use Java 21 features (records, sealed classes, pattern matching, text blocks) where they improve clarity
- Prefer immutable records for domain models and configuration holders
- Do not add Lombok — use Java records for data classes
- Keep `main` methods thin; delegate to service classes immediately
- Prefer small, cohesive classes over large utility classes
- Prefer composition over inheritance
- Avoid generic names like `Utils`, `Helper`, or `Manager`
- Add JavaDoc only where it explains non-obvious behaviour or guarantees

## Kafka Configuration

- Keep all Kafka producer and consumer properties centralised in factory classes (`OrderProducerFactory`, `OrderConsumerFactory`)
- Producer defaults: `acks=all`, `enable.idempotence=true`, `retries=Integer.MAX_VALUE`, `max.in.flight.requests.per.connection=5`
- Consumer defaults: `enable.auto.commit=false`, `isolation.level=read_committed`
- Never enable auto-commit without an explicit exercise requiring it
- Use bounded retries and timeouts; never allow unbounded retry loops
- Always close producers, consumers, AdminClient instances, and streams (try-with-resources or explicit close in finally)
- Avoid blocking Kafka producer callback threads — do not call `Future.get()` in a callback
- Use `orderId` as the default key for order lifecycle events to preserve partition ordering

## Domain Model

- Keep domain models (`plain-java/common`) independent of Spring, Kafka, and all frameworks
- Validate in compact constructors — fail fast with actionable `IllegalArgumentException` messages
- No null returns from public API methods; use Optional or throw

## Consumer Behaviour

- Commit offsets only after successful processing (at-least-once semantics by default)
- Use partition-specific offset commits (`Map<TopicPartition, OffsetAndMetadata>`)
- Never swallow `WakeupException` except during expected consumer shutdown
- Handle graceful shutdown with `consumer.wakeup()` in a shutdown hook

## Logging

- Use SLF4J + Logback; no `System.out.println` in production code
- Use structured logging placeholders (`log.info("event={} partition={}", ...)`)
- Always include topic, partition, and offset in Kafka error logs
- Never log full sensitive payloads by default

## Error Handling

- Include actionable exception messages
- Avoid catching `Exception` unless at an application boundary (e.g., `main`)
- Preserve interrupt status when handling `InterruptedException` (`Thread.currentThread().interrupt()`)
- Add tests for failure paths, not just the happy path

## Ordering

- Preserve partition ordering assumptions — all order lifecycle events use `orderId` as key
- Do not publish events for the same `orderId` to multiple topics in a way that breaks causal order

## Exactly-Once Semantics

- Do not claim exactly-once behaviour for external systems (databases, HTTP APIs)
- Exactly-once guarantees in this lab apply only to Kafka-internal pipelines (transactions or Kafka Streams EOS)
- Clearly mark educational shortcuts with `// TODO (educational shortcut): ...` comments

## Testing

- Tests use JUnit 5 + AssertJ; prefer `assertThat()` over `assertTrue/assertEquals`
- Unit tests end in `Test`; integration tests end in `IT`
- Integration tests use Testcontainers and must clean up resources
- Use Awaitility for asynchronous assertions; avoid `Thread.sleep` with hard-coded values
- Tests must be deterministic and use bounded timeouts

## Spring Modules (future)

- Use constructor injection only; do not use field injection (`@Autowired` on fields)
- Keep domain models independent of Spring annotations

## What NOT to Do

- Do not introduce ZooKeeper — this lab uses KRaft exclusively
- Do not add unnecessary abstraction layers
- Do not use auto-commit for Kafka consumers in exercises unless the exercise explicitly demonstrates its risks
- Do not use `snappy` compression (requires native library) — use `lz4` instead

## Package Structure

```
com.example.kafkalab.common        # shared models, serialisation, config
com.example.kafkalab.admin         # AdminClient topic provisioner
com.example.kafkalab.producer      # plain-java producer
com.example.kafkalab.consumer      # plain-java consumer
com.example.kafkalab.avro          # avro-clients (planned)
com.example.kafkalab.order         # spring order-service (planned)
com.example.kafkalab.inventory     # spring inventory-service (planned)
com.example.kafkalab.payment       # spring payment-service (planned)
com.example.kafkalab.streams       # kafka streams apps (planned)
```

