# Kafka Practical Lab

Kafka Practical Lab is a production-quality educational Java 21 multi-module project for learning Kafka producers, consumers, admin tooling, streams, Avro, and service-oriented event choreography.

## Modules

- `plain-java/common`: shared models, JSON serialization helpers, and Kafka defaults
- `plain-java/producer`: idempotent JSON producer for order-created events
- `plain-java/consumer`: manual-commit consumer with simulated processing behavior
- `plain-java/admin-client`: topic catalog and provisioning utility
- `plain-java/transactional-processor`: placeholder for exactly-once transaction exercises
- `avro-clients/*`: placeholders for Schema Registry and Avro labs
- `spring-services/*`: placeholders for Spring Boot microservices
- `streams/*`: placeholders for Kafka Streams topologies
- `load-tests`: placeholder for benchmarking and load generation

## Requirements

- Java 21 at `/usr/lib/jvm/temurin-21-jdk-amd64`
- Maven 3.9+
- Docker with Compose for local Kafka

## Quick Start

```bash
./mvnw --batch-mode clean test
docker compose -f docker/compose.yaml up -d
./scripts/wait-for-kafka.sh
./scripts/create-topics.sh
./mvnw -pl plain-java/admin-client exec:java -Dexec.mainClass=com.example.kafkalab.admin.AdminClientMain
./mvnw -pl plain-java/producer exec:java -Dexec.mainClass=com.example.kafkalab.producer.ProducerMain -Dexec.args="--count 25"
./mvnw -pl plain-java/consumer exec:java -Dexec.mainClass=com.example.kafkalab.consumer.ConsumerMain
```

The wrapper sets `JAVA_HOME` automatically so Maven uses Java 21.

## Current Implemented Labs

### Topic provisioning

The admin client provisions a fixed topic catalog covering the core event-driven order workflow, DLT handling, fraud alerts, and compacted statistics topics.

### JSON producer

The producer publishes `OrderCreated` records as JSON with metadata headers, idempotence enabled, `acks=all`, retries, batching, and `lz4` compression.

### Manual-commit consumer

The consumer reads JSON events, deserializes them into immutable records, simulates business processing, and commits offsets only after successful handling.

## Documentation

- `docs/architecture.md`
- `docs/getting-started.md`
- `docs/testing.md`
- `schemas/README.md`
