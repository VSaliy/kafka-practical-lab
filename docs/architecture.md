# Architecture Overview

Kafka Practical Lab models an order-processing ecosystem with three implemented core building blocks and several roadmap modules.

## Event flow

1. `producer` emits `OrderCreated` JSON events to `orders.created.v1`.
2. `consumer` reads the topic with manual offset management and simulated business processing.
3. `admin-client` creates and validates the topic catalog used across future services and streams modules.

## Design principles

- Java 21 across all modules
- Immutable domain models via records
- Explicit validation in compact constructors
- SLF4J logging with Logback configuration per executable module
- Clear separation between shared code and runnable labs
