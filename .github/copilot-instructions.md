# Copilot Instructions for kafka-practical-lab

## Project Overview

This is an educational Kafka project demonstrating producers, consumers, Kafka Streams, and Schema Registry.

## Coding Rules

- Java 21 features (records, sealed classes, pattern matching, text blocks) are preferred
- All domain models are immutable records with compact constructor validation
- No null returns from public methods; use Optional or throw
- Producers use idempotence + acks=all
- Consumers use manual offset commit (enable.auto.commit=false)
- All resource types use try-with-resources
- SLF4J + Logback for logging; no System.out.println in production code
- Tests use JUnit 5 + AssertJ; never use assertTrue/assertEquals directly
- Integration tests end in IT suffix; unit tests end in Test suffix
- No Lombok; use Java records for data classes
- Package structure: com.example.kafkalab.[module]
