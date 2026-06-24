# Schema Evolution with Apache Avro

This document explains how Apache Avro schemas work, how they evolve safely, and how Confluent Schema Registry enforces compatibility rules.

---

## Why Schema Evolution Matters

In an event-driven system, producers and consumers are deployed independently. A schema change in the producer must not break existing consumers, and vice versa.

Avro provides:
- **Binary serialisation** — compact on the wire
- **Schema-based decoding** — the reader uses a schema to interpret bytes
- **Writer/reader schema resolution** — the reader can use a *different but compatible* schema to the one used when writing

---

## Avro Schema Basics

An Avro schema is a JSON document:

```json
{
  "type": "record",
  "namespace": "com.example.kafkalab.events.v1",
  "name": "OrderCreated",
  "fields": [
    {"name": "eventId",    "type": "string"},
    {"name": "orderId",    "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount",     "type": {"type": "bytes", "logicalType": "decimal", "precision": 19, "scale": 4}},
    {"name": "currency",   "type": "string"},
    {"name": "createdAt",  "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

Schemas are stored in `schemas/` and registered in **Confluent Schema Registry**.

---

## Confluent Schema Registry

Schema Registry stores schemas and assigns integer IDs. The Avro serialiser (`KafkaAvroSerializer`) writes a magic byte and the schema ID at the start of each message, so deserializers can retrieve the exact schema used to write the record.

```
[magic byte: 0x00] [schema-id: 4 bytes] [avro binary payload]
```

### Subjects

Each Kafka topic has two associated Schema Registry subjects:
- `<topic>-key` — for the key schema
- `<topic>-value` — for the value schema

For example, `orders.created.v1-value` stores the `OrderCreated` schema.

### Viewing registered schemas

```bash
# List all subjects
curl http://localhost:8081/subjects

# Get latest version of a schema
curl http://localhost:8081/subjects/orders.created.v1-value/versions/latest

# Or open AKHQ at http://localhost:8080
```

---

## Compatibility Modes

Schema Registry enforces a compatibility rule before accepting a new schema version.

| Mode | Rule | Typical use |
|---|---|---|
| `BACKWARD` | New schema can read data written with the previous schema | **Default.** Consumers upgrade first |
| `FORWARD` | Previous schema can read data written with the new schema | Producers upgrade first |
| `FULL` | Both backward and forward compatible | Safest, most restrictive |
| `BACKWARD_TRANSITIVE` | Compatible with all previous versions | Long retention environments |
| `FORWARD_TRANSITIVE` | All previous schemas can read new data | Long retention environments |
| `FULL_TRANSITIVE` | Fully compatible with all previous versions | Strictest |
| `NONE` | No compatibility check | Prototyping only — never production |

### Setting compatibility for a subject

```bash
curl -X PUT http://localhost:8081/config/orders.created.v1-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FULL_TRANSITIVE"}'
```

---

## Safe Schema Changes

### Always safe (backward + forward compatible)

These changes can be made without breaking existing producers or consumers:

- **Adding a field with a default value**
  ```json
  {"name": "source", "type": "string", "default": "web"}
  ```
- **Adding a new `null` union to an existing field** (making it optional)
  ```json
  {"name": "couponCode", "type": ["null", "string"], "default": null}
  ```
- **Adding a value to an `enum` type** (forward only — old readers see unknown value)

### Breaking changes (require a new major version)

These changes break compatibility and require a new topic/subject (e.g., `orders.created.v2`):

- Removing a required field (no default)
- Renaming a field
- Changing a field's type (e.g., `int` → `long`)
- Removing an enum value that existing data uses
- Changing the logical type

---

## Schema Evolution Example

### v1 — Initial schema

```json
{
  "type": "record",
  "name": "OrderCreated",
  "fields": [
    {"name": "eventId",    "type": "string"},
    {"name": "orderId",    "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount",     "type": "string"},
    {"name": "currency",   "type": "string"},
    {"name": "createdAt",  "type": "long"}
  ]
}
```

### v2 — Add optional `channel` field (backward compatible)

```json
{
  "type": "record",
  "name": "OrderCreated",
  "fields": [
    {"name": "eventId",    "type": "string"},
    {"name": "orderId",    "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount",     "type": "string"},
    {"name": "currency",   "type": "string"},
    {"name": "createdAt",  "type": "long"},
    {"name": "channel",    "type": ["null", "string"], "default": null}
  ]
}
```

Old consumers reading v2 data will see `channel = null` via default resolution. Old consumers reading v1 data will simply not see `channel` — it does not exist in the writer schema.

---

## Specific vs Generic Records

Avro supports two client models:

| Model | Class | When to use |
|---|---|---|
| **Specific** | `SpecificRecord` (code-generated from `.avsc`) | Compile-time type safety, IDE support |
| **Generic** | `GenericRecord` | Schema not known at compile time, dynamic pipelines |

Code generation is handled by the Avro Maven plugin in the `avro-clients/` modules.

---

## Lab Exercises

The `avro-clients/` modules demonstrate both patterns:

| Module | Description |
|---|---|
| `avro-clients/avro-producer` *(planned)* | Produce `OrderCreated` using `KafkaAvroSerializer` with specific records |
| `avro-clients/specific-consumer` *(planned)* | Consume with generated `SpecificRecord` class |
| `avro-clients/generic-consumer` *(planned)* | Consume with `GenericRecord` — works without generated code |

Avro schemas live in `schemas/` as `.avsc` files and are registered in Schema Registry on first use.

See the [Learning Roadmap](../README.md#learning-roadmap) for the recommended exercise sequence.
