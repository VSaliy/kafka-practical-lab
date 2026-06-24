# Local Development Environment

This document explains how the local Kafka environment works, how to troubleshoot connectivity issues, and how data is persisted.

---

## KRaft Mode (No ZooKeeper)

Since Kafka 3.3, **KRaft** (Kafka Raft metadata mode) replaces ZooKeeper as the metadata store. All brokers in this lab run in KRaft mode.

### How KRaft works

- A subset of brokers acts as **controllers** (can also be combined with the broker role)
- Controllers use the Raft consensus protocol to agree on cluster metadata (topics, partitions, ISR, ACLs)
- The `CONTROLLER` listener handles internal controller communication
- No separate ZooKeeper process is needed

### Configuration in this lab

```yaml
# From docker/compose.yaml
KAFKA_PROCESS_ROLES: broker,controller        # combined mode
KAFKA_NODE_ID: 1
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
```

The `CLUSTER_ID` must be a stable base64-encoded UUID. It is set at first boot and must not change when reusing existing volumes.

---

## Listener Configuration

Kafka uses **listeners** to accept connections. This lab configures three:

| Listener name | Address | Purpose |
|---|---|---|
| `CONTROLLER` | `kafka:29093` | Internal Raft controller communication |
| `PLAINTEXT` | `kafka:29092` | Container-to-container communication within the Docker network |
| `PLAINTEXT_HOST` | `0.0.0.0:9092` | Host machine access via `localhost:9092` |

### Advertised listeners

The **advertised listeners** are what brokers broadcast to clients so they know where to reconnect after the initial metadata request.

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```

This dual-listener setup is what makes the broker reachable from **both**:
- Other containers in the `kafka-lab` Docker network using `kafka:29092`
- Applications on the host machine using `localhost:9092`

### Common mistake: wrong advertised listener

If your application runs inside a container and tries to connect to `localhost:9092`, it will fail — because `localhost` inside a container refers to the container itself, not the host.

**Fix:** Set `bootstrap.servers=kafka:29092` for containerised applications. Use `localhost:9092` only for host-machine processes.

---

## Docker Networking

All services defined in `docker/compose.yaml` share a named Docker bridge network:

```yaml
networks:
  kafka-lab:
    name: kafka-lab
    driver: bridge
```

| Service | Hostname (inside network) | Port |
|---|---|---|
| Kafka broker | `kafka` | 29092 (internal), 9092 (host) |
| Schema Registry | `schema-registry` | 8081 |
| AKHQ | `akhq` | 8080 |

Docker's internal DNS resolves service names to container IPs. Containers address each other by service name (e.g., `http://schema-registry:8081`).

---

## Persistent Volumes

Data is stored in named Docker volumes so it survives `docker compose down`:

```yaml
volumes:
  kafka-data:
    name: kafka-lab-data
  schema-registry-data:
    name: kafka-lab-schema-registry-data
```

### Volume contents

| Volume | Contains |
|---|---|
| `kafka-lab-data` | Kafka log segments, KRaft metadata, consumer group offsets |
| `kafka-lab-schema-registry-data` | Registered Avro schemas |

### Resetting state

```bash
# Stop containers but keep volumes
make down

# Stop containers AND remove all volumes (destructive)
make reset
# or
bash scripts/reset-environment.sh --destroy
```

> **Warning:** Removing volumes wipes all messages, consumer offsets, and registered schemas. The `CLUSTER_ID` is also lost, so Kafka will initialise a fresh cluster on next start.

---

## Common Connection Failures

### `Connection refused` to `localhost:9092`

The broker is not running or not yet ready.

```bash
# Start the environment
make up

# Wait for readiness
bash scripts/wait-for-kafka.sh

# Check logs
make logs
```

### `LEADER_NOT_AVAILABLE` or timeout on first produce

Kafka brokers take a few seconds to elect a partition leader after startup, especially after a fresh cluster init. Retry after a few seconds or increase the producer's `max.block.ms`.

### `org.apache.kafka.common.errors.UnknownTopicOrPartitionException`

The topic does not exist. Auto-create is disabled in this lab (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`).

```bash
# Create all topics
make topics
```

### Containers start but Schema Registry fails

Schema Registry waits for Kafka, but the health-check interval may not be tuned to your machine's startup speed.

```bash
# Restart Schema Registry after Kafka is healthy
docker compose -f docker/compose.yaml restart schema-registry
```

### `Could not find a version of the cluster id`

The `CLUSTER_ID` in `compose.yaml` does not match what was previously stored in the volume.

```bash
# Reset the environment completely
bash scripts/reset-environment.sh --destroy
make up
```

### Consumer gets no messages (offset already committed)

If you are reusing a consumer group, all messages may already be committed. Reset the offset:

```bash
docker compose -f docker/compose.yaml exec kafka \
  kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group order-audit-consumer \
    --topic orders.created.v1 \
    --reset-offsets --to-earliest --execute
```

### `NetworkException` from inside a container to `localhost:9092`

Containers must use `kafka:29092`, not `localhost:9092`. Update the `BOOTSTRAP_SERVERS` environment variable for the container.

---

## Health Checks

The `compose.yaml` includes health checks for Kafka and Schema Registry:

```yaml
healthcheck:
  test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
  interval: 10s
  timeout: 5s
  retries: 5
```

Dependent services use `condition: service_healthy` to wait for Kafka before starting.

---

## Starting and Stopping

```bash
# Start all services in the background
make up

# Tail logs for all services
make logs

# Stop containers (preserve data)
make down

# Stop and delete all data
make reset
```

For the three-broker setup:

```bash
COMPOSE_FILE=docker/compose-three-brokers.yaml make up
```
