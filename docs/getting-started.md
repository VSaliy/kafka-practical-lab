# Getting Started

## 1. Build the project

```bash
./mvnw --batch-mode clean test
```

## 2. Start Kafka

```bash
docker compose -f docker/compose.yaml up -d
./scripts/wait-for-kafka.sh
```

## 3. Create topics

```bash
./scripts/create-topics.sh
```
