ifeq ($(OS),Windows_NT)
MAVEN_OPTS := $(strip $(MAVEN_OPTS) -Djavax.net.ssl.trustStoreType=Windows-ROOT)
export MAVEN_OPTS
MVN = mvnw.cmd --batch-mode
else
JAVA_HOME ?= /usr/lib/jvm/temurin-21-jdk-amd64
MVN = JAVA_HOME=$(JAVA_HOME) ./mvnw --batch-mode
endif
COMPOSE_FILE ?= docker/compose.yaml
K6_IMAGE ?= kafka-practical-lab/k6-kafka:local
K6_DOCKER_NETWORK ?= kafka-lab
K6_KAFKA_BROKERS ?= kafka:29092

.PHONY: help clean compile build test verify up down destroy reset logs topics produce consume transactional admin groups load-k6-build load-k6

help:
	@echo "Available targets:"
	@echo "  make compile   - Compile all modules with Java 21"
	@echo "  make build     - Compile all modules with Java 21"
	@echo "  make test      - Run unit tests"
	@echo "  make verify    - Run full Maven verify"
	@echo "  make up        - Start local Kafka stack"
	@echo "  make down      - Stop local Kafka stack"
	@echo "  make destroy   - Stop stack and remove volumes"
	@echo "  make reset     - Stop stack and remove volumes"
	@echo "  make logs      - Tail Docker logs"
	@echo "  make topics    - Create lab topics"
	@echo "  make admin     - Run topic provisioner"
	@echo "  make produce   - Produce sample order events"
	@echo "  make consume   - Run the sample consumer"
	@echo "  make transactional - Run the transactional processor"
	@echo "  make groups    - Inspect consumer groups"
	@echo "  make load-k6   - Run k6 Kafka producer load test"

clean:
	$(MVN) clean

compile:
	$(MVN) compile -DskipTests

build: compile

test:
	$(MVN) test

verify:
	$(MVN) verify -DskipITs

load-k6-build:
	docker build -f load-tests/k6/Dockerfile -t $(K6_IMAGE) load-tests/k6

load-k6:
	docker run --rm --network $(K6_DOCKER_NETWORK) -e KAFKA_BROKERS=$(K6_KAFKA_BROKERS) -e KAFKA_TOPIC=$(KAFKA_TOPIC) -e K6_RATE=$(K6_RATE) -e K6_BATCH_SIZE=$(K6_BATCH_SIZE) -e K6_DURATION=$(K6_DURATION) -e K6_PRE_ALLOCATED_VUS=$(K6_PRE_ALLOCATED_VUS) -e K6_MAX_VUS=$(K6_MAX_VUS) -e K6_CUSTOMER_COUNT=$(K6_CUSTOMER_COUNT) $(K6_IMAGE) run /scripts/orders-producer.js

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

destroy:
	DESTROY_VOLUMES=true ./scripts/reset-environment.sh --destroy

reset: destroy

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

topics:
	./scripts/create-topics.sh

admin:
	$(MVN) -pl plain-java/admin-client exec:java -Dexec.mainClass=com.example.kafkalab.admin.AdminClientMain

produce:
	$(MVN) -pl plain-java/producer exec:java -Dexec.mainClass=com.example.kafkalab.producer.ProducerMain

consume:
	$(MVN) -pl plain-java/consumer exec:java -Dexec.mainClass=com.example.kafkalab.consumer.ConsumerMain

transactional:
	$(MVN) -pl plain-java/transactional-processor exec:java -Dexec.mainClass=com.example.kafkalab.transactional.TransactionalProcessorMain

groups:
	./scripts/inspect-consumer-groups.sh
