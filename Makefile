JAVA_HOME ?= /usr/lib/jvm/temurin-21-jdk-amd64
MVN = JAVA_HOME=$(JAVA_HOME) ./mvnw --batch-mode
COMPOSE_FILE ?= docker/compose.yaml

.PHONY: help clean compile test verify up down destroy topics produce consume admin groups

help:
	@echo "Available targets:"
	@echo "  make compile   - Compile all modules with Java 21"
	@echo "  make test      - Run unit tests"
	@echo "  make verify    - Run full Maven verify"
	@echo "  make up        - Start local Kafka stack"
	@echo "  make down      - Stop local Kafka stack"
	@echo "  make destroy   - Stop stack and remove volumes"
	@echo "  make topics    - Create lab topics"
	@echo "  make admin     - Run topic provisioner"
	@echo "  make produce   - Produce sample order events"
	@echo "  make consume   - Run the sample consumer"
	@echo "  make groups    - Inspect consumer groups"

clean:
	$(MVN) clean

compile:
	$(MVN) compile -DskipTests

test:
	$(MVN) test

verify:
	$(MVN) verify -DskipITs

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

destroy:
	DESTROY_VOLUMES=true ./scripts/reset-environment.sh --destroy

topics:
	./scripts/create-topics.sh

admin:
	$(MVN) -pl plain-java/admin-client exec:java -Dexec.mainClass=com.example.kafkalab.admin.AdminClientMain

produce:
	$(MVN) -pl plain-java/producer exec:java -Dexec.mainClass=com.example.kafkalab.producer.ProducerMain

consume:
	$(MVN) -pl plain-java/consumer exec:java -Dexec.mainClass=com.example.kafkalab.consumer.ConsumerMain

groups:
	./scripts/inspect-consumer-groups.sh
