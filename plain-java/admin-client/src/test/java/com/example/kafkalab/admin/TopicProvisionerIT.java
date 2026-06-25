package com.example.kafkalab.admin;

import com.example.kafkalab.common.model.TopicDefinition;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
class TopicProvisionerIT {

    @Container
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

    @Test
    void shouldCreateTopicsInKafka() throws ExecutionException, InterruptedException {
        String bootstrapServers = kafka.getBootstrapServers();
        List<TopicDefinition> topics = List.of(
            new TopicDefinition("test-topic-1", 1, (short) 1, Map.of()),
            new TopicDefinition("test-topic-2", 2, (short) 1, Map.of())
        );

        new TopicProvisioner().provision(bootstrapServers, topics);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> topicNames = adminClient.listTopics().names().get();
            assertThat(topicNames).contains("test-topic-1", "test-topic-2");
        }
    }

    @Test
    void shouldBeIdempotent() throws ExecutionException, InterruptedException {
        String bootstrapServers = kafka.getBootstrapServers();
        List<TopicDefinition> topics = List.of(
            new TopicDefinition("idempotent-topic", 1, (short) 1, Map.of())
        );

        new TopicProvisioner().provision(bootstrapServers, topics);
        new TopicProvisioner().provision(bootstrapServers, topics);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> topicNames = adminClient.listTopics().names().get();
            assertThat(topicNames).contains("idempotent-topic");
        }
    }
}
