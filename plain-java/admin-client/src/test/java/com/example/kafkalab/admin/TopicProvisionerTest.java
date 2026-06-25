package com.example.kafkalab.admin;

import com.example.kafkalab.common.model.TopicDefinition;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicProvisionerTest {

    @Test
    void shouldWarnWhenExistingTopicHasWrongConfig() {
        TopicDefinition expected = new TopicDefinition(
            "customer-order-statistics.v1",
            3,
            (short) 1,
            Map.of(
                "cleanup.policy", "compact",
                "retention.ms", "-1"
            )
        );
        TopicDescription actual = topicDescription("customer-order-statistics.v1", 3, 1);
        Config actualConfig = new Config(List.of(
            new ConfigEntry("cleanup.policy", "delete"),
            new ConfigEntry("retention.ms", "604800000")
        ));

        List<String> warnings = TopicProvisioner.compatibilityWarnings(expected, actual, actualConfig);

        assertThat(warnings).containsExactlyInAnyOrder(
            "Topic customer-order-statistics.v1 has cleanup.policy=delete but expected compact",
            "Topic customer-order-statistics.v1 has retention.ms=604800000 but expected -1"
        );
    }

    @Test
    void shouldWarnWhenExistingTopicHasWrongReplicationFactor() {
        TopicDefinition expected = new TopicDefinition(
            "orders.created.v1",
            3,
            (short) 3,
            Map.of("cleanup.policy", "delete")
        );
        TopicDescription actual = topicDescription("orders.created.v1", 3, 1);
        Config actualConfig = new Config(List.of(new ConfigEntry("cleanup.policy", "delete")));

        List<String> warnings = TopicProvisioner.compatibilityWarnings(expected, actual, actualConfig);

        assertThat(warnings).containsExactly(
            "Topic orders.created.v1 has replication factors [1] but expected 3"
        );
    }

    @Test
    void shouldReturnNoWarningsWhenExistingTopicMatchesDefinition() {
        TopicDefinition expected = new TopicDefinition(
            "orders.created.v1",
            3,
            (short) 1,
            Map.of("cleanup.policy", "delete")
        );
        TopicDescription actual = topicDescription("orders.created.v1", 3, 1);
        Config actualConfig = new Config(List.of(new ConfigEntry("cleanup.policy", "delete")));

        List<String> warnings = TopicProvisioner.compatibilityWarnings(expected, actual, actualConfig);

        assertThat(warnings).isEmpty();
    }

    private static TopicDescription topicDescription(String topicName, int partitions, int replicationFactor) {
        Node leader = new Node(1, "localhost", 9092);
        List<Node> replicas = replicas(replicationFactor);
        List<TopicPartitionInfo> partitionInfos = java.util.stream.IntStream.range(0, partitions)
            .mapToObj(partition -> new TopicPartitionInfo(partition, leader, replicas, replicas))
            .toList();

        return new TopicDescription(topicName, false, partitionInfos);
    }

    private static List<Node> replicas(int replicationFactor) {
        return java.util.stream.IntStream.rangeClosed(1, replicationFactor)
            .mapToObj(id -> new Node(id, "localhost", 9091 + id))
            .toList();
    }
}
