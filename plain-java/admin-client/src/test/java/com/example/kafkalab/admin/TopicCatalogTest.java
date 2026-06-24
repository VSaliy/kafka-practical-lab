package com.example.kafkalab.admin;

import com.example.kafkalab.common.model.TopicDefinition;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicCatalogTest {

    @Test
    void shouldReturnTenTopics() {
        List<TopicDefinition> topics = TopicCatalog.all();
        assertThat(topics).hasSize(10);
    }

    @Test
    void shouldContainOrdersCreatedTopic() {
        List<TopicDefinition> topics = TopicCatalog.all();
        assertThat(topics).extracting(TopicDefinition::name)
            .contains("orders.created.v1");
    }

    @Test
    void shouldHaveUniqueTopicNames() {
        List<TopicDefinition> topics = TopicCatalog.all();
        Set<String> names = topics.stream().map(TopicDefinition::name).collect(Collectors.toSet());
        assertThat(names).hasSameSizeAs(topics);
    }

    @Test
    void shouldHavePositivePartitions() {
        TopicCatalog.all().forEach(topic ->
            assertThat(topic.partitions()).as("partitions for %s", topic.name()).isGreaterThan(0));
    }

    @Test
    void shouldContainDltTopic() {
        assertThat(TopicCatalog.all()).extracting(TopicDefinition::name)
            .contains("orders.created.v1-dlt");
    }

    @Test
    void shouldContainCompactedStatisticsTopic() {
        TopicDefinition stats = TopicCatalog.all().stream()
            .filter(topic -> topic.name().equals("customer-order-statistics.v1"))
            .findFirst()
            .orElseThrow();
        assertThat(stats.configuration()).containsEntry("cleanup.policy", "compact");
    }
}
