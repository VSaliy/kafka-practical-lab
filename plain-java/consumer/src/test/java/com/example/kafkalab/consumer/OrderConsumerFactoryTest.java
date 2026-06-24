package com.example.kafkalab.consumer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderConsumerFactoryTest {

    @Test
    void shouldDisableAutoCommit() {
        Properties props = OrderConsumerFactory.buildProperties("localhost:9092", "test-group");
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
    }

    @Test
    void shouldSetEarliestOffset() {
        Properties props = OrderConsumerFactory.buildProperties("localhost:9092", "test-group");
        assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
    }

    @Test
    void shouldSetReadCommittedIsolation() {
        Properties props = OrderConsumerFactory.buildProperties("localhost:9092", "test-group");
        assertThat(props.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
    }

    @Test
    void shouldSetGroupId() {
        Properties props = OrderConsumerFactory.buildProperties("localhost:9092", "my-group");
        assertThat(props.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("my-group");
    }
}
