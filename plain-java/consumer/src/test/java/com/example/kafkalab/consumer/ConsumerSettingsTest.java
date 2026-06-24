package com.example.kafkalab.consumer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ConsumerSettingsTest {

    @Test
    void shouldCreateValidSettings() {
        ConsumerSettings settings = new ConsumerSettings("localhost:9092", "topic", "group", 0, 0.0);
        assertThat(settings.bootstrapServers()).isEqualTo("localhost:9092");
        assertThat(settings.groupId()).isEqualTo("group");
    }

    @Test
    void shouldRejectBlankBootstrapServers() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new ConsumerSettings("", "topic", "group", 0, 0.0));
    }

    @Test
    void shouldRejectNegativeDelay() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new ConsumerSettings("localhost:9092", "topic", "group", -1, 0.0));
    }

    @Test
    void shouldRejectInvalidFailureRate() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new ConsumerSettings("localhost:9092", "topic", "group", 0, 1.5));
    }
}
