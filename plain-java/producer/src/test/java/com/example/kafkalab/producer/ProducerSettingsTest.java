package com.example.kafkalab.producer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ProducerSettingsTest {

    @Test
    void shouldCreateValidSettings() {
        ProducerSettings settings = new ProducerSettings("localhost:9092", "my-topic", 10, 5);
        assertThat(settings.bootstrapServers()).isEqualTo("localhost:9092");
        assertThat(settings.topic()).isEqualTo("my-topic");
        assertThat(settings.count()).isEqualTo(10);
        assertThat(settings.customerCount()).isEqualTo(5);
    }

    @Test
    void shouldRejectBlankBootstrapServers() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new ProducerSettings("", "topic", 1, 1));
    }

    @Test
    void shouldRejectZeroCount() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new ProducerSettings("localhost:9092", "topic", 0, 1));
    }
}
