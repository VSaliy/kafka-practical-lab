package com.example.kafkalab.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderProducerFactoryTest {

    @Test
    void shouldBuildPropertiesWithIdempotenceEnabled() {
        Properties props = OrderProducerFactory.buildProperties("localhost:9092");
        assertThat(props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
    }

    @Test
    void shouldUseAcksAll() {
        Properties props = OrderProducerFactory.buildProperties("localhost:9092");
        assertThat(props.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
    }

    @Test
    void shouldUseLz4Compression() {
        Properties props = OrderProducerFactory.buildProperties("localhost:9092");
        assertThat(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG)).isEqualTo("lz4");
    }

    @Test
    void shouldSetBootstrapServers() {
        Properties props = OrderProducerFactory.buildProperties("broker1:9092,broker2:9092");
        assertThat(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("broker1:9092,broker2:9092");
    }
}
