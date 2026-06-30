package com.example.kafkalab.transactional;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionalProcessorFactoryTest {

    @Test
    void shouldBuildReadCommittedManualCommitConsumerProperties() {
        TransactionalProcessorSettings settings = TransactionalProcessorSettings.defaults();

        Properties props = TransactionalProcessorFactory.consumerProperties(settings);

        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(props.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
        assertThat(props.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo(settings.groupId());
    }

    @Test
    void shouldBuildTransactionalProducerProperties() {
        TransactionalProcessorSettings settings = TransactionalProcessorSettings.defaults();

        Properties props = TransactionalProcessorFactory.producerProperties(settings);

        assertThat(props.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
        assertThat(props.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo(settings.transactionalId());
    }
}
