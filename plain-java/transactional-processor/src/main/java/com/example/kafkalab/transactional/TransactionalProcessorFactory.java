package com.example.kafkalab.transactional;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class TransactionalProcessorFactory {

    private TransactionalProcessorFactory() {
    }

    static TransactionalKafkaClient create(TransactionalProcessorSettings settings) {
        return new KafkaTransactionalClient(
            new KafkaConsumer<>(consumerProperties(settings)),
            new KafkaProducer<>(producerProperties(settings))
        );
    }

    public static Properties consumerProperties(TransactionalProcessorSettings settings) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return props;
    }

    public static Properties producerProperties(TransactionalProcessorSettings settings) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, settings.transactionalId());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, settings.transactionalId());
        return props;
    }
}
