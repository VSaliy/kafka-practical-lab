package com.example.kafkalab.transactional;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

interface TransactionalKafkaClient extends AutoCloseable {

    void initTransactions();

    void subscribe(Collection<String> topics);

    ConsumerRecords<String, String> poll(Duration timeout);

    void beginTransaction();

    void send(ProducerRecord<String, String> record);

    void sendOffsetsToTransaction(
        Map<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata
    );

    void commitTransaction();

    void abortTransaction();

    ConsumerGroupMetadata groupMetadata();

    void wakeup();

    @Override
    void close();
}
