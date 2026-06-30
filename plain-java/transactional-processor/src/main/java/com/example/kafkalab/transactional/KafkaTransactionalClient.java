package com.example.kafkalab.transactional;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

class KafkaTransactionalClient implements TransactionalKafkaClient {

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;

    KafkaTransactionalClient(KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer) {
        this.consumer = consumer;
        this.producer = producer;
    }

    @Override
    public void initTransactions() {
        producer.initTransactions();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    public ConsumerRecords<String, String> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    @Override
    public void beginTransaction() {
        producer.beginTransaction();
    }

    @Override
    public void send(ProducerRecord<String, String> record) {
        producer.send(record);
    }

    @Override
    public void sendOffsetsToTransaction(
        Map<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata
    ) {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() {
        producer.commitTransaction();
    }

    @Override
    public void abortTransaction() {
        producer.abortTransaction();
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return consumer.groupMetadata();
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } finally {
            producer.close();
        }
    }
}
