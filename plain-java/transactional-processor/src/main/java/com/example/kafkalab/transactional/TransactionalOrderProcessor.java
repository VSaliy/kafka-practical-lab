package com.example.kafkalab.transactional;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalOrderProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TransactionalOrderProcessor.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final TransactionalKafkaClient client;
    private final OrderWorkflowTransformer transformer;
    private final String inputTopic;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TransactionalOrderProcessor(TransactionalProcessorSettings settings) {
        this(TransactionalProcessorFactory.create(settings), new OrderWorkflowTransformer(settings), settings.inputTopic());
    }

    TransactionalOrderProcessor(TransactionalKafkaClient client, OrderWorkflowTransformer transformer, String inputTopic) {
        this.client = client;
        this.transformer = transformer;
        this.inputTopic = inputTopic;
    }

    public void run() {
        client.initTransactions();
        client.subscribe(Collections.singletonList(inputTopic));
        log.info("Transactional processor subscribed to {}", inputTopic);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = client.poll(POLL_TIMEOUT);
                if (!records.isEmpty()) {
                    processRecords(records);
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.info("Transactional processor shutdown requested");
        } finally {
            closeClient();
        }
    }

    void processRecords(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        client.beginTransaction();
        try {
            for (ConsumerRecord<String, String> record : records) {
                OrderCreated order = JsonSerde.fromJson(record.value(), OrderCreated.class);
                ProcessingDecision decision = transformer.transform(order);
                client.send(new ProducerRecord<>(decision.topic(), decision.key(), decision.value()));
                offsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)
                );
            }

            client.sendOffsetsToTransaction(offsets, client.groupMetadata());
            client.commitTransaction();
            log.info("Committed transactional batch with {} records", records.count());
        } catch (RuntimeException e) {
            abortTransaction(e);
            throw e;
        }
    }

    private void abortTransaction(RuntimeException failure) {
        try {
            client.abortTransaction();
            log.warn("Aborted transaction after processing failure: {}", failure.getMessage());
        } catch (KafkaException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            client.wakeup();
        }
    }

    @Override
    public void close() {
        if (!closed.get()) {
            shutdown();
            closeClient();
        }
    }

    private void closeClient() {
        if (closed.compareAndSet(false, true)) {
            client.close();
            log.info("Transactional processor closed");
        }
    }
}
