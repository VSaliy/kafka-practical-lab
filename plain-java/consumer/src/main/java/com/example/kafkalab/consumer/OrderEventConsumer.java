package com.example.kafkalab.consumer;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderEventConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaConsumer<String, String> consumer;
    private final OrderProcessor processor;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public OrderEventConsumer(ConsumerSettings settings) {
        this.consumer = OrderConsumerFactory.create(settings.bootstrapServers(), settings.groupId());
        this.processor = new OrderProcessor(settings.processingDelayMs(), settings.failureRate());
        this.topic = settings.topic();
    }

    OrderEventConsumer(KafkaConsumer<String, String> consumer, OrderProcessor processor, String topic) {
        this.consumer = consumer;
        this.processor = processor;
        this.topic = topic;
    }

    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("Subscribed to topic: {}", topic);

            while (running.get()) {
                ConsumerRecords<String, String> records = poll();
                if (!records.isEmpty()) {
                    processRecords(records);
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.info("Consumer shutdown requested");
        } finally {
            consumer.close();
            log.info("Consumer closed");
        }
    }

    ConsumerRecords<String, String> poll() {
        return consumer.poll(POLL_TIMEOUT);
    }

    void processRecords(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        for (ConsumerRecord<String, String> record : records) {
            try {
                OrderCreated order = deserialize(record);
                process(order);
                offsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)
                );
            } catch (ProcessingException e) {
                log.error("Failed to process record at partition={} offset={}: {}",
                    record.partition(), record.offset(), e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error processing record at partition={} offset={}",
                    record.partition(), record.offset(), e);
            }
        }

        if (!offsets.isEmpty()) {
            commit(offsets);
        }
    }

    OrderCreated deserialize(ConsumerRecord<String, String> record) {
        return JsonSerde.fromJson(record.value(), OrderCreated.class);
    }

    void process(OrderCreated order) {
        processor.process(order);
    }

    void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
        log.debug("Committed offsets: {}", offsets);
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void close() {
        shutdown();
    }
}
