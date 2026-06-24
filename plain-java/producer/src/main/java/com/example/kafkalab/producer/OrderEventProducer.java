package com.example.kafkalab.producer;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderEventProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final Random RANDOM = new Random();
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD"};

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final AtomicInteger failedCount = new AtomicInteger();

    public OrderEventProducer(String bootstrapServers, String topic) {
        this(OrderProducerFactory.create(bootstrapServers), topic);
    }

    OrderEventProducer(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public int sendOrders(int count, int customerCount) {
        log.info("Sending {} orders across {} customers to topic {}", count, customerCount, topic);
        failedCount.set(0);

        List<String> customerIds = new ArrayList<>();
        for (int i = 0; i < customerCount; i++) {
            customerIds.add("customer-" + String.format("%03d", i + 1));
        }

        for (int i = 0; i < count; i++) {
            OrderCreated event = generateOrder(customerIds);
            send(event);
        }

        producer.flush();
        int failures = failedCount.get();
        log.info("Completed sending {} orders. Failures: {}", count, failures);
        return failures;
    }

    private OrderCreated generateOrder(List<String> customerIds) {
        String customerId = customerIds.get(RANDOM.nextInt(customerIds.size()));
        BigDecimal amount = BigDecimal.valueOf(RANDOM.nextDouble() * 999 + 1)
            .setScale(2, RoundingMode.HALF_UP);
        String currency = CURRENCIES[RANDOM.nextInt(CURRENCIES.length)];

        return new OrderCreated(
            UUID.randomUUID(),
            "order-" + UUID.randomUUID().toString().substring(0, 8),
            customerId,
            amount,
            currency,
            Instant.now()
        );
    }

    private void send(OrderCreated event) {
        String key = event.orderId();
        String value = JsonSerde.toJson(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        addHeaders(record, event);

        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("Failed to send order {}: {}", event.orderId(), exception.getMessage());
                failedCount.incrementAndGet();
            } else {
                log.debug("Sent order {} to partition {} at offset {}",
                    event.orderId(), metadata.partition(), metadata.offset());
            }
        });
    }

    private void addHeaders(ProducerRecord<String, String> record, OrderCreated event) {
        record.headers().add(new RecordHeader("eventId",
            event.eventId().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventType",
            "OrderCreated".getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("contentType",
            "application/json".getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("correlationId",
            UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
    }

    public int getFailedCount() {
        return failedCount.get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
