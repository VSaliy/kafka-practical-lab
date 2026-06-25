package com.example.kafkalab.consumer;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderEventConsumerTest {

    @Test
    void shouldNotCommitPastFailedRecordInSamePartition() {
        TopicPartition partition0 = new TopicPartition("orders.created.v1", 0);
        TopicPartition partition1 = new TopicPartition("orders.created.v1", 1);
        RecordingConsumer consumer = new RecordingConsumer();
        ConsumerRecords<String, String> records = new ConsumerRecords<>(Map.of(
            partition0, List.of(
                record(0, 0, "ok-before-failure"),
                record(0, 1, "fail"),
                record(0, 2, "ok-after-failure")
            ),
            partition1, List.of(record(1, 0, "ok-other-partition"))
        ));

        consumer.processRecords(records);

        assertThat(consumer.processedOrderIds)
            .containsExactlyInAnyOrder("ok-before-failure", "ok-other-partition")
            .doesNotContain("ok-after-failure");
        assertThat(consumer.committedOffsets).containsOnly(
            Map.entry(partition0, new OffsetAndMetadata(1)),
            Map.entry(partition1, new OffsetAndMetadata(1))
        );
    }

    private static ConsumerRecord<String, String> record(int partition, long offset, String orderId) {
        return new ConsumerRecord<>(
            "orders.created.v1",
            partition,
            offset,
            orderId,
            JsonSerde.toJson(order(orderId))
        );
    }

    private static OrderCreated order(String orderId) {
        return new OrderCreated(
            UUID.randomUUID(),
            orderId,
            "customer-001",
            new BigDecimal("42.00"),
            "USD",
            Instant.parse("2024-01-01T10:00:00Z")
        );
    }

    private static final class RecordingConsumer extends OrderEventConsumer {
        private final List<String> processedOrderIds = new java.util.ArrayList<>();
        private Map<TopicPartition, OffsetAndMetadata> committedOffsets = Map.of();

        private RecordingConsumer() {
            super((KafkaConsumer<String, String>) null, new OrderProcessor(0, 0), "orders.created.v1");
        }

        @Override
        void process(OrderCreated order) {
            if ("fail".equals(order.orderId())) {
                throw new ProcessingException("expected failure");
            }
            processedOrderIds.add(order.orderId());
        }

        @Override
        void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            committedOffsets = Map.copyOf(offsets);
        }
    }
}
