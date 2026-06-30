package com.example.kafkalab.transactional;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;

class TransactionalOrderProcessorTest {

    @Test
    void shouldCommitOutputsAndOffsetsInOneTransaction() {
        RecordingClient client = new RecordingClient();
        TransactionalOrderProcessor processor = new TransactionalOrderProcessor(
            client,
            transformer(),
            "orders.created.v1"
        );
        TopicPartition partition = new TopicPartition("orders.created.v1", 0);

        processor.processRecords(new ConsumerRecords<>(Map.of(
            partition,
            List.of(record(0, 4, "order-1", "42.00"))
        )));

        assertThat(client.events).containsExactly(
            "beginTransaction",
            "send:orders.completed.v1:order-1",
            "sendOffsetsToTransaction",
            "commitTransaction"
        );
        assertThat(client.offsets).containsEntry(partition, new OffsetAndMetadata(5));
        assertThat(client.aborted).isFalse();
    }

    @Test
    void shouldAbortTransactionWhenSendFails() {
        RecordingClient client = new RecordingClient();
        client.failOnSend = true;
        TransactionalOrderProcessor processor = new TransactionalOrderProcessor(
            client,
            transformer(),
            "orders.created.v1"
        );

        assertThatRuntimeException()
            .isThrownBy(() -> processor.processRecords(new ConsumerRecords<>(Map.of(
                new TopicPartition("orders.created.v1", 0),
                List.of(record(0, 0, "order-1", "42.00"))
            ))));

        assertThat(client.events).containsExactly(
            "beginTransaction",
            "abortTransaction"
        );
        assertThat(client.committed).isFalse();
        assertThat(client.offsets).isEmpty();
    }

    private static OrderWorkflowTransformer transformer() {
        return new OrderWorkflowTransformer(
            "orders.completed.v1",
            "orders.failed.v1",
            new BigDecimal("500.00"),
            java.time.Clock.fixed(Instant.parse("2024-01-01T10:00:00Z"), java.time.ZoneOffset.UTC)
        );
    }

    private static ConsumerRecord<String, String> record(int partition, long offset, String orderId, String amount) {
        return new ConsumerRecord<>(
            "orders.created.v1",
            partition,
            offset,
            orderId,
            JsonSerde.toJson(order(orderId, amount))
        );
    }

    private static OrderCreated order(String orderId, String amount) {
        return new OrderCreated(
            UUID.randomUUID(),
            orderId,
            "customer-001",
            new BigDecimal(amount),
            "USD",
            Instant.parse("2024-01-01T09:00:00Z")
        );
    }

    private static final class RecordingClient implements TransactionalKafkaClient {
        private final List<String> events = new ArrayList<>();
        private Map<TopicPartition, OffsetAndMetadata> offsets = Map.of();
        private boolean failOnSend;
        private boolean committed;
        private boolean aborted;

        @Override
        public void initTransactions() {
            events.add("initTransactions");
        }

        @Override
        public void subscribe(Collection<String> topics) {
            events.add("subscribe:" + topics);
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            return ConsumerRecords.empty();
        }

        @Override
        public void beginTransaction() {
            events.add("beginTransaction");
        }

        @Override
        public void send(ProducerRecord<String, String> record) {
            if (failOnSend) {
                throw new RuntimeException("send failed");
            }
            events.add("send:" + record.topic() + ":" + record.key());
        }

        @Override
        public void sendOffsetsToTransaction(
            Map<TopicPartition, OffsetAndMetadata> offsets,
            ConsumerGroupMetadata groupMetadata
        ) {
            this.offsets = Map.copyOf(offsets);
            events.add("sendOffsetsToTransaction");
        }

        @Override
        public void commitTransaction() {
            committed = true;
            events.add("commitTransaction");
        }

        @Override
        public void abortTransaction() {
            aborted = true;
            events.add("abortTransaction");
        }

        @Override
        public ConsumerGroupMetadata groupMetadata() {
            return new ConsumerGroupMetadata("order-transactional-processor");
        }

        @Override
        public void wakeup() {
        }

        @Override
        public void close() {
        }
    }
}
