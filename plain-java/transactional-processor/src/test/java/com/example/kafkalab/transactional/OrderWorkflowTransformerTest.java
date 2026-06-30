package com.example.kafkalab.transactional;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderWorkflowTransformerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2024-01-01T10:00:00Z"), ZoneOffset.UTC);

    @Test
    void shouldRouteSmallOrdersToCompletedTopic() {
        OrderWorkflowTransformer transformer = transformer();

        ProcessingDecision decision = transformer.transform(order("order-1", "99.99"));

        assertThat(decision.topic()).isEqualTo("orders.completed.v1");
        assertThat(decision.key()).isEqualTo("order-1");
        OrderCompleted completed = JsonSerde.fromJson(decision.value(), OrderCompleted.class);
        assertThat(completed.orderId()).isEqualTo("order-1");
        assertThat(completed.completedAt()).isEqualTo(Instant.parse("2024-01-01T10:00:00Z"));
    }

    @Test
    void shouldRouteLargeOrdersToFailedTopic() {
        OrderWorkflowTransformer transformer = transformer();

        ProcessingDecision decision = transformer.transform(order("order-2", "500.00"));

        assertThat(decision.topic()).isEqualTo("orders.failed.v1");
        assertThat(decision.key()).isEqualTo("order-2");
        OrderFailed failed = JsonSerde.fromJson(decision.value(), OrderFailed.class);
        assertThat(failed.reason()).isEqualTo("ORDER_AMOUNT_REQUIRES_MANUAL_REVIEW");
        assertThat(failed.failedAt()).isEqualTo(Instant.parse("2024-01-01T10:00:00Z"));
    }

    private static OrderWorkflowTransformer transformer() {
        return new OrderWorkflowTransformer(
            "orders.completed.v1",
            "orders.failed.v1",
            new BigDecimal("500.00"),
            CLOCK
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
}
