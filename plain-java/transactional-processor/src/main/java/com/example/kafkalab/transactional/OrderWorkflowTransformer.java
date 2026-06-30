package com.example.kafkalab.transactional;

import com.example.kafkalab.common.model.OrderCreated;
import com.example.kafkalab.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

class OrderWorkflowTransformer {

    static final BigDecimal DEFAULT_FAILURE_THRESHOLD = new BigDecimal("500.00");

    private final String completedTopic;
    private final String failedTopic;
    private final BigDecimal failureThreshold;
    private final Clock clock;

    OrderWorkflowTransformer(TransactionalProcessorSettings settings) {
        this(settings.completedTopic(), settings.failedTopic(), DEFAULT_FAILURE_THRESHOLD, Clock.systemUTC());
    }

    OrderWorkflowTransformer(String completedTopic, String failedTopic, BigDecimal failureThreshold, Clock clock) {
        this.completedTopic = Objects.requireNonNull(completedTopic, "completedTopic must not be null");
        this.failedTopic = Objects.requireNonNull(failedTopic, "failedTopic must not be null");
        this.failureThreshold = Objects.requireNonNull(failureThreshold, "failureThreshold must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    ProcessingDecision transform(OrderCreated order) {
        if (order.amount().compareTo(failureThreshold) >= 0) {
            OrderFailed failed = new OrderFailed(
                UUID.randomUUID(),
                order.orderId(),
                order.customerId(),
                order.amount(),
                order.currency(),
                "ORDER_AMOUNT_REQUIRES_MANUAL_REVIEW",
                Instant.now(clock)
            );
            return new ProcessingDecision(failedTopic, order.orderId(), JsonSerde.toJson(failed));
        }

        OrderCompleted completed = new OrderCompleted(
            UUID.randomUUID(),
            order.orderId(),
            order.customerId(),
            order.amount(),
            order.currency(),
            Instant.now(clock)
        );
        return new ProcessingDecision(completedTopic, order.orderId(), JsonSerde.toJson(completed));
    }
}
